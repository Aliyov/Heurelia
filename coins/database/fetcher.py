"""Fetch Binance Spot 4-hour candles into one structured JSON and log per coin.

Usage from another Python file:
    from fetcher import fetch_prices
    results = fetch_prices()

Each call performs one update and returns. The caller controls scheduling.
Requires Python 3.9+; no third-party packages or API credentials.

Files are created in data_crypto_sets beside this module, or in output_dir.
The output directory is created automatically if it does not exist.
All logging goes to files; nothing is printed to the terminal.
The newest candle may still be forming; it is refreshed next cycle.
Existing per-coin CSVs are migrated when the corresponding JSON is missing.
Legacy combined prices.json files are not imported. New histories start at START_DATE.
See price-data-format.md for the versioned JSON contract.
"""

import csv
import json
import logging
import os
import tempfile
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from logging.handlers import RotatingFileHandler
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


# Add or remove base coins here. Each is paired with QUOTE_ASSET.
COINS = ["XRP", "ETH", "BTC", "SOL", "BNB"]
QUOTE_ASSET = "USDT"
CANDLE_INTERVAL = "4h"
START_DATE = datetime(2024, 1, 1, tzinfo=timezone.utc)
PAGE_LIMIT = 1000
REQUEST_TIMEOUT_SECONDS = 30
REQUEST_ATTEMPTS = 3
API_BASE_URL = "https://api.binance.com"
LOG_MAX_MB = 50
LOG_BACKUPS = 3
TIME_FORMAT = "%Y-%m-%d %H:%M:%S UTC"
SCHEMA_VERSION = 1
LEGACY_CSV_FIELDS = [
    "symbol", "open_time", "close_time", "open", "high", "low",
    "close", "volume", "trades",
]
__all__ = ["fetch_prices"]


class FileOnlyHandler(RotatingFileHandler):
    def handleError(self, record):
        # Logging failures must not trigger logging's default stderr traceback.
        # JSON/request failures are separately returned to the caller.
        pass


def setup_logger(name: str, log_path: Path) -> logging.Logger:
    # A private logger prevents the calling application's logging setup from
    # forwarding these messages to a terminal or adding its own handlers.
    logger = logging.Logger(name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    formatter = logging.Formatter(
        "%(asctime)s UTC %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S"
    )
    formatter.converter = time.gmtime
    file_handler = FileOnlyHandler(
        log_path, maxBytes=LOG_MAX_MB * 1024 * 1024,
        backupCount=LOG_BACKUPS, encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def timestamp_text(milliseconds: int) -> str:
    return datetime.fromtimestamp(milliseconds / 1000, timezone.utc).strftime(TIME_FORMAT)


def timestamp_ms(text: str) -> int:
    return int(datetime.strptime(text, TIME_FORMAT).replace(tzinfo=timezone.utc).timestamp() * 1000)


def timestamp_iso(milliseconds: int) -> str:
    return datetime.fromtimestamp(milliseconds / 1000, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def request_json(path: str, params: dict, log: logging.Logger):
    url = API_BASE_URL.rstrip("/") + path
    if params:
        url += "?" + urlencode(params)
    request = Request(url, headers={"User-Agent": "multi-coin-candle-fetcher/1.0"})

    for attempt in range(1, REQUEST_ATTEMPTS + 1):
        try:
            with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                return json.load(response)
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            # Invalid symbols, access restrictions, and IP bans need a later retry.
            if exc.code != 429 and not 500 <= exc.code < 600:
                raise RuntimeError(f"Binance HTTP {exc.code}: {detail}") from exc
            if attempt == REQUEST_ATTEMPTS:
                raise RuntimeError(f"Binance HTTP {exc.code}: {detail}") from exc
            delay = max(float(exc.headers.get("Retry-After", "0")), 2 ** attempt)
            log.warning("HTTP %s; retrying in %.1fs (%d/%d).", exc.code, delay, attempt, REQUEST_ATTEMPTS)
        except (URLError, TimeoutError, ConnectionError) as exc:
            if attempt == REQUEST_ATTEMPTS:
                raise RuntimeError(f"Request failed after {attempt} attempts: {exc}") from exc
            delay = 2 ** attempt
            log.warning("Network error; retrying in %ds (%d/%d): %s", delay, attempt, REQUEST_ATTEMPTS, exc)
        time.sleep(delay)


def validate_candle(candle: dict) -> None:
    """Validate the core contract; allow extra fields for later extensions."""
    if not isinstance(candle, dict):
        raise ValueError("Candle must be an object")
    opening = candle.get("open_time_ms")
    closing = candle.get("close_time_ms")
    if type(opening) is not int or type(closing) is not int or opening < 0 or closing < opening:
        raise ValueError("Candle timestamps must be nonnegative millisecond integers with close >= open")
    if candle.get("open_time") != timestamp_iso(opening) or candle.get("close_time") != timestamp_iso(closing):
        raise ValueError("Candle ISO timestamps do not match millisecond timestamps")
    if type(candle.get("trades")) is not int or candle["trades"] < 0:
        raise ValueError("Candle trades must be a nonnegative integer")
    if type(candle.get("is_closed")) is not bool:
        raise ValueError("Candle is_closed must be a boolean")

    for section, fields in {
        "ohlc": ("open", "high", "low", "close"),
        "volume": ("base", "quote", "taker_buy_base", "taker_buy_quote"),
    }.items():
        values = candle.get(section)
        if not isinstance(values, dict):
            raise ValueError(f"Candle {section} must be an object")
        for field in fields:
            if field not in values:
                raise ValueError(f"Missing candle field: {section}.{field}")
            value = values[field]
            # CSV history did not contain quote or taker-buy volumes.
            if section == "volume" and field != "base" and value is None:
                continue
            if not isinstance(value, str):
                raise ValueError(f"{section}.{field} must be a decimal string")
            try:
                number = Decimal(value)
            except InvalidOperation as exc:
                raise ValueError(f"Invalid decimal in {section}.{field}") from exc
            if not number.is_finite() or number < 0:
                raise ValueError(f"Invalid decimal in {section}.{field}")


def load_candles(json_path: Path, symbol: str) -> tuple:
    """Return candles indexed by open time and their document metadata.

    Invalid files and unsupported versions raise instead of being overwritten.
    """
    if not json_path.exists():
        return {}, {}
    with json_path.open("r", encoding="utf-8") as handle:
        document = json.load(handle)
    if not isinstance(document, dict) or type(document.get("schema_version")) is not int or document["schema_version"] != SCHEMA_VERSION:
        raise ValueError(f"Unsupported or missing schema_version in {json_path.name}")
    market = document.get("market")
    if not isinstance(market, dict) or any(market.get(field) != value for field, value in {
        "exchange": "binance", "type": "spot", "symbol": symbol,
        "base_asset": symbol[:-len(QUOTE_ASSET)], "quote_asset": QUOTE_ASSET,
        "interval": CANDLE_INTERVAL, "timezone": "UTC",
    }.items()):
        raise ValueError(f"Market metadata does not match {symbol}/{CANDLE_INTERVAL} in {json_path.name}")
    if not isinstance(document.get("metadata"), dict) or not isinstance(document.get("candles"), list):
        raise ValueError(f"Missing metadata or candles in {json_path.name}")
    candles = {}
    for candle in document["candles"]:
        validate_candle(candle)
        opening = candle["open_time_ms"]
        if opening in candles:
            raise ValueError(f"Duplicate candle {opening} in {json_path.name}")
        candles[opening] = candle
    metadata = document["metadata"]
    if metadata.get("decimal_encoding") != "string" or metadata.get("timestamp_unit") != "milliseconds":
        raise ValueError(f"Invalid storage encoding in {json_path.name}")
    for field in ("created_at", "updated_at"):
        value = metadata.get(field)
        if not isinstance(value, str):
            raise ValueError(f"Invalid {field} in {json_path.name}")
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    server_time_ms = metadata.get("as_of_server_time_ms")
    if type(server_time_ms) is not int or server_time_ms < 0 or metadata.get("as_of_server_time") != timestamp_iso(server_time_ms):
        raise ValueError(f"Invalid server timestamp in {json_path.name}")
    if type(metadata.get("candle_count")) is not int or metadata["candle_count"] != len(candles):
        raise ValueError(f"Incorrect candle_count in {json_path.name}")
    for field, value in {
        "first_open_time_ms": min(candles) if candles else None,
        "last_open_time_ms": max(candles) if candles else None,
    }.items():
        if field not in metadata or metadata[field] != value:
            raise ValueError(f"Incorrect {field} in {json_path.name}")
    return candles, document


def load_legacy_csv(csv_path: Path, symbol: str) -> dict:
    """Migrate the previous per-coin CSV format without modifying the CSV."""
    candles = {}
    with csv_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != LEGACY_CSV_FIELDS:
            raise ValueError(f"Unexpected CSV header in {csv_path.name}")
        for row in reader:
            if None in row or any(row.get(field) in (None, "") for field in LEGACY_CSV_FIELDS):
                raise ValueError(f"Incomplete row at line {reader.line_num} in {csv_path.name}")
            if row["symbol"] != symbol:
                raise ValueError(f"Wrong symbol at line {reader.line_num} in {csv_path.name}")
            opening = timestamp_ms(row["open_time"])
            # The CSV format truncated Binance's inclusive close time to seconds.
            closing = timestamp_ms(row["close_time"]) + 999
            candle = {
                "open_time_ms": opening, "close_time_ms": closing,
                "open_time": timestamp_iso(opening), "close_time": timestamp_iso(closing),
                "ohlc": {field: row[field] for field in ("open", "high", "low", "close")},
                "volume": {"base": row["volume"], "quote": None, "taker_buy_base": None, "taker_buy_quote": None},
                "trades": int(row["trades"]), "is_closed": False,
            }
            validate_candle(candle)
            if opening in candles:
                raise ValueError(f"Duplicate candle {opening} in {csv_path.name}")
            candles[opening] = candle
    return candles


def fetch_klines_since(symbol: str, start_ms: int, end_ms: int, log: logging.Logger) -> dict:
    """Fetch all pages. A failed page raises, leaving the saved JSON untouched."""
    candles = {}
    current_start = start_ms
    while current_start <= end_ms:
        raw_klines = request_json("/api/v3/klines", {
            "symbol": symbol,
            "interval": CANDLE_INTERVAL,
            "startTime": current_start,
            "endTime": end_ms,
            "limit": PAGE_LIMIT,
        }, log)
        if not isinstance(raw_klines, list):
            raise ValueError(f"Unexpected Binance response: {raw_klines}")
        if not raw_klines:
            break

        for kline in raw_klines:
            opening = int(kline[0])
            if not current_start <= opening <= end_ms:
                raise ValueError("Binance returned a candle outside the requested range")
            # Keep decimal strings exactly as supplied by Binance.
            closing = int(kline[6])
            candle = {
                "open_time_ms": opening,
                "close_time_ms": closing,
                "open_time": timestamp_iso(opening),
                "close_time": timestamp_iso(closing),
                "ohlc": {"open": str(kline[1]), "high": str(kline[2]), "low": str(kline[3]), "close": str(kline[4])},
                "volume": {
                    "base": str(kline[5]), "quote": str(kline[7]),
                    "taker_buy_base": str(kline[9]), "taker_buy_quote": str(kline[10]),
                },
                "trades": int(kline[8]),
                "is_closed": closing < end_ms,
            }
            validate_candle(candle)
            candles[opening] = candle

        log.debug("[%s] Downloaded page of %d candles.", symbol, len(raw_klines))
        if len(raw_klines) < PAGE_LIMIT:
            break
        current_start = max(int(kline[0]) for kline in raw_klines) + 1
        time.sleep(0.3)
    return candles


def write_json(json_path: Path, coin: str, candles: dict, previous: dict, server_time_ms: int) -> None:
    """Write a versioned document atomically, retaining extension fields."""
    now = timestamp_iso(int(datetime.now(timezone.utc).timestamp() * 1000))
    ordered = [candles[key] for key in sorted(candles)]
    for candle in ordered:
        candle["is_closed"] = candle["close_time_ms"] < server_time_ms
    metadata = dict(previous.get("metadata", {}))
    metadata.update({
        "created_at": metadata.get("created_at", now),
        "updated_at": now,
        "as_of_server_time": timestamp_iso(server_time_ms),
        "as_of_server_time_ms": server_time_ms,
        "candle_count": len(ordered),
        "first_open_time_ms": ordered[0]["open_time_ms"] if ordered else None,
        "last_open_time_ms": ordered[-1]["open_time_ms"] if ordered else None,
        "decimal_encoding": "string",
        "timestamp_unit": "milliseconds",
    })
    market = dict(previous.get("market", {}))
    market.update({
        "exchange": "binance", "type": "spot", "symbol": coin + QUOTE_ASSET,
        "base_asset": coin, "quote_asset": QUOTE_ASSET,
        "interval": CANDLE_INTERVAL, "timezone": "UTC",
    })
    document = dict(previous)
    document.update({"schema_version": SCHEMA_VERSION, "market": market, "metadata": metadata, "candles": ordered})
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=json_path.parent,
            prefix=json_path.name + ".", suffix=".tmp", delete=False,
        ) as handle:
            temp_path = Path(handle.name)
            json.dump(document, handle, indent=2, ensure_ascii=False, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, json_path)
    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink()


def update_coin(coin: str, output_dir: Path, log: logging.Logger) -> dict:
    symbol = coin + QUOTE_ASSET
    json_path = output_dir / f"{coin.lower()}_{CANDLE_INTERVAL}.json"
    csv_path = json_path.with_suffix(".csv")
    existing, document = load_candles(json_path, symbol)
    migrated = not json_path.exists() and csv_path.exists()
    if migrated:
        existing = load_legacy_csv(csv_path, symbol)
        document = {"metadata": {"migrated_from": csv_path.name}}
        log.info("[%s] Loaded %d candles for migration from %s.", symbol, len(existing), csv_path.name)

    if existing:
        # Fetch the newest saved candle again so a forming candle can be finalized.
        resume_ms = max(existing)
        log.info("[%s] Resuming from %s (%d saved candles).", symbol, timestamp_text(resume_ms), len(existing))
    else:
        resume_ms = int(START_DATE.timestamp() * 1000)
        log.info("[%s] Starting from %s.", symbol, START_DATE.date())

    end_ms = int(request_json("/api/v3/time", {}, log)["serverTime"])
    downloaded = fetch_klines_since(symbol, resume_ms, end_ms, log)
    if not downloaded and not migrated:
        log.info("[%s] No candles returned; saved file unchanged.", symbol)
        return {"status": "unchanged", "added": 0, "refreshed": 0, "total": len(existing)}

    added = len(downloaded.keys() - existing.keys())
    refreshed = len(downloaded) - added
    for opening, candle in downloaded.items():
        old_candle = existing.get(opening, {})
        merged = {**old_candle, **candle}
        for field in ("ohlc", "volume"):
            merged[field] = {**old_candle.get(field, {}), **candle[field]}
        existing[opening] = merged
    write_json(json_path, coin, existing, document, end_ms)
    latest = existing[max(existing)] if existing else None
    log.info(
        "[%s] Saved %s: %d added, %d refreshed, %d total; latest close=%s.",
        symbol, json_path.name, added, refreshed, len(existing), latest["ohlc"]["close"] if latest else "n/a",
    )
    return {"status": "updated", "added": added, "refreshed": refreshed, "total": len(existing), "migrated_from_csv": migrated}


def fetch_prices(output_dir=None, coins=None) -> dict:
    """Update all requested coins once, silently, and return per-coin results.

    output_dir: directory for JSONs/logs; defaults to data_crypto_sets beside this module.
    coins: optional base tickers (e.g. ["XRP", "ETH"]); defaults to COINS.

    Results are keyed by base coin. Each contains json_file, log_file and status:
    'updated' (with added/refreshed/total counts), 'unchanged', or 'error'
    (with an error message). Operational failures are logged and returned;
    other coins continue. Invalid arguments raise ValueError.
    There is no background loop or four-hour sleep. Network retries and
    short request pacing may still delay the call. Call serially.
    """
    requested = COINS if coins is None else coins
    if isinstance(requested, str):
        raise ValueError("coins must be a collection of base tickers, not a string")
    selected = list(dict.fromkeys(str(coin).strip().upper() for coin in requested))
    if not selected or any(not coin.isascii() or not coin.isalnum() for coin in selected):
        raise ValueError("coins must contain at least one valid base coin ticker")
    directory = (Path(output_dir) if output_dir is not None
                 else Path(__file__).resolve().parent / "data_crypto_sets")
    results = {}

    for coin in selected:
        log = None
        result = {
            "json_file": str(directory / f"{coin.lower()}_{CANDLE_INTERVAL}.json"),
            "log_file": str(directory / f"{coin.lower()}_{CANDLE_INTERVAL}.log"),
        }
        try:
            directory.mkdir(parents=True, exist_ok=True)
            log = setup_logger(f"fetcher.{coin}", Path(result["log_file"]))
            log.info("[%s%s] Starting update.", coin, QUOTE_ASSET)
            result.update(update_coin(coin, directory, log))
        except Exception as exc:
            result.update({"status": "error", "error": str(exc)})
            if log is not None:
                log.exception("[%s%s] Update failed; continuing with the other coins.", coin, QUOTE_ASSET)
        finally:
            if log is not None:
                for handler in list(log.handlers):
                    handler.close()
                    log.removeHandler(handler)
        results[coin] = result
        time.sleep(0.3)
    return results


