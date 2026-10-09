"""Three Musketeers: $100 paper accounts for database/data_crypto_sets/*_4h.json.

Call database/fetcher.py::fetch_prices() immediately, then every four hours.
Replay closed candles on the first run; subsequently process only new candles.
No account credentials, account access, or exchange order placement is used.
Python 3.10+; the strategy itself requires only the standard library.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
import importlib.util
import json
import logging
import math
import os
from pathlib import Path
import sys
import tempfile
import time

BASE_DIR = Path(__file__).resolve().parent
CHECK_INTERVAL_SECONDS = 4 * 60 * 60
INITIAL_BALANCE_USD = 100.0
MIN_TRADE_USD = 10.0
YELLOW_WAIT_CANDLES = 12  # 48 hours of consecutive 4h candles.
MAX_ORDERS_IN_FILE = 1000
LONG_MA_UP = 183
LONG_MA_DOWN = 178
SHORT_MA = 17
SIG_BUY_MA = 183
CRIT_BUY_MA = 183

log = logging.getLogger("strategy")


@dataclass
class MarketData:
    symbol: str
    quote_asset: str
    candles: list[dict]


def positive_number(value, field: str) -> float:
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise ValueError(f"{field} must be finite and greater than zero")
    return number


def load_prices(path: Path, now: float | None = None) -> MarketData:
    """Read the supplied schema_version=1, nested-OHLC, millisecond format."""
    with path.open(encoding="utf-8") as handle:
        document = json.load(handle)
    if not isinstance(document, dict) or document.get("schema_version") != 1:
        raise ValueError("Expected a schema_version=1 JSON object")
    market = document["market"]
    if market.get("interval") != "4h":
        raise ValueError("Only 4h candle files are supported")
    symbol = market["symbol"]
    quote = market["quote_asset"].upper()
    if not isinstance(symbol, str) or not symbol:
        raise ValueError("market.symbol must be a nonempty string")
    if quote not in {"USD", "USDT", "USDC"}:
        raise ValueError("A $100 simulation requires a USD, USDT, or USDC quote")
    cutoff = time.time() if now is None else now
    server_ms = document.get("metadata", {}).get("as_of_server_time_ms")
    if server_ms is not None:
        cutoff = min(cutoff, positive_number(server_ms, "server time") / 1000)
    rows = document["candles"]
    if not isinstance(rows, list):
        raise ValueError("candles must be a list")
    by_close_time = {}
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"Candle {index} must be an object")
        if row.get("is_closed") is not True:
            continue
        open_time = positive_number(row["open_time_ms"], "open_time_ms") / 1000
        close_time = positive_number(row["close_time_ms"], "close_time_ms") / 1000
        if close_time > cutoff:
            continue
        if not math.isclose(close_time - open_time, CHECK_INTERVAL_SECONDS - 0.001,
                            abs_tol=0.002, rel_tol=0):
            raise ValueError(f"Candle {index} does not span four hours")
        ohlc = row["ohlc"]
        candle = {"open_time": open_time, "close_time": close_time}
        for field in ("open", "high", "low", "close"):
            candle[field] = positive_number(ohlc[field], field)
        if not (candle["low"] <= min(candle["open"], candle["close"])
                <= max(candle["open"], candle["close"]) <= candle["high"]):
            raise ValueError(f"Candle {index} has inconsistent OHLC values")
        prior = by_close_time.get(close_time)
        if prior is not None and prior != candle:
            raise ValueError(f"Conflicting duplicate candle at {close_time}")
        by_close_time[close_time] = candle
    candles = sorted(by_close_time.values(), key=lambda candle: candle["close_time"])
    return MarketData(symbol, quote, candles)


def initial_state() -> dict:
    """Each file gets its own $100 once; later cycles retain cash and profits."""
    return {
        "initial_balance_usd": INITIAL_BALANCE_USD,
        "balance": INITIAL_BALANCE_USD,
        "invested_value": 0.0,
        "total_units": 0.0,
        "avg_buy_price": None,
        "trade_counter": 0,
        "red_buy_done": False,
        "sig_buy_done": False,
        "crit_buy_done": False,
        "in_position": False,
        "sell_armed": False,
        "first_buy": None,
        "processed_candles": 0,
        "purple_first_trigger_sequence": None,
        "last_close_time": None,
    }


def atomic_write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8",
                                         dir=path.parent, prefix=f".{path.name}.",
                                         suffix=".tmp", delete=False) as handle:
            temporary_path = Path(handle.name)
            json.dump(payload, handle, indent=2, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def load_portfolio(path: Path, market: MarketData) -> tuple[dict, list[dict]]:
    if not path.exists():
        return initial_state(), []
    with path.open(encoding="utf-8") as handle:
        saved = json.load(handle)
    if saved.get("schema_version") != 1:
        raise ValueError("Unsupported strategy checkpoint version")
    if saved["symbol"] != market.symbol or saved["quote_asset"] != market.quote_asset:
        raise ValueError("Market changed for this file; use a new results directory")
    state, orders = saved["state"], saved["orders"]
    if not isinstance(state, dict) or not isinstance(orders, list):
        raise ValueError("Invalid checkpoint: expected state object and orders list")
    missing = set(initial_state()) - set(state)
    if missing:
        raise ValueError(f"Checkpoint is missing state fields: {sorted(missing)}")
    return state, orders


def save_portfolio(path: Path, market: MarketData, state: dict,
                   orders: list[dict]) -> list[dict]:
    """Commit state and orders together, avoiding duplicate trades on restart."""
    if len(orders) > MAX_ORDERS_IN_FILE:
        overflow = orders[:-MAX_ORDERS_IN_FILE]
        orders = orders[-MAX_ORDERS_IN_FILE:]
        # Key by the starting ID so retries with a larger replay replace the
        # pending archive rather than leave overlapping ranges of order IDs.
        archive = (path.parent / "orders_archive" / path.stem
                   / f"orders_{overflow[0]['id']}.json")
        atomic_write_json(archive, overflow)
    atomic_write_json(path, {
        "schema_version": 1,
        "symbol": market.symbol,
        "quote_asset": market.quote_asset,
        "state": state,
        "orders": orders,
    })
    return orders


def sma(values: list[float], period: int) -> list[float | None]:
    return [None if index + 1 < period
            else sum(values[index + 1 - period:index + 1]) / period
            for index in range(len(values))]


def crossunder(now_a: float, prev_a: float, now_b: float, prev_b: float) -> bool:
    return prev_a - prev_b >= -1e-9 and now_a < now_b


def candle_time_iso(timestamp: float) -> str:
    """Format a candle's epoch seconds as an ISO 8601 UTC timestamp."""
    return (datetime.fromtimestamp(timestamp, timezone.utc)
            .isoformat(timespec="milliseconds").replace("+00:00", "Z"))


def buy(state: dict, orders: list[dict], candle: dict, symbol: str,
        fraction: float, flag: str, tag: str, first: str) -> bool:
    if state[flag]:
        return False
    spend = state["balance"] * fraction
    if spend < MIN_TRADE_USD:
        return False
    units = spend / candle["low"]
    state["total_units"] += units
    state["invested_value"] += spend
    state["balance"] -= spend
    state["avg_buy_price"] = state["invested_value"] / state["total_units"]
    state["trade_counter"] += 1
    state["in_position"] = True
    state[flag] = True
    if state["first_buy"] is None:
        state["first_buy"] = first
        if first == "purple":
            state["purple_first_trigger_sequence"] = state["processed_candles"]
    orders.append({
        "id": state["trade_counter"], "type": "BUY", "tag": tag,
        "symbol": symbol, "time": candle["close_time"],
        "candle_open_time": candle_time_iso(candle["open_time"]),
        "candle_close_time": candle_time_iso(candle["close_time"]),
        "price": round(candle["low"], 8), "spent": round(spend, 8),
        "units": round(units, 12),
        "total_units_after": round(state["total_units"], 12),
        "avg_buy_price": round(state["avg_buy_price"], 8),
        "cash_left": round(state["balance"], 8), "dry_run": True,
    })
    return True


def sell_if_ready(state: dict, orders: list[dict], candle: dict, symbol: str) -> bool:
    if not (state["sell_armed"] and state["in_position"]
            and state["avg_buy_price"] is not None):
        return False
    if candle["high"] < state["avg_buy_price"] * 1.12:
        return False
    proceeds = state["total_units"] * candle["high"]
    profit = proceeds - state["invested_value"]
    state["trade_counter"] += 1
    orders.append({
        "id": state["trade_counter"], "type": "SELL", "tag": "Blue",
        "symbol": symbol, "time": candle["close_time"],
        "candle_open_time": candle_time_iso(candle["open_time"]),
        "candle_close_time": candle_time_iso(candle["close_time"]),
        "price": round(candle["high"], 8),
        "units": round(state["total_units"], 12),
        "total_units_after": 0.0,  # Blue sells the entire position.
        "proceeds": round(proceeds, 8), "profit": round(profit, 8),
        "profit_pct": round(profit / state["invested_value"] * 100, 2),
        "dry_run": True,
    })
    state["balance"] += proceeds
    for field in ("total_units", "invested_value"):
        state[field] = 0.0
    for field in ("avg_buy_price", "first_buy", "purple_first_trigger_sequence"):
        state[field] = None
    for field in ("in_position", "red_buy_done", "sig_buy_done", "crit_buy_done", "sell_armed"):
        state[field] = False
    return True


def apply_signals(candle: dict, state: dict, orders: list[dict], symbol: str,
                  red: bool, blue: bool, purple: bool, yellow: bool) -> bool:
    if blue and state["in_position"]:
        state["sell_armed"] = True
    # Resolve an eligible exit before buys, using the pre-buy position.
    # The old script charged for same-candle buys, then erased their units.
    if sell_if_ready(state, orders, candle, symbol):
        return True

    first = state["first_buy"]
    if first is None:
        # Preserve the original priority when multiple signals coincide.
        if yellow:
            first = "yellow"
        elif purple:
            first = "purple"
        elif red:
            first = "red"
    added = False
    if first == "yellow":
        if yellow:
            added = buy(state, orders, candle, symbol, 0.75, "crit_buy_done",
                        "Yellow (First, 75%)", "yellow")
    elif first == "purple":
        if purple:
            added = buy(state, orders, candle, symbol, 0.75, "sig_buy_done",
                        "Purple (First, 75%)", "purple")
        trigger = state["purple_first_trigger_sequence"]
        if trigger is not None and state["processed_candles"] - trigger >= YELLOW_WAIT_CANDLES:
            added = buy(state, orders, candle, symbol, 1.0, "crit_buy_done",
                        "Yellow (Delayed, 100% of remainder)", "yellow") or added
    else:
        if red:
            added = buy(state, orders, candle, symbol, 0.5, "red_buy_done", "Red", "red")
        if purple:
            added = buy(state, orders, candle, symbol, 0.5, "sig_buy_done", "Purple", "purple") or added
        if yellow:
            added = buy(state, orders, candle, symbol, 1.0, "crit_buy_done",
                        "Yellow (Critical)", "yellow") or added
    return added


def run_strategy(candles: list[dict], state: dict, orders: list[dict],
                 symbol: str) -> tuple[dict, list[dict], bool]:
    if not candles:
        return state, orders, False
    closes = [candle["close"] for candle in candles]
    up, down, short, border, second = (
        sma(closes, period) for period in
        (LONG_MA_UP, LONG_MA_DOWN, SHORT_MA, SIG_BUY_MA, CRIT_BUY_MA)
    )
    cursor = state["last_close_time"]
    first_new = next((index for index, candle in enumerate(candles)
                      if cursor is None or candle["close_time"] > cursor), None)
    if first_new is None:
        return state, orders, False
    # On later runs, require the preceding 183 candles for valid crossovers.
    # If a fetcher trims too much history, do not silently advance the cursor.
    lookback = max(LONG_MA_UP, LONG_MA_DOWN, SHORT_MA, SIG_BUY_MA, CRIT_BUY_MA)
    if cursor is not None and state["processed_candles"] >= lookback and first_new < lookback:
        raise ValueError(f"Keep at least {lookback} old candles before the first new candle")
    added = False
    for index in range(first_new, len(candles)):
        candle = candles[index]
        state["processed_candles"] += 1
        state["last_close_time"] = candle["close_time"]
        if index == 0 or any(series[index] is None or series[index - 1] is None
                             for series in (up, down, short, border, second)):
            continue
        added = apply_signals(
            candle, state, orders, symbol,
            red=crossunder(short[index], short[index - 1], down[index], down[index - 1]),
            blue=crossunder(short[index], short[index - 1], up[index] * 1.1, up[index - 1] * 1.1),
            purple=crossunder(short[index], short[index - 1], border[index] / 1.3, border[index - 1] / 1.3),
            yellow=crossunder(short[index], short[index - 1], second[index] / 1.5, second[index - 1] / 1.5),
        ) or added
    return state, orders, added


@contextmanager
def database_context(database_dir: Path):
    """Allow an existing fetcher to use sibling imports and relative paths."""
    old_directory, old_path = Path.cwd(), sys.path[:]
    try:
        os.chdir(database_dir)
        sys.path.insert(0, str(database_dir))
        yield
    finally:
        os.chdir(old_directory)
        sys.path[:] = old_path


@lru_cache(maxsize=None)
def load_fetcher(database_dir: Path):
    fetcher_path = database_dir / "fetcher.py"
    if not fetcher_path.is_file():
        raise FileNotFoundError(f"Missing {fetcher_path}")
    spec = importlib.util.spec_from_file_location("_crypto_strategy_fetcher", fetcher_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot import {fetcher_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(spec.name, None)
        raise
    function = getattr(module, "fetch_prices", None)
    if not callable(function):
        raise ImportError("fetcher.py must define a callable fetch_prices()")
    return function


def refresh_prices(database_dir: Path):
    with database_context(database_dir):
        fetch_prices = load_fetcher(database_dir)
        results = fetch_prices()
    log.info("Price fetch completed")
    return results


def run_cycle(database_dir: Path, results_dir: Path, fetch: bool = True) -> bool:
    success = True
    if fetch:
        try:
            refresh_prices(database_dir)
        except Exception:
            success = False
            log.exception("Price fetch failed; processing available closed candles")
    prices_dir = database_dir / "data_crypto_sets"
    # Read old layouts until the fetcher creates its new output directory.
    if not prices_dir.is_dir():
        prices_dir = database_dir
    files = sorted(prices_dir.glob("*_4h.json"))
    if not files:
        log.error("No *_4h.json files found in %s", prices_dir)
        return False
    for source in files:
        try:
            market = load_prices(source)
            if not market.candles:
                log.warning("%s: no closed candles available", source.name)
                continue
            checkpoint = results_dir / f"{source.stem}_strategy.json"
            state, orders = load_portfolio(checkpoint, market)
            if state["last_close_time"] is not None and market.candles[-1]["close_time"] < state["last_close_time"]:
                raise ValueError("Price history is older than the saved strategy cursor")
            previous_cursor = state["last_close_time"]
            previous_trades = state["trade_counter"]
            state, orders, _ = run_strategy(market.candles, state, orders, market.symbol)
            if state["last_close_time"] != previous_cursor:
                save_portfolio(checkpoint, market, state, orders)
            log.info("%s: cash %.2f %s, units %.10f, %d new paper orders",
                     market.symbol, state["balance"], market.quote_asset,
                     state["total_units"], state["trade_counter"] - previous_trades)
        except Exception:
            success = False
            log.exception("Failed to process %s; continuing with other files", source.name)
    return success


def next_deadline(previous_deadline: float, now: float) -> float:
    """Keep a four-hour cadence; skip missed slots after an unusually slow cycle."""
    deadline = previous_deadline + CHECK_INTERVAL_SECONDS
    if deadline <= now:
        deadline += (math.floor((now - deadline) / CHECK_INTERVAL_SECONDS) + 1) * CHECK_INTERVAL_SECONDS
    return deadline


def run(database_dir: Path, results_dir: Path, once: bool = False,
        fetch: bool = True) -> int:
    deadline = time.monotonic()
    try:
        while True:
            success = run_cycle(database_dir, results_dir, fetch=fetch)
            if once:
                return 0 if success else 1
            deadline = next_deadline(deadline, time.monotonic())
            log.info("Next price refresh in %.0f seconds", deadline - time.monotonic())
            time.sleep(max(0.0, deadline - time.monotonic()))
    except KeyboardInterrupt:
        log.info("Stopped by user")
        return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-dir", type=Path, default=BASE_DIR / "database")
    parser.add_argument("--results-dir", type=Path, default=BASE_DIR / "strategy_results")
    parser.add_argument("--once", action="store_true", help="Fetch/process once, then exit")
    parser.add_argument("--no-fetch", action="store_true", help="Use existing JSON without calling fetcher")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    database_dir, results_dir = args.database_dir.resolve(), args.results_dir.resolve()
    if not database_dir.is_dir():
        parser.error(f"Database directory does not exist: {database_dir}")
    return run(database_dir, results_dir, once=args.once, fetch=not args.no_fetch)


if __name__ == "__main__":
    raise SystemExit(main())
