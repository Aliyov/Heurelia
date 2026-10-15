#!/usr/bin/env python3
"""Send previously unsent Heurelia 4H strategy orders to a Telegram group.

On a clean install, ALL historical orders are sent. Afterwards, SQLite keeps
track of notifications across restarts. The program polls every four hours
when a pass succeeds, or retries after one minute when a pass fails.

Only Python's standard library is required.
"""

import argparse
import hashlib
import html
import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get(
    "HEURELIA_DATA_DIR", str(BASE_DIR / "strategy_results")
))
DB_PATH = Path(os.environ.get(
    "HEURELIA_NOTIFY_DB", "/home/savage/.local/state/heurelia/telegram_notifier.sqlite3"
))
# Credentials are read from telegram.env next to this Python script.
ENV_FILE = Path(__file__).resolve().with_name("telegram.env")


def load_telegram_env():
    """Read bot credentials from adjacent telegram.env without extra packages.

    Already-exported environment variables take precedence.
    """
    if not ENV_FILE.is_file():
        return
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        name, separator, value = line.partition("=")
        name = name.strip()
        if separator != "=" or name not in ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"):
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("\"", "'"):
            value = value[1:-1]
        os.environ.setdefault(name, value)


CHECK_SECONDS = 4 * 60 * 60
RETRY_SECONDS = 60 * 60 *2
SEND_DELAY_SECONDS = 3.5  # Conservative per-group rate: <20 messages/minute.


class TelegramError(Exception):
    """Telegram request failed without exposing the secret token in logs."""


def log(message):
    print(f"[{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC] {message}", flush=True)


def make_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(DB_PATH)
    db.execute("""
        CREATE TABLE IF NOT EXISTS sent_orders (
            order_key TEXT PRIMARY KEY,
            source_file TEXT NOT NULL,
            symbol TEXT NOT NULL,
            order_id TEXT NOT NULL,
            sent_at TEXT NOT NULL
        )
    """)
    db.commit()
    return db


def order_key(path, symbol, order):
    # IDs are expected to be stable within a given strategy file.
    # Include the time and side so that restarted/reset order counters
    # don't silently hide different trades.
    if order.get("id") is not None:
        identity = {
            "file": path.name,
            "symbol": symbol,
            "id": order["id"],
            "time": order.get("time", order.get("candle_close_time")),
            "type": order.get("type"),
        }
    else:
        identity = {"file": path.name, "symbol": symbol, "order": order}
    canonical = json.dumps(identity, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def event_time(order):
    try:
        return float(order["time"])
    except (KeyError, TypeError, ValueError):
        try:
            timestamp = str(order["candle_close_time"]).replace("Z", "+00:00")
            return datetime.fromisoformat(timestamp).timestamp()
        except (KeyError, ValueError, TypeError, OverflowError):
            return 0.0


def order_date(order):
    seconds = event_time(order)
    if seconds:
        try:
            return datetime.fromtimestamp(seconds, tz=timezone.utc).strftime(
                "%d %b %Y, %H:%M UTC"
            )
        except (OverflowError, OSError, ValueError):
            pass
    return "Not provided"


def safe(value):
    return html.escape(str(value), quote=True)


def number(value, decimals=2):
    try:
        val = float(value)
        return f"{val:,.{decimals}f}"
    except (TypeError, ValueError, OverflowError):
        return safe(value)


def price(value):
    try:
        v = float(value)
        return f"${v:,.2f}" if abs(v) >= 1 else f"${v:,.8f}"
    except (TypeError, ValueError, OverflowError):
        return safe(value)


def format_order(symbol, quote, order):
    side = str(order.get("type", "UNKNOWN")).upper()
    icon = {"BUY": "🟢", "SELL": "🔴"}.get(side, "🔔")
    display_pair = (
        f"{symbol[:-len(quote)]}/{quote}"
        if quote and symbol.endswith(quote) and len(symbol) > len(quote)
        else symbol
    )
    lines = [
        f"{icon} <b>{safe(side)} ORDER — {safe(display_pair)}</b>",
        "",
        f"- <b>Strategy:</b> {safe(order.get('tag', 'Unknown'))}",
        f"- <b>Order:</b> #{safe(order.get('id', '?'))}",
        f"- <b>Price:</b> {price(order.get('price'))}",
    ]

    if side == "BUY":
        if "spent" in order:
            lines.append(f"- <b>Invested:</b> ${number(order['spent'])}")
        if "units" in order:
            lines.append(f"- <b>Bought:</b> {number(order['units'], 8)} {safe(symbol[:-len(quote)] if quote and symbol.endswith(quote) else symbol)}")
        if "avg_buy_price" in order:
            lines.append(f"- <b>Avg. buy price:</b> {price(order['avg_buy_price'])}")
        if "cash_left" in order:
            lines.append(f"- <b>Cash remaining:</b> ${number(order['cash_left'])}")
    elif side == "SELL":
        if "units" in order:
            lines.append(f"- <b>Sold units:</b> {number(order['units'], 8)}")
        if "proceeds" in order:
            lines.append(f"- <b>Proceeds:</b> ${number(order['proceeds'])}")
        if "profit" in order:
            p = order["profit"]
            profit_icon = "-" if isinstance(p, (int, float)) and p >= 0 else "🔻"
            lines.append(f"{profit_icon} <b>Profit/Loss:</b> ${number(p)}")
        if "profit_pct" in order:
            lines.append(f"- <b>Return:</b> {number(order['profit_pct'])}%")

    if "total_units_after" in order:
        lines.append(f"- <b>Holdings after:</b> {number(order['total_units_after'], 8)}")

    lines += [
        f"- <b>Trade time:</b> {safe(order_date(order))}",
        "- <b></b>" if order.get("dry_run") is True
        else "⚙️ <b>Mode:</b> Not marked as dry run" if order.get("dry_run") is False
        else "⚙️ <b>Mode:</b> Unspecified",
        "",
        "<i>Heurelia • </i>",
    ]
    return "\n".join(lines)


def send_message(text):
    token = os.environ["TELEGRAM_BOT_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = json.dumps({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }).encode("utf-8")

    for attempt in range(5):
        request = Request(
            url, data=payload,
            headers={"Content-Type": "application/json"}, method="POST",
        )
        try:
            with urlopen(request, timeout=25) as response:
                result = json.load(response)
            if not result.get("ok"):
                raise TelegramError("Telegram reported an unsuccessful response")
            return
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            try:
                details = json.loads(body)
            except ValueError:
                details = {}
            if exc.code == 429 and attempt < 4:
                retry_after = details.get("parameters", {}).get("retry_after", 30)
                try:
                    delay = max(1, min(600, int(retry_after) + 1))
                except (ValueError, TypeError):
                    delay = 30
                log(f"Telegram rate limit; waiting {delay} seconds")
                time.sleep(delay)
                continue
            raise TelegramError(
                f"Telegram HTTP {exc.code}: {details.get('description', 'request rejected')}"
            ) from None
        except (URLError, TimeoutError, OSError) as exc:
            # Avoid logging request URLs because they contain the bot token.
            raise TelegramError(f"Telegram connection problem ({type(exc).__name__})") from None
    raise TelegramError("Telegram rate limit retries exhausted")


def scan(db):
    """Send every unsent order. Returns True only if all files/sends succeeded."""
    if not DATA_DIR.is_dir():
        log(f"ERROR: JSON directory doesn't exist: {DATA_DIR}")
        return False

    files = sorted(DATA_DIR.glob("*.json"))
    if not files:
        log(f"ERROR: No JSON files found in {DATA_DIR}")
        return False

    pending = []
    failed = False
    for path in files:
        try:
            with path.open("r", encoding="utf-8") as file:
                document = json.load(file)
            if not isinstance(document, dict) or not isinstance(document.get("orders"), list):
                log(f"Skipping {path.name}: no 'orders' array")
                continue
            orders = document["orders"]
            symbol = str(document.get("symbol") or path.stem.split("_")[0].upper())
            quote = str(document.get("quote_asset") or "USDT")
            for order in orders:
                if not isinstance(order, dict):
                    log(f"ERROR: invalid order in {path.name}")
                    failed = True
                    continue
                key = order_key(path, symbol, order)
                exists = db.execute(
                    "SELECT 1 FROM sent_orders WHERE order_key = ?", (key,)
                ).fetchone()
                if not exists:
                    pending.append((event_time(order), path.name, key, symbol, quote, order))
        except (OSError, ValueError, TypeError) as exc:
            # A writer may be halfway through replacing a JSON file.
            log(f"ERROR reading {path.name}: {type(exc).__name__}: {exc}")
            failed = True

    pending.sort(key=lambda item: (item[0], item[1]))
    log(f"Scanned {len(files)} JSON files; {len(pending)} unsent orders")

    for index, (_, filename, key, symbol, quote, order) in enumerate(pending, 1):
        try:
            send_message(format_order(symbol, quote, order))
        except TelegramError as exc:
            log(f"ERROR sending {filename} order #{order.get('id', '?')}: {exc}")
            failed = True
            break  # Next scan retries it and all later unsent orders.
        else:
            # Persist only AFTER Telegram confirms successful delivery.
            db.execute(
                "INSERT OR IGNORE INTO sent_orders VALUES (?, ?, ?, ?, ?)",
                (key, filename, symbol, str(order.get("id", "?")),
                 datetime.now(timezone.utc).isoformat()),
            )
            db.commit()
            log(f"Sent {index}/{len(pending)}: {symbol} #{order.get('id', '?')} {order.get('type', '?')}")
            if index < len(pending):
                time.sleep(SEND_DELAY_SECONDS)

    return not failed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test", action="store_true", help="Send one test message; don't touch order history")
    parser.add_argument("--once", action="store_true", help="Scan and exit instead of running forever")
    args = parser.parse_args()

    load_telegram_env()
    if not os.environ.get("TELEGRAM_BOT_TOKEN") or not os.environ.get("TELEGRAM_CHAT_ID"):
        parser.error(f"Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID. Set both in {ENV_FILE}")

    if args.test:
        send_message("✅ <b>Heurelia Telegram notifier is online!</b>\nRaspberry Pi 4B connection successful.")
        log("Test message successfully sent")
        return

    db = make_db()
    log("Heurelia notifier started")
    try:
        while True:
            complete = scan(db)
            if args.once:
                if not complete:
                    raise SystemExit(1)
                return
            wait = CHECK_SECONDS if complete else RETRY_SECONDS
            log(f"Next scan in {wait // 60} minutes")
            time.sleep(wait)
    except KeyboardInterrupt:
        log("Stopped")
    finally:
        db.close()


if __name__ == "__main__":
    main()
