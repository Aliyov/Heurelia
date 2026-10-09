
import os
import sys
import time
import subprocess
from pathlib import Path

# =====================================
# CONFIGURATION
# =====================================

MAIN = Path("/home/savage/Heurelia/coins/main.py")
TELEGRAM = Path("/home/savage/Heurelia/coins/Telegram/telegram.py")

START_DELAY = 30
CHECK_INTERVAL = 24 * 60 * 60  # 24 hours


# =====================================
# CHECK IF A PROGRAM IS RUNNING
# =====================================

def is_running(script):
    target = script.resolve()

    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue

        try:
            args = (proc / "cmdline").read_bytes().split(b"\0")

            for raw in args[1:]:
                arg = os.fsdecode(raw)

                if not arg.endswith(".py"):
                    continue

                candidate = Path(arg)

                if not candidate.is_absolute():
                    cwd = (proc / "cwd").resolve()
                    candidate = cwd / candidate

                if candidate.resolve() == target:
                    return True

        except (OSError, ValueError):
            continue

    return False


# =====================================
# START PROGRAM IF NOT RUNNING
# =====================================

def ensure_running(script):
    if is_running(script):
        print(f"✅ {script.name} is running", flush=True)
        return

    print(f"🚀 Starting {script.name}", flush=True)

    subprocess.Popen(
        [sys.executable, "-u", str(script)],
        cwd=str(script.parent)
    )


# =====================================
# MAIN WATCHDOG
# =====================================

def main():
    print("🔥 Heurelia Watchdog Started", flush=True)

    # First startup
    ensure_running(MAIN)

    print("⏳ Waiting 30 seconds...", flush=True)
    time.sleep(START_DELAY)

    ensure_running(TELEGRAM)

    # Verify both after startup
    ensure_running(MAIN)
    ensure_running(TELEGRAM)

    # Check forever
    while True:
        print("😴 Sleeping for 24 hours...", flush=True)
        time.sleep(CHECK_INTERVAL)

        print("🔍 Checking applications...", flush=True)

        ensure_running(MAIN)
        ensure_running(TELEGRAM)


if __name__ == "__main__":
    main()

