#!/usr/bin/env python3
"""Configuration and constants for ALL Trade UI Live Paper Trading System."""
import os
from pathlib import Path
import zoneinfo

BASE_DIR = Path(__file__).resolve().parent
IST = zoneinfo.ZoneInfo("Asia/Kolkata")


def _load_dotenv(env_path: Path) -> None:
    """Populate os.environ from a local .env file without overriding exported vars."""
    if not env_path.is_file():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


_load_dotenv(BASE_DIR.parent / ".env")

# Upstox User Access Token (loaded from environment or gitignored .env)
UPSTOX_ACCESS_TOKEN = os.environ.get("UPSTOX_ACCESS_TOKEN", "").strip()

UPSTOX_HEADERS = {
    "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}",
    "Accept": "application/json",
    "User-Agent": "ALL-Trade-UI/1.0",
}

# Market Instruments
NIFTY_INSTRUMENT_KEY = "NSE_INDEX|Nifty 50"
VIX_INSTRUMENT_KEY = "NSE_INDEX|India VIX"

# Default Parameters
DEFAULT_INITIAL_WALLET = float(os.environ.get("ALL_TRADE_UI_WALLET", "50000.0"))
LOT_SIZE = 65  # Nifty 50 standard lot size
ROUND_TRIP_CHARGES = 60.0  # Brokerage (Rs 40) + STT + GST + turnover charges

# Server Port
DEFAULT_PORT = int(os.environ.get("ALL_TRADE_UI_PORT", "8888"))
