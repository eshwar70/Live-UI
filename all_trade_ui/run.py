#!/usr/bin/env python3
"""Launcher for ALL Trade UI Live Paper Trading System.

Usage:
    python3 -m all_trade_ui.run [--port 8888] [--no-browser]
"""
import argparse
from pathlib import Path
import sys
import threading
import time
import webbrowser

DIR_PATH = Path(__file__).resolve().parent
sys.path.insert(0, str(DIR_PATH.parent))

from all_trade_ui.config import DEFAULT_INITIAL_WALLET, DEFAULT_PORT, UPSTOX_ACCESS_TOKEN
from all_trade_ui.server import run_server


def main():
    parser = argparse.ArgumentParser(description="ALL Trade UI — Live Multi-Bot System")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Port to bind server (default: 8888)")
    parser.add_argument("--no-browser", action="store_true", help="Do not automatically open browser")
    args = parser.parse_args()

    url = f"http://localhost:{args.port}"
    token_status = "Configured" if UPSTOX_ACCESS_TOKEN else "MISSING (set UPSTOX_ACCESS_TOKEN in .env)"
    print("\n=======================================================")
    print("  ⚡ ALL TRADE UI — LIVE PAPER TRADING ARENA")
    print("=======================================================")
    print("  - Built-in Bots     : Winner 51, 51.1, Chad 25, Chandler Runner, Live Trading Bot, Champion Omega")
    print("  - Drop-in Folder    : bots/*.json (auto-loaded at startup)")
    print(f"  - Upstox Token Feed : {token_status}")
    print(f"  - Starting Wallet   : Rs {DEFAULT_INITIAL_WALLET:,.2f} (Configurable via UI)")
    print(f"  - Dashboard URL     : {url}")
    print("=======================================================\n")

    if not args.no_browser:
        def open_b():
            time.sleep(1.0)
            webbrowser.open(url)
        threading.Thread(target=open_b, daemon=True).start()

    run_server(args.port)


if __name__ == "__main__":
    main()
