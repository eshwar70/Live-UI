#!/bin/bash
# ALL Trade UI — One-Click Daily Morning Launcher
# Runs all 5 bots concurrently on live Upstox data in paper-trading mode

cd "$(dirname "$0")/.."
echo "Starting ALL Trade UI..."
python3 -m all_trade_ui.run "$@"
