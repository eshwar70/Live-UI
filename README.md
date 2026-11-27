# Live Trading UI — Multi-Bot Nifty 50 Arena

A real-time multi-bot Nifty 50 options paper-trading arena powered by Upstox WebSocket V3, instant Server-Sent Events (SSE) UI streaming, time-lapse historical replay, and automatic end-of-day CSV archiving.

## Built-In Trading Bots

| Slot | Bot ID | Name | Strategy / Family |
|---|---|---|---|
| 1 | `winner_51` | **Winner 51** | `Chennai_WideRunner_32 [g]` |
| 2 | `bot_51_1` | **51.1** | `Uncapped Profit Runner` |
| 3 | `chad_25` | **Chad 25** | `Chandelier Compounding 4L` |
| 4 | `chandler_runer` | **Chandler Runner** | `Chandelier Wide-Target` |
| 5 | `live_trading_bot` | **Live Trading Bot** | `Gravitas Capital Shield` |
| 6 | `champion_omega` | **Champion Omega** | `Apex Omega Prime [0.4-omega]` (VIX-tier + Profit Ratchet) |

## Key Features

- **Low-Latency Live Data:** Upstox Market Data Feed V3 WebSocket with dead-socket recovery and REST polling fallback.
- **Instant Dashboard Updates:** Push SSE stream (`TCP_NODELAY`, lightweight `: keepalive` heartbeats, and `requestAnimationFrame` DOM diffing).
- **Dynamic Arena Grid:** Hides `OFF` bots cleanly, scales `3x2` for up to 6 active bots, and expands to `4x2` when 7+ active bots are enabled.
- **Individual & Global Controls:** Independent per-bot Start/Stop (`enabled`), Restart/Reset, and Kill switches, plus configurable starting wallet (`Rs 50,000` default).
- **Time-Lapse Practice Replay:** Replays historical Nifty 50 sessions rapidly without disturbing live session state, then summarizes completed trades at the end of replay.
- **End-of-Day Archiving:** Saves full per-bot summaries, reset rounds, and unified session CSVs into `arena- YYYY-MM-DD/` via the UI or `POST /api/save_arena_archive`.

## Quick Start

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
2. **Configure your Upstox Access Token:**
   ```bash
   cp .env.example .env
   # Edit .env and set UPSTOX_ACCESS_TOKEN=<your_token>
   ```
   *(Never commit `.env` or live session state files — both are listed in [`.gitignore`](./.gitignore).)*
3. **Launch the UI:**
   ```bash
   ./all_trade_ui/start_ui.sh
   # or:
   python3 -m all_trade_ui.run --port 8888
   ```
   Open [http://localhost:8888](http://localhost:8888) in your browser.

## Adding New Bots (`bots/`)

Drop any new bot JSON configuration into the [`bots/`](./bots/README.md) folder to include it automatically in the dashboard on startup:

```bash
cp bots/_template_bot.json.example bots/my_new_bot.json
# Edit bot_id, name, family, enabled, and cfg
```

- Up to 6 active bots use the standard **3x2** layout; enabling a 7th+ bot automatically switches the dashboard to **4x2**.
- For hot-adding a bot without restarting the server, `POST` the same JSON payload to `/api/add_bot`.
- See [`bots/README.md`](./bots/README.md) and [`bots/_template_bot.json.example`](./bots/_template_bot.json.example) for the full configuration reference.

## Project Layout

```text
live-ui/
├── bots/                         # Drop custom *.json bot definitions here
│   ├── README.md                 # Schema & usage guide for custom bots
│   └── _template_bot.json.example
├── all_trade_ui/
│   ├── __init__.py
│   ├── config.py                 # Loads UPSTOX_ACCESS_TOKEN from env / .env
│   ├── indicators.py             # Option pricing & market indicator helpers
│   ├── market_feed.py            # Upstox V3 WebSocket, REST fallback & replay
│   ├── bot_engine.py             # Multi-bot engine, VIX regimes & bots/ loader
│   ├── server.py                 # HTTP API + SSE broadcaster
│   ├── run.py                    # CLI launcher
│   ├── start_ui.sh
│   ├── test_ui.py                # Unit & integration test suite
│   └── templates/index.html      # Live Arena dashboard UI
├── .env.example
├── .gitignore
├── requirements.txt
└── Run_ALL_Trade_UI.command
```

## Running Tests

```bash
python3 -m py_compile all_trade_ui/*.py
python3 all_trade_ui/test_ui.py
```
