# Drop-In Bots Directory (`bots/`)

Place any new bot configuration `*.json` file in this directory to include it automatically in the **Live Trading UI** (`BotEngineHub._load_dropin_bots()`).

## Quick Start

1. Copy [`_template_bot.json.example`](./_template_bot.json.example) to a new `.json` file, e.g.:
   ```bash
   cp bots/_template_bot.json.example bots/my_momentum_bot.json
   ```
2. Edit `bot_id`, `name`, `family`, `enabled`, and `cfg` parameters.
   - Files starting with `_` or `.` or ending with `.example.json` are skipped automatically.
   - Each `.json` file can contain either a single JSON object (`{ ... }`) or a JSON array of bot objects (`[ { ... }, { ... } ]`).
3. Restart the UI server (`./all_trade_ui/start_ui.sh`).
   - Up to 6 active bots render in a **3x2** grid.
   - Adding a 7th+ active bot automatically expands the dashboard to a **4x2** grid.

## Supported `cfg` Fields

| Field | Type | Description |
|---|---|---|
| `capital_floor` | float | Minimum wallet threshold before profit lock / capital protection checks |
| `daily_profit_lock` | float / null | Stop trading for the day once realized PnL reaches this target (`null` = uncapped) |
| `daily_loss_limit` | float / null | Daily stop-loss threshold (`null` = unrestricted) |
| `max_trades_day` | int / null | Max closed trades per day (`null` = unrestricted) |
| `max_lots` | int | Nifty lots per trade (`1` lot = `65` qty) |
| `target_delta` | float | Desired option delta (e.g. `0.55`) |
| `stop_fraction` | float | Initial option premium stop loss (e.g. `0.085` = `-8.5%`) |
| `target_fraction` | float | Option premium profit target (e.g. `0.35` = `+35%`) |
| `trail_activation` | float / null | Profit fraction where trailing stop activates |
| `trail_fraction` | float / null | Trailing stop distance as fraction of premium |
| `breakeven_activation_fraction` | float / null | Profit fraction to lock breakeven+ cushion |
| `breakeven_lock_fraction` | float / null | Locked floor fraction once breakeven triggers |
| `step_trail_levels` | list[[float, float]] | Stepped `[trigger_gain, locked_gain]` ratchet tiers |
| `vix_tier_low_max`, `vix_tier_mid_max`, `vix_tier_high_max`, `vix_regimes` | object | Optional India VIX tier overrides (`low`, `mid`, `high`, `crisis`) |

## Dynamic Runtime Addition (No Restart Required)

You can also add a bot dynamically at runtime via the HTTP API `POST /api/add_bot` (handled by `AllTradeHandler.handle_add_bot` in [`all_trade_ui/server.py`](../all_trade_ui/server.py)):

```bash
curl -X POST http://localhost:8888/api/add_bot \
  -H 'Content-Type: application/json' \
  -d @bots/my_momentum_bot.json
```

