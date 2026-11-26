#!/usr/bin/env python3
"""Comprehensive Automated Verification Test Suite for ALL Trade UI.

Validates:
1. Upstox Token & Market Feed Connectivity
2. 5-Bot Simultaneous Initialization with Rs 50,000 Starting Wallet
3. Dynamic Common Wallet Updates
4. Signal Radar & Step-Trailing Stop Execution
5. Individual & Master Kill Switch Squareoffs
6. Full Server API & Endpoints (GET /, GET /api/state, POST endpoints)
"""
from __future__ import annotations

import json
from pathlib import Path
import shutil
import sys
import tempfile
import threading
import time
import unittest

DIR_PATH = Path(__file__).resolve().parent
sys.path.insert(0, str(DIR_PATH.parent))

from all_trade_ui.config import (
    UPSTOX_ACCESS_TOKEN,
    UPSTOX_HEADERS,
    DEFAULT_INITIAL_WALLET,
)
from all_trade_ui.indicators import (
    calc_supertrend,
    calc_ema,
    calc_rsi,
    bs_option_metrics,
    compute_market_features,
)
from all_trade_ui.market_feed import MarketFeed
from all_trade_ui.bot_engine import BotEngineHub
from all_trade_ui.server import ThreadedHTTPServer, AllTradeHandler, StateBroadcaster
import requests


class TestAllTradeUI(unittest.TestCase):
    """Test suite for the ALL Trade UI system."""

    @classmethod
    def setUpClass(cls):
        # Start test HTTP server on port 8899 using isolated temp archive directory
        cls.port = 8899
        cls.tmp_dir = Path(tempfile.mkdtemp(prefix="all_trade_ui_test_"))
        cls.feed = MarketFeed(enable_ws=False)
        cls.hub = BotEngineHub(initial_wallet=DEFAULT_INITIAL_WALLET, archive_root=cls.tmp_dir)
        cls.broadcaster = StateBroadcaster(cls.feed, cls.hub)
        cls.broadcaster.start()
        AllTradeHandler.feed = cls.feed
        AllTradeHandler.hub = cls.hub
        AllTradeHandler.broadcaster = cls.broadcaster
        
        cls.server = ThreadedHTTPServer(("127.0.0.1", cls.port), AllTradeHandler)
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()
        time.sleep(0.3)

    @classmethod
    def tearDownClass(cls):
        cls.broadcaster.stop()
        cls.feed.stop_polling_loop()
        cls.hub.shutdown()
        cls.server.shutdown()
        cls.server.server_close()
        shutil.rmtree(cls.tmp_dir, ignore_errors=True)

    def test_01_upstox_connectivity(self):
        """Verify Upstox API responds with user access token."""
        if not UPSTOX_ACCESS_TOKEN:
            self.skipTest("UPSTOX_ACCESS_TOKEN not set; configure .env to run live connectivity test.")
        url = "https://api.upstox.com/v2/market/status/NSE"
        r = requests.get(url, headers=UPSTOX_HEADERS, timeout=5)
        self.assertEqual(r.status_code, 200, f"Upstox status returned {r.status_code}")
        data = r.json()
        self.assertEqual(data.get("status"), "success")
        self.assertIn("data", data)
        print(f"✓ Upstox API Token Verified. Market Status: {data['data'].get('status')}")

    def test_02_five_bots_initialized(self):
        """Verify all 6 arena bots are loaded with exact required initial states."""
        expected_bots = ["winner_51", "bot_51_1", "chad_25", "chandler_runer", "live_trading_bot", "champion_of_50"]
        state = self.hub.get_state(self.feed.get_snapshot())
        bots = state.get("bots", {})
        
        self.assertEqual(len(bots), 6, f"Expected 6 bots, found {len(bots)}")
        for b_id in expected_bots:
            self.assertIn(b_id, bots, f"Missing bot: {b_id}")
            b = bots[b_id]
            self.assertEqual(b["initial_wallet"], 50000.0)
            self.assertEqual(b["wallet"], 50000.0)
            self.assertEqual(b["day_pnl"], 0.0)
            self.assertTrue(b["enabled"])
            self.assertIn("radar", b)
            self.assertIn("targeted_option", b["radar"])
            print(f"✓ Bot Initialized: {b['name']} ({b['family']}) | Wallet: Rs {b['wallet']:,.2f}")

    def test_03_common_wallet_update(self):
        """Verify starting wallet can be modified globally across all bots."""
        new_wallet = 75000.0
        self.hub.set_starting_wallet(new_wallet)
        state = self.hub.get_state(self.feed.get_snapshot())
        self.assertEqual(state["common_starting_wallet"], 75000.0)
        for b_id, bot in state["bots"].items():
            self.assertEqual(bot["initial_wallet"], 75000.0)
            self.assertEqual(bot["wallet"], 75000.0)
        
        # Reset back to 50000.0
        self.hub.set_starting_wallet(50000.0)
        print("✓ Common Starting Wallet Update & Synchronization Verified.")

    def test_04_bot_toggle_filtering(self):
        """Verify individual bots can be enabled/disabled via checkboxes."""
        self.hub.toggle_bot("chad_25", False)
        state = self.hub.get_state(self.feed.get_snapshot())
        self.assertFalse(state["bots"]["chad_25"]["enabled"])
        
        self.hub.toggle_bot("chad_25", True)
        state = self.hub.get_state(self.feed.get_snapshot())
        self.assertTrue(state["bots"]["chad_25"]["enabled"])
        print("✓ Bot Toggle and Multi-Select Filter Verified.")

    def test_05_trade_execution_and_trailing_sl(self):
        """Simulate trade entry and verify trailing stop ratchet."""
        bot = self.hub.bots["chad_25"]
        # Manually inject open position to test tracking & trailing
        bot.current_trade = {
            "trade_id": "T1_TEST",
            "strike": "NIFTY 25400 CE",
            "strike_val": 25400,
            "kind": "CE",
            "lots": 2,
            "qty": 130,
            "entry_price": 100.0,
            "current_price": 100.0,
            "peak_price": 100.0,
            "target_price": 200.0,
            "stop_loss": 90.0,
            "trailing_sl": 90.0,
            "current_pnl": 0.0,
            "current_pnl_pct": 0.0,
            "entered_at": "09:30:00",
            "exit_price": None,
            "exit_reason": None,
        }
        
        # Simulate price jump (cur_opt_price will be ~147.82, triggering step trailing)
        snap = {
            "spot": 25420.0,
            "vix": 17.7,
            "is_open": True,
            "simulation": True,
            "features": {"price": 25420.0, "st5m_dir": 1},
        }
        self.hub.update_market_cycle(snap)
        
        # Verify trailing SL ratcheted up above 100.0
        t = bot.current_trade
        self.assertIsNotNone(t, "Trade should remain open under target 200.0")
        self.assertGreater(t["trailing_sl"], 90.0, "Trailing SL failed to ratchet up")
        print(f"✓ Trailing SL Ratchet Verified: New Trail Level Rs {t['trailing_sl']:.2f} (Entry: Rs 100.00)")

    def test_06_individual_kill_switch(self):
        """Verify individual bot kill switch force-exits position immediately."""
        bot = self.hub.bots["chad_25"]
        # Ensure trade exists for testing kill switch
        if not bot.current_trade:
            bot.current_trade = {
                "trade_id": "T_KILL_TEST",
                "strike": "NIFTY 25400 CE",
                "strike_val": 25400,
                "kind": "CE",
                "lots": 2,
                "qty": 130,
                "entry_price": 100.0,
                "current_price": 110.0,
                "peak_price": 110.0,
                "target_price": 200.0,
                "stop_loss": 90.0,
                "trailing_sl": 90.0,
                "current_pnl": 1300.0,
                "current_pnl_pct": 10.0,
                "entered_at": "09:30:00",
                "exit_price": None,
                "exit_reason": None,
            }
        
        # Execute kill switch
        res = self.hub.kill_bot("chad_25")
        self.assertTrue(res)
        self.assertIsNone(bot.current_trade, "Trade should be squared off after kill switch")
        self.assertGreaterEqual(len(bot.trades_history), 1)
        self.assertEqual(bot.trades_history[0]["exit_reason"], "MANUAL_KILL_SWITCH")
        print("✓ Individual Bot Kill Switch Verified: Position Squared Off & Recorded.")

    def test_07_master_kill_all_bots(self):
        """Verify master kill switch exits positions across all bots."""
        # Inject position in Winner 51 and Live Bot
        self.hub.bots["winner_51"].current_trade = {
            "trade_id": "T_W51",
            "strike": "NIFTY 25350 CE",
            "strike_val": 25350,
            "kind": "CE",
            "lots": 1,
            "qty": 65,
            "entry_price": 120.0,
            "current_price": 125.0,
            "peak_price": 125.0,
            "target_price": 180.0,
            "stop_loss": 108.0,
            "trailing_sl": 108.0,
            "current_pnl": 325.0,
            "current_pnl_pct": 4.17,
            "entered_at": "10:00:00",
            "exit_price": None,
            "exit_reason": None,
        }
        self.hub.bots["live_trading_bot"].current_trade = {
            "trade_id": "T_LIVE",
            "strike": "NIFTY 25300 PE",
            "strike_val": 25300,
            "kind": "PE",
            "lots": 1,
            "qty": 65,
            "entry_price": 110.0,
            "current_price": 112.0,
            "peak_price": 112.0,
            "target_price": 137.5,
            "stop_loss": 100.1,
            "trailing_sl": 100.1,
            "current_pnl": 130.0,
            "current_pnl_pct": 1.82,
            "entered_at": "10:05:00",
            "exit_price": None,
            "exit_reason": None,
        }
        
        killed_count = self.hub.kill_all_bots()
        self.assertEqual(killed_count, 2)
        self.assertIsNone(self.hub.bots["winner_51"].current_trade)
        self.assertIsNone(self.hub.bots["live_trading_bot"].current_trade)
        print("✓ Master Emergency Kill Switch Verified: All Positions Squared Off.")

    def test_08_http_api_endpoints(self):
        """Verify all HTTP REST endpoints return valid responses."""
        base_url = f"http://127.0.0.1:{self.port}"
        
        # 1. GET /
        r_index = requests.get(f"{base_url}/", timeout=3)
        self.assertEqual(r_index.status_code, 200)
        self.assertIn("ALL Trade UI", r_index.text)
        
        # 2. GET /api/state
        r_state = requests.get(f"{base_url}/api/state", timeout=3)
        self.assertEqual(r_state.status_code, 200)
        state_json = r_state.json()
        self.assertIn("bots", state_json)
        self.assertIn("market", state_json)
        self.assertIn("server_time", state_json)
        self.assertTrue(state_json["market"].get("active_expiry"), "Market snapshot must include active_expiry")
        self.assertTrue(state_json["market"].get("expiry_label"), "Market snapshot must include expiry_label")
        self.assertNotIn("option_chain", state_json["market"], "Raw option_chain should not bloat frontend state")
        
        # 3. POST /api/set_wallet
        r_wallet = requests.post(f"{base_url}/api/set_wallet", json={"amount": 60000.0}, timeout=3)
        self.assertEqual(r_wallet.status_code, 200)
        self.assertEqual(r_wallet.json().get("new_wallet"), 60000.0)
        
        # 4. POST /api/kill_all
        r_kill = requests.post(f"{base_url}/api/kill_all", json={}, timeout=3)
        self.assertEqual(r_kill.status_code, 200)
        
        # 5. POST /api/toggle_all_bots
        r_toggle_all = requests.post(f"{base_url}/api/toggle_all_bots", json={"enabled": False}, timeout=3)
        self.assertEqual(r_toggle_all.status_code, 200)
        self.assertFalse(r_toggle_all.json().get("all_enabled"))
        r_toggle_back = requests.post(f"{base_url}/api/toggle_all_bots", json={"enabled": True}, timeout=3)
        self.assertEqual(r_toggle_back.status_code, 200)
        self.assertTrue(r_toggle_back.json().get("all_enabled"))

        # 6. POST /api/start_replay and /api/stop_replay
        r_start_replay = requests.post(
            f"{base_url}/api/start_replay",
            json={"from_date": "2026-11-20", "to_date": "2026-11-23"},
            timeout=10,
        )
        self.assertEqual(r_start_replay.status_code, 200)
        self.assertEqual(r_start_replay.json().get("status"), "ok")
        self.assertGreaterEqual(r_start_replay.json().get("candles_count", 0), 1)

        r_stop_replay = requests.post(f"{base_url}/api/stop_replay", json={}, timeout=3)
        self.assertEqual(r_stop_replay.status_code, 200)
        self.assertEqual(r_stop_replay.json().get("mode"), "LIVE")

        # 7. POST /api/reset_bots and Pre-Open Guard (use neutral simulation features so live market breakout doesn't auto-enter mid-assert)
        orig_features = dict(self.feed.features)
        orig_open = self.feed.is_market_open
        orig_sim = self.feed.simulation_mode
        self.feed.simulation_mode = True
        self.feed.is_market_open = True
        self.feed.features = {
            "price": 25800.0, "vix": 17.0, "st5m_dir": 1, "trend_5m": 1,
            "eff_5m": 0.1, "esep_5m": 0.1, "e9": 25800.0, "e21": 25800.0,
            "don10_hi": 25850.0, "don10_lo": 25750.0, "rsi14": 50.0, "roc3": 0.0,
            "today_bars_count": 20,
        }
        try:
            self.hub.bots["winner_51"].status = "DAILY_LOSS_LIMIT"
            self.hub.bots["winner_51"].day_pnl = -5000.0
            self.hub.bots["winner_51"].trade_count = 3
            r_reset = requests.post(f"{base_url}/api/reset_bots", json={}, timeout=3)
            self.assertEqual(r_reset.status_code, 200)
            self.assertEqual(r_reset.json().get("reset_count"), 6)
            self.assertEqual(self.hub.bots["winner_51"].day_pnl, 0.0)
            self.assertEqual(self.hub.bots["winner_51"].trade_count, 0)
            self.assertEqual(self.hub.bots["winner_51"].status, "SCANNING")
        finally:
            self.feed.features = orig_features
            self.feed.is_market_open = orig_open
            self.feed.simulation_mode = orig_sim

        # Verify Pre-Open market closed guard blocks entries
        self.hub.reset_bots()
        preopen_snap = {
            "spot": 25507.7,
            "vix": 17.7,
            "status": "PRE_OPEN_START",
            "is_open": False,
            "simulation": False,
            "features": {
                "price": 25507.7,
                "st5m_dir": -1,
                "eff_5m": 0.4,
                "esep_5m": 0.8,
                "e9": 25366.0,
                "e21": 25368.0,
                "don10_hi": 25366.0,
                "don10_lo": 25366.0,
                "rsi14": 50.0,
                "roc3": -0.001,
                "today_bars_count": 0,
            },
        }
        self.hub.update_market_cycle(preopen_snap)
        for bot in self.hub.bots.values():
            self.assertIsNone(bot.current_trade, "No trade should open during PRE_OPEN_START")
            self.assertEqual(bot.trade_count, 0)
            self.assertEqual(bot.status, "WAIT_OPEN")

        # 8. POST /api/toggle_simulation
        r_sim = requests.post(f"{base_url}/api/toggle_simulation", json={}, timeout=3)
        self.assertEqual(r_sim.status_code, 200)
        
        print("✓ All HTTP REST API Endpoints, Pre-Open Guard & Reset Verified.")

    def test_09_ws_tick_and_candle_sync(self):
        """Verify WebSocket V3 message ingestion updates Spot, VIX, I1 candle, and Option LTP immediately."""
        feed = self.feed
        feed.simulation_mode = False
        feed.is_market_open = True
        feed.key_to_option["NSE_FO|TEST25750CE"] = (25750, "CE")

        ts_ms = str(int(time.time() * 1000))
        bar_ms = str(int(time.time() // 60 * 60 * 1000))
        sample_ws_payload = {
            "feeds": {
                "NSE_INDEX|Nifty 50": {
                    "fullFeed": {
                        "indexFF": {
                            "ltpc": {"ltp": 25788.4, "ltt": ts_ms},
                            "marketOHLC": {
                                "ohlc": [
                                    {"interval": "I1", "open": 25780.0, "high": 25790.0, "low": 25775.0, "close": 25788.4, "ts": bar_ms}
                                ]
                            },
                        }
                    }
                },
                "NSE_INDEX|India VIX": {
                    "fullFeed": {"indexFF": {"ltpc": {"ltp": 17.05, "ltt": ts_ms}}}
                },
                "NSE_FO|TEST25750CE": {
                    "fullFeed": {
                        "marketFF": {
                            "ltpc": {"ltp": 261.35, "ltt": ts_ms},
                            "marketLevel": {"bidAskQuote": [{"bidP": 261.0, "askP": 261.5}]},
                            "optionGreeks": {"delta": 0.56},
                        }
                    }
                },
            }
        }
        feed._handle_ws_message(sample_ws_payload)
        self.assertEqual(feed.spot, 25788.4)
        self.assertEqual(feed.vix, 17.05)
        self.assertEqual(feed.feed_source, "WS_V3_LIVE")
        opt = feed.option_chain.get((25750, "CE"))
        self.assertIsNotNone(opt)
        self.assertEqual(opt["ltp"], 261.35)
        print("✓ Real-Time WebSocket V3 Tick, Candle & Option Chain Ingestion Verified.")

    def test_10_bot_script_lifecycle_and_market_data_gating(self):
        """Verify individual ON/OFF starts/terminates bot scripts and pauses Upstox access when OFF."""
        gated_feed = MarketFeed(enable_ws=False, auto_connect=False)
        gated_hub = BotEngineHub(initial_wallet=DEFAULT_INITIAL_WALLET)
        gated_hub.set_feed_listener(gated_feed.configure_active_consumers)
        try:
            # 1. All ON bots have live individual worker scripts running
            for bot in gated_hub.bots.values():
                self.assertTrue(bot.is_script_alive(), f"{bot.bot_id} script should be running when ON")

            # 2. Turning OFF one bot terminates its worker immediately and stops feeding it signals
            gated_hub.toggle_bot("bot_51_1", False)
            off_bot = gated_hub.bots["bot_51_1"]
            self.assertFalse(off_bot.enabled)
            self.assertFalse(off_bot.is_script_alive(), "OFF bot script thread must terminate")
            self.assertEqual(off_bot.status, "STOPPED")
            self.assertIn("TERMINATED", off_bot.radar.get("status", ""))

            # Even if strong breakout signal arrives, OFF bot receives ZERO evaluation & opens ZERO trades
            strong_snap = {
                "spot": 25820.0,
                "vix": 16.5,
                "is_open": True,
                "simulation": True,
                "features": {
                    "price": 25820.0, "st5m_dir": 1, "eff_5m": 0.6, "esep_5m": 1.2,
                    "e9": 25800.0, "e21": 25770.0, "don10_hi": 25810.0, "don10_lo": 25750.0,
                    "rsi14": 68.0, "roc3": 0.002, "today_bars_count": 20,
                },
            }
            gated_hub.update_market_cycle(strong_snap)
            self.assertIsNone(off_bot.current_trade)
            self.assertEqual(off_bot.trade_count, 0)
            self.assertEqual(off_bot.status, "STOPPED")

            # 3. Turning OFF an IN_TRADE bot cleanly closes position & terminates script
            ch_bot = gated_hub.bots["chandler_runer"]
            if not ch_bot.current_trade:
                ch_bot.current_trade = {
                    "trade_id": "T_TERM", "strike": "NIFTY 25800 CE", "strike_val": 25800,
                    "kind": "CE", "lots": 2, "qty": 130, "entry_price": 200.0,
                    "current_price": 210.0, "peak_price": 210.0, "target_price": 300.0,
                    "stop_loss": 180.0, "trailing_sl": 180.0, "current_pnl": 1300.0,
                    "current_pnl_pct": 5.0, "entered_at": "10:00:00", "exit_price": None, "exit_reason": None,
                }
            gated_hub.toggle_bot("chandler_runer", False)
            self.assertIsNone(ch_bot.current_trade)
            self.assertFalse(ch_bot.is_script_alive())
            self.assertEqual(ch_bot.status, "STOPPED")

            # 4. Turning OFF all bots spends ZERO Upstox access-token calls
            gated_hub.toggle_all_bots(False)
            self.assertEqual(gated_feed._active_consumers_count, 0)
            self.assertFalse(gated_feed.live_data_enabled)
            self.assertEqual(gated_feed.feed_source, "PAUSED (0 BOTS ON)")

            http_calls = []
            orig_get = gated_feed.http.get
            def spy_get(*args, **kwargs):
                http_calls.append(args[0] if args else "")
                return orig_get(*args, **kwargs)
            gated_feed.http.get = spy_get

            gated_feed.poll_market_quotes(force_status=True)
            gated_feed.poll_option_chain()
            gated_feed._sync_intraday_candles()
            self.assertEqual(len(http_calls), 0, "Zero Upstox HTTP calls allowed when all bots are OFF")
            self.assertEqual(gated_feed._get_subscription_keys(25800.0), [])

            # 5. Turning ONE bot ON starts only its script and re-enables feed for it
            gated_hub.toggle_bot("winner_51", True)
            self.assertTrue(gated_hub.bots["winner_51"].is_script_alive())
            self.assertFalse(gated_hub.bots["chad_25"].is_script_alive())
            self.assertTrue(gated_feed.live_data_enabled)
            self.assertEqual(gated_feed._active_consumers_count, 1)
            sub_keys = gated_feed._get_subscription_keys(25800.0)
            self.assertIn("NSE_INDEX|Nifty 50", sub_keys)
            print("✓ Individual Bot Script Start/Terminate & Zero-Spend Access Gating Verified.")
        finally:
            gated_feed.stop_polling_loop()
            gated_hub.shutdown()

    def test_11_individual_bot_reset_and_grid_layout(self):
        """Verify /api/reset_bot resets only the target bot cleanly and HTML renders 3x2 grid with slot 6."""
        base_url = f"http://127.0.0.1:{self.port}"
        orig_features = dict(self.feed.features)
        orig_sim = self.feed.simulation_mode
        self.feed.simulation_mode = True
        self.feed.features = {
            "price": 25800.0, "vix": 17.0, "st5m_dir": 1, "trend_5m": 1,
            "eff_5m": 0.1, "esep_5m": 0.1, "e9": 25800.0, "e21": 25800.0,
            "don10_hi": 25850.0, "don10_lo": 25750.0, "rsi14": 50.0, "roc3": 0.0,
            "today_bars_count": 20,
        }
        try:
            self.hub.reset_bots()

            # Give two bots non-zero state and disable target bot
            w51 = self.hub.bots["winner_51"]
            w51.day_pnl = 950.0
            w51.wallet = 50950.0
            w51.trade_count = 2
            w51.win_count = 1
            w51.trades_history = [{"trade_id": "T_OLD", "net_pnl": 950.0}]
            self.hub.toggle_bot("winner_51", False)

            c25 = self.hub.bots["chad_25"]
            c25.day_pnl = 1234.5
            c25.wallet = 51234.5
            c25.trade_count = 1

            # Reset ONLY winner_51 via HTTP endpoint
            r = requests.post(f"{base_url}/api/reset_bot", json={"bot_id": "winner_51"}, timeout=3)
            self.assertEqual(r.status_code, 200)
            self.assertEqual(r.json().get("status"), "ok")
            self.assertTrue(r.json().get("reset"))

            self.assertTrue(w51.enabled)
            self.assertTrue(w51.is_script_alive())
            self.assertEqual(w51.status, "SCANNING")
            self.assertEqual(w51.day_pnl, 0.0)
            self.assertEqual(w51.wallet, w51.initial_wallet)
            self.assertEqual(w51.trade_count, 0)
            self.assertEqual(w51.win_count, 0)
            self.assertEqual(w51.trades_history, [])
            self.assertIsNone(w51.current_trade)

            # Verify other bot (chad_25) stayed completely untouched
            self.assertEqual(c25.day_pnl, 1234.5)
            self.assertEqual(c25.wallet, 51234.5)
            self.assertEqual(c25.trade_count, 1)

            # Verify unknown bot returns error gracefully
            r_bad = requests.post(f"{base_url}/api/reset_bot", json={"bot_id": "missing_bot"}, timeout=3)
            self.assertEqual(r_bad.status_code, 200)
            self.assertEqual(r_bad.json().get("status"), "error")

            # Verify dynamic 3x2 / 4x2 grid CSS, OFF-hiding logic, and individual restart function exist in UI
            html = requests.get(f"{base_url}/", timeout=3).text
            self.assertIn(".grid-bots.layout-3x2", html)
            self.assertIn(".grid-bots.layout-4x2", html)
            self.assertIn("grid-template-columns: repeat(4, minmax(0, 1fr))", html)
            self.assertIn("syncBotsGrid(", html)
            self.assertIn("orderedBots.filter(b => b.enabled)", html)
            self.assertIn("resetBot(", html)
            self.assertNotIn('id="bot-slot-6"', html)
            print("✓ Individual Bot Restart API & Dynamic ON-Only 3x2/4x2 UI Rules Verified.")
        finally:
            self.feed.features = orig_features
            self.feed.simulation_mode = orig_sim

    def test_12_dynamic_bots_and_eod_arena_archive(self):
        """Verify adding >6 bots cleanly switches capacity and 'arena- <date>' saves full day + resets."""
        base_url = f"http://127.0.0.1:{self.port}"
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            hub = BotEngineHub(
                initial_wallet=50000.0,
                state_file=td_path / "session.json",
                archive_root=td_path,
            )
            try:
                # Simulate Cycle 1 trade + individual reset on winner_51
                w51 = hub.bots["winner_51"]
                w51.trade_count = 1
                w51.win_count = 1
                w51.day_pnl = 820.0
                w51.wallet = 50820.0
                w51.trades_history = [{
                    "trade_id": "T1_PRE", "strike": "NIFTY 25750 CE", "expiry_label": "01 DEC 26",
                    "lots": 2, "qty": 130, "entered_at": "09:30:00", "entry_price": 200.0,
                    "exit_time": "09:42:00", "exit_price": 206.77, "gross_pnl": 880.0,
                    "net_pnl": 820.0, "exit_reason": "TRAILING_STOP_HIT",
                }]
                self.assertTrue(hub.reset_bot("winner_51"))
                self.assertEqual(len(w51.reset_history), 1)
                self.assertEqual(w51.day_pnl, 0.0)
                self.assertEqual(w51.trades_history, [])

                # Simulate Cycle 2 trade on winner_51
                w51.trade_count = 1
                w51.win_count = 1
                w51.day_pnl = 450.0
                w51.wallet = 50450.0
                w51.trades_history = [{
                    "trade_id": "T2_POST", "strike": "NIFTY 25800 CE", "expiry_label": "01 DEC 26",
                    "lots": 2, "qty": 130, "entered_at": "10:15:00", "entry_price": 220.0,
                    "exit_time": "10:28:00", "exit_price": 223.92, "gross_pnl": 510.0,
                    "net_pnl": 450.0, "exit_reason": "TARGET_HIT",
                }]

                # Dynamically add 2 custom bots (>6 bots total -> 4x2 layout)
                hub.add_bot("custom_bot_6", "Scalper 6", "Custom Alpha", {"target_fraction": 0.4, "stop_fraction": 0.1}, enabled=True)
                hub.add_bot("custom_bot_7", "Trend 7", "Custom Beta", {"target_fraction": 0.5, "stop_fraction": 0.1}, enabled=True)
                self.assertEqual(len(hub.bots), 8)
                self.assertTrue(hub.bots["custom_bot_6"].is_script_alive())
                self.assertTrue(hub.bots["custom_bot_7"].is_script_alive())

                archive_dir = hub.save_daily_arena_archive("2026-11-25")
                self.assertEqual(archive_dir.name, "arena- 2026-11-25")
                self.assertTrue((archive_dir / "SUMMARY.md").exists())
                self.assertTrue((archive_dir / "arena_full_day.json").exists())
                self.assertTrue((archive_dir / "resets_and_events.csv").exists())
                self.assertTrue((archive_dir / "all_bots_summary.csv").exists())
                self.assertTrue((archive_dir / "bots" / "winner_51.json").exists())
                self.assertTrue((archive_dir / "bots" / "winner_51_trades.csv").exists())

                all_csv = (archive_dir / "all_bots_summary.csv").read_text()
                self.assertIn("T1_PRE", all_csv)
                self.assertIn("RESET #1", all_csv)
                self.assertIn("T2_POST", all_csv)
                self.assertIn("BOT_TOTAL", all_csv)
                self.assertIn("GRAND_TOTAL", all_csv)
                self.assertLess(all_csv.index("T1_PRE"), all_csv.index("RESET #1"))
                self.assertLess(all_csv.index("RESET #1"), all_csv.index("T2_POST"))

                doc = json.loads((archive_dir / "arena_full_day.json").read_text())
                w_doc = doc["bots"]["winner_51"]
                self.assertEqual(w_doc["resets_count"], 1)
                self.assertEqual(w_doc["cumulative_day_pnl"], 1270.0)
                self.assertEqual(w_doc["cumulative_trade_count"], 2)
                csv_text = (archive_dir / "bots" / "winner_51_trades.csv").read_text()
                self.assertIn("T1_PRE", csv_text)
                self.assertIn("T2_POST", csv_text)
                summary_text = (archive_dir / "SUMMARY.md").read_text()
                self.assertIn("Reset #1", summary_text)
                self.assertIn("custom_bot_7", summary_text)
            finally:
                hub.shutdown()

        # Verify HTTP /api/save_arena_archive works too (writes into cls.tmp_dir)
        r_save = requests.post(f"{base_url}/api/save_arena_archive", json={"date": "2026-11-25"}, timeout=3)
        self.assertEqual(r_save.status_code, 200)
        self.assertEqual(r_save.json().get("status"), "ok")
        self.assertIn("arena- 2026-11-25", r_save.json().get("folder", ""))
        print("✓ Dynamic Bot Addition (>6) & Full Daily Archive ('arena- 2026-11-25' w/ Resets) Verified.")

    def test_13_timelapse_replay_and_live_preservation(self):
        """Verify fast time-lapse replay finishes without looping, settles trades, and restores live state."""
        hub = BotEngineHub(initial_wallet=50000.0, archive_root=self.tmp_dir)
        feed = MarketFeed(enable_ws=False, auto_connect=False, on_replay_candle=hub.update_market_cycle)
        try:
            # Put live profit + OFF bot before entering replay
            hub.toggle_bot("bot_51_1", False)
            hub.bots["winner_51"].day_pnl = 1550.0
            hub.bots["winner_51"].wallet = 51550.0
            hub.bots["winner_51"].trade_count = 1
            hub.bots["winner_51"].trades_history = [{"trade_id": "LIVE_T1", "net_pnl": 1550.0}]

            # Build synthetic trending 90-bar 1m candle series for fast time-lapse
            bars = []
            for i in range(90):
                hh = 9 + (15 + i) // 60
                mm = (15 + i) % 60
                px = 25600.0 + (i * 2.2)
                bars.append({
                    "timestamp": f"2026-11-23T{hh:02d}:{mm:02d}:00+05:30",
                    "open": px - 1.0, "high": px + 3.0, "low": px - 2.0, "close": px, "volume": 1000,
                })

            with feed._lock:
                feed.sim_candles = bars
                feed.candles_1m = list(bars[:30])
                feed.sim_index = 30
                feed.replay_total = len(bars)
                feed.replay_progress = 33.3
                feed.replay_completed = False
                feed.replay_run_id += 1
                feed.simulation_mode = True
                feed.replay_range = {"from": "2026-11-23", "to": "2026-11-23"}
                feed.is_market_open = True
                feed.feed_source = "REPLAY_TIMELAPSE"
                feed.spot = bars[29]["close"]
                feed.sim_timestamp = bars[29]["timestamp"]

            hub.enter_replay_mode()
            self.assertFalse(hub.bots["bot_51_1"].enabled, "OFF bot must remain OFF in replay")
            self.assertTrue(hub.bots["winner_51"].enabled)
            self.assertEqual(hub.bots["winner_51"].day_pnl, 0.0)

            feed.start_polling_loop()
            t_wait = time.monotonic()
            while not feed.replay_completed and (time.monotonic() - t_wait) < 8.0:
                time.sleep(0.05)

            self.assertTrue(feed.replay_completed, "Time-lapse replay must complete quickly and cleanly")
            self.assertEqual(feed.replay_progress, 100.0)
            self.assertEqual(feed.sim_index, len(bars), "Replay must stop at end instead of looping back to 0")
            self.assertEqual(feed.feed_source, "REPLAY_COMPLETE")
            self.assertIsNone(hub.bots["winner_51"].current_trade, "Open replay trade must settle at REPLAY_SESSION_END")
            self.assertEqual(hub.bots["bot_51_1"].trade_count, 0)

            snap = hub.get_state(feed.get_snapshot())
            self.assertTrue(snap["market"]["replay_completed"])

            # Exit replay -> verify exact live bot state restored untouched
            feed.stop_replay()
            hub.exit_replay_mode()
            self.assertFalse(hub.in_replay_mode)
            self.assertFalse(hub.bots["bot_51_1"].enabled)
            self.assertEqual(hub.bots["winner_51"].day_pnl, 1550.0)
            self.assertEqual(hub.bots["winner_51"].trades_history[0]["trade_id"], "LIVE_T1")
            print("✓ Fast Time-Lapse Replay Completion & Live State Preservation Verified.")
        finally:
            feed.stop_polling_loop()
            hub.shutdown()

    def test_14_ws_disconnect_recovery_and_sse_keepalive(self):
        """Verify dead WS detection triggers fast rebuild flags and SSE idle wait avoids duplicate payloads."""
        feed = MarketFeed(enable_ws=False, auto_connect=False)
        hub = BotEngineHub(initial_wallet=50000.0, archive_root=self.tmp_dir)
        broadcaster = StateBroadcaster(feed, hub)
        try:
            ver, msg = broadcaster.wait_next_sse(broadcaster._version, timeout=0.05)
            self.assertEqual(ver, broadcaster._version)
            self.assertIsNone(msg, "Unchanged version timeout must emit keepalive None instead of duplicate payload")

            broadcaster.trigger()
            broadcaster.publish_now()
            ver2, msg2 = broadcaster.wait_next_sse(ver - 1, timeout=0.05)
            self.assertGreater(ver2, ver - 1)
            self.assertIsNotNone(msg2)

            feed._on_ws_open()
            self.assertFalse(feed._ws_closed)
            self.assertFalse(feed._ws_needs_rebuild)
            feed._on_ws_close(1006)
            self.assertTrue(feed._ws_closed)
            feed._on_ws_auto_reconnect_stopped("exhausted")
            self.assertTrue(feed._ws_needs_rebuild)

            html = (Path(__file__).resolve().parent / "templates" / "index.html").read_text()
            self.assertIn("scheduleUIUpdate", html)
            self.assertIn("_lastChecklistSig", html)
            self.assertIn("champion_of_50", html)

            c50 = hub.bots["champion_of_50"]
            tier_h, cfg_h = c50._resolve_cfg(17.2)
            self.assertEqual(tier_h, "high")
            self.assertEqual(cfg_h["trail_fraction"], 0.048)
            self.assertEqual(cfg_h["daily_profit_lock"], 5500.0)
            print("✓ Zero-Stall WS Recovery Flags, SSE Keepalive, rAF/DOM Diffing & Champion of 50 Verified.")
        finally:
            broadcaster.stop()
            hub.shutdown()

    def test_15_dropin_bots_directory_loader(self):
        """Verify JSON bots dropped into bots/ auto-load while templates/invalid files are ignored."""
        bots_dir = self.tmp_dir / "bots"
        bots_dir.mkdir(parents=True, exist_ok=True)
        (bots_dir / "_ignored_template.json").write_text('{"bot_id":"ignored_1","cfg":{}}', encoding="utf-8")
        (bots_dir / "sample.example.json").write_text('{"bot_id":"ignored_2","cfg":{}}', encoding="utf-8")
        (bots_dir / "broken.json").write_text("{not valid json", encoding="utf-8")
        (bots_dir / "alpha_scalper.json").write_text(
            json.dumps({
                "bot_id": "alpha_scalper",
                "name": "Alpha Scalper",
                "family": "Custom Drop-In",
                "enabled": True,
                "cfg": {
                    "capital_floor": 14000.0,
                    "daily_profit_lock": 1800.0,
                    "daily_loss_limit": 2500.0,
                    "max_trades_day": 3,
                    "max_lots": 2,
                    "target_delta": 0.55,
                    "stop_fraction": 0.08,
                    "target_fraction": 0.25,
                },
            }),
            encoding="utf-8",
        )
        (bots_dir / "beta_pack.json").write_text(
            json.dumps([
                {
                    "bot_id": "beta_paused",
                    "name": "Beta Paused",
                    "family": "Paused Drop-In",
                    "enabled": False,
                    "cfg": {
                        "capital_floor": 15000.0,
                        "daily_profit_lock": 2000.0,
                        "daily_loss_limit": 2000.0,
                        "max_trades_day": 2,
                        "max_lots": 1,
                        "target_delta": 0.50,
                        "stop_fraction": 0.09,
                        "target_fraction": 0.30,
                    },
                }
            ]),
            encoding="utf-8",
        )

        hub = BotEngineHub(initial_wallet=50000.0, archive_root=self.tmp_dir, bots_dir=bots_dir)
        try:
            self.assertNotIn("ignored_1", hub.bots)
            self.assertNotIn("ignored_2", hub.bots)
            self.assertIn("alpha_scalper", hub.bots)
            self.assertIn("beta_paused", hub.bots)
            self.assertTrue(hub.bots["alpha_scalper"].enabled)
            self.assertEqual(hub.bots["alpha_scalper"].status, "SCANNING")
            self.assertFalse(hub.bots["beta_paused"].enabled)
            self.assertEqual(hub.bots["beta_paused"].status, "STOPPED")
            snap = hub.get_state({})
            self.assertEqual(len(snap["bots"]), 8)
            print("✓ Drop-in bots/ JSON Auto-Loader & Layout Scaling Verified.")
        finally:
            hub.shutdown()


if __name__ == "__main__":
    unittest.main()
