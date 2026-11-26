#!/usr/bin/env python3
"""Centralized Zero-Lag Market Data Feed Manager for ALL Trade UI.

Combines:
1. Upstox WebSocket V3 (MarketDataStreamerV3) for sub-50ms real-time exchange ticks
   across Nifty 50 Spot, India VIX, live 1m I1 OHLC candles, and ATM option chains.
2. Pooled requests.Session REST fallback & intraday candle reconciliation.
3. Custom Historical Date Range Replay via Upstox 1m Historical API.
"""
from __future__ import annotations

from datetime import datetime, date, timedelta
import json
import logging
from pathlib import Path
import threading
import time
from typing import Callable, Dict, List, Optional, Set, Tuple
import requests
import upstox_client

from .config import (
    UPSTOX_ACCESS_TOKEN,
    UPSTOX_HEADERS,
    NIFTY_INSTRUMENT_KEY,
    VIX_INSTRUMENT_KEY,
    IST,
)
from .indicators import compute_market_features, bs_option_metrics

logger = logging.getLogger("market_feed")


class MarketFeed:
    """Centralized live data feed manager with WebSocket V3 streaming."""

    def __init__(
        self,
        enable_ws: bool = True,
        on_update: Optional[Callable[[], None]] = None,
        auto_connect: bool = True,
        on_replay_candle: Optional[Callable[[dict], None]] = None,
    ):
        self.spot: float = 25349.65
        self.vix: float = 17.70
        self.market_status: str = "OFFLINE"
        self.last_updated: str = ""
        self.is_market_open: bool = False
        self.feed_source: str = "REST"
        self.enable_ws: bool = enable_ws
        self.on_update: Optional[Callable[[], None]] = on_update
        self.on_replay_candle: Optional[Callable[[dict], None]] = on_replay_candle

        # Persistent pooled HTTP session (eliminates TLS handshake latency)
        self.http = requests.Session()
        self.http.headers.update(UPSTOX_HEADERS)

        # Candles & Indicators
        self.warmup_candles: List[dict] = []
        self.today_candles: List[dict] = []
        self.candles_1m: List[dict] = []
        self.features: dict = {}
        self._last_feature_spot: Optional[float] = None
        self._last_feature_min: Optional[str] = None
        self._last_candles_len: int = -1
        self._sync_in_flight: bool = False

        # Thread control (re-entrant lock to avoid self-deadlock on refresh)
        self.running: bool = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()

        # Active ON-bot consumer gating (spend Upstox access code ONLY for ON bots)
        self._consumers_configured: bool = False
        self._active_consumers_count: int = 1 if auto_connect else 0
        self._enabled_bot_ids: Set[str] = set()
        self._need_scanning_quotes: bool = True
        self._required_open_options: Set[Tuple[int, str]] = set()
        self.live_data_enabled: bool = bool(auto_connect)
        self._bootstrapped: bool = False
        self._last_saved_market_status: str = "OFFLINE"

        # Simulation / Time-Lapse Date-range Replay state
        self.simulation_mode: bool = False
        self.sim_index: int = 0
        self.sim_candles: List[dict] = []
        self.replay_range: Optional[Dict[str, str]] = None
        self.sim_timestamp: Optional[str] = None
        self.replay_total: int = 0
        self.replay_progress: float = 0.0
        self.replay_completed: bool = False
        self.replay_run_id: int = 0

        # Active Expiry & Real Upstox Option Chain Cache
        self.active_expiry: str = self._next_tuesday_iso(datetime.now(IST).date())
        self.expiry_label: str = self._format_expiry_label(self.active_expiry)
        self.expiry_dte: int = max(1, (date.fromisoformat(self.active_expiry) - datetime.now(IST).date()).days)
        self._contracts: List[dict] = []
        self.key_to_option: Dict[str, Tuple[int, str]] = {}
        self.option_chain: Dict[Tuple[int, str], dict] = {}
        self._last_expiry_refresh: Optional[str] = None
        self._last_status_check_mono: float = 0.0

        # WebSocket V3 state
        self._ws_streamer = None
        self._subscribed_keys: Set[str] = set()
        self._ws_center_spot: float = 0.0
        self._last_ws_msg_mono: float = 0.0
        self._ws_connecting: bool = False
        self._ws_closed: bool = False
        self._ws_needs_rebuild: bool = False

        if auto_connect:
            self._ensure_live_bootstrap()
        else:
            self.market_status = "IDLE (0 BOTS ON)"
            self.feed_source = "PAUSED (0 BOTS ON)"

    def _is_live_access_allowed(self) -> bool:
        """Return True only when not replaying and at least one bot is powered ON."""
        if self.simulation_mode:
            return False
        if self._consumers_configured and self._active_consumers_count <= 0:
            return False
        return self.live_data_enabled

    def _ensure_live_bootstrap(self):
        """Bootstrap expiry, historical 1m candles, and initial option chain once needed."""
        if self._bootstrapped:
            return
        self.refresh_active_expiry()
        self.init_historical_data()
        self.poll_market_quotes(force_status=True)
        self.poll_option_chain()
        self._bootstrapped = True

    def configure_active_consumers(self, reqs: dict):
        """Gate Upstox WS/REST data usage strictly by currently ON bots."""
        active_count = int(reqs.get("active_count", 0))
        enabled_bots = set(reqs.get("enabled_bots") or [])
        need_scanning = bool(reqs.get("need_scanning_quotes", False))
        open_opts = {(int(s), str(k)) for (s, k) in (reqs.get("open_option_keys") or [])}

        need_bootstrap = False
        need_quote_refresh = False
        need_ws_start = False
        need_ws_stop = False
        keys_to_sub: List[str] = []
        keys_to_unsub: List[str] = []

        with self._lock:
            self._consumers_configured = True
            self._active_consumers_count = active_count
            self._enabled_bot_ids = enabled_bots
            self._need_scanning_quotes = need_scanning
            self._required_open_options = open_opts

            if active_count <= 0:
                self.live_data_enabled = False
                if not self.simulation_mode:
                    if self.market_status not in ("IDLE (0 BOTS ON)", "OFFLINE"):
                        self._last_saved_market_status = self.market_status
                    self.market_status = "IDLE (0 BOTS ON)"
                    self.feed_source = "PAUSED (0 BOTS ON)"
                need_ws_stop = self._ws_streamer is not None
            else:
                was_paused = not self.live_data_enabled
                self.live_data_enabled = True
                if not self.simulation_mode:
                    if self.market_status == "IDLE (0 BOTS ON)" and self._last_saved_market_status != "OFFLINE":
                        self.market_status = self._last_saved_market_status
                    need_bootstrap = not self._bootstrapped
                    need_quote_refresh = was_paused and self._bootstrapped
                    if self.enable_ws and self.running and self._ws_streamer is None:
                        need_ws_start = True
                    elif self._ws_streamer is not None:
                        desired = set(self._get_subscription_keys(self.spot or 25750.0))
                        keys_to_sub = list(desired - self._subscribed_keys)
                        keys_to_unsub = list(self._subscribed_keys - desired)

        if need_ws_stop:
            self._stop_websocket()
            logger.info("Paused Upstox feed & disconnected WS V3 (0 bots ON)")
        elif active_count > 0 and not self.simulation_mode:
            if need_bootstrap:
                self._ensure_live_bootstrap()
            elif need_quote_refresh:
                self.refresh_active_expiry()
                self._sync_intraday_candles_async()
                self.poll_market_quotes(force_status=True)
            if need_ws_start:
                self._start_websocket()
            elif self._ws_streamer is not None:
                if keys_to_sub:
                    try:
                        self._ws_streamer.subscribe(keys_to_sub, "full")
                        self._subscribed_keys.update(keys_to_sub)
                    except Exception:
                        self._ws_needs_rebuild = True
                if keys_to_unsub:
                    try:
                        self._ws_streamer.unsubscribe(keys_to_unsub)
                        self._subscribed_keys.difference_update(keys_to_unsub)
                    except Exception:
                        self._ws_needs_rebuild = True
        self._notify_update()

    def _sync_intraday_candles_async(self):
        """Run 1m candle reconciliation off-thread so live ticks and SSE never block."""
        if self._sync_in_flight or not self._is_live_access_allowed():
            return
        self._sync_in_flight = True

        def _worker():
            try:
                self._sync_intraday_candles()
            finally:
                self._sync_in_flight = False

        threading.Thread(target=_worker, daemon=True).start()

    def _sync_intraday_candles(self):
        """Reconcile today's 1-minute candles from Upstox intraday endpoint."""
        if not self._is_live_access_allowed():
            return
        try:
            r = self.http.get(
                f"https://api.upstox.com/v2/historical-candle/intraday/{NIFTY_INSTRUMENT_KEY}/1minute",
                timeout=2,
            )
            if r.status_code != 200:
                return
            raw_candles = r.json().get("data", {}).get("candles", [])
            if not raw_candles:
                return
            parsed = [
                {
                    "timestamp": c[0],
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4]),
                    "volume": int(c[5]) if len(c) > 5 else 0,
                }
                for c in reversed(raw_candles)
            ]
            today_str = datetime.now(IST).strftime("%Y-%m-%d")
            with self._lock:
                last_official_ts = str(parsed[-1]["timestamp"])[:16]
                forming = [
                    b for b in self.today_candles
                    if str(b.get("timestamp", ""))[:16] > last_official_ts
                ]
                self.today_candles = parsed + forming
                self.candles_1m = self.warmup_candles[-200:] + self.today_candles
                self.features = compute_market_features(
                    self.candles_1m, self.spot, self.vix, today_date=today_str
                )
        except Exception:
            pass

    @staticmethod
    def _next_tuesday_iso(ref_date: date) -> str:
        """Compute upcoming NSE Tuesday weekly expiry on or after ref_date."""
        days_ahead = (1 - ref_date.weekday()) % 7
        return (ref_date + timedelta(days=days_ahead)).isoformat()

    @staticmethod
    def _format_expiry_label(iso_date: str) -> str:
        """Convert YYYY-MM-DD expiry into concise trading label like '01 DEC 26'."""
        try:
            d = date.fromisoformat(iso_date[:10])
            return d.strftime("%d %b %y").upper()
        except Exception:
            return iso_date

    def _notify_update(self):
        cb = self.on_update
        if cb is not None:
            try:
                cb()
            except Exception:
                pass

    def refresh_active_expiry(self):
        """Fetch real active Nifty 50 front-week expiry & contracts from Upstox."""
        today_dt = datetime.now(IST).date()
        today_s = today_dt.isoformat()
        if self._last_expiry_refresh == today_s and self.active_expiry and self._contracts:
            return
        try:
            r = self.http.get(
                "https://api.upstox.com/v2/option/contract",
                params={"instrument_key": NIFTY_INSTRUMENT_KEY},
                timeout=6,
            )
            if r.status_code == 200:
                contracts = r.json().get("data", [])
                expiries = sorted({c["expiry"] for c in contracts if c.get("expiry", "") >= today_s})
                if expiries:
                    active_exp = expiries[0]
                    k2o: Dict[str, Tuple[int, str]] = {}
                    for c in contracts:
                        if c.get("expiry") == active_exp and c.get("instrument_key"):
                            k2o[c["instrument_key"]] = (int(round(float(c.get("strike_price", 0)))), c.get("instrument_type", "CE"))
                    with self._lock:
                        self._contracts = contracts
                        self.active_expiry = active_exp
                        self.expiry_label = self._format_expiry_label(active_exp)
                        self.expiry_dte = max(1, (date.fromisoformat(active_exp) - today_dt).days)
                        self.key_to_option = k2o
                        self._last_expiry_refresh = today_s
                    logger.info("Active Upstox Nifty Expiry resolved: %s (%s, %dd, %d contracts)", self.active_expiry, self.expiry_label, self.expiry_dte, len(k2o))
                    return
        except Exception as e:
            logger.warning("Failed to fetch Upstox expiry contracts: %s", e)

    def poll_option_chain(self):
        """Fetch real Bid/Ask/LTP/Greeks from Upstox option chain for active_expiry."""
        if not self._is_live_access_allowed() or not self.active_expiry:
            return
        try:
            r = self.http.get(
                "https://api.upstox.com/v2/option/chain",
                params={"instrument_key": NIFTY_INSTRUMENT_KEY, "expiry_date": self.active_expiry},
                timeout=4,
            )
            if r.status_code == 200:
                rows = r.json().get("data", [])
                chain_map: Dict[Tuple[int, str], dict] = {}
                for row in rows:
                    strike = int(round(float(row.get("strike_price", 0))))
                    for side, kind in [("call_options", "CE"), ("put_options", "PE")]:
                        opt = row.get(side) or {}
                        md = opt.get("market_data") or {}
                        gk = opt.get("option_greeks") or {}
                        ltp = float(md.get("ltp") or 0.0)
                        bid = float(md.get("bid_price") or ltp)
                        ask = float(md.get("ask_price") or ltp)
                        if ltp > 0 or bid > 0:
                            chain_map[(strike, kind)] = {
                                "key": opt.get("instrument_key"),
                                "strike": strike,
                                "kind": kind,
                                "expiry": self.active_expiry,
                                "expiry_label": self.expiry_label,
                                "ltp": ltp if ltp > 0 else bid,
                                "bid": bid if bid > 0 else ltp,
                                "ask": ask if ask > 0 else ltp,
                                "delta": round(float(gk.get("delta") or 0.0), 3),
                                "iv": round(float(gk.get("iv") or 0.0), 2),
                                "oi": int(md.get("oi") or 0),
                                "volume": int(md.get("volume") or 0),
                            }
                if chain_map:
                    with self._lock:
                        self.option_chain.update(chain_map)
        except Exception as e:
            logger.warning("Option chain poll retry: %s", e)

    def init_historical_data(self):
        """Pre-load candles from Upstox API for immediate readiness."""
        today_str = datetime.now(IST).strftime("%Y-%m-%d")
        try:
            url = f"https://api.upstox.com/v2/historical-candle/{NIFTY_INSTRUMENT_KEY}/1minute/{today_str}"
            r = self.http.get(url, timeout=6)
            parsed: List[dict] = []
            if r.status_code == 200:
                raw_candles = r.json().get("data", {}).get("candles", [])
                for c in reversed(raw_candles):
                    parsed.append({
                        "timestamp": c[0],
                        "open": float(c[1]),
                        "high": float(c[2]),
                        "low": float(c[3]),
                        "close": float(c[4]),
                        "volume": int(c[5]) if len(c) > 5 else 0,
                    })

            # Also pull today's intraday candles immediately if market is already open
            try:
                intra_r = self.http.get(f"https://api.upstox.com/v2/historical-candle/intraday/{NIFTY_INSTRUMENT_KEY}/1minute", timeout=4)
                if intra_r.status_code == 200:
                    raw_intra = intra_r.json().get("data", {}).get("candles", [])
                    today_parsed = []
                    for c in reversed(raw_intra):
                        today_parsed.append({
                            "timestamp": c[0],
                            "open": float(c[1]),
                            "high": float(c[2]),
                            "low": float(c[3]),
                            "close": float(c[4]),
                            "volume": int(c[5]) if len(c) > 5 else 0,
                        })
                    if today_parsed:
                        parsed = [b for b in parsed if not str(b["timestamp"]).startswith(today_str)] + today_parsed
            except Exception:
                pass

            if parsed:
                with self._lock:
                    self.warmup_candles = [b for b in parsed[-750:] if not str(b["timestamp"]).startswith(today_str)]
                    self.today_candles = [b for b in parsed if str(b["timestamp"]).startswith(today_str)]
                    self.candles_1m = self.warmup_candles[-200:] + self.today_candles
                    self.sim_candles = list(parsed[-750:])
                    if self.candles_1m:
                        self.spot = self.candles_1m[-1]["close"]
                        self.features = compute_market_features(
                            self.candles_1m, self.spot, self.vix, today_date=today_str
                        )
                logger.info("Successfully loaded %d candles from Upstox (%d today)", len(parsed), len(self.today_candles))
                return
        except Exception as e:
            logger.warning("Historical candle fetch fallback: %s", e)

        if not self.candles_1m:
            base_p = 25350.0
            synth = []
            start_dt = datetime.now(IST).replace(hour=9, minute=15, second=0, microsecond=0)
            for i in range(120):
                t = start_dt + timedelta(minutes=i)
                p = base_p + (i * 0.4)
                synth.append({
                    "timestamp": t.isoformat(),
                    "open": p - 2.0,
                    "high": p + 5.0,
                    "low": p - 3.0,
                    "close": p,
                    "volume": 25000,
                })
            with self._lock:
                self.warmup_candles = synth
                self.today_candles = []
                self.candles_1m = list(synth)
                self.sim_candles = list(synth)
                self.spot = synth[-1]["close"]
                self.features = compute_market_features(
                    self.candles_1m, self.spot, self.vix, today_date=today_str
                )

    def _upsert_today_bar(self, bar_ts: str, o: float, h: float, l: float, c: float, vol: int = 1):
        """Insert or update a 1-minute candle in today_candles matching bar_ts[:16]."""
        today_str = bar_ts[:10]
        if self.today_candles and not str(self.today_candles[-1]["timestamp"]).startswith(today_str):
            self.today_candles = []

        minute_prefix = bar_ts[:16]
        # Fast path: check tail of today_candles
        if self.today_candles and str(self.today_candles[-1]["timestamp"])[:16] == minute_prefix:
            cur = self.today_candles[-1]
            cur["open"] = o
            cur["high"] = max(cur["high"], h)
            cur["low"] = min(cur["low"], l)
            cur["close"] = c
            cur["volume"] = max(cur.get("volume", 0), vol)
            return

        for bar in reversed(self.today_candles[-5:]):
            if str(bar["timestamp"])[:16] == minute_prefix:
                bar["open"] = o
                bar["high"] = max(bar["high"], h)
                bar["low"] = min(bar["low"], l)
                bar["close"] = c
                bar["volume"] = max(bar.get("volume", 0), vol)
                return

        if not self.today_candles or minute_prefix > str(self.today_candles[-1]["timestamp"])[:16]:
            self.today_candles.append({
                "timestamp": bar_ts,
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": vol,
            })

    def _update_live_forming_bar(self, spot_price: float, tick_dt: datetime):
        """Maintain today's forming 1-minute candle from live LTP ticks during market hours."""
        today_str = tick_dt.strftime("%Y-%m-%d")
        minute_ts = tick_dt.replace(second=0, microsecond=0).isoformat()
        if self.today_candles and not str(self.today_candles[-1]["timestamp"]).startswith(today_str):
            self.today_candles = []

        if not self.today_candles or str(self.today_candles[-1]["timestamp"])[:16] != minute_ts[:16]:
            self.today_candles.append({
                "timestamp": minute_ts,
                "open": spot_price,
                "high": spot_price,
                "low": spot_price,
                "close": spot_price,
                "volume": 1,
            })
        else:
            bar = self.today_candles[-1]
            bar["high"] = max(bar["high"], spot_price)
            bar["low"] = min(bar["low"], spot_price)
            bar["close"] = spot_price
            bar["volume"] = bar.get("volume", 0) + 1

    def _get_subscription_keys(self, center_spot: float) -> List[str]:
        """Collect Nifty 50, India VIX, and option keys needed ONLY by currently ON bots."""
        if self._consumers_configured and self._active_consumers_count <= 0:
            return []
        keys = [NIFTY_INSTRUMENT_KEY, VIX_INSTRUMENT_KEY]
        opt_to_key = {v: k for k, v in self.key_to_option.items()}
        for req_opt in self._required_open_options:
            k = opt_to_key.get(req_opt)
            if k:
                keys.append(k)
        if (not self._consumers_configured) or self._need_scanning_quotes:
            atm = int(round((center_spot or 25750.0) / 50.0) * 50)
            for k, (strike, _) in self.key_to_option.items():
                if abs(strike - atm) <= 150:
                    keys.append(k)
        return list(dict.fromkeys(keys))

    def _handle_ws_message(self, data_dict: dict):
        """Process real-time Upstox WebSocket V3 tick payloads (<50ms latency)."""
        if not isinstance(data_dict, dict):
            return
        if self._consumers_configured and self._active_consumers_count <= 0:
            return

        # 1. Market segment status push
        if data_dict.get("type") == "market_info":
            seg = (data_dict.get("marketInfo") or {}).get("segmentStatus") or {}
            st = seg.get("NSE_FO") or seg.get("NSE_INDEX")
            if st and not self.simulation_mode:
                with self._lock:
                    self._last_saved_market_status = st
                    if self.live_data_enabled:
                        self.market_status = st
                    self.is_market_open = (st == "NORMAL_OPEN")
            return

        feeds = data_dict.get("feeds")
        if not feeds:
            return

        now_mono = time.monotonic()
        self._last_ws_msg_mono = now_mono

        if self.simulation_mode:
            return

        spot_changed = False
        updated_any = False
        now_dt = datetime.now(IST)
        max_ts_ms = 0
        if data_dict.get("currentTs"):
            try:
                max_ts_ms = int(data_dict["currentTs"])
            except Exception:
                max_ts_ms = 0

        with self._lock:
            self.feed_source = "WS_V3_LIVE"

            # Process Nifty 50 Spot + I1 1-minute candle
            nifty_feed = feeds.get(NIFTY_INSTRUMENT_KEY)
            if nifty_feed:
                idx_ff = (nifty_feed.get("fullFeed") or {}).get("indexFF") or {}
                ltpc = idx_ff.get("ltpc") or nifty_feed.get("ltpc") or {}
                ltp_val = ltpc.get("ltp")
                ltt_ms = ltpc.get("ltt")
                nifty_tick_dt = now_dt
                if ltt_ms:
                    try:
                        ltt_i = int(ltt_ms)
                        nifty_tick_dt = datetime.fromtimestamp(ltt_i / 1000.0, IST)
                        max_ts_ms = max(max_ts_ms, ltt_i)
                    except Exception:
                        nifty_tick_dt = now_dt

                # Sync I1 1m exchange candle directly if present
                ohlc_list = (idx_ff.get("marketOHLC") or {}).get("ohlc") or []
                for o_bar in ohlc_list:
                    if o_bar.get("interval") == "I1" and o_bar.get("ts"):
                        try:
                            bar_dt = datetime.fromtimestamp(int(o_bar["ts"]) / 1000.0, IST)
                            bar_ts = bar_dt.replace(second=0, microsecond=0).isoformat()
                            self._upsert_today_bar(
                                bar_ts,
                                float(o_bar["open"]),
                                float(o_bar["high"]),
                                float(o_bar["low"]),
                                float(o_bar["close"]),
                            )
                        except Exception:
                            pass

                if ltp_val is not None and float(ltp_val) > 0:
                    new_spot = float(ltp_val)
                    if new_spot != self.spot:
                        spot_changed = True
                    self.spot = new_spot
                    if self.is_market_open:
                        self._update_live_forming_bar(self.spot, nifty_tick_dt)
                    updated_any = True

            # Process India VIX
            vix_feed = feeds.get(VIX_INSTRUMENT_KEY)
            if vix_feed:
                v_ff = (vix_feed.get("fullFeed") or {}).get("indexFF") or {}
                v_ltpc = v_ff.get("ltpc") or vix_feed.get("ltpc") or {}
                if v_ltpc.get("ltp") is not None and float(v_ltpc["ltp"]) > 0:
                    self.vix = float(v_ltpc["ltp"])
                    updated_any = True

            # Process Option Chain ticks (LTP, best Bid/Ask, Greeks, IV)
            for key, f_obj in feeds.items():
                opt_meta = self.key_to_option.get(key)
                if not opt_meta:
                    continue
                strike, kind = opt_meta
                mff = (f_obj.get("fullFeed") or {}).get("marketFF") or {}
                ltpc = mff.get("ltpc") or f_obj.get("ltpc") or {}
                if ltpc.get("ltt"):
                    try:
                        max_ts_ms = max(max_ts_ms, int(ltpc["ltt"]))
                    except Exception:
                        pass
                depth = (mff.get("marketLevel") or {}).get("bidAskQuote") or []
                greeks = mff.get("optionGreeks") or {}

                ltp = float(ltpc.get("ltp") or 0.0)
                bid = float(depth[0].get("bidP") or ltp) if depth else ltp
                ask = float(depth[0].get("askP") or ltp) if depth else ltp
                delta = round(float(greeks.get("delta") or 0.0), 3)
                raw_iv = float(mff.get("iv") or 0.0)
                iv_pct = round(raw_iv * 100.0 if 0.0 < raw_iv < 5.0 else raw_iv, 2)

                if ltp > 0 or bid > 0:
                    existing = self.option_chain.get((strike, kind), {})
                    self.option_chain[(strike, kind)] = {
                        "key": key,
                        "strike": strike,
                        "kind": kind,
                        "expiry": self.active_expiry,
                        "expiry_label": self.expiry_label,
                        "ltp": ltp if ltp > 0 else bid,
                        "bid": bid if bid > 0 else ltp,
                        "ask": ask if ask > 0 else ltp,
                        "delta": delta if delta != 0.0 else existing.get("delta", 0.0),
                        "iv": iv_pct if iv_pct > 0.0 else existing.get("iv", 0.0),
                        "oi": int(mff.get("oi") or existing.get("oi", 0)),
                        "volume": int(mff.get("vtt") or existing.get("volume", 0)),
                    }
                    updated_any = True

            if updated_any:
                ts_dt = datetime.fromtimestamp(max_ts_ms / 1000.0, IST) if max_ts_ms > 0 else now_dt
                today_str = now_dt.strftime("%Y-%m-%d")
                minute_key = ts_dt.strftime("%Y-%m-%dT%H:%M")
                self.last_updated = ts_dt.strftime("%H:%M:%S.%f")[:-3]
                cur_today_len = len(self.today_candles)
                if cur_today_len != self._last_candles_len:
                    self._last_candles_len = cur_today_len
                    self.candles_1m = self.warmup_candles[-200:] + self.today_candles
                if spot_changed or minute_key != self._last_feature_min or not self.features:
                    self._last_feature_spot = self.spot
                    self._last_feature_min = minute_key
                    if self.candles_1m:
                        self.features = compute_market_features(
                            self.candles_1m, self.spot, self.vix, today_date=today_str
                        )

        if updated_any:
            self._notify_update()

    def _on_ws_open(self, *args):
        self._last_ws_msg_mono = time.monotonic()
        self._ws_closed = False
        self._ws_needs_rebuild = False
        logger.info("WS V3 stream opened")

    def _on_ws_close(self, *args):
        self._ws_closed = True
        logger.info("WS V3 closed (%s)", args[0] if args else "eof")

    def _on_ws_auto_reconnect_stopped(self, *args):
        self._ws_closed = True
        self._ws_needs_rebuild = True
        logger.warning("WS V3 auto-reconnect exhausted (%s); scheduling fresh rebuild", args[0] if args else "")

    def _start_websocket(self):
        """Connect Upstox WebSocket V3 streamer only when ON bots require live data."""
        if not self.enable_ws or not self._is_live_access_allowed() or self._ws_connecting or self._ws_streamer is not None:
            return
        center = self.spot if self.spot > 0 else 25750.0
        keys = self._get_subscription_keys(center)
        if not keys:
            return
        self._ws_connecting = True
        try:
            cfg = upstox_client.Configuration()
            cfg.access_token = UPSTOX_ACCESS_TOKEN
            streamer = upstox_client.MarketDataStreamerV3(
                upstox_client.ApiClient(cfg), keys, mode="full"
            )
            streamer.auto_reconnect(True, interval=1, retry_count=5)
            streamer.on("open", self._on_ws_open)
            streamer.on("message", self._handle_ws_message)
            streamer.on("error", lambda err: logger.warning("WS V3 error: %s", err))
            streamer.on("close", self._on_ws_close)
            streamer.on("autoReconnectStopped", self._on_ws_auto_reconnect_stopped)
            self._ws_streamer = streamer
            self._subscribed_keys = set(keys)
            self._ws_center_spot = center
            self._ws_closed = False
            self._ws_needs_rebuild = False
            self._last_ws_msg_mono = time.monotonic()
            streamer.connect()
            logger.info("Connected Upstox WebSocket V3 stream (%d keys around spot %.1f)", len(keys), center)
        except Exception as e:
            logger.warning("Failed to start WebSocket V3, falling back to pooled REST: %s", e)
            self._ws_streamer = None
            self._ws_closed = True
        finally:
            self._ws_connecting = False

    def _stop_websocket(self):
        s = self._ws_streamer
        self._ws_streamer = None
        self._subscribed_keys = set()
        self._ws_closed = False
        self._ws_needs_rebuild = False
        if s is not None:
            try:
                s.disconnect()
            except Exception:
                pass

    def poll_market_quotes(self, force_status: bool = False):
        """Fast pooled REST quote poll strictly gated by ON bots."""
        if not self._is_live_access_allowed():
            return
        try:
            now_mono = time.monotonic()
            if force_status or (now_mono - self._last_status_check_mono) >= 30.0:
                self._last_status_check_mono = now_mono
                st_res = self.http.get("https://api.upstox.com/v2/market/status/NSE", timeout=2)
                if st_res.status_code == 200:
                    st = st_res.json().get("data", {}).get("status", "UNKNOWN")
                    with self._lock:
                        self._last_saved_market_status = st
                        if not self.simulation_mode and self._is_live_access_allowed():
                            self.market_status = st
                            self.is_market_open = (st == "NORMAL_OPEN")

            # Fetch ONLY Spot + VIX + strikes actively needed by ON bots in ONE single REST call
            keys = self._get_subscription_keys(self.spot or 25750.0)
            if not keys:
                return

            q_res = self.http.get(
                "https://api.upstox.com/v2/market-quote/quotes",
                params={"instrument_key": ",".join(keys)},
                timeout=2,
            )
            if q_res.status_code == 200:
                data = q_res.json().get("data", {})
                by_tok = {q.get("instrument_token"): q for q in data.values() if isinstance(q, dict)}
                nifty_q = by_tok.get(NIFTY_INSTRUMENT_KEY) or data.get("NSE_INDEX:Nifty 50")
                vix_q = by_tok.get(VIX_INSTRUMENT_KEY) or data.get("NSE_INDEX:India VIX")

                with self._lock:
                    if not self.simulation_mode:
                        now_dt = datetime.now(IST)
                        today_str = now_dt.strftime("%Y-%m-%d")
                        if nifty_q and "last_price" in nifty_q:
                            self.spot = float(nifty_q["last_price"])
                        if vix_q and "last_price" in vix_q:
                            self.vix = float(vix_q["last_price"])

                        for tok, q in by_tok.items():
                            opt_meta = self.key_to_option.get(tok)
                            if not opt_meta:
                                continue
                            strike, kind = opt_meta
                            ltp = float(q.get("last_price") or 0.0)
                            depth = q.get("depth") or {}
                            buys, sells = depth.get("buy") or [], depth.get("sell") or []
                            bid = float(buys[0].get("price") or ltp) if buys else ltp
                            ask = float(sells[0].get("price") or ltp) if sells else ltp
                            if ltp > 0 or bid > 0:
                                existing = self.option_chain.get((strike, kind), {})
                                self.option_chain[(strike, kind)] = {
                                    "key": tok,
                                    "strike": strike,
                                    "kind": kind,
                                    "expiry": self.active_expiry,
                                    "expiry_label": self.expiry_label,
                                    "ltp": ltp if ltp > 0 else bid,
                                    "bid": bid if bid > 0 else ltp,
                                    "ask": ask if ask > 0 else ltp,
                                    "delta": existing.get("delta", 0.0),
                                    "iv": existing.get("iv", 0.0),
                                    "oi": int(q.get("oi") or existing.get("oi", 0)),
                                    "volume": int(q.get("volume") or existing.get("volume", 0)),
                                }

                        self.last_updated = now_dt.strftime("%H:%M:%S.%f")[:-3]
                        if self.is_market_open and self.spot > 0:
                            self._update_live_forming_bar(self.spot, now_dt)
                        self.candles_1m = self.warmup_candles[-200:] + self.today_candles
                        if self.candles_1m:
                            self.features = compute_market_features(
                                self.candles_1m, self.spot, self.vix, today_date=today_str
                            )
                self._notify_update()
        except Exception as e:
            logger.warning("Upstox quote poll error: %s", e)

    def start_replay(self, from_date: str, to_date: str) -> dict:
        """Fetch 1-minute historical candles from Upstox API for the date range and activate replay."""
        try:
            url = f"https://api.upstox.com/v2/historical-candle/{NIFTY_INSTRUMENT_KEY}/1minute/{to_date}/{from_date}"
            r = self.http.get(url, timeout=10)
            if r.status_code != 200:
                return {
                    "status": "error",
                    "message": f"Upstox API returned HTTP {r.status_code}: {r.text[:100]}"
                }
            raw_candles = r.json().get("data", {}).get("candles", [])
            if not raw_candles:
                return {
                    "status": "error",
                    "message": f"No candles found for {from_date} to {to_date} (market closed/holiday)."
                }
            
            parsed = []
            for c in reversed(raw_candles):
                parsed.append({
                    "timestamp": c[0],
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4]),
                    "volume": int(c[5]) if len(c) > 5 else 0,
                })

            with self._lock:
                self.sim_candles = parsed
                seed_len = min(30, len(parsed))
                self.candles_1m = list(parsed[:seed_len])
                self.sim_index = seed_len
                self.replay_total = len(parsed)
                self.replay_progress = round((seed_len / max(1, len(parsed))) * 100.0, 1)
                self.replay_completed = False
                self.replay_run_id += 1
                self.simulation_mode = True
                self.replay_range = {"from": from_date, "to": to_date}
                self.market_status = f"TIMELAPSE ({from_date} to {to_date})"
                self.is_market_open = True
                self.feed_source = "REPLAY_TIMELAPSE"
                self.spot = self.candles_1m[-1]["close"]
                self.sim_timestamp = self.candles_1m[-1]["timestamp"]
                self.last_updated = self.sim_timestamp
                self.features = compute_market_features(self.candles_1m, self.spot, self.vix)

            self._notify_update()
            logger.info("Started time-lapse replay with %d candles for %s to %s", len(parsed), from_date, to_date)
            return {
                "status": "ok",
                "candles_count": len(parsed),
                "from_date": from_date,
                "to_date": to_date,
                "replay_run_id": self.replay_run_id,
            }
        except Exception as e:
            logger.error("Failed to start replay: %s", e)
            return {"status": "error", "message": str(e)}

    def stop_replay(self) -> dict:
        """Switch back to live market feed (or paused idle state if 0 bots ON)."""
        with self._lock:
            self.simulation_mode = False
            self.replay_range = None
            self.sim_timestamp = None
            self.replay_completed = False
            self.replay_progress = 0.0
            self.is_market_open = False
            if self._is_live_access_allowed():
                self.market_status = "SYNCING_LIVE"
                self.feed_source = "WS_V3_LIVE" if self._ws_streamer else "REST"
                self.candles_1m = self.warmup_candles[-200:] + self.today_candles
            else:
                self.market_status = "IDLE (0 BOTS ON)"
                self.feed_source = "PAUSED (0 BOTS ON)"
        if self._is_live_access_allowed():
            self._ensure_live_bootstrap()
            self.poll_market_quotes(force_status=True)
            if self.enable_ws and self.running and self._ws_streamer is None:
                self._start_websocket()
        else:
            self._notify_update()
        return {"status": "ok", "mode": "LIVE" if self._is_live_access_allowed() else "PAUSED"}

    def toggle_simulation(self, enable: Optional[bool] = None) -> bool:
        """Toggle or set simulation mode."""
        with self._lock:
            if enable is None:
                self.simulation_mode = not self.simulation_mode
            else:
                self.simulation_mode = enable
            if self.simulation_mode:
                seed_len = min(30, len(self.sim_candles))
                self.candles_1m = list(self.sim_candles[:seed_len]) if self.sim_candles else []
                self.sim_index = seed_len
                self.replay_total = len(self.sim_candles)
                self.replay_progress = 0.0
                self.replay_completed = False
                self.replay_run_id += 1
                self.market_status = "TIMELAPSE REPLAY"
                self.is_market_open = True
                self.feed_source = "REPLAY_TIMELAPSE"
                self._notify_update()
                return True
            self.replay_completed = False
            self.replay_progress = 0.0
            self.is_market_open = False
            if self._is_live_access_allowed():
                self.market_status = "SYNCING_LIVE"
                self.feed_source = "WS_V3_LIVE" if self._ws_streamer else "REST"
                self.candles_1m = self.warmup_candles[-200:] + self.today_candles
            else:
                self.market_status = "IDLE (0 BOTS ON)"
                self.feed_source = "PAUSED (0 BOTS ON)"
        if self._is_live_access_allowed():
            self._ensure_live_bootstrap()
            self.poll_market_quotes(force_status=True)
        else:
            self._notify_update()
        return False

    def start_polling_loop(self):
        """Start WebSocket V3 + background reconciliation thread when active bots require data."""
        if self.running:
            return
        self.running = True
        if self._is_live_access_allowed():
            self._start_websocket()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop_polling_loop(self):
        self.running = False
        self._stop_websocket()
        if self._thread:
            self._thread.join(timeout=1.0)

    def _run_loop(self):
        """Background reconciliation loop: steps replay OR handles WS/REST strictly for ON bots."""
        tick_count = 0
        while self.running:
            try:
                if self.simulation_mode and self.sim_candles:
                    if self.replay_completed:
                        time.sleep(0.25)
                        continue
                    if self._consumers_configured and self._active_consumers_count <= 0:
                        time.sleep(0.3)
                        continue

                    # Time-lapse engine: evaluate every 1m candle in fast batches (~50 frames total)
                    total = len(self.sim_candles)
                    batch_size = max(1, total // 50)
                    step_cb = self.on_replay_candle
                    for _ in range(batch_size):
                        is_final = False
                        step_snap = None
                        with self._lock:
                            if not self.simulation_mode or self.sim_index >= total:
                                break
                            c = self.sim_candles[self.sim_index]
                            self.sim_index += 1
                            self.candles_1m.append(c)
                            if len(self.candles_1m) > 260:
                                self.candles_1m = self.candles_1m[-260:]
                            self.spot = c["close"]
                            self.sim_timestamp = c["timestamp"]
                            self.last_updated = c["timestamp"]
                            self.replay_progress = round((self.sim_index / max(1, total)) * 100.0, 1)
                            is_final = (self.sim_index >= total)
                            if is_final:
                                self.replay_completed = True
                                self.replay_progress = 100.0
                                self.feed_source = "REPLAY_COMPLETE"
                                rng = self.replay_range or {}
                                self.market_status = f"REPLAY DONE ({rng.get('from', '')} to {rng.get('to', '')})"
                            self.features = compute_market_features(self.candles_1m, self.spot, self.vix)
                            step_snap = self._build_snapshot_locked()
                            step_snap["replay_final"] = is_final
                        if step_cb is not None and step_snap is not None:
                            try:
                                step_cb(step_snap)
                            except Exception as cb_err:
                                logger.warning("Replay step evaluation error: %s", cb_err)
                        if is_final:
                            break

                    self._notify_update()
                    time.sleep(0.06)
                else:
                    if not self._is_live_access_allowed():
                        if self._ws_streamer is not None:
                            self._stop_websocket()
                        time.sleep(0.5)
                        continue

                    tick_count += 1
                    now_mono = time.monotonic()
                    stale_thresh = 1.5 if self.is_market_open else 5.0
                    since_ws = (now_mono - self._last_ws_msg_mono) if self._last_ws_msg_mono > 0 else 999.0
                    ws_healthy = (
                        self._ws_streamer is not None
                        and not self._ws_closed
                        and not self._ws_needs_rebuild
                        and since_ws < stale_thresh
                    )

                    # Rebuild dead/silent/exhausted WebSocket cleanly so stream never stays stuck
                    rebuild_thresh = 5.0 if self.is_market_open else 15.0
                    if self.enable_ws and self._ws_streamer is not None and (
                        self._ws_needs_rebuild or since_ws >= rebuild_thresh
                    ):
                        logger.warning("Rebuilding stale/disconnected WS V3 stream (since_ws=%.1fs)", since_ws)
                        self._stop_websocket()

                    # Expand/prune WS subscriptions dynamically when spot shifts >= 50 pts for ON bots
                    if ws_healthy and self._ws_streamer is not None and abs(self.spot - self._ws_center_spot) >= 50.0:
                        desired = set(self._get_subscription_keys(self.spot))
                        new_keys = list(desired - self._subscribed_keys)
                        stale_keys = list(self._subscribed_keys - desired)
                        if new_keys:
                            try:
                                self._ws_streamer.subscribe(new_keys, "full")
                                self._subscribed_keys.update(new_keys)
                            except Exception:
                                self._ws_needs_rebuild = True
                        if stale_keys:
                            try:
                                self._ws_streamer.unsubscribe(stale_keys)
                                self._subscribed_keys.difference_update(stale_keys)
                            except Exception:
                                self._ws_needs_rebuild = True
                        self._ws_center_spot = self.spot

                    # Fast REST fallback whenever WS isn't actively delivering sub-1.5s ticks
                    if not ws_healthy:
                        self.feed_source = "REST_FAST"
                        self.poll_market_quotes()
                        if self.enable_ws and self._ws_streamer is None and tick_count % 8 == 0:
                            self._start_websocket()
                        time.sleep(0.25)
                    else:
                        if tick_count % 120 == 0 and self.is_market_open:
                            # Offload 60s 1m candle reconciliation off-thread (zero loop pause)
                            self._sync_intraday_candles_async()
                        time.sleep(0.5)
            except Exception as e:
                logger.error("MarketFeed error in loop: %s", e)
                time.sleep(1.0)

    def _build_snapshot_locked(self) -> dict:
        if self.simulation_mode and self.sim_timestamp:
            try:
                sim_d = date.fromisoformat(self.sim_timestamp[:10])
                exp_iso = self._next_tuesday_iso(sim_d)
                dte = max(1, (date.fromisoformat(exp_iso) - sim_d).days)
            except Exception:
                exp_iso = self.active_expiry
                dte = self.expiry_dte
            exp_label = self._format_expiry_label(exp_iso)
            chain_snap = {}
        else:
            exp_iso = self.active_expiry
            exp_label = self.expiry_label
            dte = self.expiry_dte
            chain_snap = {f"{k[0]}_{k[1]}": dict(v) for k, v in self.option_chain.items()}

        return {
            "spot": self.spot,
            "vix": self.vix,
            "status": self.market_status,
            "is_open": self.is_market_open,
            "simulation": self.simulation_mode,
            "sim_timestamp": self.sim_timestamp,
            "replay_range": self.replay_range,
            "replay_progress": self.replay_progress,
            "replay_completed": self.replay_completed,
            "replay_run_id": self.replay_run_id,
            "replay_total_candles": self.replay_total,
            "active_expiry": exp_iso,
            "expiry_label": exp_label,
            "expiry_dte": dte,
            "feed_source": self.feed_source,
            "option_chain": chain_snap,
            "last_updated": self.last_updated or datetime.now(IST).strftime("%H:%M:%S"),
            "features": dict(self.features),
        }

    def get_snapshot(self) -> dict:
        """Return safe copy of current market snapshot."""
        with self._lock:
            return self._build_snapshot_locked()
