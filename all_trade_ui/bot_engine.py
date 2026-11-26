#!/usr/bin/env python3
"""Unified Multi-Bot Execution Hub for ALL Trade UI.

Runs all 5 bots concurrently in live paper trading mode:
1. Winner 51 (Chennai_WideRunner_32 [g] Champion)
2. 51.1 (Uncapped Profit & Unrestricted Trades Variant)
3. Chad 25 (Chandelier 4L F14k L2500 T70%)
4. Chandler Runner (Chandelier Runner Tgt50% Lock Rs2200)
5. Live Trading Bot (Gravitas Daily & Weekly Profit Preservation Shield)
"""
from __future__ import annotations

import copy
import csv
from datetime import datetime
import json
import logging
from pathlib import Path
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from .config import BASE_DIR, DEFAULT_INITIAL_WALLET, LOT_SIZE, ROUND_TRIP_CHARGES, IST
from .indicators import bs_option_metrics

logger = logging.getLogger("bot_engine")


class SingleBot:
    """State and execution container for one bot."""

    def __init__(
        self,
        bot_id: str,
        name: str,
        family: str,
        cfg: dict,
        initial_wallet: float,
        auto_start: bool = True,
    ):
        self.bot_id = bot_id
        self.name = name
        self.family = family
        self.cfg = cfg
        self.initial_wallet = initial_wallet
        self.wallet = initial_wallet
        self.enabled = True
        self.status = "SCANNING"  # SCANNING, IN_TRADE, DAILY_LOCK, STOPPED, KILLED
        self.kill_switch_active = False

        self.day_pnl = 0.0
        self.trade_count = 0
        self.win_count = 0
        self.loss_count = 0
        self.loss_streak = 0
        
        self.current_trade: Optional[Dict[str, Any]] = None
        self.trades_history: List[Dict[str, Any]] = []
        self.reset_history: List[Dict[str, Any]] = []
        self.last_exit_time: Optional[datetime] = None
        self._last_sim_exit_dt: Optional[datetime] = None
        self._sim_day_date: Optional[str] = None
        self._sim_day_pnl: float = 0.0
        self._sim_day_trades: int = 0

        # Individual per-bot script runtime controls (ON = run worker, OFF = terminated)
        self._state_lock = threading.RLock()
        self._tick_cond = threading.Condition(self._state_lock)
        self._stop_event = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None
        self._pending_task: Optional[tuple] = None

        # Planning Radar
        self.radar: Dict[str, Any] = {
            "targeted_option": "NIFTY ATM CE",
            "target_delta": 0.60,
            "estimated_premium": 150.0,
            "status": "Scanning Signals",
            "checklist": [],
            "notes": "Analyzing market structure",
        }

        if auto_start:
            self.start()

    def is_script_alive(self) -> bool:
        """Return True only when this individual bot script thread is running."""
        with self._state_lock:
            t = self._worker_thread
            return bool(self.enabled and t is not None and t.is_alive() and not self._stop_event.is_set())

    def start(self):
        """Start this bot's dedicated individual execution worker script."""
        t_to_start: Optional[threading.Thread] = None
        with self._state_lock:
            self.enabled = True
            if self.status in ("STOPPED", "KILLED"):
                self.status = "SCANNING"
                self.radar = {
                    "targeted_option": self.radar.get("targeted_option", "NIFTY ATM CE"),
                    "target_delta": self.cfg.get("target_delta", 0.60),
                    "estimated_premium": 0.0,
                    "status": "Script Started — Awaiting Tick",
                    "checklist": [],
                    "notes": "Bot script active. Consuming live market feed.",
                }
            if self._worker_thread is None or not self._worker_thread.is_alive() or self._stop_event.is_set():
                self._stop_event.clear()
                self._pending_task = None
                t_to_start = threading.Thread(
                    target=self._run_bot_script,
                    name=f"bot-script-{self.bot_id}",
                    daemon=True,
                )
                self._worker_thread = t_to_start
        if t_to_start is not None:
            t_to_start.start()
            logger.info("Started individual bot script [%s] (%s)", self.name, t_to_start.name)

    def terminate(self, reason: str = "BOT_TERMINATED"):
        """Terminate this bot's individual script worker and release market data usage."""
        with self._state_lock:
            self.enabled = False
            if self.current_trade:
                self.force_exit_trade(reason)
            self.status = "STOPPED"
            self.radar = {
                "targeted_option": "NONE (OFF)",
                "expiry": "",
                "expiry_label": "",
                "expiry_dte": 0,
                "target_delta": 0.0,
                "estimated_premium": 0.0,
                "status": "SCRIPT TERMINATED (OFF)",
                "checklist": [],
                "notes": "Bot script terminated. Zero market data / access code spent.",
            }
            self._stop_event.set()
            pending = self._pending_task
            self._pending_task = None
            if pending is not None:
                _, done_ev, _ = pending
                done_ev.set()
            t = self._worker_thread
            self._tick_cond.notify_all()

        if t is not None and t is not threading.current_thread() and t.is_alive():
            t.join(timeout=1.0)
        with self._state_lock:
            if self._worker_thread is t:
                self._worker_thread = None
        logger.info("Terminated individual bot script [%s] (%s)", self.name, reason)

    def dispatch_tick(self, ctx: dict) -> Tuple[threading.Event, List[bool]]:
        """Dispatch one market tick strictly to this running bot worker."""
        done_ev = threading.Event()
        changed_box = [False]
        with self._state_lock:
            if not self.enabled or self._stop_event.is_set():
                done_ev.set()
                return done_ev, changed_box
            if self._pending_task is not None:
                _, prev_ev, _ = self._pending_task
                prev_ev.set()
            self._pending_task = (ctx, done_ev, changed_box)
            self._tick_cond.notify_all()
        return done_ev, changed_box

    def _run_bot_script(self):
        """Individual per-bot execution loop running only while bot is powered ON."""
        while not self._stop_event.is_set():
            with self._state_lock:
                while self._pending_task is None and not self._stop_event.is_set():
                    self._tick_cond.wait(timeout=0.5)
                if self._stop_event.is_set():
                    break
                task = self._pending_task
                self._pending_task = None
            if task is None:
                continue
            ctx, done_ev, changed_box = task
            try:
                with self._state_lock:
                    if self.enabled and not self._stop_event.is_set():
                        if self.process_market_tick(ctx):
                            changed_box[0] = True
            except Exception as e:
                logger.error("Bot script [%s] evaluation error: %s", self.name, e)
            finally:
                done_ev.set()

    def _resolve_cfg(self, vix: float, fixed_tier: Optional[str] = None) -> Tuple[Optional[str], dict]:
        """Return (tier_name, effective_cfg) merging optional VIX tier overrides uniformly."""
        regimes = self.cfg.get("vix_regimes")
        if not isinstance(regimes, dict):
            return None, self.cfg
        tier = fixed_tier if (fixed_tier and fixed_tier in regimes) else None
        if tier is None:
            low_max = float(self.cfg.get("vix_tier_low_max", 14.0))
            mid_max = float(self.cfg.get("vix_tier_mid_max", 17.0))
            high_max = float(self.cfg.get("vix_tier_high_max", 18.5))
            if vix < low_max and "low" in regimes:
                tier = "low"
            elif vix < mid_max and "mid" in regimes:
                tier = "mid"
            elif vix < high_max and "high" in regimes:
                tier = "high"
            elif "crisis" in regimes:
                tier = "crisis"
        if tier and tier in regimes:
            return tier, {**self.cfg, **regimes[tier]}
        return None, self.cfg

    def process_market_tick(self, ctx: dict) -> bool:
        """Evaluate open trade / signals for this individual bot on one market tick."""
        if not self.enabled:
            self.status = "STOPPED"
            return False

        spot: float = ctx["spot"]
        vix: float = ctx["vix"]
        feats: dict = ctx["feats"]
        is_open: bool = ctx["is_open"]
        is_sim: bool = ctx["is_sim"]
        market_status: str = ctx["market_status"]
        active_expiry: str = ctx["active_expiry"]
        expiry_label: str = ctx["expiry_label"]
        expiry_dte: int = ctx["expiry_dte"]
        now_dt: datetime = ctx["now_dt"]
        now_clock: str = ctx["now_clock"]
        atm_strike: int = ctx["atm_strike"]
        t_exp: float = ctx["t_exp"]
        chain: dict = ctx["chain"]
        ce_price: float = ctx["ce_price"]
        ce_delta: float = ctx["ce_delta"]
        pe_price: float = ctx["pe_price"]
        pe_delta: float = ctx["pe_delta"]

        if not is_open and not is_sim:
            self.status = "WAIT_OPEN"
            self.radar = {
                "targeted_option": f"NIFTY {atm_strike} CE / PE",
                "expiry": active_expiry,
                "expiry_label": expiry_label,
                "expiry_dte": expiry_dte,
                "target_delta": self.cfg.get("target_delta", 0.60),
                "estimated_premium": ce_price,
                "status": f"Awaiting 09:15 Open ({market_status})",
                "checklist": [
                    {"name": "NSE Status", "required": "NORMAL_OPEN", "current": market_status, "met": False},
                    {"name": "Live Spot", "required": "Active Feed", "current": f"{spot:,.2f}", "met": True},
                    {"name": "India VIX", "required": "10 - 35", "current": f"{vix:.2f}", "met": 10.0 <= vix <= 35.0},
                    {"name": "Session Ready", "required": "09:15:00 IST", "current": now_clock, "met": False},
                ],
                "notes": f"Pre-Open Live Spot: {spot:,.2f} | VIX: {vix:.2f} | Exp: {expiry_label}",
            }
            return False

        # 1. Update active trade if in position
        if self.current_trade:
            trade = self.current_trade
            if not trade.get("expiry"):
                trade["expiry"] = active_expiry
                trade["expiry_label"] = expiry_label
            is_ce = (trade["kind"] == "CE")
            opt_strike = int(trade["strike_val"])
            opt_key = f"{opt_strike}_{trade['kind']}"

            if trade.get("pricing_source") == "upstox_chain" and opt_key in chain:
                opt_q = chain[opt_key]
                cur_opt_price = round(float(opt_q.get("ltp") or opt_q["bid"]), 2)
            else:
                trade_t_exp = float(trade.get("bs_t_exp", 2.0 / 365.0 if trade.get("pricing_source") is None else t_exp))
                cur_opt_price, _ = bs_option_metrics(spot, opt_strike, trade_t_exp, vix, is_ce=is_ce)

            trade["current_price"] = cur_opt_price
            trade["peak_price"] = max(trade["peak_price"], cur_opt_price)

            gross_pnl = (cur_opt_price - trade["entry_price"]) * trade["qty"]
            ret = (cur_opt_price - trade["entry_price"]) / trade["entry_price"]
            trade["current_pnl"] = round(gross_pnl, 2)
            trade["current_pnl_pct"] = round(ret * 100.0, 2)

            _, cfg = self._resolve_cfg(vix, trade.get("regime_tier"))
            entry_p = trade["entry_price"]
            peak_ret = (trade["peak_price"] - entry_p) / entry_p if entry_p > 0 else ret
            for trig, lock in cfg.get("step_trail_levels", []):
                if ret >= trig:
                    new_sl = round(entry_p * (1.0 + lock), 2)
                    if new_sl > trade["trailing_sl"]:
                        trade["trailing_sl"] = new_sl

            be_act = cfg.get("breakeven_activation_fraction")
            if be_act is not None and peak_ret >= float(be_act):
                be_sl = round(entry_p * (1.0 + float(cfg.get("breakeven_lock_fraction", 0.025))), 2)
                if be_sl > trade["trailing_sl"]:
                    trade["trailing_sl"] = be_sl

            trail_act = cfg.get("trail_activation", cfg.get("trail_activation_fraction"))
            if trail_act is not None and max(ret, peak_ret) >= float(trail_act):
                trail_f = float(cfg.get("trail_fraction", 0.052))
                chand_sl = round(trade["peak_price"] * (1.0 - trail_f), 2)
                if chand_sl > trade["trailing_sl"]:
                    trade["trailing_sl"] = chand_sl

            if self.bot_id == "live_trading_bot":
                if ret >= cfg.get("breakeven_trigger", 0.05) and trade["trailing_sl"] < entry_p:
                    trade["trailing_sl"] = entry_p
                if ret >= cfg.get("trail_trigger", 0.10):
                    trail_level = round(entry_p + (trade["peak_price"] - entry_p) * cfg.get("trail_ratio", 0.75), 2)
                    if trail_level > trade["trailing_sl"]:
                        trade["trailing_sl"] = trail_level

            exit_reason = None
            replay_final = bool(ctx.get("replay_final", False))
            if is_sim and replay_final:
                exit_reason = "REPLAY_SESSION_END"
            elif cur_opt_price >= trade["target_price"]:
                exit_reason = "TARGET_HIT"
            elif cur_opt_price <= trade["trailing_sl"]:
                exit_reason = "TRAILING_STOP_HIT" if trade["trailing_sl"] > trade["stop_loss"] else "STOP_LOSS_HIT"
            elif is_open and not is_sim and now_clock >= "15:15:00":
                exit_reason = "EOD_SQUAREOFF"
            elif is_sim and ctx.get("sim_date") and now_clock >= "15:15:00":
                exit_reason = "REPLAY_EOD_SQUAREOFF"

            if exit_reason:
                sim_dt = ctx.get("sim_dt")
                if is_sim and sim_dt is not None:
                    self._last_sim_exit_dt = sim_dt
                ex = self.force_exit_trade(exit_reason, exit_time=ctx.get("sim_clock_full") or now_clock)
                if is_sim and ex is not None:
                    self._sim_day_pnl = round(self._sim_day_pnl + float(ex.get("net_pnl", 0.0)), 2)
                return True
            self.status = "IN_TRADE"
            return False

        if is_sim and ctx.get("replay_completed"):
            return False

        # In multi-day replay, reset per-day risk locks cleanly at start of each simulated session date
        if is_sim and ctx.get("sim_date"):
            s_date = ctx["sim_date"]
            if self._sim_day_date != s_date:
                self._sim_day_date = s_date
                self._sim_day_pnl = 0.0
                self._sim_day_trades = 0

        regime_tier, eff_cfg = self._resolve_cfg(vix)
        eff_day_pnl = self._sim_day_pnl if is_sim else self.day_pnl
        eff_day_trades = self._sim_day_trades if is_sim else self.trade_count

        # 2. Check Daily Risk / Profit Locks
        if eff_day_pnl >= eff_cfg.get("daily_profit_lock", 2500.0):
            self.status = "DAILY_LOCK"
            self.radar = {
                "targeted_option": f"NIFTY {atm_strike} CE",
                "expiry": active_expiry,
                "expiry_label": expiry_label,
                "expiry_dte": expiry_dte,
                "target_delta": self.cfg.get("target_delta", 0.60),
                "estimated_premium": ce_price,
                "status": "Daily Profit Locked",
                "checklist": self.radar.get("checklist", []),
                "notes": f"Goal reached: +Rs {self.day_pnl:,.2f}. Capital secured.",
            }
            return False

        if eff_day_pnl <= -eff_cfg.get("daily_loss_limit", 4000.0):
            self.status = "DAILY_LOSS_LIMIT"
            self.radar = {
                "targeted_option": f"NIFTY {atm_strike} CE",
                "expiry": active_expiry,
                "expiry_label": expiry_label,
                "expiry_dte": expiry_dte,
                "target_delta": eff_cfg.get("target_delta", 0.60),
                "estimated_premium": ce_price,
                "status": "Daily Loss Limit Reached",
                "checklist": self.radar.get("checklist", []),
                "notes": "Circuit triggered to prevent capital erosion.",
            }
            return False

        if eff_day_trades >= eff_cfg.get("max_trades_day", 4):
            self.status = "MAX_TRADES_MET"
            self.radar = {
                "targeted_option": f"NIFTY {atm_strike} CE",
                "expiry": active_expiry,
                "expiry_label": expiry_label,
                "expiry_dte": expiry_dte,
                "target_delta": self.cfg.get("target_delta", 0.60),
                "estimated_premium": ce_price,
                "status": "Max Daily Trades Reached",
                "checklist": self.radar.get("checklist", []),
                "notes": f"Completed {self.trade_count} trades.",
            }
            return False

        # 3. Radar & Signal Evaluation
        self.status = "SCANNING"
        p = feats.get("price", spot)
        st5m = feats.get("st5m_dir", 1)
        e9 = feats.get("e9", p)
        e21 = feats.get("e21", p)
        eff = feats.get("eff_5m", 0.3)
        esep = feats.get("esep_5m", 0.3)
        rsi = feats.get("rsi14", 50.0)
        don_hi = feats.get("don10_hi", p + 5)
        don_lo = feats.get("don10_lo", p - 5)
        roc3 = feats.get("roc3", 0.0)

        bullish_signal = (st5m == 1 and eff >= 0.25 and esep >= 0.30 and p > e9 > e21 and (p > don_hi or roc3 > 0.0005) and rsi <= 76.0)
        bearish_signal = (st5m == -1 and eff >= 0.25 and esep >= 0.30 and p < e9 < e21 and (p < don_lo or roc3 < -0.0005) and rsi >= 24.0)

        target_kind = "CE" if st5m == 1 else "PE"
        target_strike_str = f"NIFTY {atm_strike} {target_kind}"
        est_prem = ce_price if target_kind == "CE" else pe_price
        est_delta = ce_delta if target_kind == "CE" else pe_delta

        checklist = [
            {"name": "5m SuperTrend", "required": "Bullish (+1)" if target_kind == "CE" else "Bearish (-1)", "current": "Bullish" if st5m == 1 else "Bearish", "met": (st5m == 1 if target_kind == "CE" else st5m == -1)},
            {"name": "EMA 9/21 Alignment", "required": "EMA9 > EMA21" if target_kind == "CE" else "EMA9 < EMA21", "current": f"{e9:.1f} vs {e21:.1f}", "met": (e9 > e21 if target_kind == "CE" else e9 < e21)},
            {"name": "Trend Efficiency", "required": ">= 0.25", "current": f"{eff:.2f}", "met": eff >= 0.25},
            {"name": "Donchian / ROC", "required": "Breakout / Momentum", "current": f"ROC {roc3:+.4f}", "met": (p > don_hi or roc3 > 0.0005) if target_kind == "CE" else (p < don_lo or roc3 < -0.0005)},
        ]

        all_met = all(item["met"] for item in checklist)
        today_bars = feats.get("today_bars_count", 0)
        if not is_sim and today_bars < 5:
            self.status = "WARMUP"
            self.radar = {
                "targeted_option": target_strike_str,
                "expiry": active_expiry,
                "expiry_label": expiry_label,
                "expiry_dte": expiry_dte,
                "target_delta": abs(est_delta),
                "estimated_premium": est_prem,
                "status": f"Opening Warmup ({today_bars}/5 bars — 09:20)",
                "checklist": checklist,
                "notes": f"Building today's opening 5m structure ({today_bars}/5 mins) to avoid gap whipsaws.",
            }
            return False

        self.radar = {
            "targeted_option": target_strike_str,
            "expiry": active_expiry,
            "expiry_label": expiry_label,
            "expiry_dte": expiry_dte,
            "target_delta": abs(est_delta),
            "estimated_premium": est_prem,
            "status": "ENTRY TRIGGER READY" if all_met else "SCANNING FOR ALIGNMENT",
            "checklist": checklist,
            "notes": f"Exp: {expiry_label} | VIX: {vix:.1f} | ATM: {atm_strike} | Bias: {'BULLISH' if target_kind == 'CE' else 'BEARISH'}",
        }

        # 4. Trigger New Entry if All Criteria Met
        signal_dir = 1 if bullish_signal else (-1 if bearish_signal else 0)
        if signal_dir != 0 and not (is_sim and ctx.get("sim_date") and now_clock >= "15:10:00"):
            if is_sim:
                sim_dt = ctx.get("sim_dt")
                if self._last_sim_exit_dt and sim_dt and (sim_dt - self._last_sim_exit_dt).total_seconds() < 60.0:
                    return False
            elif self.last_exit_time:
                elapsed = (now_dt - self.last_exit_time).total_seconds()
                if elapsed < 20.0:
                    return False

            kind = "CE" if signal_dir == 1 else "PE"
            entry_p = ce_price if kind == "CE" else pe_price
            if entry_p <= 5.0:
                return False

            used_chain = (not is_sim and f"{atm_strike}_{kind}" in chain and float(chain[f"{atm_strike}_{kind}"].get("ltp") or chain[f"{atm_strike}_{kind}"].get("ask") or 0) > 0)
            lots = min(eff_cfg.get("max_lots", 2), 2 if self.wallet < 60000 else 4)
            qty = lots * LOT_SIZE

            target_f = eff_cfg.get("target_fraction", 0.50)
            stop_f = eff_cfg.get("stop_fraction", 0.10)

            tgt_p = round(entry_p * (1.0 + target_f), 2)
            sl_p = round(entry_p * (1.0 - stop_f), 2)

            self.trade_count += 1
            if is_sim:
                self._sim_day_trades += 1
            entered_lbl = ctx.get("sim_clock_full") if is_sim else now_clock
            self.current_trade = {
                "trade_id": f"T{self.trade_count}_{now_clock.replace(':', '')}",
                "strike": f"NIFTY {atm_strike} {kind}",
                "strike_val": atm_strike,
                "kind": kind,
                "expiry": active_expiry,
                "expiry_label": expiry_label,
                "expiry_dte": expiry_dte,
                "regime_tier": regime_tier,
                "pricing_source": "upstox_chain" if used_chain else "bs",
                "bs_t_exp": t_exp,
                "lots": lots,
                "qty": qty,
                "entry_price": entry_p,
                "current_price": entry_p,
                "peak_price": entry_p,
                "target_price": tgt_p,
                "stop_loss": sl_p,
                "trailing_sl": sl_p,
                "current_pnl": 0.0,
                "current_pnl_pct": 0.0,
                "entered_at": entered_lbl,
                "exit_price": None,
                "exit_reason": None,
            }
            self.status = "IN_TRADE"
            logger.info("Bot [%s] entered %s (%s) @ Rs %.2f (Qty: %d, SL: %.2f, Tgt: %.2f)", self.name, self.current_trade["strike"], expiry_label, entry_p, qty, sl_p, tgt_p)
            return True
        return False

    def reset_wallet(self, new_wallet: float):
        """Set initial wallet common starting amount."""
        self.initial_wallet = new_wallet
        self.wallet = new_wallet + self.day_pnl

    def force_exit_trade(self, reason: str = "MANUAL_KILL_SWITCH", exit_time: Optional[str] = None):
        """Instantly exit active trade via kill switch."""
        if not self.current_trade:
            return None
        t = self.current_trade
        exit_price = t["current_price"]
        gross = (exit_price - t["entry_price"]) * t["qty"]
        net = round(gross - ROUND_TRIP_CHARGES, 2)
        
        t["exit_price"] = exit_price
        t["exit_reason"] = reason
        t["exit_time"] = exit_time or datetime.now(IST).strftime("%H:%M:%S")
        t["net_pnl"] = net
        t["gross_pnl"] = round(gross, 2)
        
        self.day_pnl = round(self.day_pnl + net, 2)
        self.wallet = round(self.wallet + net, 2)
        if net >= 0:
            self.win_count += 1
            self.loss_streak = 0
        else:
            self.loss_count += 1
            self.loss_streak += 1
            
        self.trades_history.insert(0, copy.deepcopy(t))
        exited_trade = copy.deepcopy(t)
        self.current_trade = None
        if any(tag in reason for tag in ("DISABLED", "TERMINATED", "STOPPED")):
            self.status = "STOPPED"
        elif "KILL" in reason:
            self.status = "KILLED"
        else:
            self.status = "SCANNING"
        self.last_exit_time = datetime.now(IST)
        return exited_trade


class BotEngineHub:
    """Master controller managing all concurrent bots, archives, and replay isolation."""

    def __init__(
        self,
        initial_wallet: float = DEFAULT_INITIAL_WALLET,
        state_file: Optional[Path] = None,
        on_active_bots_changed: Optional[Callable[[Dict[str, Any]], None]] = None,
        archive_root: Optional[Path] = None,
        bots_dir: Optional[Path] = None,
    ):
        self.initial_wallet = initial_wallet
        self.state_file = Path(state_file) if state_file else None
        self.archive_root = Path(archive_root) if archive_root else BASE_DIR.parent
        self.bots_dir = Path(bots_dir) if bots_dir else (BASE_DIR.parent / "bots")
        self._on_active_bots_changed = on_active_bots_changed
        self.bots: Dict[str, SingleBot] = {}
        self.day_events: List[Dict[str, Any]] = []
        self.in_replay_mode: bool = False
        self._live_session_backup: Optional[Dict[str, Any]] = None
        self._eod_archived_date: Optional[str] = None
        self._lock = threading.Lock()
        self._persist_lock = threading.Lock()
        self._init_bots()
        self._load_dropin_bots()
        self.load_state()
        for bot in self.bots.values():
            if bot.enabled:
                bot.start()
            else:
                bot.terminate("INIT_STOPPED")
        self._notify_feed_requirements()

    def _record_event(self, bot_id: str, event: str, details: Optional[dict] = None):
        """Record chronological live trading / lifecycle / reset events (skipped in replay)."""
        if self.in_replay_mode:
            return
        self.day_events.append({
            "timestamp": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
            "bot_id": bot_id,
            "event": event,
            "details": details or {},
        })
        if len(self.day_events) > 2000:
            self.day_events = self.day_events[-2000:]

    def add_bot(
        self,
        bot_id: str,
        name: str,
        family: str,
        cfg: dict,
        enabled: bool = True,
    ) -> SingleBot:
        """Dynamically add a bot beyond the initial 5 (UI automatically switches >6 to 4x2)."""
        with self._lock:
            if bot_id in self.bots:
                bot = self.bots[bot_id]
                bot.cfg = cfg
                bot.name = name
                bot.family = family
            else:
                bot = SingleBot(bot_id, name, family, cfg, self.initial_wallet, auto_start=False)
                self.bots[bot_id] = bot
            if enabled:
                bot.start()
            else:
                bot.terminate("ADDED_STOPPED")
            self._record_event(bot_id, "BOT_ADDED", {"name": name, "enabled": enabled})
            self.save_state()
        self._notify_feed_requirements()
        return bot

    def set_feed_listener(self, cb: Optional[Callable[[Dict[str, Any]], None]]):
        """Register callback notified whenever active ON bots or open positions change."""
        self._on_active_bots_changed = cb
        self._notify_feed_requirements()

    def get_feed_requirements(self) -> Dict[str, Any]:
        """Compute exact instrument requirements for currently ON bots only."""
        with self._lock:
            enabled_bots = [b_id for b_id, b in self.bots.items() if b.enabled]
            need_scanning = any(
                b.enabled
                and b.current_trade is None
                and b.status not in ("DAILY_LOCK", "DAILY_LOSS_LIMIT", "MAX_TRADES_MET", "KILLED")
                for b in self.bots.values()
            )
            open_opts = [
                (int(b.current_trade["strike_val"]), b.current_trade["kind"])
                for b in self.bots.values()
                if b.enabled and b.current_trade is not None
            ]
            return {
                "enabled_bots": enabled_bots,
                "active_count": len(enabled_bots),
                "need_scanning_quotes": need_scanning,
                "open_option_keys": open_opts,
            }

    def _notify_feed_requirements(self):
        cb = self._on_active_bots_changed
        if cb is not None:
            try:
                cb(self.get_feed_requirements())
            except Exception as e:
                logger.warning("Feed requirements listener error: %s", e)

    def shutdown(self):
        """Cleanly archive EOD if due and terminate all running per-bot script threads."""
        try:
            now_dt = datetime.now(IST)
            if self.state_file is not None and not self.in_replay_mode and now_dt.strftime("%H:%M:%S") >= "15:15:00":
                has_activity = any(
                    b.trade_count > 0 or len(b.reset_history) > 0 or b.current_trade is not None
                    for b in self.bots.values()
                )
                if has_activity:
                    self.save_daily_arena_archive(now_dt.strftime("%Y-%m-%d"))
        except Exception as e:
            logger.warning("EOD shutdown archive skipped: %s", e)

        for bot in self.bots.values():
            with bot._state_lock:
                bot._stop_event.set()
                t = bot._worker_thread
                bot._tick_cond.notify_all()
            if t is not None and t.is_alive():
                t.join(timeout=1.0)
            with bot._state_lock:
                if bot._worker_thread is t:
                    bot._worker_thread = None

    def _write_state_file(self, target_path: Path, serialized: str):
        with self._persist_lock:
            try:
                tmp = target_path.with_suffix(".tmp")
                tmp.write_text(serialized, encoding="utf-8")
                tmp.replace(target_path)
            except Exception as e:
                logger.warning("State persistence write skipped: %s", e)

    def save_state(self, async_write: bool = False):
        """Persist today's live trading state atomically to disk if configured."""
        if not self.state_file or self.in_replay_mode:
            return
        try:
            payload = {
                "date": datetime.now(IST).strftime("%Y-%m-%d"),
                "initial_wallet": self.initial_wallet,
                "day_events": copy.deepcopy(self.day_events[-1000:]),
                "bots": {
                    b_id: {
                        "name": bot.name,
                        "family": bot.family,
                        "cfg": bot.cfg,
                        "enabled": bot.enabled,
                        "status": bot.status,
                        "wallet": bot.wallet,
                        "initial_wallet": bot.initial_wallet,
                        "day_pnl": bot.day_pnl,
                        "trade_count": bot.trade_count,
                        "win_count": bot.win_count,
                        "loss_count": bot.loss_count,
                        "loss_streak": bot.loss_streak,
                        "current_trade": copy.deepcopy(bot.current_trade),
                        "trades_history": copy.deepcopy(bot.trades_history[:100]),
                        "reset_history": copy.deepcopy(bot.reset_history),
                    }
                    for b_id, bot in self.bots.items()
                },
            }
            serialized = json.dumps(payload, indent=2)
            target_path = self.state_file
            if async_write:
                threading.Thread(target=self._write_state_file, args=(target_path, serialized), daemon=True).start()
            else:
                self._write_state_file(target_path, serialized)
        except Exception as e:
            logger.warning("State persistence write skipped: %s", e)

    def load_state(self):
        """Restore today's live session state if saved on disk."""
        if not self.state_file or not self.state_file.exists():
            return
        try:
            data = json.loads(self.state_file.read_text(encoding="utf-8"))
            today_s = datetime.now(IST).strftime("%Y-%m-%d")
            if data.get("date") != today_s:
                return
            if "initial_wallet" in data:
                self.initial_wallet = float(data["initial_wallet"])
            self.day_events = data.get("day_events", [])
            saved_bots = data.get("bots", {})
            for b_id, sb in saved_bots.items():
                if b_id not in self.bots and isinstance(sb, dict) and sb.get("cfg"):
                    self.bots[b_id] = SingleBot(
                        b_id,
                        sb.get("name", b_id),
                        sb.get("family", "Custom"),
                        sb["cfg"],
                        self.initial_wallet,
                        auto_start=False,
                    )
                bot = self.bots.get(b_id)
                if not bot or not isinstance(sb, dict):
                    continue
                bot.enabled = bool(sb.get("enabled", True))
                bot.status = sb.get("status", bot.status)
                bot.wallet = float(sb.get("wallet", self.initial_wallet))
                bot.initial_wallet = float(sb.get("initial_wallet", self.initial_wallet))
                bot.day_pnl = float(sb.get("day_pnl", 0.0))
                bot.trade_count = int(sb.get("trade_count", 0))
                bot.win_count = int(sb.get("win_count", 0))
                bot.loss_count = int(sb.get("loss_count", 0))
                bot.loss_streak = int(sb.get("loss_streak", 0))
                bot.current_trade = sb.get("current_trade")
                bot.trades_history = sb.get("trades_history", [])
                bot.reset_history = sb.get("reset_history", [])
            logger.info("Restored today's live session state from %s", self.state_file)
        except Exception as e:
            logger.warning("State restore fallback: %s", e)

    def _init_bots(self):
        """Initialize all 5 bots with their distinct parameter configurations."""
        # 1. Winner 51
        cfg_w51 = {
            "capital_floor": 14000.0,
            "daily_profit_lock": 1450.0,
            "daily_loss_limit": 3000.0,
            "max_trades_day": 4,
            "max_lots": 2,
            "target_delta": 0.65,
            "stop_fraction": 0.10,
            "target_fraction": 0.70,
            "low_vix_stop": 0.075,
            "low_vix_target": 0.145,
            "step_trail_levels": [
                [0.085, 0.038],
                [0.15, 0.095],
                [0.23, 0.165],
                [0.32, 0.250],
                [0.42, 0.340],
            ],
        }
        self.bots["winner_51"] = SingleBot(
            "winner_51", "Winner 51", "Chennai_WideRunner_32 [g]", cfg_w51, self.initial_wallet, auto_start=False
        )

        # 2. 51.1 (Uncapped variant)
        cfg_51_1 = {
            "capital_floor": 14000.0,
            "daily_profit_lock": 1000000.0,  # Uncapped
            "daily_loss_limit": 5000.0,
            "max_trades_day": 999,  # Unrestricted
            "max_lots": 2,
            "target_delta": 0.65,
            "stop_fraction": 0.10,
            "target_fraction": 0.70,
            "step_trail_levels": [
                [0.085, 0.035],
                [0.15, 0.090],
                [0.22, 0.150],
                [0.28, 0.215],
            ],
        }
        self.bots["bot_51_1"] = SingleBot(
            "bot_51_1", "51.1", "Uncapped Profit Runner", cfg_51_1, self.initial_wallet, auto_start=False
        )

        # 3. Chad 25 (Chandelier 4L)
        cfg_chad25 = {
            "capital_floor": 14000.0,
            "daily_profit_lock": 2500.0,
            "daily_loss_limit": 4000.0,
            "max_trades_day": 4,
            "max_lots": 4,
            "target_delta": 0.60,
            "stop_fraction": 0.10,
            "target_fraction": 0.70,
            "trail_fraction": 0.052,
            "trail_activation": 0.13,
            "step_trail_levels": [
                [0.085, 0.038],
                [0.15, 0.095],
                [0.23, 0.165],
                [0.32, 0.250],
                [0.42, 0.340],
            ],
        }
        self.bots["chad_25"] = SingleBot(
            "chad_25", "Chad 25", "Chandelier Compounding 4L", cfg_chad25, self.initial_wallet, auto_start=False
        )

        # 4. Chandler Runner
        cfg_chandler = {
            "capital_floor": 30000.0,
            "daily_profit_lock": 2200.0,
            "daily_loss_limit": 4000.0,
            "max_trades_day": 4,
            "max_lots": 2,
            "target_delta": 0.60,
            "stop_fraction": 0.10,
            "target_fraction": 0.50,
            "trail_fraction": 0.055,
            "trail_activation": 0.13,
            "step_trail_levels": [
                [0.085, 0.038],
                [0.15, 0.095],
                [0.23, 0.165],
                [0.32, 0.250],
                [0.42, 0.340],
            ],
        }
        self.bots["chandler_runer"] = SingleBot(
            "chandler_runer", "Chandler Runner", "Chandelier Wide-Target", cfg_chandler, self.initial_wallet, auto_start=False
        )

        # 5. Live Trading Bot (Gravitas Shield)
        cfg_live_bot = {
            "capital_floor": 50000.0,
            "daily_profit_lock": 2500.0,
            "daily_loss_limit": 2000.0,
            "max_trades_day": 2,
            "max_lots": 1,
            "target_delta": 0.60,
            "stop_fraction": 0.09,
            "target_fraction": 0.25,
            "breakeven_trigger": 0.05,
            "trail_trigger": 0.10,
            "trail_ratio": 0.75,
        }
        self.bots["live_trading_bot"] = SingleBot(
            "live_trading_bot", "Live Trading Bot", "Gravitas Capital Shield", cfg_live_bot, self.initial_wallet, auto_start=False
        )

        # 6. Champion of 50 (Apex OmniTitan X - AllWeather_Hybrid, config_50k.json)
        cfg_champion_50 = {
            "capital_floor": 30000.0,
            "daily_profit_lock": 3500.0,
            "daily_loss_limit": 4000.0,
            "max_trades_day": 5,
            "max_lots": 4,
            "target_delta": 0.56,
            "stop_fraction": 0.095,
            "target_fraction": 0.35,
            "trail_activation": 0.11,
            "trail_fraction": 0.045,
            "breakeven_activation_fraction": 0.06,
            "breakeven_lock_fraction": 0.025,
            "step_trail_levels": [
                [0.085, 0.035],
                [0.145, 0.090],
                [0.215, 0.150],
                [0.285, 0.215],
                [0.360, 0.290],
            ],
            "vix_tier_low_max": 14.0,
            "vix_tier_mid_max": 17.0,
            "vix_tier_high_max": 18.5,
            "vix_regimes": {
                "low": {
                    "max_lots": 2,
                    "max_trades_day": 1,
                    "daily_profit_lock": 2000.0,
                    "stop_fraction": 0.075,
                    "target_fraction": 0.145,
                    "breakeven_activation_fraction": 0.045,
                    "breakeven_lock_fraction": 0.025,
                    "trail_activation": 0.065,
                    "trail_fraction": 0.028,
                    "step_trail_levels": [[0.085, 0.035], [0.15, 0.090], [0.22, 0.150], [0.28, 0.215]],
                },
                "mid": {
                    "max_lots": 8,
                    "max_trades_day": 4,
                    "daily_profit_lock": 5000.0,
                    "stop_fraction": 0.095,
                    "target_fraction": 0.55,
                    "breakeven_activation_fraction": 0.065,
                    "breakeven_lock_fraction": 0.028,
                    "trail_activation": 0.13,
                    "trail_fraction": 0.052,
                    "step_trail_levels": [
                        [0.085, 0.035], [0.15, 0.090], [0.22, 0.150],
                        [0.28, 0.215], [0.36, 0.290], [0.45, 0.380], [0.54, 0.460],
                    ],
                },
                "high": {
                    "max_lots": 4,
                    "max_trades_day": 4,
                    "daily_profit_lock": 5500.0,
                    "stop_fraction": 0.095,
                    "target_fraction": 0.55,
                    "breakeven_activation_fraction": 0.065,
                    "breakeven_lock_fraction": 0.028,
                    "trail_activation": 0.13,
                    "trail_fraction": 0.048,
                    "step_trail_levels": [
                        [0.085, 0.035], [0.15, 0.090], [0.22, 0.150],
                        [0.28, 0.215], [0.36, 0.290], [0.45, 0.380], [0.54, 0.460],
                    ],
                },
                "crisis": {
                    "max_lots": 2,
                    "daily_loss_limit": 2400.0,
                    "daily_profit_lock": 2100.0,
                    "target_delta": 0.60,
                    "stop_fraction": 0.10,
                    "target_fraction": 0.32,
                    "breakeven_activation_fraction": None,
                    "trail_activation": None,
                    "step_trail_levels": [[0.10, 0.0], [0.16, 0.08], [0.22, 0.14], [0.28, 0.22]],
                },
            },
        }
        self.bots["champion_of_50"] = SingleBot(
            "champion_of_50", "Champion of 50", "Apex OmniTitan X [Hybrid]", cfg_champion_50, self.initial_wallet, auto_start=False
        )

    def _load_dropin_bots(self):
        """Auto-load custom JSON bot definitions placed in the repository's `bots/` folder."""
        if not self.bots_dir or not self.bots_dir.is_dir():
            return
        for path in sorted(self.bots_dir.glob("*.json")):
            if path.name.startswith(("_", ".")) or path.name.endswith(".example.json"):
                continue
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except Exception as exc:
                logger.warning("Skipping invalid drop-in bot file %s: %s", path.name, exc)
                continue
            specs = raw if isinstance(raw, list) else ([raw] if isinstance(raw, dict) else [])
            for item in specs:
                if not isinstance(item, dict):
                    continue
                bot_id = str(item.get("bot_id", "")).strip()
                cfg = item.get("cfg")
                if not bot_id or bot_id in self.bots or not isinstance(cfg, dict):
                    continue
                name = str(item.get("name") or bot_id)
                family = str(item.get("family") or "Custom Drop-In")
                enabled = bool(item.get("enabled", True))
                bot = SingleBot(bot_id, name, family, cfg, self.initial_wallet, auto_start=False)
                if not enabled:
                    bot.enabled = False
                    bot.status = "STOPPED"
                self.bots[bot_id] = bot
                logger.info("Loaded drop-in bot '%s' (%s) from %s [enabled=%s]", bot_id, name, path.name, enabled)

    def set_starting_wallet(self, amount: float):
        """Update common starting wallet for all bots."""
        with self._lock:
            self.initial_wallet = float(amount)
            for bot in self.bots.values():
                with bot._state_lock:
                    bot.reset_wallet(self.initial_wallet)
            self._record_event("ALL", "WALLET_UPDATE", {"amount": self.initial_wallet})
            self.save_state()
            logger.info("Updated starting wallet to Rs %.2f across all bots", self.initial_wallet)

    def toggle_bot(self, bot_id: str, enabled: Optional[bool] = None) -> bool:
        """Start individual bot script (ON) or terminate it cleanly (OFF)."""
        with self._lock:
            bot = self.bots.get(bot_id)
            if not bot:
                return False
            target = (not bot.enabled) if enabled is None else bool(enabled)
            if target:
                bot.start()
            else:
                bot.terminate("BOT_TERMINATED")
            self._record_event(bot_id, "BOT_ON" if target else "BOT_OFF", {"status": bot.status})
            self.save_state()
            new_state = bot.enabled
        self._notify_feed_requirements()
        return new_state

    def toggle_all_bots(self, enabled: Optional[bool] = None) -> bool:
        """Master toggle: start or terminate all individual bot scripts simultaneously."""
        with self._lock:
            if enabled is None:
                any_enabled = any(bot.enabled for bot in self.bots.values())
                target_state = not any_enabled
            else:
                target_state = bool(enabled)

            for bot in self.bots.values():
                if target_state:
                    bot.start()
                else:
                    bot.terminate("ALL_BOTS_STOPPED")
            self._record_event("ALL", "ALL_BOTS_ON" if target_state else "ALL_BOTS_OFF", {})
            self.save_state()
            logger.info("Master Bot Toggle: all bots set to enabled=%s", target_state)
        self._notify_feed_requirements()
        return target_state

    def _reset_single_bot_locked(
        self,
        bot: SingleBot,
        reason: str = "INDIVIDUAL_RESET",
        preserve_enabled: bool = False,
        archive_prior: bool = True,
    ):
        """Reset one bot cleanly under hub lock, archiving prior live cycle/resets first."""
        with bot._state_lock:
            if archive_prior and not self.in_replay_mode:
                if bot.current_trade:
                    bot.force_exit_trade(f"{reason}_SQUAREOFF")
                had_prior = (
                    bot.trade_count > 0
                    or len(bot.trades_history) > 0
                    or bot.day_pnl != 0.0
                    or bot.wallet != bot.initial_wallet
                    or bot.status in ("DAILY_LOCK", "DAILY_LOSS_LIMIT", "MAX_TRADES_MET", "KILLED")
                )
                if had_prior:
                    snap = {
                        "reset_number": len(bot.reset_history) + 1,
                        "reset_at": datetime.now(IST).strftime("%H:%M:%S"),
                        "reason": reason,
                        "pre_reset_wallet": round(bot.wallet, 2),
                        "pre_reset_initial_wallet": round(bot.initial_wallet, 2),
                        "pre_reset_day_pnl": round(bot.day_pnl, 2),
                        "pre_reset_trade_count": bot.trade_count,
                        "pre_reset_win_count": bot.win_count,
                        "pre_reset_loss_count": bot.loss_count,
                        "pre_reset_status": bot.status,
                        "trades_history": copy.deepcopy(bot.trades_history),
                    }
                    bot.reset_history.append(snap)
                    self._record_event(
                        bot.bot_id,
                        reason,
                        {
                            "reset_number": snap["reset_number"],
                            "pre_reset_day_pnl": snap["pre_reset_day_pnl"],
                            "pre_reset_trades": snap["pre_reset_trade_count"],
                        },
                    )

            bot.wallet = self.initial_wallet
            bot.initial_wallet = self.initial_wallet
            bot.day_pnl = 0.0
            bot.trade_count = 0
            bot.win_count = 0
            bot.loss_count = 0
            bot.loss_streak = 0
            bot.current_trade = None
            bot.trades_history = []
            bot.last_exit_time = None
            bot._last_sim_exit_dt = None
            bot._sim_day_date = None
            bot._sim_day_pnl = 0.0
            bot._sim_day_trades = 0
            bot.status = "SCANNING" if (not preserve_enabled or bot.enabled) else "STOPPED"
        if not preserve_enabled or bot.enabled:
            bot.start()
        with bot._state_lock:
            bot.radar = {
                "targeted_option": "NIFTY ATM CE" if bot.enabled else "NONE (OFF)",
                "target_delta": bot.cfg.get("target_delta", 0.60) if bot.enabled else 0.0,
                "estimated_premium": 0.0,
                "status": "Session Reset — Scanning" if bot.enabled else "SCRIPT TERMINATED (OFF)",
                "checklist": [],
                "notes": (
                    f"Fresh wallet Rs {self.initial_wallet:,.2f} ready."
                    if bot.enabled
                    else "Bot script terminated. Zero market data / access code spent."
                ),
            }

    def reset_bot(self, bot_id: str) -> bool:
        """Reset one individual bot back to fresh initial wallet, 0 trades, and active script."""
        with self._lock:
            bot = self.bots.get(bot_id)
            if not bot:
                return False
            self._reset_single_bot_locked(bot, reason="INDIVIDUAL_RESET")
            self.save_state()
            logger.info("Reset individual bot [%s] to fresh initial wallet Rs %.2f", bot.name, self.initial_wallet)
        self._notify_feed_requirements()
        return True

    def reset_bots(self) -> int:
        """Reset all bots to fresh initial session state with clean wallet and 0 trades."""
        with self._lock:
            for bot in self.bots.values():
                self._reset_single_bot_locked(bot, reason="RESET_ALL")
            self.save_state()
            logger.info("Reset all %d bots to fresh initial wallet Rs %.2f", len(self.bots), self.initial_wallet)
            n_bots = len(self.bots)
        self._notify_feed_requirements()
        return n_bots

    def enter_replay_mode(self):
        """Snapshot live bots without losing today's live state, then prep ON bots for fast replay."""
        with self._lock:
            if not self.in_replay_mode:
                self._live_session_backup = {
                    "date": datetime.now(IST).strftime("%Y-%m-%d"),
                    "initial_wallet": self.initial_wallet,
                    "bots": {
                        b_id: {
                            "enabled": bot.enabled,
                            "status": bot.status,
                            "wallet": bot.wallet,
                            "initial_wallet": bot.initial_wallet,
                            "day_pnl": bot.day_pnl,
                            "trade_count": bot.trade_count,
                            "win_count": bot.win_count,
                            "loss_count": bot.loss_count,
                            "loss_streak": bot.loss_streak,
                            "current_trade": copy.deepcopy(bot.current_trade),
                            "trades_history": copy.deepcopy(bot.trades_history),
                            "reset_history": copy.deepcopy(bot.reset_history),
                        }
                        for b_id, bot in self.bots.items()
                    },
                }
            self.in_replay_mode = True
            for bot in self.bots.values():
                self._reset_single_bot_locked(
                    bot,
                    reason="REPLAY_START",
                    preserve_enabled=True,
                    archive_prior=False,
                )
        self._notify_feed_requirements()

    def exit_replay_mode(self):
        """Exit replay and seamlessly restore prior live bots, trades, P/L, and resets."""
        with self._lock:
            backup = self._live_session_backup
            self._live_session_backup = None
            self.in_replay_mode = False
            today_s = datetime.now(IST).strftime("%Y-%m-%d")
            if backup and backup.get("date") == today_s:
                self.initial_wallet = float(backup.get("initial_wallet", self.initial_wallet))
                saved_bots = backup.get("bots", {})
                for b_id, bot in self.bots.items():
                    sb = saved_bots.get(b_id)
                    if not sb:
                        continue
                    with bot._state_lock:
                        bot.enabled = bool(sb.get("enabled", True))
                        bot.status = sb.get("status", "SCANNING")
                        bot.wallet = float(sb.get("wallet", self.initial_wallet))
                        bot.initial_wallet = float(sb.get("initial_wallet", self.initial_wallet))
                        bot.day_pnl = float(sb.get("day_pnl", 0.0))
                        bot.trade_count = int(sb.get("trade_count", 0))
                        bot.win_count = int(sb.get("win_count", 0))
                        bot.loss_count = int(sb.get("loss_count", 0))
                        bot.loss_streak = int(sb.get("loss_streak", 0))
                        bot.current_trade = copy.deepcopy(sb.get("current_trade"))
                        bot.trades_history = copy.deepcopy(sb.get("trades_history", []))
                        bot.reset_history = copy.deepcopy(sb.get("reset_history", []))
                        bot._sim_day_date = None
                        bot._sim_day_pnl = 0.0
                        bot._sim_day_trades = 0
                    if bot.enabled:
                        bot.start()
                    else:
                        bot.terminate("RESTORED_OFF")
            else:
                for bot in self.bots.values():
                    self._reset_single_bot_locked(bot, reason="EXIT_REPLAY", archive_prior=False)
            self.save_state()
        self._notify_feed_requirements()

    def kill_bot(self, bot_id: str) -> bool:
        """Execute Kill Switch on a specific bot."""
        with self._lock:
            bot = self.bots.get(bot_id)
            if not bot:
                return False
            with bot._state_lock:
                if bot.current_trade:
                    bot.force_exit_trade("MANUAL_KILL_SWITCH")
                    logger.warning("Kill switch triggered for %s. Position squared off.", bot.name)
                else:
                    bot.status = "KILLED"
            self._record_event(bot_id, "MANUAL_KILL_SWITCH", {})
            self.save_state()
        self._notify_feed_requirements()
        return True

    def kill_all_bots(self) -> int:
        """Emergency master kill switch: square off all positions across all bots."""
        with self._lock:
            killed_count = 0
            for bot in self.bots.values():
                with bot._state_lock:
                    if bot.current_trade:
                        bot.force_exit_trade("EMERGENCY_KILL_ALL")
                        killed_count += 1
                    bot.status = "KILLED"
            self._record_event("ALL", "EMERGENCY_KILL_ALL", {"killed_count": killed_count})
            self.save_state()
            logger.warning("Master Kill Switch activated: %d positions closed.", killed_count)
        self._notify_feed_requirements()
        return killed_count

    def save_daily_arena_archive(self, archive_date: Optional[str] = None) -> Path:
        """Export full-day per-bot trades, P/L, pre-reset cycles, and resets to 'arena- <date>'."""
        with self._lock:
            target_date = archive_date or datetime.now(IST).strftime("%Y-%m-%d")
            folder = self.archive_root / f"arena- {target_date}"
            bots_dir = folder / "bots"
            bots_dir.mkdir(parents=True, exist_ok=True)

            full_payload: Dict[str, Any] = {
                "date": target_date,
                "generated_at": datetime.now(IST).isoformat(),
                "initial_wallet": self.initial_wallet,
                "bots": {},
                "events_timeline": copy.deepcopy(self.day_events),
            }

            summary_lines = [
                f"# ALL Trade UI — Daily Arena Archive (`{target_date}`)",
                "",
                f"- **Generated:** `{datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}`",
                f"- **Default Starting Wallet:** Rs {self.initial_wallet:,.2f}",
                "",
                "## Combined Bot Performance Summary (Including Pre-Reset Cycles)",
                "",
                "| Bot | Current Status | Resets | Current P/L | Cumulative Day P/L | Total Trades | End Wallet |",
                "|---|---|---:|---:|---:|---:|---:|",
            ]

            for b_id, bot in self.bots.items():
                with bot._state_lock:
                    resets = copy.deepcopy(bot.reset_history)
                    cur_trades = copy.deepcopy(bot.trades_history)
                    active_trade = copy.deepcopy(bot.current_trade)

                    cum_pnl = float(bot.day_pnl) + sum(float(r.get("pre_reset_day_pnl", 0.0)) for r in resets)
                    cum_trades = int(bot.trade_count) + sum(int(r.get("pre_reset_trade_count", 0)) for r in resets)

                    cycles: List[Dict[str, Any]] = []
                    for r in resets:
                        cyc_trades = sorted(
                            list(r.get("trades_history", [])),
                            key=lambda tr: str(tr.get("entered_at", "")),
                        )
                        cycles.append({
                            "cycle_label": f"Before Reset #{r['reset_number']} ({r['reset_at']} - {r['reason']})",
                            "reset_meta": r,
                            "wallet": r["pre_reset_wallet"],
                            "initial_wallet": r.get("pre_reset_initial_wallet", self.initial_wallet),
                            "day_pnl": r["pre_reset_day_pnl"],
                            "trade_count": r["pre_reset_trade_count"],
                            "win_count": r["pre_reset_win_count"],
                            "loss_count": r["pre_reset_loss_count"],
                            "status": r["pre_reset_status"],
                            "trades": cyc_trades,
                        })
                    final_trades = sorted(cur_trades, key=lambda tr: str(tr.get("entered_at", "")))
                    cycles.append({
                        "cycle_label": "Final / Current Session",
                        "reset_meta": None,
                        "wallet": round(bot.wallet, 2),
                        "initial_wallet": round(bot.initial_wallet, 2),
                        "day_pnl": round(bot.day_pnl, 2),
                        "trade_count": bot.trade_count,
                        "win_count": bot.win_count,
                        "loss_count": bot.loss_count,
                        "status": bot.status,
                        "active_trade": active_trade,
                        "trades": final_trades,
                    })

                    bot_events = [ev for ev in self.day_events if ev.get("bot_id") in (b_id, "ALL")]
                    bot_doc = {
                        "bot_id": b_id,
                        "name": bot.name,
                        "family": bot.family,
                        "enabled": bot.enabled,
                        "status": bot.status,
                        "resets_count": len(resets),
                        "cumulative_day_pnl": round(cum_pnl, 2),
                        "cumulative_trade_count": cum_trades,
                        "current_wallet": round(bot.wallet, 2),
                        "current_day_pnl": round(bot.day_pnl, 2),
                        "reset_history": resets,
                        "cycles": cycles,
                        "events": bot_events,
                    }
                    full_payload["bots"][b_id] = bot_doc

                    summary_lines.append(
                        f"| {bot.name} (`{b_id}`) | {bot.status} | {len(resets)} | "
                        f"Rs {bot.day_pnl:+,.2f} | Rs {cum_pnl:+,.2f} | {cum_trades} | Rs {bot.wallet:,.2f} |"
                    )

                    # Write per-bot JSON & CSV
                    (bots_dir / f"{b_id}.json").write_text(json.dumps(bot_doc, indent=2), encoding="utf-8")
                    with (bots_dir / f"{b_id}_trades.csv").open("w", newline="", encoding="utf-8") as cf:
                        w = csv.writer(cf)
                        w.writerow([
                            "cycle", "trade_id", "strike", "expiry", "lots", "qty",
                            "entry_time", "entry_price", "exit_time", "exit_price",
                            "gross_pnl", "net_pnl", "exit_reason",
                        ])
                        for cy in cycles:
                            for tr in cy.get("trades", []):
                                w.writerow([
                                    cy["cycle_label"],
                                    tr.get("trade_id", ""),
                                    tr.get("strike", ""),
                                    tr.get("expiry_label") or tr.get("expiry", ""),
                                    tr.get("lots", ""),
                                    tr.get("qty", ""),
                                    tr.get("entered_at", ""),
                                    tr.get("entry_price", ""),
                                    tr.get("exit_time", ""),
                                    tr.get("exit_price", ""),
                                    tr.get("gross_pnl", ""),
                                    tr.get("net_pnl", ""),
                                    tr.get("exit_reason", ""),
                                ])

            # Per-bot detailed Markdown breakdown including every reset
            summary_lines.append("")
            summary_lines.append("## Detailed Bot Cycles, Trades & Resets")
            for b_id, bot_doc in full_payload["bots"].items():
                summary_lines.extend([
                    "",
                    f"### {bot_doc['name']} (`{b_id}`)",
                    f"- Strategy: `{bot_doc['family']}` | Resets: `{bot_doc['resets_count']}` | Cumulative P/L: `Rs {bot_doc['cumulative_day_pnl']:+,.2f}`",
                ])
                if bot_doc["reset_history"]:
                    summary_lines.append("- **Resets Today:**")
                    for r in bot_doc["reset_history"]:
                        summary_lines.append(
                            f"  - Reset #{r['reset_number']} at `{r['reset_at']}` ({r['reason']}): "
                            f"Prior P/L `Rs {r['pre_reset_day_pnl']:+,.2f}`, Wallet `Rs {r['pre_reset_wallet']:,.2f}`, "
                            f"Trades `{r['pre_reset_trade_count']}` ({r['pre_reset_win_count']}W/{r['pre_reset_loss_count']}L)"
                        )
                for cy in bot_doc["cycles"]:
                    summary_lines.append(f"- **Cycle:** {cy['cycle_label']} — P/L `Rs {cy['day_pnl']:+,.2f}`, Trades `{cy['trade_count']}`")
                    if cy.get("trades"):
                        summary_lines.append("  | Strike | Entry Time | Entry | Exit Time | Exit | Net P/L | Reason |")
                        summary_lines.append("  |---|---|---:|---|---:|---:|---|")
                        for tr in cy["trades"]:
                            summary_lines.append(
                                f"  | {tr.get('strike','')} | {tr.get('entered_at','')} | {tr.get('entry_price',0):.2f} | "
                                f"{tr.get('exit_time','')} | {float(tr.get('exit_price') or 0):.2f} | "
                                f"{float(tr.get('net_pnl') or 0):+.2f} | {tr.get('exit_reason','')} |"
                            )

            (folder / "SUMMARY.md").write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
            (folder / "arena_full_day.json").write_text(json.dumps(full_payload, indent=2), encoding="utf-8")

            with (folder / "resets_and_events.csv").open("w", newline="", encoding="utf-8") as ef:
                ew = csv.writer(ef)
                ew.writerow(["timestamp", "bot_id", "event", "details_json"])
                for ev in self.day_events:
                    ew.writerow([ev.get("timestamp", ""), ev.get("bot_id", ""), ev.get("event", ""), json.dumps(ev.get("details", {}))])

            # Single combined chronological timeline CSV across ALL bots & resets (trade1,2,3 -> RESET -> next trades)
            all_csv_path = folder / "all_bots_summary.csv"
            with all_csv_path.open("w", newline="", encoding="utf-8") as af:
                aw = csv.writer(af)
                aw.writerow([
                    "bot_name", "bot_id", "row_type", "cycle_num", "step",
                    "trade_id", "strike", "expiry", "lots", "qty",
                    "entry_time", "entry_price", "exit_time", "exit_price",
                    "gross_pnl", "net_pnl", "cumulative_bot_pnl", "wallet_after", "reason_or_notes",
                ])
                grand_pnl = 0.0
                grand_trades = 0
                grand_resets = 0

                for b_id, bot_doc in full_payload["bots"].items():
                    running_cum_pnl = 0.0
                    bot_trade_seq = 0
                    for cyc_idx, cy in enumerate(bot_doc["cycles"]):
                        running_wallet = float(cy.get("initial_wallet", self.initial_wallet))
                        for cyc_trade_idx, tr in enumerate(cy.get("trades", []), start=1):
                            bot_trade_seq += 1
                            net_p = float(tr.get("net_pnl") or 0.0)
                            gross_p = float(tr.get("gross_pnl") or 0.0)
                            running_cum_pnl = round(running_cum_pnl + net_p, 2)
                            running_wallet = round(running_wallet + net_p, 2)
                            aw.writerow([
                                bot_doc["name"], b_id, "TRADE", cyc_idx,
                                f"Trade {bot_trade_seq} (C{cyc_idx}.{cyc_trade_idx})",
                                tr.get("trade_id", ""),
                                tr.get("strike", ""),
                                tr.get("expiry_label") or tr.get("expiry", ""),
                                tr.get("lots", ""),
                                tr.get("qty", ""),
                                tr.get("entered_at", ""),
                                tr.get("entry_price", ""),
                                tr.get("exit_time", ""),
                                tr.get("exit_price", ""),
                                round(gross_p, 2),
                                round(net_p, 2),
                                running_cum_pnl,
                                running_wallet,
                                tr.get("exit_reason", ""),
                            ])

                        rm = cy.get("reset_meta")
                        if rm is not None:
                            grand_resets += 1
                            aw.writerow([
                                bot_doc["name"], b_id, "RESET", cyc_idx,
                                f"RESET #{rm['reset_number']}",
                                f"RST_{rm['reset_number']}", "—", "—", "", "",
                                rm.get("reset_at", ""), "", "", "",
                                "", round(float(rm.get("pre_reset_day_pnl", 0.0)), 2),
                                running_cum_pnl, round(float(self.initial_wallet), 2),
                                f"{rm.get('reason', 'RESET')} | Cycle P/L Rs {rm.get('pre_reset_day_pnl', 0.0):+,.2f} | Prior Wallet Rs {rm.get('pre_reset_wallet', 0.0):,.2f} -> Reset to Rs {self.initial_wallet:,.2f}",
                            ])

                    grand_pnl = round(grand_pnl + float(bot_doc["cumulative_day_pnl"]), 2)
                    grand_trades += int(bot_doc["cumulative_trade_count"])
                    aw.writerow([
                        bot_doc["name"], b_id, "BOT_TOTAL", "",
                        f"Total ({bot_doc['cumulative_trade_count']} trades, {bot_doc['resets_count']} resets)",
                        "", "", "", "", "", "", "", "", "", "",
                        round(float(bot_doc["current_day_pnl"]), 2),
                        round(float(bot_doc["cumulative_day_pnl"]), 2),
                        round(float(bot_doc["current_wallet"]), 2),
                        f"Final Status: {bot_doc['status']} | Current Cycle P/L: Rs {bot_doc['current_day_pnl']:+,.2f}",
                    ])

                aw.writerow([
                    "ALL BOTS", "ALL", "GRAND_TOTAL", "",
                    f"{grand_trades} trades | {grand_resets} resets",
                    "", "", "", "", "", "", "", "", "", "", "",
                    grand_pnl, "",
                    f"Combined Cumulative P/L across all {len(full_payload['bots'])} bots",
                ])

            logger.info("Saved full daily arena archive cleanly to %s", folder)
            return folder

    def update_market_cycle(self, market_snapshot: dict):
        """Dispatch market tick strictly to currently ON bot scripts concurrently."""
        with self._lock:
            active_bots = [b for b in self.bots.values() if b.enabled]
            stopped_bots = [b for b in self.bots.values() if not b.enabled]

            # Ensure stopped bots remain terminated and consume ZERO market data
            for bot in stopped_bots:
                if bot.status != "STOPPED" or bot.is_script_alive():
                    bot.terminate("BOT_STOPPED")

            spot = market_snapshot.get("spot", 25350.0)
            vix = market_snapshot.get("vix", 17.7)
            feats = market_snapshot.get("features", {})
            is_open = bool(market_snapshot.get("is_open", False))
            is_sim = bool(market_snapshot.get("simulation", False))
            market_status = market_snapshot.get("status", "UNKNOWN")
            active_expiry = market_snapshot.get("active_expiry") or "2026-12-01"
            expiry_label = market_snapshot.get("expiry_label") or "01 DEC 26"
            expiry_dte = int(market_snapshot.get("expiry_dte") or 6)
            chain = market_snapshot.get("option_chain") or {}

            now_dt = datetime.now(IST)
            today_s = now_dt.strftime("%Y-%m-%d")
            sim_time = market_snapshot.get("sim_timestamp")
            sim_dt: Optional[datetime] = None
            sim_date: Optional[str] = None
            sim_clock_full: Optional[str] = None
            if sim_time:
                now_clock = sim_time.split("T")[-1][:8] if "T" in sim_time else sim_time[:8]
                sim_date = sim_time[:10]
                sim_clock_full = f"{sim_time[5:10]} {now_clock[:5]}"
                try:
                    sim_dt = datetime.fromisoformat(sim_time)
                except Exception:
                    sim_dt = None
            else:
                now_clock = now_dt.strftime("%H:%M:%S")

            dispatched: List[Tuple[SingleBot, threading.Event, List[bool]]] = []
            prev_trade_ids: Dict[str, Optional[str]] = {}
            if active_bots:
                atm_strike = int(round(spot / 50.0) * 50)
                t_exp = max(1, expiry_dte) / 365.0

                need_atm_pricing = any(b.current_trade is None for b in active_bots)
                ce_price, ce_delta = 0.0, 0.55
                pe_price, pe_delta = 0.0, -0.45
                if need_atm_pricing:
                    ce_q = chain.get(f"{atm_strike}_CE") if (not is_sim and chain) else None
                    pe_q = chain.get(f"{atm_strike}_PE") if (not is_sim and chain) else None

                    if ce_q and float(ce_q.get("ltp") or ce_q.get("ask") or 0) > 0:
                        ce_price = round(float(ce_q.get("ltp") or ce_q["ask"]), 2)
                        ce_delta = float(ce_q.get("delta") or 0.55)
                    else:
                        ce_price, ce_delta = bs_option_metrics(spot, atm_strike, t_exp, vix, is_ce=True)

                    if pe_q and float(pe_q.get("ltp") or pe_q.get("ask") or 0) > 0:
                        pe_price = round(float(pe_q.get("ltp") or pe_q["ask"]), 2)
                        pe_delta = float(pe_q.get("delta") or -0.45)
                    else:
                        pe_price, pe_delta = bs_option_metrics(spot, atm_strike, t_exp, vix, is_ce=False)

                ctx = {
                    "spot": spot,
                    "vix": vix,
                    "feats": feats,
                    "is_open": is_open,
                    "is_sim": is_sim,
                    "market_status": market_status,
                    "active_expiry": active_expiry,
                    "expiry_label": expiry_label,
                    "expiry_dte": expiry_dte,
                    "now_dt": now_dt,
                    "now_clock": now_clock,
                    "sim_dt": sim_dt,
                    "sim_date": sim_date,
                    "sim_clock_full": sim_clock_full,
                    "replay_completed": bool(market_snapshot.get("replay_completed", False)),
                    "replay_final": bool(market_snapshot.get("replay_final", False)),
                    "atm_strike": atm_strike,
                    "t_exp": t_exp,
                    "chain": chain,
                    "ce_price": ce_price,
                    "ce_delta": ce_delta,
                    "pe_price": pe_price,
                    "pe_delta": pe_delta,
                }

                for bot in active_bots:
                    if not bot.is_script_alive():
                        bot.start()
                    prev_trade_ids[bot.bot_id] = bot.current_trade["trade_id"] if bot.current_trade else None
                    done_ev, changed_box = bot.dispatch_tick(ctx)
                    dispatched.append((bot, done_ev, changed_box))

        for _, done_ev, _ in dispatched:
            done_ev.wait(timeout=0.5)

        any_changed = any(changed_box[0] for _, _, changed_box in dispatched)
        if any_changed:
            with self._lock:
                for bot, _, changed_box in dispatched:
                    if changed_box[0] and not self.in_replay_mode:
                        prev_tid = prev_trade_ids.get(bot.bot_id)
                        if bot.current_trade and bot.current_trade["trade_id"] != prev_tid:
                            self._record_event(bot.bot_id, "TRADE_ENTRY", {
                                "trade_id": bot.current_trade["trade_id"],
                                "strike": bot.current_trade["strike"],
                                "entry_price": bot.current_trade["entry_price"],
                            })
                        elif bot.trades_history and prev_tid and bot.trades_history[0].get("trade_id") == prev_tid:
                            self._record_event(bot.bot_id, "TRADE_EXIT", {
                                "trade_id": prev_tid,
                                "exit_price": bot.trades_history[0].get("exit_price"),
                                "net_pnl": bot.trades_history[0].get("net_pnl"),
                                "reason": bot.trades_history[0].get("exit_reason"),
                            })
                self.save_state(async_write=True)
            threading.Thread(target=self._notify_feed_requirements, daemon=True).start()

        # Auto-archive full day data into "arena- <YYYY-MM-DD>" once EOD trading is done
        if not is_sim:
            should_eod = False
            with self._lock:
                has_activity = any(
                    b.trade_count > 0 or len(b.reset_history) > 0 or b.current_trade is not None
                    for b in self.bots.values()
                )
                is_closed_after_hours = (now_clock >= "15:30:00") or (
                    now_clock >= "15:15:00"
                    and not is_open
                    and market_status not in ("OFFLINE", "IDLE (0 BOTS ON)")
                )
                if has_activity and is_closed_after_hours and (self._eod_archived_date != today_s or any_changed):
                    should_eod = True
            if should_eod:
                self.save_daily_arena_archive(today_s)
                with self._lock:
                    self._eod_archived_date = today_s

    def get_state(self, market_snapshot: dict) -> dict:
        """Produce full serialized JSON snapshot for frontend."""
        with self._lock:
            market_ui = {k: v for k, v in market_snapshot.items() if k != "option_chain"}
            is_replay = bool(market_snapshot.get("simulation", False) or self.in_replay_mode)
            bots_data = {}
            for b_id, bot in self.bots.items():
                with bot._state_lock:
                    hist_slice = bot.trades_history if is_replay else bot.trades_history[:50]
                    bots_data[b_id] = {
                        "id": bot.bot_id,
                        "name": bot.name,
                        "family": bot.family,
                        "enabled": bot.enabled,
                        "script_running": bot.is_script_alive(),
                        "status": bot.status,
                        "wallet": round(bot.wallet, 2),
                        "initial_wallet": round(bot.initial_wallet, 2),
                        "day_pnl": round(bot.day_pnl, 2),
                        "day_pnl_pct": round((bot.day_pnl / bot.initial_wallet) * 100.0, 2) if bot.initial_wallet > 0 else 0.0,
                        "trade_count": bot.trade_count,
                        "win_count": bot.win_count,
                        "loss_count": bot.loss_count,
                        "win_rate": round((bot.win_count / bot.trade_count) * 100.0, 1) if bot.trade_count > 0 else 0.0,
                        "resets_count": len(bot.reset_history),
                        "current_trade": copy.deepcopy(bot.current_trade),
                        "radar": copy.deepcopy(bot.radar),
                        "trades_history": copy.deepcopy(hist_slice),
                    }

            return {
                "server_time": datetime.now(IST).strftime("%H:%M:%S"),
                "common_starting_wallet": self.initial_wallet,
                "market": market_ui,
                "bots": bots_data,
            }
