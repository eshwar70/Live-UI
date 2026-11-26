#!/usr/bin/env python3
"""Technical indicators and Option Pricing Engine for ALL Trade UI.
"""
from __future__ import annotations

import math
from typing import Dict, List, Optional, Tuple


def norm_cdf(x: float) -> float:
    """Standard normal cumulative distribution function."""
    return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0


def bs_option_metrics(
    spot: float, strike: float, t_years: float, vix: float, is_ce: bool = True
) -> Tuple[float, float]:
    """Calculate Black-Scholes option theoretical price and delta.
    
    Returns (price, delta).
    """
    t = max(0.0005, t_years)
    sigma = max(0.08, vix / 100.0)
    r = 0.065  # 6.5% risk-free rate (India RBI repo rate)
    
    d1 = (math.log(spot / strike) + (r + 0.5 * sigma * sigma) * t) / (sigma * math.sqrt(t))
    d2 = d1 - sigma * math.sqrt(t)
    
    if is_ce:
        price = spot * norm_cdf(d1) - strike * math.exp(-r * t) * norm_cdf(d2)
        delta = norm_cdf(d1)
    else:
        price = strike * math.exp(-r * t) * norm_cdf(-d2) - spot * norm_cdf(-d1)
        delta = norm_cdf(d1) - 1.0  # Negative delta for puts
        
    intrinsic = max(0.0, (spot - strike) if is_ce else (strike - spot))
    fair_price = max(price, intrinsic + 2.0)
    return round(fair_price, 2), round(delta, 3)


def calc_ema(values: List[float], period: int) -> float:
    """Compute current exponential moving average."""
    if not values:
        return 0.0
    if len(values) < period:
        return sum(values) / len(values)
    k = 2.0 / (period + 1.0)
    ema_val = sum(values[:period]) / period
    for val in values[period:]:
        ema_val = val * k + ema_val * (1.0 - k)
    return ema_val


def calc_rsi(values: List[float], period: int = 14) -> float:
    """Compute Relative Strength Index."""
    if len(values) < period + 1:
        return 50.0
    changes = [b - a for a, b in zip(values[-period - 1 : -1], values[-period:])]
    gains = sum(max(v, 0.0) for v in changes)
    losses = sum(max(-v, 0.0) for v in changes)
    if losses == 0.0:
        return 100.0
    if gains == 0.0:
        return 0.0
    rs = gains / losses
    return round(100.0 - (100.0 / (1.0 + rs)), 2)


def calc_supertrend(
    highs: List[float], lows: List[float], closes: List[float], period: int = 10, mult: float = 2.0
) -> Tuple[int, float, float, float]:
    """Compute Supertrend indicator.
    
    Returns: (direction [1 for bull, -1 for bear], upper_band, lower_band, current_atr)
    """
    n = len(closes)
    if n < 2:
        return 1, highs[-1], lows[-1], highs[-1] - lows[-1]

    trs = [highs[0] - lows[0]]
    for i in range(1, n):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        trs.append(tr)

    atr = [trs[0]]
    alpha = 1.0 / period
    for i in range(1, n):
        atr.append(alpha * trs[i] + (1.0 - alpha) * atr[-1])

    direction = [1] * n
    upper = [0.0] * n
    lower = [0.0] * n

    for i in range(n):
        hl2 = (highs[i] + lows[i]) / 2.0
        up = hl2 + mult * atr[i]
        lo = hl2 - mult * atr[i]

        if i > 0:
            if closes[i - 1] > lower[i - 1]:
                lo = max(lo, lower[i - 1])
            if closes[i - 1] < upper[i - 1]:
                up = min(up, upper[i - 1])

            if closes[i] > upper[i - 1]:
                direction[i] = 1
            elif closes[i] < lower[i - 1]:
                direction[i] = -1
            else:
                direction[i] = direction[i - 1]

        upper[i] = up
        lower[i] = lo

    return direction[-1], upper[-1], lower[-1], atr[-1]


def synthesize_5m_bars(one_min_bars: List[dict]) -> List[dict]:
    """Aggregate 1-minute candles into completed 5-minute candles."""
    bars_5m = []
    bucket = []
    current_bucket_key = None

    for b in one_min_bars:
        ts_str = b.get("timestamp", "")
        # Format e.g. 2026-11-23T09:16:00+05:30
        try:
            minute_part = int(ts_str[14:16]) if len(ts_str) >= 16 else 0
            bucket_min = (minute_part // 5) * 5
            bucket_key = ts_str[:14] + f"{bucket_min:02d}:00"
        except Exception:
            bucket_key = ts_str[:14]

        if current_bucket_key is None:
            current_bucket_key = bucket_key

        if bucket_key != current_bucket_key and bucket:
            # Complete the 5m bar
            c5 = {
                "timestamp": current_bucket_key,
                "open": bucket[0]["open"],
                "high": max(x["high"] for x in bucket),
                "low": min(x["low"] for x in bucket),
                "close": bucket[-1]["close"],
                "volume": sum(x.get("volume", 0) for x in bucket),
            }
            bars_5m.append(c5)
            bucket = []
            current_bucket_key = bucket_key

        bucket.append(b)

    if bucket:
        c5 = {
            "timestamp": current_bucket_key,
            "open": bucket[0]["open"],
            "high": max(x["high"] for x in bucket),
            "low": min(x["low"] for x in bucket),
            "close": bucket[-1]["close"],
            "volume": sum(x.get("volume", 0) for x in bucket),
        }
        bars_5m.append(c5)

    return bars_5m


def compute_market_features(
    one_min_bars: List[dict],
    current_spot: float,
    current_vix: float,
    today_date: Optional[str] = None,
) -> dict:
    """Compute all technical metrics across 1m and 5m bars for bot decision making."""
    if not one_min_bars:
        return {
            "price": current_spot,
            "vix": current_vix,
            "st5m_dir": 1,
            "trend_5m": 1,
            "eff_5m": 0.35,
            "esep_5m": 0.40,
            "e9": current_spot,
            "e21": current_spot,
            "e50": current_spot,
            "don10_hi": current_spot + 10,
            "don10_lo": current_spot - 10,
            "rsi14": 50.0,
            "roc3": 0.0,
            "day_open": current_spot,
            "vwap": current_spot,
            "bars_5m_count": 0,
            "today_bars_count": 0,
        }

    bars_5m = synthesize_5m_bars(one_min_bars)
    c_1m = [b["close"] for b in one_min_bars]
    h_1m = [b["high"] for b in one_min_bars]
    l_1m = [b["low"] for b in one_min_bars]
    v_1m = [b.get("volume", 1) for b in one_min_bars]

    c_5m = [b["close"] for b in bars_5m]
    h_5m = [b["high"] for b in bars_5m]
    l_5m = [b["low"] for b in bars_5m]

    p = current_spot if current_spot > 0 else c_1m[-1]

    # 5m SuperTrend
    if len(c_5m) >= 3:
        st5m_dir, st_up, st_lo, atr_5m = calc_supertrend(h_5m, l_5m, c_5m, period=10, mult=2.0)
    else:
        st5m_dir, st_up, st_lo, atr_5m = 1, p + 20, p - 20, 20.0

    # EMAs
    e9 = calc_ema(c_1m, 9)
    e21 = calc_ema(c_1m, 21)
    e50 = calc_ema(c_1m, 50)

    # Donchian 10-bar on 1m
    don_window = c_1m[-11:-1] if len(c_1m) >= 11 else c_1m
    don10_hi = max(don_window) if don_window else p + 5
    don10_lo = min(don_window) if don_window else p - 5

    # RSI & ROC
    rsi14 = calc_rsi(c_1m, 14)
    roc3 = (c_1m[-1] - c_1m[-4]) / c_1m[-4] if len(c_1m) >= 4 and c_1m[-4] > 0 else 0.0

    # Trend efficiency & separation on 5m
    if len(c_5m) >= 21:
        path = sum(abs(c_5m[i] - c_5m[i - 1]) for i in range(len(c_5m) - 20, len(c_5m)))
        eff_5m = abs(c_5m[-1] - c_5m[-21]) / path if path > 0 else 0.0
        e9_5m = calc_ema(c_5m, 9)
        e21_5m = calc_ema(c_5m, 21)
        esep_5m = abs(e9_5m - e21_5m) / atr_5m if atr_5m > 0 else 0.0
        trend_5m = 1 if e9_5m > e21_5m else -1
    else:
        eff_5m = 0.35
        esep_5m = 0.40
        trend_5m = st5m_dir

    # Day Open & VWAP scoped to latest session date (or today_date if specified)
    session_date = today_date or str(one_min_bars[-1].get("timestamp", ""))[:10]
    session_bars = [b for b in one_min_bars if str(b.get("timestamp", "")).startswith(session_date)]
    if session_bars:
        day_open = session_bars[0]["open"]
        tot_pv = sum(b["close"] * b.get("volume", 1) for b in session_bars)
        tot_v = sum(b.get("volume", 1) for b in session_bars)
        vwap = (tot_pv / tot_v) if tot_v > 0 else p
        today_bars_count = len(session_bars)
    else:
        day_open = p
        vwap = p
        today_bars_count = 0

    return {
        "price": p,
        "vix": current_vix,
        "st5m_dir": st5m_dir,
        "trend_5m": trend_5m,
        "eff_5m": round(eff_5m, 3),
        "esep_5m": round(esep_5m, 3),
        "e9": round(e9, 2),
        "e21": round(e21, 2),
        "e50": round(e50, 2),
        "don10_hi": round(don10_hi, 2),
        "don10_lo": round(don10_lo, 2),
        "rsi14": rsi14,
        "roc3": round(roc3, 5),
        "day_open": round(day_open, 2),
        "vwap": round(vwap, 2),
        "bars_5m_count": len(bars_5m),
        "today_bars_count": today_bars_count,
    }
