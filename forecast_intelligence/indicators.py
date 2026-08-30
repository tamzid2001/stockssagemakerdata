from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def _regime(value: float | None, *, low: float, high: float, low_name: str, high_name: str) -> str:
    if value is None:
        return "unavailable"
    if value <= low:
        return low_name
    if value >= high:
        return high_name
    return "neutral"


def _last(series: pd.Series) -> float | None:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    return float(clean.iloc[-1]) if not clean.empty else None


def point_in_time_indicators(frame: pd.DataFrame, *, as_of: str | pd.Timestamp) -> dict[str, Any]:
    """Calculate trailing-only indicators after removing all rows after ``as_of``."""
    if "timestamp" not in frame or "close" not in frame:
        raise ValueError("timestamp and close columns are required")
    data = frame.copy()
    data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True, errors="coerce")
    cutoff = pd.Timestamp(as_of)
    cutoff = cutoff.tz_localize("UTC") if cutoff.tzinfo is None else cutoff.tz_convert("UTC")
    data = data[data["timestamp"].notna() & (data["timestamp"] <= cutoff)].sort_values("timestamp")
    if data.empty:
        raise ValueError("no market data is available at or before as_of")
    close = pd.to_numeric(data["close"], errors="coerce")
    high = pd.to_numeric(data.get("high", close), errors="coerce")
    low = pd.to_numeric(data.get("low", close), errors="coerce")
    volume = pd.to_numeric(data.get("volume", pd.Series(index=data.index, dtype=float)), errors="coerce")

    sma20 = close.rolling(20, min_periods=20).mean()
    ema20 = close.ewm(span=20, adjust=False, min_periods=20).mean()
    ema12 = close.ewm(span=12, adjust=False, min_periods=12).mean()
    ema26 = close.ewm(span=26, adjust=False, min_periods=26).mean()
    macd = ema12 - ema26
    macd_signal = macd.ewm(span=9, adjust=False, min_periods=9).mean()
    macd_hist = macd - macd_signal

    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))

    rolling_std = close.rolling(20, min_periods=20).std(ddof=0)
    bb_upper = sma20 + 2 * rolling_std
    bb_lower = sma20 - 2 * rolling_std
    previous_close = close.shift(1)
    true_range = pd.concat(
        [(high - low).abs(), (high - previous_close).abs(), (low - previous_close).abs()], axis=1
    ).max(axis=1)
    atr14 = true_range.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    returns = close.pct_change(fill_method=None)
    realized_vol20 = returns.rolling(20, min_periods=20).std(ddof=0) * np.sqrt(252)
    roc12 = close.pct_change(12, fill_method=None) * 100
    momentum10 = close - close.shift(10)
    volume_sma20 = volume.rolling(20, min_periods=20).mean()
    volume_ratio = volume / volume_sma20.replace(0, np.nan)

    values = {
        "rsi": _last(rsi),
        "sma20": _last(sma20),
        "ema20": _last(ema20),
        "macd": _last(macd),
        "macd_signal": _last(macd_signal),
        "macd_histogram": _last(macd_hist),
        "bollinger_upper": _last(bb_upper),
        "bollinger_middle": _last(sma20),
        "bollinger_lower": _last(bb_lower),
        "atr14": _last(atr14),
        "realized_volatility20": _last(realized_vol20),
        "momentum10": _last(momentum10),
        "roc12": _last(roc12),
        "volume_ratio20": _last(volume_ratio),
    }
    last_close = float(close.dropna().iloc[-1])
    direction_score = 0
    direction_score += 1 if values["ema20"] is not None and last_close > values["ema20"] else -1
    direction_score += 1 if (values["macd_histogram"] or 0) > 0 else -1
    direction_score += 1 if (values["roc12"] or 0) > 0 else -1
    direction = "bullish" if direction_score >= 2 else "bearish" if direction_score <= -2 else "neutral"
    return {
        "as_of": data["timestamp"].iloc[-1].isoformat(),
        "history_points": int(len(data)),
        "latest_close": last_close,
        "rsi": {"value": values["rsi"], "regime": _regime(values["rsi"], low=30, high=70, low_name="oversold", high_name="overbought")},
        "sma20": {"value": values["sma20"], "regime": "above" if values["sma20"] and last_close > values["sma20"] else "below"},
        "ema20": {"value": values["ema20"], "regime": "above" if values["ema20"] and last_close > values["ema20"] else "below"},
        "macd": {"value": values["macd"], "signal": values["macd_signal"], "histogram": values["macd_histogram"], "regime": "bullish" if (values["macd_histogram"] or 0) > 0 else "bearish"},
        "bollinger": {"upper": values["bollinger_upper"], "middle": values["bollinger_middle"], "lower": values["bollinger_lower"]},
        "atr14": {"value": values["atr14"]},
        "realized_volatility20": {"value": values["realized_volatility20"]},
        "momentum10": {"value": values["momentum10"], "regime": "positive" if (values["momentum10"] or 0) > 0 else "negative"},
        "roc12": {"value": values["roc12"]},
        "volume_trend": {"ratio_to_sma20": values["volume_ratio20"], "regime": "above_average" if (values["volume_ratio20"] or 0) > 1 else "below_average"},
        "direction": direction,
        "direction_score": direction_score,
    }
