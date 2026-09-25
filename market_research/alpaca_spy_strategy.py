"""Deterministic SPY 0DTE hourly rules; all prices here are *underlying* prices.

The caller supplies completed one-minute bars and an immutable forecast made
at the start of the hour. This module never submits an order or fills a trade.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Iterable, Mapping
from zoneinfo import ZoneInfo


NEW_YORK = ZoneInfo("America/New_York")
MAX_PREMIUM_DOLLARS = Decimal("200")


@dataclass(frozen=True)
class Window:
    start: datetime
    end: datetime


@dataclass(frozen=True)
class MinuteBar:
    end: datetime
    close: float
    high: float
    low: float


def tradable_windows(market_open: datetime, market_close: datetime) -> tuple[Window, ...]:
    """End 30 minutes before the actual exchange close, including early closes."""
    if market_open.tzinfo is None or market_close.tzinfo is None:
        raise ValueError("exchange schedule must be timezone-aware")
    opening = market_open.astimezone(NEW_YORK)
    closing = market_close.astimezone(NEW_YORK)
    if opening.date() != closing.date() or opening.hour != 9 or opening.minute != 30:
        return ()
    last_exit = min(closing - timedelta(minutes=30), opening.replace(hour=15, minute=30))
    windows = []
    cursor = opening
    while cursor + timedelta(hours=1) <= last_exit:
        windows.append(Window(cursor, cursor + timedelta(hours=1)))
        cursor += timedelta(hours=1)
    return tuple(windows)


def active_window(now: datetime, windows: Iterable[Window]) -> Window | None:
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    local = now.astimezone(NEW_YORK)
    return next((window for window in windows if window.start <= local < window.end), None)


def forecast_levels(rows: Iterable[Mapping], bar_end: datetime) -> tuple[float, float] | None:
    """Exact-minute lookup: never borrow a later or earlier quantile row."""
    target = bar_end.astimezone(timezone.utc)
    for row in rows:
        parsed = datetime.fromisoformat(str(row.get("timestamp", "")).replace("Z", "+00:00"))
        if parsed.tzinfo is not None and parsed.astimezone(timezone.utc) == target:
            values = row.get("quantiles") or {}
            p50, p90 = float(values.get("0.5", "nan")), float(values.get("0.9", "nan"))
            if 0 < p50 <= p90:
                return p50, p90
    return None


def entry_signal(previous: MinuteBar, current: MinuteBar, rows: Iterable[Mapping]) -> str | None:
    """P90 down-cross buys a put; up-cross buys a call, on completed closes."""
    if current.end - previous.end != timedelta(minutes=1):
        return None
    before, after = forecast_levels(rows, previous.end), forecast_levels(rows, current.end)
    if before is None or after is None:
        return None
    if previous.close > before[1] and current.close < after[1]:
        return "put"
    if previous.close < before[1] and current.close > after[1]:
        return "call"
    return None


def exit_reason(kind: str, bar: MinuteBar, rows: Iterable[Mapping], window: Window) -> str | None:
    """Stop-first on a bar that also touches the put's median target."""
    if bar.end.astimezone(NEW_YORK) >= window.end:
        return "window_end"
    levels = forecast_levels(rows, bar.end)
    if levels is None:
        return None
    p50, p90 = levels
    if kind == "put":
        if bar.high >= p90:
            return "p90_stop"
        if bar.low <= p50:
            return "median_target"
    elif kind == "call":
        if bar.low <= p90:
            return "p90_stop"
    else:
        raise ValueError("option kind must be call or put")
    return None


def nearest_atm_contract(
    contracts: Iterable[Mapping], option_type: str, spot: float, expiration: str,
    quotes: Mapping[str, Mapping], *, now: datetime, max_premium: Decimal = MAX_PREMIUM_DOLLARS,
) -> tuple[str, Decimal] | None:
    """Choose the nearest listed strike, then reject it if its ask exceeds $200.

    Do not move farther out of the money just to find a cheaper contract.
    """
    if option_type not in {"call", "put"} or spot <= 0:
        raise ValueError("invalid option selection")
    eligible = [contract for contract in contracts if contract.get("type") == option_type
                and contract.get("expiration_date") == expiration and contract.get("tradable") is True
                and str(contract.get("underlying_symbol")) == "SPY"]
    if not eligible:
        return None
    # Prefer the in-the-money side only when two strikes are equally close.
    def rank(contract):
        strike = float(contract["strike_price"])
        itm = strike <= spot if option_type == "call" else strike >= spot
        return (abs(strike - spot), not itm, str(contract.get("symbol")))
    selected = min(eligible, key=rank)
    symbol = str(selected.get("symbol") or "")
    quote = quotes.get(symbol) or {}
    timestamp = datetime.fromisoformat(str(quote.get("t", "")).replace("Z", "+00:00")) if quote.get("t") else None
    if timestamp is None or timestamp.tzinfo is None or not timedelta(0) <= now - timestamp <= timedelta(seconds=30):
        return None
    ask = Decimal(str(quote.get("ap") or "0"))
    if ask <= 0 or ask * 100 > max_premium:
        return None
    return symbol, ask
