from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import pandas as pd


HORIZON_SESSIONS: dict[str, int] = {
    "1_trading_day": 1,
    "3_trading_days": 3,
    "5_trading_days": 5,
    "10_trading_days": 10,
    "2_weeks": 10,
    "1_month": 21,
    "3_months": 63,
}

HORIZON_ALIASES = {
    "1d": "1_trading_day",
    "3d": "3_trading_days",
    "5d": "5_trading_days",
    "10d": "10_trading_days",
    "2w": "2_weeks",
    "1m": "1_month",
    "3m": "3_months",
}


@dataclass(frozen=True)
class ResolvedHorizon:
    label: str
    sessions: int
    target_date: str
    session_dates: tuple[str, ...]
    frequency: str = "1d"
    calendar: str = "XNYS"


def _fallback_sessions(after: pd.Timestamp, count: int) -> pd.DatetimeIndex:
    return pd.bdate_range(start=after.normalize() + pd.Timedelta(days=1), periods=count, tz="UTC")


def _exchange_sessions(after: pd.Timestamp, count: int, calendar_name: str) -> pd.DatetimeIndex:
    try:
        import exchange_calendars as xcals

        calendar = xcals.get_calendar(calendar_name)
        start = after.normalize().tz_localize(None) + pd.Timedelta(days=1)
        end = start + pd.Timedelta(days=max(30, count * 3))
        sessions = calendar.sessions_in_range(start, end)
        while len(sessions) < count:
            end += pd.Timedelta(days=max(30, count * 2))
            sessions = calendar.sessions_in_range(start, end)
        return pd.DatetimeIndex(sessions[:count]).tz_localize("UTC") if sessions.tz is None else sessions[:count]
    except ImportError:
        return _fallback_sessions(after, count)


def resolve_horizon(
    timeframe: str,
    as_of: str | datetime | pd.Timestamp,
    *,
    calendar_name: str = "XNYS",
) -> ResolvedHorizon:
    raw = str(timeframe or "").strip().lower()
    label = HORIZON_ALIASES.get(raw, raw)
    if label not in HORIZON_SESSIONS:
        raise ValueError(f"unsupported timeframe: {timeframe}")
    stamp = pd.Timestamp(as_of)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize(timezone.utc)
    else:
        stamp = stamp.tz_convert(timezone.utc)
    count = HORIZON_SESSIONS[label]
    sessions = _exchange_sessions(stamp, count, calendar_name)
    dates = tuple(pd.Timestamp(value).date().isoformat() for value in sessions)
    return ResolvedHorizon(label, count, dates[-1], dates, "1d", calendar_name)
