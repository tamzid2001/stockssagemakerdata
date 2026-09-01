from __future__ import annotations

import pandas as pd


def build_future_timestamps(
    last_observed: str,
    *,
    prediction_length: int,
    horizon_mode: str,
    frequency: str,
    calendar: str = "NYSE",
) -> tuple[str, ...]:
    if prediction_length < 1:
        raise ValueError("prediction_length must be >= 1")
    last = pd.Timestamp(last_observed)
    last = last.tz_localize("UTC") if last.tzinfo is None else last.tz_convert("UTC")
    if horizon_mode in {"trading_sessions", "calendar_days"}:
        if calendar.upper() not in {"NYSE", "XNYS"}:
            raise ValueError("trading-session horizons currently require the NYSE calendar")
        dates = _nyse_dates(last, prediction_length, horizon_mode)
    elif horizon_mode == "frequency_periods":
        offset = pd.tseries.frequencies.to_offset(frequency)
        dates = pd.date_range(start=last + offset, periods=prediction_length, freq=offset)
    else:
        raise ValueError("unsupported horizon_mode")
    return tuple(_iso(value) for value in dates)


def _nyse_dates(last: pd.Timestamp, length: int, mode: str) -> pd.DatetimeIndex:
    try:
        import pandas_market_calendars as mcal
    except ImportError as exc:
        raise RuntimeError("pandas-market-calendars is required for NYSE horizons") from exc
    nyse = mcal.get_calendar("NYSE")
    start = (last + pd.Timedelta(days=1)).date()
    if mode == "calendar_days":
        end = (last + pd.Timedelta(days=length)).date()
        result = pd.DatetimeIndex(nyse.schedule(start_date=start, end_date=end).index)
    else:
        end = (last + pd.Timedelta(days=max(60, length * 2))).date()
        while True:
            schedule = nyse.schedule(start_date=start, end_date=end)
            if len(schedule) >= length:
                result = pd.DatetimeIndex(schedule.index[:length])
                break
            end = (pd.Timestamp(end) + pd.Timedelta(days=max(30, length))).date()
    if result.tz is None:
        result = result.tz_localize("UTC")
    return result


def _iso(value: pd.Timestamp) -> str:
    parsed = pd.Timestamp(value)
    parsed = parsed.tz_localize("UTC") if parsed.tzinfo is None else parsed.tz_convert("UTC")
    return parsed.isoformat().replace("+00:00", "Z")
