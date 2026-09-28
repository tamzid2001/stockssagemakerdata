from __future__ import annotations

import pandas as pd

FREQUENCIES = [
    {"frequency": frequency, "bucket_timezone": "UTC", "completed_bars_only": True,
     "missing_intervals": "not_filled", "calendar_period": "month" if frequency == "1MS" else "week" if frequency == "1W-MON" else None}
    for frequency in ("1min", "5min", "15min", "30min", "1h", "4h", "1D", "1W-MON", "1MS")
]

ALIASES = {
    "1m": "1min", "1min": "1min", "5m": "5min", "5min": "5min",
    "15m": "15min", "15min": "15min", "30m": "30min", "30min": "30min",
    "1h": "1h", "1hour": "1h", "4h": "4h", "4hour": "4h",
    "1d": "1D", "1day": "1D", "1w": "1W-MON", "1week": "1W-MON",
    "1w-mon": "1W-MON", "1month": "1MS", "1mo": "1MS", "1ms": "1MS",
}


def normalize_frequency(value: str) -> str:
    """Keep explicit pandas dataset offsets; normalize the public market aliases."""
    if str(value).strip() == "1M":
        return "1ME"
    return ALIASES.get(str(value).strip().lower(), str(value).strip())


def frequency_offset(value: str):
    return pd.tseries.frequencies.to_offset(normalize_frequency(value))
