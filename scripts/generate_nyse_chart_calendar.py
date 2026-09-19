"""Build the small public chart calendar from the worker's NYSE calendar.

This contains session dates/close times only, not market data. No missing price observation is
classified as a holiday. Rebuild when exchange closure rules change.
"""
import json
from pathlib import Path
import importlib.metadata
import pandas_market_calendars as mcal

if __name__ == "__main__":
    start, end = "2000-01-01", "2040-12-31"
    calendar = mcal.get_calendar("NYSE")
    schedule = calendar.schedule(start, end)
    days = schedule.index
    payload = {"exchange": "NYSE", "timezone": "America/New_York", "start": start, "end": end,
               "calendar_library": "pandas_market_calendars", "calendar_version": importlib.metadata.version("pandas_market_calendars"),
               "sessions": [str(day.date()) for day in days],
               "close_minute_utc": [int((close.value - day.tz_localize("UTC").value) // 60_000_000_000)
                                    for day, close in zip(days, schedule.market_close)]}
    target = Path(__file__).resolve().parents[1] / "quantura_site/public/market-calendars/nyse.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, separators=(",", ":")) + "\n", encoding="utf8")
    print(f"Published calendar: {len(days)} exchange sessions, {target.stat().st_size} bytes")
