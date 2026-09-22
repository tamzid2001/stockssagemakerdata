"""Gate UTC Actions schedules against the actual NYSE close, including DST/early closes."""
import argparse
import datetime as dt
from zoneinfo import ZoneInfo


def eligible(now: dt.datetime) -> bool:
    import pandas_market_calendars as mcal
    if now.tzinfo is None:
        raise ValueError("Timezone required")
    date = now.astimezone(ZoneInfo("America/New_York")).date()
    sessions = mcal.get_calendar("NYSE").schedule(start_date=date, end_date=date)
    if sessions.empty:
        return False
    elapsed = (now - sessions.iloc[0]["market_close"].to_pydatetime()).total_seconds()
    return 0 <= elapsed < 45 * 60


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--manual", action="store_true")
    args = parser.parse_args()
    print("should_run=" + str(args.manual or eligible(dt.datetime.now(dt.timezone.utc))).lower())
