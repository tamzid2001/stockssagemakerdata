"""Select an unpublished completed NYSE session, including delayed Actions runs."""
import argparse
import datetime as dt
import json
from pathlib import Path
from zoneinfo import ZoneInfo


def latest_completed_session(now: dt.datetime) -> tuple[str, dt.datetime]:
    import pandas_market_calendars as mcal
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("Timezone required")
    date = now.astimezone(ZoneInfo("America/New_York")).date()
    sessions = mcal.get_calendar("NYSE").schedule(start_date=date - dt.timedelta(days=31), end_date=date)
    completed = sessions[sessions["market_close"] <= now]
    if completed.empty:
        raise ValueError("No completed NYSE session")
    last = completed.iloc[-1]
    return str(completed.index[-1].date()), last["market_close"].to_pydatetime()


def publication_date(manifest: dict) -> str:
    """Only a validated publication marker can suppress the next scan."""
    if manifest.get("coverage_ok") is not True or manifest.get("status") != "complete":
        return ""
    date = manifest.get("scan_date")
    if not isinstance(date, str) or dt.date.fromisoformat(date).isoformat() != date:
        raise ValueError("Invalid published scan date")
    return date


def scan_plan(now: dt.datetime, published_date: str = "", *, manual: bool = False) -> dict[str, str | bool]:
    date, close = latest_completed_session(now)
    run = manual or published_date < date
    return {"should_run": run, "scan_date": date, "session_close": close.isoformat(),
            "reason": "manual" if manual else "unpublished_session" if run else "already_published"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--manual", action="store_true")
    parser.add_argument("--publication-manifest", type=Path)
    args = parser.parse_args()
    manifest = json.loads(args.publication_manifest.read_text()) if args.publication_manifest and args.publication_manifest.exists() else {}
    for key, value in scan_plan(dt.datetime.now(dt.timezone.utc), publication_date(manifest), manual=args.manual).items():
        print(f"{key}={str(value).lower() if isinstance(value, bool) else value}")
