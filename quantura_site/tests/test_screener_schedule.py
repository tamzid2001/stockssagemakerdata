import datetime as dt
import pytest
from scripts.screener_schedule import latest_completed_session, publication_date, scan_plan


@pytest.mark.parametrize("timestamp,session,close", [
    ("2026-09-28T20:05:00+00:00", "2026-09-28", "2026-09-28T20:00:00+00:00"),
    # The actual delayed scheduled run missed the former 45-minute window.
    ("2026-09-28T22:29:45+00:00", "2026-09-28", "2026-09-28T20:00:00+00:00"),
    ("2026-09-29T02:00:00+00:00", "2026-09-28", "2026-09-28T20:00:00+00:00"),
    ("2026-09-29T08:00:00+00:00", "2026-09-28", "2026-09-28T20:00:00+00:00"),
    ("2026-09-28T19:59:00+00:00", "2026-09-25", "2026-09-25T20:00:00+00:00"),
    ("2026-11-23T21:05:00+00:00", "2026-11-23", "2026-11-23T21:00:00+00:00"),
    ("2026-11-23T20:05:00+00:00", "2026-11-20", "2026-11-20T21:00:00+00:00"),
    ("2026-11-27T18:05:00+00:00", "2026-11-27", "2026-11-27T18:00:00+00:00"),
    ("2026-11-27T21:05:00+00:00", "2026-11-27", "2026-11-27T18:00:00+00:00"),
    ("2026-11-26T21:05:00+00:00", "2026-11-25", "2026-11-25T21:00:00+00:00"),
    ("2026-09-27T20:05:00+00:00", "2026-09-25", "2026-09-25T20:00:00+00:00"),
])
def test_latest_real_completed_session_survives_delays_dst_and_holidays(timestamp, session, close):
    date, actual_close = latest_completed_session(dt.datetime.fromisoformat(timestamp))
    assert date == session
    assert actual_close.isoformat() == close


def test_only_an_unpublished_completed_session_runs():
    now = dt.datetime.fromisoformat("2026-09-29T02:00:00+00:00")
    assert scan_plan(now, "2026-09-25")["should_run"] is True
    current = scan_plan(now, "2026-09-28")
    assert current["should_run"] is False
    assert current["reason"] == "already_published"
    assert scan_plan(now, "2026-09-28", manual=True)["should_run"] is True


def test_before_close_and_weekends_do_not_rescan_a_published_session():
    for timestamp in ["2026-09-27T20:05:00+00:00", "2026-09-28T19:59:00+00:00"]:
        assert scan_plan(dt.datetime.fromisoformat(timestamp), "2026-09-25")["should_run"] is False


def test_failed_publication_cannot_suppress_retry():
    assert publication_date({"scan_date": "2026-09-28", "coverage_ok": False, "status": "degraded"}) == ""
    assert publication_date({"scan_date": "2026-09-28", "coverage_ok": True, "status": "complete"}) == "2026-09-28"
    assert publication_date({}) == ""
    with pytest.raises(ValueError):
        publication_date({"scan_date": "2026-09-99", "coverage_ok": True, "status": "complete"})
    with pytest.raises(ValueError, match="Timezone required"):
        latest_completed_session(dt.datetime(2026, 9, 28))
