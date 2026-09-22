import datetime as dt
import pytest
from scripts.screener_schedule import eligible


@pytest.mark.parametrize("timestamp,expected", [
    ("2026-09-21T20:05:00+00:00", True),
    ("2026-09-21T21:05:00+00:00", False),
    ("2026-11-23T21:05:00+00:00", True),
    ("2026-11-23T20:05:00+00:00", False),
    ("2026-11-27T18:05:00+00:00", True),
    ("2026-11-27T21:05:00+00:00", False),
    ("2026-11-26T21:05:00+00:00", False),
    ("2026-09-20T20:05:00+00:00", False),
])
def test_daily_scan_runs_only_after_actual_exchange_close(timestamp, expected):
    assert eligible(dt.datetime.fromisoformat(timestamp)) is expected
