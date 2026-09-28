import pytest
from ensemble_forecasting.calendars import build_future_timestamps


@pytest.mark.parametrize("frequency,expected", [
    ("5m", "2024-02-01T00:05:00Z"),
    ("15m", "2024-02-01T00:15:00Z"),
    ("30m", "2024-02-01T00:30:00Z"),
    ("4h", "2024-02-01T04:00:00Z"),
    ("1w", "2024-02-05T00:00:00Z"),
    ("1Month", "2024-03-01T00:00:00Z"),
])
def test_new_frequencies_produce_exact_next_period(frequency, expected):
    rows = build_future_timestamps("2024-02-01T00:00:00Z",prediction_length=2,
        horizon_mode="frequency_periods",frequency=frequency,calendar="NONE")
    assert rows[0] == expected


def test_monthly_horizon_preserves_leap_year_calendar_boundaries():
    assert build_future_timestamps("2024-01-01T00:00:00Z",prediction_length=3,
        horizon_mode="frequency_periods",frequency="1Month",calendar="NONE") == (
            "2024-02-01T00:00:00Z", "2024-03-01T00:00:00Z", "2024-04-01T00:00:00Z")
