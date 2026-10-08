import pytest

from ensemble_forecasting.calendars import build_future_timestamps


def test_cfd_weekday_sessions_keep_17utc_and_skip_weekends():
    rows = build_future_timestamps('2026-09-25T17:00:00Z', prediction_length=7,
                                  horizon_mode='trading_sessions', frequency='1D', calendar='FTMO_UTC_WEEKDAYS')
    assert len(rows) == 7
    assert rows[0] == '2026-09-28T17:00:00Z' and rows[-1] == '2026-10-06T17:00:00Z'


def test_crypto_daily_sessions_include_weekends_without_changing_nyse():
    rows = build_future_timestamps('2026-09-25T17:00:00Z', prediction_length=7,
                                  horizon_mode='trading_sessions', frequency='1D', calendar='FTMO_UTC_DAILY')
    assert rows[0] == '2026-09-26T17:00:00Z'
    assert rows[-1] == '2026-10-02T17:00:00Z'
    with pytest.raises(ValueError, match='require trading_sessions'):
        build_future_timestamps('2026-09-25', prediction_length=7,
                                horizon_mode='calendar_days', frequency='1D', calendar='FTMO_UTC_DAILY')
