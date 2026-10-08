from datetime import datetime, timedelta

import pytest

from market_research.ftmo_dukas_data import UTC
from market_research.spy_one_day_stops import prepare_sessions, replay


def row(at, o, h=None, l=None, c=None):
    values = dict(o=o, h=o if h is None else h, l=o if l is None else l, c=o if c is None else c)
    return {'start': at.isoformat(), 'end': (at+timedelta(minutes=1)).isoformat(), 'bid': values, 'ask': dict(values)}


def sessions(rows):
    at = datetime.fromisoformat(rows[0]['start'])
    record = {'status': 'completed', 'day': at.date().isoformat(), 'earliest_actionable_at': at.isoformat(),
              'cutoff_close': 101., 'predictions': [{'timestamp': rows[-1]['end'], 'p99': 99., 'p50': 100.}, {'p50': 101.}]}
    return prepare_sessions(rows, [record])


def test_single_buy_exits_at_close_with_cash_and_one_percent_risk_caps():
    at = datetime(2026, 9, 21, 13, 30, tzinfo=UTC)
    s = sessions([row(at, 100.), row(at+timedelta(minutes=1), 102.)])
    result = replay(s, 2.)
    trade, = result['trades']
    assert trade['shares'] == 500
    assert trade['planned_stop_risk_usd'] == 1000.
    assert trade['reason'] == 'next_session_close'
    assert trade['exit'] == pytest.approx(101.99)
    assert result['net_pnl'] == pytest.approx(995.)
    assert result['median_duration_hours'] == pytest.approx(2/60)


def test_continuous_stop_fills_at_stop_but_gap_uses_actual_bid():
    at = datetime(2026, 9, 21, 13, 30, tzinfo=UTC)
    continuous = replay(sessions([row(at, 100.), row(at+timedelta(minutes=1), 100., l=95.)]), 2.)
    gap = replay(sessions([row(at, 100.), row(at+timedelta(minutes=1), 95.)]), 2.)
    assert continuous['trades'][0]['exit'] == 98.
    assert continuous['net_pnl'] == pytest.approx(-1000.)
    assert gap['trades'][0]['exit'] == pytest.approx(94.99)
    assert gap['net_pnl'] < -1000.
    assert continuous['closed_trades'] == gap['closed_trades'] == 1


def test_forecast_latency_excludes_earlier_opens_and_missing_close_is_rejected():
    at = datetime(2026, 9, 21, 13, 30, tzinfo=UTC)
    rows = [row(at, 100.), row(at+timedelta(minutes=1), 102.)]
    record = {'status': 'completed', 'day': '2026-09-21', 'earliest_actionable_at': (at+timedelta(seconds=1)).isoformat(),
              'cutoff_close': 101., 'predictions': [{'timestamp': rows[-1]['end'], 'p99': 99., 'p50': 100.}]}
    s = prepare_sessions(rows, [record])
    assert s[0]['quotes'][0]['at'] == at+timedelta(minutes=1)
    record['predictions'][0]['timestamp'] = (at+timedelta(minutes=3)).isoformat()
    with pytest.raises(ValueError, match='NEXT_SESSION_CLOSE_MISSING'):
        prepare_sessions(rows, [record])


def test_candle_order_changes_drawdown_but_not_fixed_stop_cash_result():
    at = datetime(2026, 9, 21, 13, 30, tzinfo=UTC)
    s = sessions([row(at, 100.), row(at+timedelta(minutes=1), 100., h=110., l=95., c=100.)])
    low = replay(s, 2., ordering='low_first')
    high = replay(s, 2., ordering='high_first')
    assert low['net_pnl'] == high['net_pnl'] == pytest.approx(-1000.)
    assert high['max_equity_drawdown'] > low['max_equity_drawdown']


def test_cash_cap_and_forecast_only_median_filter():
    at = datetime(2026, 9, 21, 13, 30, tzinfo=UTC)
    s = sessions([row(at, 100.)])
    assert replay(s, .25)['trades'][0]['shares'] == 1000
    s[0]['rising_median'] = False
    assert replay(s, .25, filter_name='above_p99_rising_median')['closed_trades'] == 0


def test_simultaneous_stop_and_target_respect_both_possible_candle_paths():
    at = datetime(2026, 9, 21, 13, 30, tzinfo=UTC)
    s = sessions([row(at, 100.), row(at+timedelta(minutes=1), 100., h=110., l=95., c=100.)])
    low = replay(s, 2., ordering='low_first', profit_target=2.)
    high = replay(s, 2., ordering='high_first', profit_target=2.)
    assert low['net_pnl'] == pytest.approx(-1000.)
    assert high['net_pnl'] == pytest.approx(1000.)
    assert low['stop_exits'] == 1 and high['target_exits'] == 1
