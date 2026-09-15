from copy import deepcopy

import pandas as pd
import pytest

from market_research.spy_weekly import (QUANTILES, aggregate_minutes, session_grid,
                                      simulate, weekly_windows, save, metrics)


def data():
    bars = []
    for i, (op, close) in enumerate([(100, 111), (112, 95), (94, 89), (88, 98), (99, 111), (112, 113)]):
        start = pd.Timestamp('2026-09-08T13:30Z')+pd.Timedelta(hours=i)
        bars.append({'start': start.isoformat(), 'timestamp': (start+pd.Timedelta(hours=1)).isoformat(),
                     'open': op, 'close': close, 'high': max(op,close)+1, 'low': min(op,close)-1})
    qs = dict(zip(map(str, QUANTILES), (80,90,95,100,105,110,120)))
    forecasts = [{'origin': '2026-09-04T20:00:00+00:00', 'week_end': bars[-1]['timestamp'],
                  'future_grid': [{'start': b['start'], 'timestamp': b['timestamp']} for b in bars],
                  'predictions': [{'timestamp': b['timestamp'], 'quantiles': dict(qs)} for b in bars]}]
    return bars, forecasts


def test_friday_weeks_and_labor_day():
    weeks = weekly_windows('2026-09-11')
    assert weeks[0][0] == '2026-08-14T20:00:00Z'
    assert len(weeks) == 4
    grid = session_grid('2026-09-05', '2026-09-11')
    assert len(grid) == 28
    assert not any(r['start'].startswith('2026-09-07') for r in grid)
    assert sum(r['minutes'] for r in grid) == 26*60
    assert grid[-1]['timestamp'] == '2026-09-11T20:00:00Z'


def test_early_close_partial_hour():
    grid = session_grid('2026-11-27', '2026-11-27')
    assert len(grid) == 4
    assert grid[-1]['minutes'] == 30
    assert grid[-1]['timestamp'].endswith('18:00:00Z')


def test_week_range_validation():
    with pytest.raises(ValueError): weekly_windows('2026-09-10')
    with pytest.raises(ValueError): weekly_windows('2026-09-11', 9)
    with pytest.raises(ValueError): weekly_windows('2026-04-03', 1)


def test_aggregate_never_fills_missing_minutes():
    grid = session_grid('2026-09-08','2026-09-08')[:1]
    rows = [{'timestamp': t.isoformat(), 'open': 100., 'high': 102., 'low': 99., 'close': 101.}
            for t in pd.date_range(grid[0]['start'], periods=60, freq='1min')]
    rows.pop(10)
    output, rejected = aggregate_minutes(rows, grid)
    assert output[0]['missing_minutes'] == 1 and output[0]['observed_minutes'] == 59
    assert not rejected
    output, rejected = aggregate_minutes(rows[:-1], grid)
    assert not output and rejected[0]['reason'] == 'MISSING_BOUNDARY_MINUTE'


def test_invalid_prices_and_duplicate_conflicts():
    b = {'timestamp':'2026-09-08T13:30:00Z', 'open':100., 'high':102., 'low':99., 'close':101.}
    with pytest.raises(ValueError): aggregate_minutes([b, {**b,'close':100.}], [])
    with pytest.raises(ValueError): aggregate_minutes([{**b,'close':float('nan')}], [])


def test_long_then_short_at_next_open_not_signal_price():
    bars, forecasts = data()
    result = simulate(bars,forecasts)
    trades = result['trades']
    assert [t['side'] for t in trades] == ['long', 'short', 'long']
    assert [(t['entry_price'],t['exit_price']) for t in trades] == [(112,88),(88,112),(112,113)]
    assert result['summary']['net_pnl_usd_one_share'] == -47
    assert result['summary']['wins'] == 1 and result['summary']['losses'] == 2
    assert trades[-1]['exit_reason'] == 'study_end_liquidation_assumption'


def test_gap_execution_does_not_fill_on_later_bar():
    bars, forecasts = data()
    del bars[1]
    result = simulate(bars,forecasts)
    assert result['summary']['skipped_missing_execution_bar'] == 1
    assert result['trades'][0]['side'] == 'short'


def test_future_ohlc_cannot_change_past_signals_or_fills():
    bars, forecasts = data()
    before = simulate(bars,forecasts)
    changed = deepcopy(bars)
    changed[-1]['high'] = 200
    after = simulate(changed,forecasts)
    assert before['trades'] == after['trades']
    assert before['summary'] == after['summary']
    assert before['signals'][0]['excursion']['frozen_tail_reached'] is False
    assert after['signals'][0]['excursion']['frozen_tail_reached'] is True


def test_signal_bar_high_is_not_future_excursion():
    bars, forecasts = data()
    bars[0]['high'] = 999
    signal = simulate(bars,forecasts)['signals'][0]
    assert signal['excursion']['high_after_signal'] == 114
    assert signal['excursion']['tail_at_signal'] == 120
    assert signal['excursion']['closest_frozen_tail_gap_usd'] == 6
    assert signal['excursion']['fraction_initial_gap_closed'] == pytest.approx(1/3)


def test_cost_and_drawdown():
    bars, forecasts = data()
    zero, cost = simulate(bars,forecasts), simulate(bars,forecasts,cost_bps=5)
    expected = sum(t['entry_price']+t['exit_price'] for t in zero['trades'])*.0005
    assert cost['summary']['net_pnl_usd_one_share'] == pytest.approx(-47-expected)
    assert cost['summary']['hourly_marked_max_drawdown_usd'] >= zero['summary']['hourly_marked_max_drawdown_usd']


def test_reject_overlapping_or_past_forecasts():
    bars, forecasts = data()
    with pytest.raises(ValueError): simulate(bars, forecasts+forecasts)
    forecasts[0]['origin'] = bars[-1]['timestamp']
    with pytest.raises(ValueError): simulate(bars, forecasts)


def test_immutable_record(tmp_path):
    path=tmp_path/'checkpoint.json'
    save(path, {'a':1})
    save(path, {'a':1})
    with pytest.raises(ValueError): save(path, {'a':2})
    assert path.stat().st_mode & 0o777 == 0o600


def test_metrics_out_of_sample_only():
    bars, forecasts = data()
    result = metrics(bars, forecasts)[0]
    assert result['evaluated_bars'] == 6
    assert result['mae'] == pytest.approx(sum(abs(100-b['close']) for b in bars)/6)
    assert set(result['pinball_loss']) == set(map(str, QUANTILES))
