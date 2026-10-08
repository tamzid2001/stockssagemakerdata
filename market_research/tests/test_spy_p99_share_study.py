from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from market_research.ftmo_dukas_data import UTC
from market_research.ftmo_p99_engine import Candidate
from market_research.ftmo_p99_study import read_json
from market_research.spy_p99_share_study import (
    COSTS, DataAPI, MODELS, NY, QUANTILES, assumed_quotes, calendar_rows,
    fetch_bars, forecast_day, regular_minutes, run_share_replay, source,
)


def minute(at, price=100., low=None):
    values = {'o': price, 'h': price, 'l': price if low is None else low, 'c': price}
    return {'start': at.isoformat(), 'end': (at+timedelta(minutes=1)).isoformat(),
            'bid': values, 'ask': dict(values)}


def signal(at, cutoff=101., p99=99.):
    rows = [{'p01': 90., 'p90': 99., 'p99': p99} for _ in range(7)]
    rows[-1]['p99'] = 110.
    return {'status': 'completed', 'origin': at.isoformat(), 'earliest_actionable_at': at.isoformat(),
            'cutoff_close': cutoff, 'predictions': rows}


def test_read_only_client_rejects_brokerage_order_endpoints(monkeypatch):
    monkeypatch.setenv('ALPACA_API_KEY', 'test-key')
    monkeypatch.setenv('ALPACA_SECRET_KEY', 'test-secret')
    api = DataAPI()
    with pytest.raises(ValueError, match='READ_ONLY_ENDPOINT_REQUIRED'):
        api.get('/v2/orders', {})


def test_calendar_respects_early_closes_and_dst():
    rows = calendar_rows([{'date': '2026-11-27', 'open': '09:30', 'close': '13:00'},
                          {'date': '2026-07-01', 'open': '09:30', 'close': '16:00'}])
    assert rows[0]['open'] == '2026-07-01T13:30:00+00:00'
    assert rows[1]['close'] == '2026-11-27T18:00:00+00:00'
    with pytest.raises(ValueError, match='DUPLICATE'):
        calendar_rows([{'date': '2026-07-01', 'open': '09:30', 'close': '16:00'}]*2)


def test_regular_minutes_exclude_pre_after_hours_and_incomplete_close_bar():
    calendar = calendar_rows([{'date': '2026-11-27', 'open': '09:30', 'close': '13:00'}])
    opening = datetime(2026, 11, 27, 14, 30, tzinfo=UTC)
    closing = datetime(2026, 11, 27, 18, tzinfo=UTC)
    bars = [{'timestamp': at.isoformat(), 'o': 100., 'h': 101., 'l': 99., 'c': 100.}
            for at in (opening-timedelta(minutes=1), opening, closing-timedelta(minutes=1), closing)]
    rows = regular_minutes(bars, calendar)
    assert len(rows) == 2 and rows[-1]['end'] == closing.isoformat()


def test_pagination_and_inclusive_end_do_not_admit_future_bar():
    at = datetime(2026, 9, 21, 13, 30, tzinfo=UTC)
    class API:
        def get(self, path, params):
            assert path == '/v2/stocks/SPY/bars' and params['feed'] == 'sip'
            offset = 1 if params.get('page_token') else 0
            return {'bars': [{'t': (at+timedelta(minutes=offset)).isoformat(),
                             'o': 100., 'h': 101., 'l': 99., 'c': 100.}],
                    'next_page_token': None if offset else 'second'}
    rows = fetch_bars(API(), at, at+timedelta(minutes=1), '1Min', 'sip')
    assert len(rows) == 1 and rows[0]['timestamp'] == at.isoformat()


def test_pagination_stall_and_conflicting_provider_prices_rejected():
    at = datetime(2026, 9, 21, tzinfo=UTC)
    class API:
        def get(self, path, params):
            return {'bars': [], 'next_page_token': 'repeat'}
    with pytest.raises(ValueError, match='PAGINATION_STALLED'):
        fetch_bars(API(), at, at+timedelta(days=1), '1Day', 'sip')


def test_share_quotes_have_exact_assumed_spread_and_cent_ticks():
    row = minute(datetime(2026, 9, 21, tzinfo=UTC), 100.005)
    for costs in COSTS:
        bid, ask = assumed_quotes(row, 'SPY', costs)
        assert ask['o'] == 100.01
        assert ask['o']-bid['o'] == pytest.approx(.01*costs.spread_factor)


@pytest.mark.parametrize('cutoff,expected', [(99., 0), (98., 0), (100., 1)])
def test_above_p99_share_entry_is_strict_risk_sized_and_unlevered(cutoff, expected):
    at = datetime(2026, 9, 21, 13, 30, tzinfo=UTC)
    result = run_share_replay([minute(at)], [signal(at, cutoff=cutoff)], at, at+timedelta(days=1), Candidate(1.), detail=True)
    assert result['above_p99_forecast_signals'] == expected
    assert result['open_entries'] == expected
    assert result['max_reserved_stop_risk'] <= 1000.
    assert result['max_invested_notional_usd'] <= 100000.
    assert result['quantity_unit'] == 'whole shares'
    if expected:
        assert result['open_basket'][0]['initial_shares'] >= 1
        assert result['open_basket'][0]['initial_shares'].is_integer()
        assert result['fixed_final_quantile_span'] == 20.
        assert result['fixed_final_p01_stop'] == 90.


def test_share_ladder_averages_lower_causally_without_p90_gate():
    at = datetime(2026, 9, 21, 13, 30, tzinfo=UTC)
    forecast = signal(at)
    for row in forecast['predictions']:
        row['p90'] = 95.
    rows = [minute(at), minute(at+timedelta(minutes=1), 99., low=98.5), minute(at+timedelta(minutes=2), 99.)]
    result = run_share_replay(rows, [forecast], at, at+timedelta(days=1), Candidate(1.), detail=True)
    assert result['open_entries'] == 2
    assert result['open_basket'][1]['at'] == (at+timedelta(minutes=2)).isoformat()
    assert result['open_basket'][1]['entry'] < result['open_basket'][0]['entry']
    assert result['max_reserved_stop_risk'] <= 1000.
    assert result['swap_pnl'] == 0.


def test_forecast_uses_only_500_completed_days_and_seven_real_future_sessions():
    day = '2026-09-25'
    cutoff = datetime(2026, 9, 25, 20, tzinfo=UTC)
    sessions = [{'day': (cutoff-timedelta(days=600-i)).date().isoformat(),
                 'timestamp': (cutoff-timedelta(days=600-i)).strftime('%Y-%m-%dT00:00:00+00:00'),
                 'close': (cutoff-timedelta(days=600-i)).isoformat(), 'target': 100+i/10} for i in range(601)]
    sessions.append({'day': '2026-09-26', 'timestamp': '2026-09-26T00:00:00+00:00', 'close': '2026-09-26T20:00:00+00:00', 'target': 999.})
    days = ['2026-09-28', '2026-09-29', '2026-09-30', '2026-10-01', '2026-10-02', '2026-10-05', '2026-10-06']
    calendar = [{'day': d, 'close': d+'T20:00:00+00:00'} for d in days]
    def execute(job, **kwargs):
        assert kwargs['minimum_history_rows'] == 500
        assert len(job['input']['rows']) == 500
        assert job['input']['rows'][-1]['target'] == 160.
        assert job['request']['calendar'] == 'NYSE'
        assert tuple(job['request']['quantiles']) == QUANTILES
        assert set(job['request']['models']) == set(MODELS)
        return {'model_runs': [{'model': m, 'status': 'completed'} for m in MODELS],
                'predictions': [{'timestamp': d+'T00:00:00Z', **{f'p{round(q*100):02d}': 100+q for q in QUANTILES}} for d in days]}
    record = forecast_day(sessions, calendar, day, execute)
    assert record['cutoff'] == cutoff.isoformat() and record['history_rows'] == 500
    assert record['predictions'][0]['timestamp'] == calendar[0]['close']
    assert record['predictions'][0]['worker_timestamp'] == '2026-09-28T00:00:00Z'
    assert datetime.fromisoformat(record['earliest_actionable_at']) >= cutoff+timedelta(minutes=15)


def test_five_models_are_required_even_if_four_return_predictions():
    at = datetime(2026, 9, 25, 20, tzinfo=UTC)
    sessions = [{'day': (at-timedelta(days=i)).date().isoformat(),
                 'timestamp': (at-timedelta(days=i)).strftime('%Y-%m-%dT00:00:00+00:00'),
                 'close': (at-timedelta(days=i)).isoformat(), 'target': 100.} for i in reversed(range(500))]
    def execute(*args, **kwargs):
        return {'model_runs': [{'model': m, 'status': 'completed'} for m in MODELS[:-1]], 'predictions': []}
    with pytest.raises(ValueError, match='FIVE_REAL_COMPLETED_MODELS_REQUIRED'):
        forecast_day(sessions, [], '2026-09-25', execute)


def short_minute(at, o=100., h=None, l=None, c=None):
    values = {'o': o, 'h': o if h is None else h, 'l': o if l is None else l,
              'c': o if c is None else c}
    return {'start': at.isoformat(), 'end': (at+timedelta(minutes=1)).isoformat(),
            'bid': values, 'ask': dict(values)}


def short_replay(rows, forecasts=None, grid=1., **kwargs):
    at = datetime(2026, 9, 21, 13, 30, tzinfo=UTC)
    return run_share_replay(rows, forecasts or [signal(at)], at, at+timedelta(days=5),
                            Candidate(grid), detail=True, side='short', **kwargs)


def test_short_sells_bid_marks_and_covers_ask_with_whole_share_risk():
    at = datetime(2026, 9, 21, 13, 30, tzinfo=UTC)
    r = short_replay([short_minute(at), short_minute(at+timedelta(minutes=1), 99.5)], grid=2.)
    entry = r['open_basket'][0]
    assert entry['entry'] == pytest.approx(99.99)
    assert r['open_net_pnl'] == pytest.approx((99.99-99.5)*entry['shares'])
    assert entry['shares'].is_integer() and entry['shares'] >= 1
    assert r['fixed_final_p99_stop'] == 110.
    assert r['fixed_final_quantile_span'] == 20.
    assert r['max_reserved_stop_risk'] <= 1000.
    assert r['stock_borrow_fees_included'] is False


@pytest.mark.parametrize('cutoff,expected', [(98., 0), (99., 0), (100., 1)])
def test_short_retains_same_strict_above_first_p99_gate(cutoff, expected):
    at = datetime(2026, 9, 21, 13, 30, tzinfo=UTC)
    r = short_replay([short_minute(at)], [signal(at, cutoff=cutoff)])
    assert r['above_p99_forecast_signals'] == expected
    assert r['open_entries'] == expected


def test_short_averages_higher_only_after_completed_breach_and_next_minute():
    at = datetime(2026, 9, 21, 13, 30, tzinfo=UTC)
    r = short_replay([short_minute(at), short_minute(at+timedelta(minutes=1), 100.5, h=102.),
                      short_minute(at+timedelta(minutes=2), 101.)])
    assert r['open_entries'] == 2
    assert r['open_basket'][1]['at'] == (at+timedelta(minutes=2)).isoformat()
    assert r['open_basket'][1]['entry'] == pytest.approx(100.99)
    assert r['open_basket'][1]['entry'] > r['open_basket'][0]['entry']
    assert r['max_reserved_stop_risk'] <= 1000.


def test_short_gap_stop_covers_actual_ask_before_pending_addition():
    at = datetime(2026, 9, 21, 13, 30, tzinfo=UTC)
    r = short_replay([short_minute(at), short_minute(at+timedelta(minutes=1), 101., h=102.),
                      short_minute(at+timedelta(minutes=2), 111.)])
    assert r['closed_entries'] == 1 and r['max_ladder'] == 1
    assert r['entries'][0]['exits'][0]['price'] == 111.
    assert r['baskets'][0]['reason'] == 'gap_stop'
    assert r['net_pnl'] < 0 and r['open_entries'] == 0


def test_short_resting_sell_limit_fills_before_continuous_upper_stop():
    at = datetime(2026, 9, 21, 13, 30, tzinfo=UTC)
    r = short_replay([short_minute(at), short_minute(at+timedelta(minutes=1), 101., h=102.),
                      short_minute(at+timedelta(minutes=2), 100., h=112., c=111.)])
    assert r['closed_entries'] == 2 and r['max_ladder'] == 2
    assert all(e['exits'][0]['price'] == 110. for e in r['entries'])
    assert r['baskets'][0]['reason'] == 'p99_stop'


@pytest.mark.parametrize('ordering', ['low_first', 'high_first'])
def test_short_profitable_trail_falls_and_covers_on_rebound_no_same_signal_reentry(ordering):
    at = datetime(2026, 9, 21, 13, 30, tzinfo=UTC)
    r = short_replay([short_minute(at), short_minute(at+timedelta(minutes=1), 100., l=97., c=98.),
                      short_minute(at+timedelta(minutes=2), 100.)], ordering=ordering)
    assert r['closed_baskets'] == 1 and r['open_entries'] == 0
    assert r['baskets'][0]['reason'] == 'trailing_stop'
    assert r['entries'][0]['exits'][0]['price'] == 98.
    assert r['net_pnl'] > 0


def test_short_fixed_final_p99_is_not_replaced_by_later_forecast():
    at = datetime(2026, 9, 21, 13, 30, tzinfo=UTC)
    later = signal(at+timedelta(days=1), cutoff=98.)
    later['predictions'][-1]['p99'] = 101.
    r = short_replay([short_minute(at), short_minute(at+timedelta(days=1), 102.)],
                     [signal(at), later])
    assert r['closed_baskets'] == 0 and r['fixed_final_p99_stop'] == 110.


def test_short_initial_entry_at_or_above_fixed_stop_is_blocked():
    at = datetime(2026, 9, 21, 13, 30, tzinfo=UTC)
    r = short_replay([short_minute(at, 111.)])
    assert r['above_p99_forecast_signals'] == 1
    assert r['open_entries'] == 0 and r['risk_or_volume_blocked_entries'] == 1
