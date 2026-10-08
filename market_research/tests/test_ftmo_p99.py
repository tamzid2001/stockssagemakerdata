from datetime import date, datetime, timedelta

import pytest

from market_research.ftmo_dukas_data import UTC, INSTRUMENTS
from market_research.ftmo_dukas_engine import Costs
from market_research.ftmo_p99_engine import Candidate, KnownConversion, candidates, floor_lots, loss_per_lot, planned_levels, replay, sized_lots, trail_distance
from market_research.ftmo_p99_study import MODELS, QUANTILES, daily_sessions, forecast_day, future_sessions, plan, selection


def spec(**changes):
    return {'contractSize': 1, 'profitCurrency': 'USD', 'commission': 0, 'commissionType': 'flat_USD',
            'swapType': 'percentage', 'swapLong': 0, 'swapShort': 0, 'digits': 2, 'leverageStandard': 100,
            'maxTradeVolume': 1000, 'maxTotalVolume': 5000, 'weekly_minutes': [[0, 10080]], **changes}


def quote(at, o, h=None, l=None, c=None, spread=.5, minutes=1):
    prices = (o, h if h is not None else o, l if l is not None else o, c if c is not None else o)
    return {'start': at.isoformat(), 'end': (at+timedelta(minutes=minutes)).isoformat(),
            'bid': dict(zip(('o','h','l','c'), prices)),
            'ask': dict(zip(('o','h','l','c'), [p+spread for p in prices]))}


def forecast(at, p01=90., p90=99., p99=99., final_p01=None, final_p99=110., cutoff=101.):
    predictions = [dict(timestamp=(at+timedelta(days=i+1)).isoformat(), p01=p01, p25=95., p50=97., p75=98., p90=p90, p99=p99)
                   for i in range(7)]
    predictions[-1].update(p01=p01 if final_p01 is None else final_p01, p99=final_p99)
    return {'status': 'completed', 'origin': at.isoformat(), 'earliest_actionable_at': (at+timedelta(seconds=5)).isoformat(),
            'cutoff_close': cutoff, 'predictions': predictions}


def simulate(rows, forecasts, grid=1., **kwargs):
    at = datetime(2026, 9, 21, 18, tzinfo=UTC)
    return replay('US500.sim', rows, forecasts, spec(), KnownConversion('USD', []),
                  at, at+timedelta(days=5), Candidate(grid), detail=True, **kwargs)


def test_365_calendar_days_and_all_ten_prespecified_asset_grids():
    result = plan(date(2025, 10, 7), date(2026, 10, 6), list(INSTRUMENTS))
    assert len(result['days']) == 365 and len(result['forecasts']) <= 256
    assert result['history_rows'] == 500 and result['risk_fraction'] == .01
    assert result['quantiles'] == QUANTILES and len(result['symbols']) == 10
    assert all(len(candidates(s)) == 18 for s in result['symbols'])
    assert all(any(c.grid == 1 for c in candidates(s)) for s in result['symbols'])
    with pytest.raises(ValueError):
        plan(date(2025, 10, 6), date(2026, 10, 6), ['US500.sim'])


def test_risk_formula_uses_final_tail_width_and_reserves_all_legs():
    at = datetime(2026, 9, 21, 18, tzinfo=UTC)
    s = spec(contractSize=100000, digits=5, commission=5)
    conv = KnownConversion('USD', [])
    candidate = Candidate(.001)
    levels = planned_levels(1.14, 1.13, 1.139, .001, .00001)
    size = sized_lots(1.14, 1.13, 1.139, 100000, [], candidate, s, conv, at, Costs('test'), risk_span=.02)
    risks = [size * loss_per_lot(p, 1.13, s, conv, at, Costs('test'), .02) for p in levels]
    assert sum(risks) <= 1000
    assert sum(risks) > 700
    assert size < 1000 / (100000 * .02)
    assert floor_lots(.025) == .02


def test_usd_conversion_never_uses_uncompleted_hour_close():
    at = datetime(2026, 9, 21, 18, tzinfo=UTC)
    rows = [quote(at, 150, h=160, l=140, c=160, spread=.02, minutes=60)]
    conv = KnownConversion('JPY', rows)
    assert conv.usd(-150, at+timedelta(minutes=30)) == pytest.approx(-1)
    assert conv.usd(-160, at+timedelta(hours=1)) == pytest.approx(-1)


def test_trailing_distance_and_lot_weight_profiles():
    assert trail_distance([{'entry': 100, 'lots': 1}], 1) == 1
    assert trail_distance([{'entry': 100, 'lots': 1}, {'entry': 96, 'lots': 1}], 1) == 3
    at = datetime(2026, 9, 21, 18, tzinfo=UTC)
    sizes = [sized_lots(100, 90, 99, 100000, [], Candidate(1, name), spec(), KnownConversion('USD', []), at,
                       Costs('test'), risk_span=20) for name in ('equal','larger_deeper','smaller_deeper')]
    assert sizes[1] < sizes[0] < sizes[2]


def test_fixed_final_p01_survives_daily_forecast_update():
    at = datetime(2026, 9, 21, 18, tzinfo=UTC)
    rows = [quote(at+timedelta(minutes=1), 100), quote(at+timedelta(days=1, minutes=1), 96),
            quote(at+timedelta(days=1, minutes=2), 96)]
    result = simulate(rows, [forecast(at, final_p01=90), forecast(at+timedelta(days=1), final_p01=98, cutoff=98)])
    assert result['closed_baskets'] == 0
    assert result['fixed_final_p01_stop'] == 90
    assert result['fixed_final_quantile_span'] == 20


def test_breach_limit_cannot_fill_in_its_creation_minute():
    at = datetime(2026, 9, 21, 18, tzinfo=UTC)
    rows = [quote(at+timedelta(minutes=1), 100),
            quote(at+timedelta(minutes=2), 99, h=100, l=97, c=99),
            quote(at+timedelta(minutes=3), 97.5)]
    result = simulate(rows, [forecast(at, p90=99)])
    assert result['open_entries'] == 2
    assert result['open_basket'][1]['at'] == (at+timedelta(minutes=3)).isoformat()
    assert result['open_basket'][1]['entry'] == 98


def test_p90_only_gates_first_average_then_lower_grid_ignores_new_p90():
    at = datetime(2026, 9, 21, 18, tzinfo=UTC)
    rows = [quote(at+timedelta(minutes=1), 100), quote(at+timedelta(minutes=2), 98),
            quote(at+timedelta(minutes=3), 98),
            quote(at+timedelta(days=1, minutes=1), 96), quote(at+timedelta(days=1, minutes=2), 96)]
    result = simulate(rows, [forecast(at, p90=99), forecast(at+timedelta(days=1), p90=92, cutoff=91)])
    assert result['open_entries'] == 3
    assert result['open_basket'][2]['entry'] == 96.5


def test_profitable_trail_and_no_reentry_on_same_forecast():
    at = datetime(2026, 9, 21, 18, tzinfo=UTC)
    rows = [quote(at+timedelta(minutes=1), 100), quote(at+timedelta(minutes=2), 100, h=103, l=100, c=101),
            quote(at+timedelta(minutes=3), 100), quote(at+timedelta(minutes=4), 100)]
    result = simulate(rows, [forecast(at)])
    assert result['closed_baskets'] == 1 and result['open_entries'] == 0
    assert result['baskets'][0]['reason'] == 'trailing_stop'
    assert result['entries'][0]['exits'][0]['price'] == 102
    assert result['net_pnl'] > 0


def test_gap_stop_uses_actual_bid_not_p01_or_limit():
    at = datetime(2026, 9, 21, 18, tzinfo=UTC)
    result = simulate([quote(at+timedelta(minutes=1), 100), quote(at+timedelta(minutes=2), 80)], [forecast(at)])
    assert result['closed_baskets'] == 1
    assert result['entries'][0]['exits'][0]['price'] == 80
    assert result['baskets'][0]['reason'] == 'gap_stop'


def test_resting_limit_fills_before_stop_on_continuous_descent():
    at = datetime(2026, 9, 21, 18, tzinfo=UTC)
    rows = [quote(at+timedelta(minutes=1), 100),
            quote(at+timedelta(minutes=2), 99, h=100, l=97, c=99),
            quote(at+timedelta(minutes=3), 99, h=100, l=85, c=91)]
    result = simulate(rows, [forecast(at, p90=99)])
    assert result['closed_entries'] == 2 and result['baskets'][0]['legs'] == 2
    assert result['entries'][1]['exits'][0]['price'] == 90


def test_daily_session_has_real_close_without_filling_holidays():
    at = datetime(2026, 9, 20, 18, tzinfo=UTC)
    rows = [quote(at, 100, minutes=60), quote(at+timedelta(hours=22), 102, minutes=60),
            quote(at+timedelta(hours=23), 900, minutes=60)]
    sessions = daily_sessions(rows, 'US500.sim')
    assert len(sessions) == 1 and sessions[0]['observed_hours'] == 2
    assert sessions[0]['target'] == 102.25
    assert sessions[0]['timestamp'] == '2026-09-21T17:00:00+00:00'


def test_forecast_exact_500_causal_daily_bars_all_models_and_asset_calendar():
    day = date(2026, 9, 25)
    cutoff = datetime(2026, 9, 25, 17, tzinfo=UTC)
    sessions = [{'timestamp': (cutoff-timedelta(days=600-i)).isoformat(), 'target': 100+i/10,
                 'observation_end': (cutoff-timedelta(days=600-i)).isoformat()} for i in range(601)]
    sessions.append({'timestamp': (cutoff+timedelta(days=1)).isoformat(), 'target': 999,
                     'observation_end': (cutoff+timedelta(days=1)).isoformat()})
    def execute(job, **kwargs):
        assert kwargs['minimum_history_rows'] == 500 and len(job['input']['rows']) == 500
        assert job['input']['rows'][-1]['timestamp'] == cutoff.isoformat()
        assert job['request']['calendar'] == 'FTMO_UTC_WEEKDAYS'
        assert tuple(job['request']['quantiles']) == QUANTILES
        assert set(job['request']['models']) == set(MODELS)
        return {'model_runs': [{'model': m, 'status': 'completed'} for m in MODELS],
                'predictions': [{'timestamp': t.isoformat(), **{f'p{round(q*100):02d}': 100+q for q in QUANTILES}}
                                for t in future_sessions(cutoff, 'US500.sim')]}
    result = forecast_day(sessions, 'US500.sim', day, execute)
    assert result['history_rows'] == 500
    assert result['predictions'][0]['timestamp'].startswith('2026-09-28T17:00:00')
    assert future_sessions(cutoff, 'BTCUSD.sim')[0].weekday() == 5
    assert forecast_day([], 'US500.sim', day, execute)['status'] == 'market_closed_no_observed_session'


def test_selection_rejects_daily_loss_or_static_floor_breach():
    def row(name, path, pnl, breach=None):
        return {'candidate':name,'ordering':path,'net_pnl':pnl,'closed_baskets':10,
                'first_daily_breach':breach,'first_total_breach':None,'first_margin_breach':None}
    values = [row('aggressive', 'low', 50000, '2026-01-01'), row('aggressive', 'high', 40000),
              row('safe', 'low', 5000), row('safe', 'high', 3000)]
    assert selection(values) == 'safe'
    assert selection(values[:2]) is None
