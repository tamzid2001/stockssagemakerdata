from copy import deepcopy

import pytest

from market_research.spy_touches import average_touches, crossing_excursions, summarize_touches, target_distance
from market_research.spy_year import strategy_comparison
from market_research.spy_weekly import aggregate_minutes


def forecast():
    quantiles = {'0.01':80.,'0.1':90.,'0.25':95.,'0.5':100.,'0.75':105.,'0.9':110.,'0.99':120.}
    grid = [{'start':'2026-09-08T13:30:00Z','timestamp':'2026-09-08T13:33:00Z','minutes':3},
            {'start':'2026-09-08T13:33:00Z','timestamp':'2026-09-08T13:36:00Z','minutes':3}]
    return {'origin':'2026-09-04T20:00:00Z','week_end':'2026-09-08T13:36:00Z','future_grid':grid,
            'predictions':[{'timestamp':g['timestamp'],'quantiles':dict(quantiles)} for g in grid]}


def bar(minute, close=100., low=None, high=None, open_=None):
    return {'timestamp':f'2026-09-08T13:{minute:02d}:00Z','open':close if open_ is None else open_,
            'high':close+1 if high is None else high,'low':close-1 if low is None else low,'close':close}


def test_touch_minutes_are_not_touch_episodes_and_all_quantiles_remain_distinct():
    rows=[bar(30),bar(31),bar(32,104),bar(33),bar(34,104),bar(35)]
    result=average_touches(forecast(),rows)
    p50=result['levels']['0.5']
    assert p50['range_touch_minutes']==4
    assert p50['touch_episodes']==3
    assert result['coverage_fraction']==1
    assert len(result['levels'])==7
    assert result['levels']['0.99']['ever_range_touched'] is False


def test_gap_is_not_a_confirmed_touch_or_consecutive_close_cross():
    rows=[bar(30,98),bar(32,102)]
    r=average_touches(forecast(),rows)
    p50=r['levels']['0.5']
    assert p50['gap_through_count']==1
    assert p50['range_touch_minutes']==0
    assert p50['upward_close_crossings']==0
    assert r['missing_minutes']==4


def test_contiguous_close_cross_and_same_bar_gap_are_explicit():
    p50=average_touches(forecast(),[bar(30,98),bar(31,102)])['levels']['0.5']
    assert p50['upward_close_crossings']==1
    assert p50['gap_through_count']==1
    assert not p50['gap_through_events'][0]['same_bar_range_touch']


def test_missing_minute_resets_episode_and_duplicates_deduplicate():
    r=average_touches(forecast(),[bar(30),bar(30),bar(32)])
    assert r['levels']['0.5']['touch_episodes']==2
    assert r['observed_minutes']==2
    with pytest.raises(ValueError,match='DUPLICATE'):
        average_touches(forecast(),[bar(30),bar(30,102)])


@pytest.mark.parametrize('bad',[float('nan'),float('inf'),0.,-1.,True])
def test_invalid_price_rejected(bad):
    with pytest.raises(ValueError,match='OHLC'):
        average_touches(forecast(),[{**bar(30),'close':bad}])


def test_outside_forecast_rows_do_not_inflate_coverage():
    r=average_touches(forecast(),[bar(29),bar(30),bar(36)])
    assert r['observed_minutes']==1
    assert r['expected_minutes']==6


def test_averages_equal_weight_forecast_steps_not_minutes():
    f=forecast();f['predictions'][1]['quantiles']={k:v+2 for k,v in f['predictions'][1]['quantiles'].items()}
    assert average_touches(f,[bar(30)])['levels']['0.5']['average_value']==101


def test_no_observations_unknown_not_zero_rate():
    r=average_touches(forecast(),[])
    assert r['levels']['0.9']['ever_range_touched'] is None
    summary=summarize_touches([{'average_touches':r}])['0.9']
    assert summary['weeks_with_observations']==0
    assert summary['observed_week_touch_rate'] is None


def test_coverage_denominators_include_missing_week_disclosure():
    yes=average_touches(forecast(),[bar(m) for m in range(30,36)])
    partial=average_touches(forecast(),[bar(30,102)])
    r=summarize_touches([{'average_touches':yes},{'average_touches':partial}])['0.5']
    assert r['observed_week_touch_rate']==.5
    assert r['full_coverage_touch_rate']==1
    assert r['weeks_with_missing_minutes']==1


def test_tail_distance_and_overshoot_separate():
    r=target_distance([bar(34,116)],120.,110.,True)
    assert r['closest_remaining_gap_usd']==3
    assert r['fraction_initial_gap_closed']==.7
    r=target_distance([bar(34,122)],120.,110.,True)
    assert r['overshoot_usd']==3
    assert r['fraction_initial_gap_closed']==1
    assert r['reached_or_exceeded'] is True
    assert r['actual_range_touch'] is False  # A gap beyond is not a touch.
    assert target_distance([],120.,110.,True)['reached_or_exceeded'] is None


def test_excursions_exclude_signal_bar_high_and_track_both_tails():
    f=forecast()
    rows=[bar(30),bar(31),bar(32,111,high=150),bar(33,115),bar(34,116),bar(35,113)]
    bars,_=aggregate_minutes(rows,f['future_grid'])
    events=crossing_excursions(f,rows,bars)['events']
    assert len(events)==2  # One per threshold basis, not repeated every high bar.
    e=events[0]
    assert e['time_et'].endswith('09:33 AM EDT')
    assert e['high_after_signal']==117
    assert e['targets']['signal_p99']['closest_remaining_gap_usd']==3
    assert e['targets']['signal_p99']['fraction_initial_gap_closed']==pytest.approx(2/3)
    assert 'signal_p1' in e['targets'] and 'average_p99' in e['targets']
    assert not e['targets']['signal_p99']['reached_or_exceeded']


def test_lower_cross_uses_p1_continuation_not_p99_and_end_is_censored():
    f=forecast()
    rows=[bar(30),bar(31),bar(32,89),bar(33,83),bar(34,82),bar(35,111)]
    bars,_=aggregate_minutes(rows,f['future_grid'])
    events=crossing_excursions(f,rows,bars)['events']
    first=events[0]
    assert first['continuation_tail']=='0.01'
    assert first['targets']['signal_p1']['closest_remaining_gap_usd']==1
    assert first['targets']['signal_p99']['closest_remaining_gap_usd']==8
    assert events[1]['right_censored']
    assert events[1]['targets']['signal_p99']['reached_or_exceeded'] is None


def test_reverse_reuses_same_signals_opposite_gross_pnl_and_no_forecast_mutation():
    f=forecast();old=deepcopy(f)
    rows=[bar(30),bar(31),bar(32,111),bar(33,112),bar(34,112),bar(35,114)]
    bars,_=aggregate_minutes(rows,f['future_grid'])
    result=strategy_comparison(bars,[f])
    momentum=result['hourly_path/momentum/0bp'];reverse=result['hourly_path/reverse/0bp']
    assert momentum['summary']['net_pnl_usd_one_share']==2
    assert reverse['summary']['net_pnl_usd_one_share']==-2
    assert reverse['trades'][0]['side']=='short'
    assert reverse['trades'][0]['entry_time']=='2026-09-08T13:33:00Z'
    assert result['hourly_path/reverse/1bp']['summary']['net_pnl_usd_one_share'] < -2
    assert len(result)==12
    assert f==old
