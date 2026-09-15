import json
import zipfile

import pandas as pd
import pytest

from market_research.spy_diagnostics import diagnose
from market_research.spy_exit_sweep import load_archive
from market_research.spy_weekly import QUANTILES, iso, simulate
from market_research.spy_options import contract, estimate
from market_research.tests.test_spy_weekly import data


def test_reverse_changes_direction_not_cost_sign():
    bars, forecasts = data()
    result = simulate(bars,forecasts,contrarian=True,cost_bps=5)
    assert [t['side'] for t in result['trades']] == ['short','long','short']
    assert sum(t['gross_pnl'] for t in result['trades']) == 47
    assert all(t['assumed_cost']>0 for t in result['trades'])
    assert result['summary']['net_pnl_usd_one_share']<47


@pytest.mark.parametrize('fraction',[0,-.01,1,float('nan'),float('inf'),True,'0.01'])
def test_bad_trailing_settings(fraction):
    bars,forecasts=data()
    with pytest.raises(ValueError,match='INVALID_TRAILING'):
        simulate(bars,forecasts,trailing_fraction=fraction)


def minute_case():
    """Eight observed minutes; sparse forecast ends preserve hourly-only signals."""
    prices=[(100,111),(112,105),(105,100),(100,102),(103,111),(111,100),(100,111),(112,105)]
    minutes=[]
    for i,(op,close) in enumerate(prices):
        start=pd.Timestamp('2026-09-08T14:30Z')+pd.Timedelta(minutes=i)
        minutes.append({'timestamp':iso(start),'open':op,'close':close,'high':max(op,close)+1,'low':min(op,close)-1})
    # Synthetic shorter bins test the engine, not a claim of real hourly data.
    grid=[];bars=[];previous=0
    for end in [1,5,6,7,8]:
        group=minutes[previous:end]
        g={'start':group[0]['timestamp'],'timestamp':iso(pd.Timestamp(group[-1]['timestamp'])+pd.Timedelta(minutes=1))}
        grid.append(g);bars.append({**g,'open':group[0]['open'],'close':group[-1]['close'],
            'high':max(b['high'] for b in group),'low':min(b['low'] for b in group)})
        previous=end
    q=dict(zip(map(str,QUANTILES),(80,90,95,100,105,110,120)))
    forecasts=[{'origin':'2026-09-04T20:00:00Z','week_end':grid[-1]['timestamp'],'future_grid':grid,
        'predictions':[{'timestamp':g['timestamp'],'quantiles':dict(q)} for g in grid]}]
    return bars,forecasts,minutes


def test_minute_trail_short_next_open_gap_and_fresh_reentry():
    bars,forecasts,minutes=minute_case()
    result=simulate(bars,forecasts,contrarian=True,trailing_fraction=.01,minute_bars=minutes)
    first=result['trades'][0]
    assert first['side']=='short' and first['entry_price']==112
    assert first['favorable_close_watermark']==100
    assert first['exit_price']==103  # Stop 101; signal close 102; actual NEXT open 103.
    assert first['exit_reason']=='trailing_stop'
    assert first['trailing_exit_signal']['stop_level']==101
    assert result['trades'][1]['entry_time']==minutes[7]['timestamp']
    assert len(result['trades'])==2  # No same-region re-entry at the minute-5 close.


def test_long_trail_uses_maximum_completed_close():
    bars,forecasts,minutes=minute_case()
    # Reflect around 100, swapping high/low and quantiles to exercise long side.
    for rows in (bars,minutes):
        for b in rows:
            b['open'],b['close']=200-b['open'],200-b['close']
            b['high'],b['low']=200-b['low'],200-b['high']
    r=simulate(bars,forecasts,contrarian=True,trailing_fraction=.01,minute_bars=minutes)
    first=r['trades'][0]
    assert first['side']=='long' and first['entry_price']==88
    assert first['favorable_close_watermark']==100
    assert first['trailing_exit_signal']['stop_level']==99
    assert first['exit_price']==97


def test_minute_extremes_do_not_fill_or_arm_a_close_trail():
    bars,forecasts,minutes=minute_case()
    before=simulate(bars,forecasts,contrarian=True,trailing_fraction=.01,minute_bars=minutes)
    minutes[1]['high']=1000;minutes[1]['low']=1
    after=simulate(bars,forecasts,contrarian=True,trailing_fraction=.01,minute_bars=minutes)
    assert before['trades']==after['trades']
    assert before['trailing_decisions']==after['trailing_decisions']


def test_opposite_hourly_signal_takes_priority_over_simultaneous_trail():
    bars,forecasts=data()
    r=simulate(bars,forecasts,contrarian=True,trailing_fraction=.01)
    assert any(t['exit_reason']=='opposite_quantile_signal' for t in r['trades'])
    assert not any(t['entry_time']==t['exit_time'] for t in r['trades'])


def test_minute_close_mismatch_and_duplicate_rejected():
    bars,forecasts,minutes=minute_case()
    with pytest.raises(ValueError,match='INVALID_MINUTE_TAPE'):
        simulate(bars,forecasts,minute_bars=minutes+minutes[:1])
    minutes[0]['close']=112
    with pytest.raises(ValueError,match='MINUTE_HOURLY_CLOSE_MISMATCH'):
        simulate(bars,forecasts,minute_bars=minutes)


def test_missing_stop_execution_bar_never_gets_a_delayed_fabricated_fill():
    bars,forecasts,minutes=minute_case()
    # Remove the next open after the stop; remove matching hourly aggregate too.
    del minutes[4];del bars[1]
    r=simulate(bars,forecasts,contrarian=True,trailing_fraction=.01,minute_bars=minutes)
    assert r['summary']['skipped_missing_execution_bar']>=1
    assert r['trades'][0]['exit_price']!=103


def test_diagnostics_separates_mean_threshold_next_fill_and_premium():
    bars,forecasts=data()
    prior={**bars[0],'timestamp':forecasts[0]['origin'],'start':'2026-09-04T19:00:00+00:00','close':101}
    r=diagnose([prior,*bars],forecasts)
    week=r['weeks'][0];s=r['crossings'][0]
    assert week['initial_future_p50']==100
    assert week['last_actual_close']==101
    assert week['p90_minus_initial_p50']['usd']==10
    assert week['p10_minus_initial_p50']['usd']==-10
    assert s['quantile']=='P90' and s['time_et'].startswith('Tue')
    assert s['next_open']==112 and s['next_open_minus_average_quantile']['usd']==2
    assert s['time_aligned_p50']==100
    assert week['hypothetical_continuous_strike_payoff']['total_per_share_before_premiums']==3
    assert week['hypothetical_continuous_strike_payoff']['options_net_profit'] is None


def test_diagnostics_rejects_incomplete_week():
    bars,forecasts=data()
    with pytest.raises(ValueError,match='INCOMPLETE_WEEK'):
        diagnose(bars[:-1],forecasts)


def test_archive_rejects_path_and_checksum_without_extraction(tmp_path):
    archive=tmp_path/'untrusted.zip'
    with zipfile.ZipFile(archive,'w') as z:
        z.writestr('manifest.json',json.dumps({'files':[{'name':'../report-spy-source.json','bytes':2,'sha256':'x'}]}))
        z.writestr('../report-spy-source.json','{}')
    with pytest.raises(ValueError,match='UNEXPECTED_RESEARCH_MEMBER'):
        load_archive(archive)
    with zipfile.ZipFile(archive,'w') as z:
        z.writestr('manifest.json',json.dumps({'files':[{'name':'report-spy-source.json','bytes':2,'sha256':'x'}]}))
        z.writestr('report-spy-source.json','{}')
    with pytest.raises(ValueError,match='SOURCE_CHECKSUM_MISMATCH'):
        load_archive(archive)


def test_option_rounding_symbols_and_intrinsic_minus_both_premiums():
    assert contract('2026-08-21','C',778.501)==('SPY260821C00779000',779)
    assert contract('2026-08-21','P',764.655)==('SPY260821P00765000',765)
    t='2026-08-14T20:05:00Z'
    c={'rows':[{'timestamp':t,'open':1.}]};p={'rows':[{'timestamp':t,'open':2.}]}
    result=estimate({'end_close':764},c,p,780,767,t,t)
    assert result['gross_debit_one_standard_pair_usd']==300
    assert result['cash_equivalent_intrinsic_per_share']==3
    assert result['estimated_pnl_usd_before_costs']==0
    assert result['status']=='trade_bar_proxy_not_verified_execution'
    assert result['premium_breakeven_down']==764


def test_option_both_otm_payoff_is_zero_not_a_strike_touch_win():
    t='2026-08-14T20:05:00Z'
    c={'rows':[{'timestamp':t,'open':1.}]};p={'rows':[{'timestamp':t,'open':2.}]}
    r=estimate({'end_close':770},c,p,780,767,t,t)
    assert r['estimated_return_pct_before_costs']==-100


def test_option_estimate_does_not_mix_stale_legs_or_unavailable_rows():
    t='2026-08-14T20:05:00Z';later='2026-08-14T20:06:00Z'
    c={'rows':[{'timestamp':t,'open':1.}]};p={'rows':[{'timestamp':later,'open':2.}]}
    r=estimate({'end_close':764},c,p,780,767,t,later)
    assert r['available'] is False
    p['rows'][0]['timestamp']=t;p['rows'][0]['open']=float('nan')
    with pytest.raises(ValueError,match='INVALID_OPTION_PREMIUM'):
        estimate({'end_close':764},c,p,780,767,t,later)
