from datetime import date, datetime, timedelta
import json
import sys
import types

import pytest

from market_research.ftmo_dukas_data import UTC, decode, digest, hourly_from_minutes, stamp
from market_research.ftmo_dukas_engine import Costs, Conversion, RULES, commission, replay, swap_quote, swap_weight
from market_research.ftmo_dukas_study import MODELS, forecast_day, plan, select_development


def test_native_hour_decoder_scales_deltas_and_rejects_misalignment():
    payload={'timestamp':0,'shift':3600000,'multiplier':.001,'open':100.,'high':102.,'low':99.,'close':101.,
        'times':[0,1],'opens':[0,1000],'highs':[0,1000],'lows':[0,1000],'closes':[0,1000],'volumes':[1,1]}
    rows=decode(payload,3)
    assert rows[1]['o']==101 and stamp(rows[1]['start']).hour==1
    payload['shift']=60000
    with pytest.raises(ValueError,match='UTC_ALIGNED'):decode(payload,3)


def test_plan_has_23_hours_utc_and_disjoint_symbol_sides():
    result=plan(date(2025,9,26),date(2026,9,25),['EURUSD.sim','XAUUSD.sim'])
    assert result['planned_forecasts']==730
    assert result['timezone']=='UTC'and result['horizon_hours']==23
    assert len(result['forecasts'])==10 and len(result['replays'])==4
    with pytest.raises(ValueError):plan(date(2025,9,26),date(2026,9,25),['EURUSD.sim; echo bad'])
    with pytest.raises(ValueError):plan(date.today()+timedelta(days=1),date.today()+timedelta(days=1),['EURUSD.sim'])


def spec():
    return {'contractSize':1,'profitCurrency':'USD','commission':0,'commissionType':'flat_USD',
        'swapType':'percentage','swapLong':0,'swapShort':0,'digits':2,'leverageStandard':50,
        'maxTradeVolume':50,'maxTotalVolume':300,'weekly_minutes':[[0,10080]]}


def test_fx_commission_is_five_roundtrip_and_percentage_basis_is_explicit():
    s=spec();s['commission']=5
    assert commission(s,.01,100,Costs('test'))*2==pytest.approx(.05)
    s.update(commission=.065,commissionType='percent')
    assert commission(s,.01,80000,Costs('test'))==pytest.approx(.52)
    assert commission(s,.01,80000,Costs('test',percent_roundtrip=True))==pytest.approx(.26)


def test_swap_is_annual_not_daily_percent_and_rollover_is_separate_from_session():
    s=spec();s.update(contractSize=100000,swapLong=-2.47,swapShort=.45)
    assert swap_quote(s,'long',.01,1.14,1,Costs('test'))==pytest.approx(1140*-.0247/360)
    assert swap_quote(s,'short',.01,1.14,3,Costs('test'))==pytest.approx(1140*.0045/360*3)
    assert swap_weight('EURUSD.sim',date(2026,9,23),Costs('test'))==3
    assert swap_weight('US500.sim',date(2026,9,25),Costs('test'))==3
    assert swap_weight('BTCUSD.sim',date(2026,9,26),Costs('test'))==1


def quote(at,o,h,l,c,spread=.5):
    return {'start':at.isoformat(),'end':(at+timedelta(hours=1)).isoformat(),
        'bid':dict(zip(('o','h','l','c'),(o,h,l,c))),
        'ask':dict(zip(('o','h','l','c'),(o+spread,h+spread,l+spread,c+spread)))}


def forecasts(origin,p01=110,p99=120,available=None):
    return [{'status':'completed','origin':origin.isoformat(),'earliest_actionable_at':(available or origin+timedelta(seconds=5)).isoformat(),
        'predictions':[{'timestamp':(origin+timedelta(hours=i)).isoformat(),'p01':p01,'p50':115,'p99':p99}for i in range(1,24)]}]


def test_long_down_only_carries_then_exits_gap_above_average_target():
    origin=datetime(2026,9,24,18,tzinfo=UTC)
    rows=[quote(origin-timedelta(hours=1),100,101,99,100),
          quote(origin+timedelta(hours=1),99.5,101,99,100),
          quote(origin+timedelta(hours=2),95,99,88,94),
          quote(origin+timedelta(hours=4),94,98,90,94),
          quote(origin+timedelta(hours=24),110,111,109,110)]
    result=replay('US500.sim','long',rows,forecasts(origin),spec(),Conversion('USD',[]),origin,origin+timedelta(hours=26),origin+timedelta(hours=24))
    assert result['full']['closed_baskets']==1 and result['full']['closed_entries']==2
    assert result['trades'][1]['entry']==90
    assert result['full']['net_equity_pnl']==pytest.approx(.3)
    assert result['max_open_lots']==.02 and not result['open_entries']
    assert stamp(result['trades'][0]['exit_at'])>=origin+timedelta(hours=24)


def test_short_up_only_is_independent_and_uses_ask_to_close():
    origin=datetime(2026,9,24,18,tzinfo=UTC)
    rows=[quote(origin-timedelta(hours=1),125,126,124,125),
          quote(origin+timedelta(hours=1),125,128,123,125),
          quote(origin+timedelta(hours=2),132,140,131,136),
          quote(origin+timedelta(hours=24),119,120,118,119)]
    result=replay('US500.sim','short',rows,forecasts(origin),spec(),Conversion('USD',[]),origin,origin+timedelta(hours=26),origin+timedelta(hours=24))
    assert result['full']['closed_entries']==2 and result['trades'][1]['entry']==135
    assert result['trades'][0]['exit']==119.5
    assert result['full']['net_equity_pnl']==pytest.approx(.21)


def test_partial_forecast_hour_cannot_trade_and_open_loss_is_in_equity():
    origin=datetime(2026,9,24,18,tzinfo=UTC)
    rows=[quote(origin-timedelta(hours=1),100,101,99,100),quote(origin,80,90,70,80),
          quote(origin+timedelta(hours=1),100,101,99,100),quote(origin+timedelta(hours=2),95,99,94,95)]
    result=replay('US500.sim','long',rows,forecasts(origin),spec(),Conversion('USD',[]),origin,origin+timedelta(hours=3),origin+timedelta(hours=24))
    assert result['open_entries']==1 and result['open_basket'][0]['entry']==100.5
    assert result['full']['net_equity_pnl']==pytest.approx(-.055)
    assert result['open_net_pnl']==pytest.approx(-.055)


def test_conversion_never_uses_uncompleted_hour_close_and_sign_is_conservative():
    at=datetime(2026,9,24,18,tzinfo=UTC);row=quote(at,150,160,140,159,1)
    c=Conversion('JPY',[row]);assert c.usd(151,at)==1 and c.usd(-150,at)==-1
    assert c.usd(160,at,'c')==1


def test_forecast_crops_real_origin_gap_and_never_reads_future_rows(monkeypatch):
    origin=datetime(2026,9,24,18,tzinfo=UTC)
    rows=[quote(origin-timedelta(hours=503-i),100,102,98,101)for i in range(500)]
    rows.append(quote(origin+timedelta(hours=1),99999,100000,99998,99999))
    captured=[];module=types.ModuleType('ensemble_forecasting.worker')
    def execute(job):
        captured.append(job);last=stamp(job['input']['rows'][-1]['timestamp'])
        assert last<=origin and len(job['input']['rows'])==500
        return {'model_runs':[{'model':m,'status':'completed'}for m in MODELS],
            'predictions':[{'timestamp':(last+timedelta(hours=i)).isoformat(),**{f'p{q:02d}':100+q/100 for q in (1,10,25,50,75,90,99)}}for i in range(1,job['request']['prediction_length']+1)]}
    module.execute_job=execute;monkeypatch.setitem(sys.modules,'ensemble_forecasting.worker',module)
    result=forecast_day(rows,'EURUSD.sim',origin.date())
    assert len(result['predictions'])==23
    assert result['history_gap_hours']==3 and captured[0]['request']['prediction_length']==26
    assert stamp(result['predictions'][-1]['timestamp'])==origin+timedelta(hours=23)
    assert captured[0]['request']['failure_policy']=='fail'


def test_later_profit_cannot_change_development_selection():
    split=datetime(2026,6,27,18,tzinfo=UTC);cases=[]
    for name in ('baseline','median'):
        for order in ('low_first','high_first'):
            cases.append({'rule':name,'costs':'reference_percentage_per_side','ordering':order,
                'development':{'net_equity_pnl':100,'max_equity_drawdown':10 if name=='median'else 20,'losing_sessions':5 if name=='median'else 10,'closed_baskets':20,'exposed_sessions':30},
                'later_carried_diagnostic':{'net_equity_pnl':-100000 if name=='median'else 100000},
                'first_daily_limit_breach':None,'first_total_limit_breach':None,'first_margin_breach':None})
    assert select_development(cases,split)=='median'


def test_current_month_minutes_are_real_utc_hourly_ohlc_without_filled_gaps():
    start=datetime(2026,9,24,18,tzinfo=UTC)
    rows=[{'start':start.isoformat(),'o':100,'h':101,'l':99,'c':100},
          {'start':(start+timedelta(minutes=59)).isoformat(),'o':102,'h':103,'l':101,'c':102},
          {'start':(start+timedelta(hours=2)).isoformat(),'o':104,'h':105,'l':103,'c':104}]
    hourly=hourly_from_minutes(rows)
    assert len(hourly)==2 and hourly[0]['o']==100 and hourly[0]['c']==102
    assert hourly[0]['h']==103 and hourly[0]['l']==99
    assert stamp(hourly[1]['start'])==start+timedelta(hours=2)


def test_daily_midnight_balance_loss_counts_overnight_open_loss():
    origin=datetime(2026,9,24,18,tzinfo=UTC)
    rows=[quote(origin-timedelta(hours=1),100,101,99,100),quote(origin+timedelta(hours=1),100,102,99,100),
          quote(origin+timedelta(hours=3),94,96,93,94),quote(origin+timedelta(hours=8),94,95,93,94)]
    r=replay('US500.sim','long',rows,forecasts(origin),spec(),Conversion('USD',[]),origin,origin+timedelta(hours=9),origin+timedelta(hours=24))
    assert r['open_entries']==1 and r['max_ftmo_midnight_balance_daily_loss']>=.065-1e-8
    assert sum(s['pnl']for s in r['session_equity_changes'])==pytest.approx(r['full']['net_equity_pnl'])


def test_nontradable_hour_still_marks_carried_loss_and_cannot_fill_target():
    origin=datetime(2026,9,24,18,tzinfo=UTC)
    rows=[quote(origin-timedelta(hours=1),100,101,99,100),quote(origin+timedelta(hours=1),100,102,99,100),
          quote(origin+timedelta(hours=2),100,150,70,100)]
    s=spec();s['weekly_minutes']=[[origin.astimezone(__import__('zoneinfo').ZoneInfo('Europe/Helsinki')).weekday()*1440+22*60,
                                origin.astimezone(__import__('zoneinfo').ZoneInfo('Europe/Helsinki')).weekday()*1440+23*60]]
    r=replay('US500.sim','long',rows,forecasts(origin),s,Conversion('USD',[]),origin,origin+timedelta(hours=3),origin+timedelta(hours=24))
    assert r['open_entries']==1 and r['full']['closed_entries']==0
    assert r['full']['max_equity_drawdown']>=.305-1e-8


def test_touching_executable_limit_fills_but_touching_raw_p01_does_not():
    origin=datetime(2026,9,24,18,tzinfo=UTC)
    rows=[quote(origin-timedelta(hours=1),100,101,99,100),
          quote(origin+timedelta(hours=1),109.5,111,109.5,110),
          quote(origin+timedelta(hours=2),109.5,111,109.49,110)]
    r=replay('US500.sim','long',rows,forecasts(origin,p01=110),spec(),Conversion('USD',[]),origin,origin+timedelta(hours=3),origin+timedelta(hours=24))
    assert r['open_entries']==1 and r['open_basket'][0]['entry']==pytest.approx(109.99)
