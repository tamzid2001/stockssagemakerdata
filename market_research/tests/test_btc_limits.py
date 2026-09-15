import pytest

from market_research.btc_limits import simulate, limit_price
from market_research.btc_minute_policy import compare
from market_research.tests.test_recovery_switch import forecasts, quote


def test_minus_one_cent_non_marketable_limits_and_reduce_only_exits():
    assert limit_price('buy',{'ask':.805,'bid':.79})==.79
    assert limit_price('sell',{'ask':.81,'bid':.795})==.81
    assert limit_price('buy',{'ask':.01,'bid':0}) is None
    assert limit_price('sell',{'ask':1,'bid':.99}) is None
    rows=[quote(180,.85,bid=.83),quote(240,.83,bid=.81),quote(300,.2,bid=.18),
          quote(360,.21,bid=.20),quote(360,.8,bid=.78,side='no'),
          quote(420,.78,bid=.76,side='no')]
    result=simulate(forecasts(),rows,{},as_of=420)
    assert [t['contract_id'] for t in result['trades']]==['g-yes','g-no']
    assert [t['quantity'] for t in result['trades']]==[1,2.5]
    first,second=result['trades']
    assert first['entry_at']==240 and first['entry_price']==.84
    assert first['exit_at']==360 and first['exit_reason']=='p10_exit_and_opposite'
    assert second['entry_at']==420  # Cannot open before prior sell fills.
    assert all(o['post_only'] and o['reduce_only']==(o['action']=='sell') for o in result['orders'])


def test_touch_is_not_fill_missing_minutes_not_backfilled():
    for rows in ([quote(180,.85,bid=.83),quote(240,.84,bid=.82)],
                 [quote(180,.85,bid=.83),quote(300,.8,bid=.79)]):
        r=simulate(forecasts(),rows,{},as_of=300)
        assert not r['trades']
        assert r['summary']['order_counts'].get('candidate_fill',0)==0


def test_first_low_quote_can_signal_paired_side_without_p90_and_hold_settlement():
    rows=[quote(180,.2,bid=.18),quote(180,.5,bid=.48,side='no'),quote(240,.48,bid=.46,side='no')]
    r=simulate(forecasts(end=600),rows,{'g-no':{'resolution_status':'resolved','settled_at':600,'selected_side_payout':1}},as_of=600)
    trade=r['trades'][0]
    assert trade['entry_reason']=='opposite_p10_entry' and trade['quantity']==1
    assert trade['exit_at']==600 and trade['exit_reason']=='authoritative_settlement'


def test_no_signal_no_forced_trade_and_no_future_or_intraminute_execution():
    rows=[quote(180,.5),quote(181,.9),quote(240,.5)]
    assert not simulate(forecasts(),rows,{},as_of=240)['trades']
    assert not simulate(forecasts(published=500),rows,{},as_of=240)['orders']


def test_opposite_p90_switch_does_not_automatically_increase_size():
    rows=[quote(180,.85,bid=.83),quote(240,.83,bid=.81),
          quote(300,.5,bid=.48),quote(300,.9,bid=.88,side='no'),
          quote(360,.52,bid=.50),quote(360,.9,bid=.88,side='no'),quote(420,.88,bid=.86,side='no')]
    r=simulate(forecasts(),rows,{},as_of=420)
    assert r['trades'][0]['exit_reason']=='opposite_p90_switch'
    assert [t['quantity'] for t in r['trades']]==[1,1]


def test_new_quote_scenarios_keep_old_book_separate():
    rows=[quote(180,.2,bid=.18),quote(180,.5,bid=.48,side='no'),quote(240,.5,side='no')]
    r=compare(forecasts(),rows,{},as_of=240)
    assert set(r['scenarios'])=={'one_percent_notional_assumption','zero_fee_sensitivity_only'}
    assert not r['post_only_limit_proxy']['execution_verified']
    for s in r['scenarios'].values():
        assert s['trades'][0]['contract_id']=='g-no'
        assert s['configuration']['max_shares']==100
        assert not s['configuration']['maker_fills_verified']


def test_repeated_p10_losses_cap_at_100_and_never_hold_both_sides():
    rows=[quote(180,.85,bid=.83),quote(240,.83,bid=.81)]
    held='yes';t=300
    for i in range(8):
        other='no' if held=='yes' else 'yes'
        rows.extend([quote(t,.2,bid=.18,side=held),quote(t+60,.21,bid=.20,side=held),
                     quote(t+60,.9,bid=.88,side=other),quote(t+120,.88,bid=.86,side=other)])
        held=other;t+=180
    result=simulate(forecasts(end=2400),rows,{},as_of=t)
    sizes=[t['quantity'] for t in result['trades']]
    assert sizes[:4]==[1,2.5,6.25,15.625]
    assert max(sizes)==100
    assert sum(t['status']=='open' for t in result['trades'])<=1
