from copy import deepcopy

import pytest

from market_research.spy_option_targets import target_comparison, summarize, prices_by_minute


def inputs():
    week={'end':'2026-08-21T20:00:00Z','average_quantiles':{'0.1':90.,'0.9':110.}}
    def bar(t,value,**extra):
        return {'timestamp':t,'open':value,'high':value,'low':value,'close':value,**extra}
    underlying=[bar('2026-08-17T13:30:00Z',100.,high=115.,low=85.),  # entry minute must be ignored
                bar('2026-08-17T14:00:00Z',109.,high=111.),
                bar('2026-08-18T14:00:00Z',91.,low=89.)]
    call={'rows':[bar('2026-08-17T13:30:00Z',2.),bar('2026-08-17T14:00:00Z',99.),
                  bar('2026-08-17T14:01:00Z',4.),bar('2026-08-18T14:01:00Z',.5),bar('2026-08-21T19:55:00Z',.01)]}
    put={'rows':[bar('2026-08-17T13:30:00Z',3.),bar('2026-08-17T14:01:00Z',1.),
                 bar('2026-08-18T14:01:00Z',6.),bar('2026-08-21T19:55:00Z',.02)]}
    return week,underlying,{'call':call,'put':put}


def calculate(mode='matching_leg',change=None):
    week,underlying,legs=inputs()
    if change:
        change(underlying,legs)
    return target_comparison(week,underlying,legs,'2026-08-17T13:30:00Z','2026-08-17T13:40:00Z',mode)


def test_matching_leg_waits_for_its_own_touch_and_never_uses_signal_bar_price():
    result=calculate()
    call,put=result['legs']
    assert call['exit_time']=='2026-08-17T14:01:00Z'
    assert call['exit_bar_open']==4  # Not signal-minute 99; not an assumed quantile fill.
    assert put['exit_time']=='2026-08-18T14:01:00Z'
    assert put['exit_bar_open']==6
    assert result['net_pnl_proxy_before_costs']==500
    assert result['target_exit_legs']==2


def test_both_legs_close_together_at_first_either_target():
    result=calculate('both_legs')
    assert {leg['exit_time'] for leg in result['legs']}=={'2026-08-17T14:01:00Z'}
    assert result['net_pnl_proxy_before_costs']==0
    assert result['net_pnl_after_065_per_contract_per_fill']==-2.60


def test_untouched_leg_has_observed_1555_close_not_expiry_intrinsic():
    result=calculate(change=lambda underlying,legs:underlying.pop())
    put=result['legs'][1]
    assert put['exit_reason']=='friday_1555_time_exit'
    assert put['exit_time']=='2026-08-21T19:55:00Z'
    assert put['exit_bar_open']==.02
    assert result['target_exit_legs']==1 and result['timed_exit_legs']==1


def test_missing_time_exit_is_incomplete_not_zero_or_expiry_fill():
    def change(underlying,legs):
        underlying.pop();legs['put']['rows'].pop()
    result=calculate(change=change)
    assert result['status']=='incomplete_exit_prices'
    assert result['net_pnl_proxy_before_costs'] is None
    assert result['legs'][1]['net_pnl_proxy_before_costs'] is None


def test_no_stale_price_when_next_minute_has_no_trade():
    def change(underlying,legs):
        del legs['call']['rows'][2]
    result=calculate(change=change)
    assert result['legs'][0]['status']=='exit_unpriced'
    assert result['legs'][0]['exit_time'] is None


def test_both_requires_common_exit_minute():
    def change(underlying,legs):
        legs['put']['rows'][1]['timestamp']='2026-08-17T14:02:00Z'
    result=calculate('both_legs',change)
    assert result['status']=='incomplete_exit_prices'


def test_later_prices_do_not_change_first_touch_exit():
    result=calculate('both_legs')
    def change(underlying,legs):
        underlying[-1]['low']=1
        legs['put']['rows'][-1].update(open=99.,high=99.,low=99.,close=99.)
    after=calculate('both_legs',change)
    assert result['net_pnl_proxy_before_costs']==after['net_pnl_proxy_before_costs']
    assert [t['exit_time'] for t in result['legs']]==[t['exit_time'] for t in after['legs']]


def test_incomplete_weeks_cannot_win_a_full_sample_ranking():
    complete=calculate()
    r=summarize([{'scenarios':{'friday_after_close/matching_leg':complete}},{'errors':[{}]}])
    s=r['friday_after_close/matching_leg']
    assert s['complete_weeks']==1 and s['incomplete_weeks']==1
    assert s['ranking_valid_for_full_sample'] is False


def test_duplicate_options_rejected():
    _,_,legs=inputs()
    legs['call']['rows'].append(deepcopy(legs['call']['rows'][0]))
    with pytest.raises(ValueError,match='DUPLICATE'):
        prices_by_minute(legs['call'])
