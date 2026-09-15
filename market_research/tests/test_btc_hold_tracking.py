from copy import deepcopy
import pytest
from market_research.btc_hold_sizing import replay
from market_research.btc_hold_tracking import compare
from market_research.tests.test_recovery_switch import forecasts, quote


def ledger(nets):
    trades=[];confirm={}
    for i,net in enumerate(nets):
        entry=100+i*100
        trades.append(dict(trade_id=str(i),game_id=str(i),contract_id=str(i),entry_at=entry,
            exit_at=entry+50,entry_price=.5,quantity=1.,status='closed',gross_pnl=net,fees=0.,net_pnl=net))
        confirm[str(i)]=entry+60
    return trades,confirm


def test_four_losses_and_capped_integer_vs_legacy_sizing():
    rows,c=ledger([-.5]*10+[.5])
    fractional=replay(rows,c,as_of=2000,policy='reset_after_win')
    integer=replay(rows,c,as_of=2000,policy='reset_after_win',quantity_step=1.)
    assert [r['quantity'] for r in fractional['trades'][:5]]==[1,2.5,6.25,15.625,39.0625]
    assert [r['quantity'] for r in integer['trades'][:8]]==[1,2,5,12,30,75,100,100]
    assert max(t['quantity'] for t in fractional['trades'])==100
    assert integer['trades'][-1]['next_size_when_confirmed']==1
    assert all(t['quantity']==1 for t in rows)


def test_recovery_partial_win_holds_size_but_reset_win_policy_resets():
    rows,c=ledger([-.5,.1,.5,.5])
    recover=replay(rows,c,as_of=1000,policy='recover_cycle')
    reset=replay(rows,c,as_of=1000,policy='reset_after_win')
    assert [r['quantity'] for r in recover['trades']]==[1,2.5,2.5,1]
    assert [r['quantity'] for r in reset['trades']]==[1,2.5,1,1]


def test_late_equal_time_and_missing_confirmation_never_size_with_hindsight():
    rows,c=ledger([-.5,.5])
    for confirmed in (None,200,201,1001):
        c['0']=confirmed
        result=replay(rows,c,as_of=1000,policy='recover_cycle')
        assert result['trades'][1]['quantity']==1
        assert result['trades'][1]['known_closed_trades_before_entry']==0


def test_open_marks_not_wins_or_sizing_losses():
    rows,c=ledger([-.5])
    rows.append(dict(trade_id='open',game_id='open',contract_id='open',entry_at=200,
                     entry_price=.5,quantity=1.,status='open',mark_net_pnl=-.2))
    r=replay(rows,c,as_of=220,policy='recover_cycle')
    assert r['summary']['closed_trades']==1 and r['summary']['open_positions']==1
    assert r['trades'][-1]['quantity']==2.5
    assert r['summary']['open_mark_net_pnl']==-.5


def inputs(history=2):
    fs=forecasts(end=600)
    for f in fs: f['history_count']=history
    rows=[]
    for t,yes,no in [(180,.91,.1),(240,.90,.12),(300,.89,.13),(360,.2,.9),(420,.15,.95)]:
        for side,ask in [('yes',yes),('no',no)]:
            rows.append({**quote(t,ask,side=side),'received_at':t+3,'collection_mode':'live'})
    outcomes={'g-yes':{'resolution_status':'resolved','settled_at':600,'checked_at':610,'selected_side_payout':1}}
    return fs,rows,outcomes


def test_cohorts_separate_and_no_p10_switch_for_hold():
    for history in (1,2):
        fs,rows,outcomes=inputs(history)
        r=compare(fs,rows,outcomes,as_of=620)
        assert (r['history_minutes'],r['horizon_minutes'])==(history,15-history)
        t=r['scenarios']['prospective_quote_integer']['fixed_one']['trades']
        assert len(t)==1 and t[0]['exit_at']==600 and t[0]['entry_at']==243
        assert t[0]['contract_id']=='g-yes' and t[0]['exit_price']==1
    fs[0]['history_count']=1
    with pytest.raises(ValueError,match='COHORTS'):compare(fs,rows,outcomes,as_of=620)


def test_untimed_late_and_historical_quotes_do_not_become_prospective():
    fs,rows,outcomes=inputs()
    for patch in ({'received_at':None},{'received_at':1000},{'collection_mode':'historical'}):
        altered=[{**q,**patch} for q in rows]
        r=compare(fs,altered,outcomes,as_of=620)
        assert r['coverage']['timely_prospective_observations']==0
        assert r['coverage']['first_signal_markets']==0
        assert r['scenarios']['prospective_quote_integer']['fixed_one']['summary']['win_rate'] is None
        assert r['scenarios']['legacy_quote_benchmark']['fixed_one']['summary']['closed_trades']==1


def test_post_only_touch_is_not_fill_and_unfilled_orders_remain_reported():
    fs,rows,outcomes=inputs()
    r=compare(fs,rows,outcomes,as_of=620)
    orders=r['orders']
    assert orders[0]['limit_price']==.90
    assert orders[0]['status']=='cancelled_reprice'  # next ask .90 only touches .90
    assert orders[1]['status']=='cancelled_reprice'  # next ask .89 only touches .89
    filled=[o for o in orders if o['status']=='candidate_fill']
    assert len(filled)==1 and filled[0]['filled_at']==363
    assert filled[0]['execution_verified'] is False
    assert r['live_readiness']['ready'] is False


def test_first_selected_side_latched_even_if_opposite_becomes_signal():
    fs,rows,outcomes=inputs()
    for q in rows:
        if q['contract_id']=='g-yes': q.update(ask=.99,bid=.98)
    r=compare(fs,rows,outcomes,as_of=620)
    assert len(r['signals'])==1 and r['signals'][0]['contract_id']=='g-yes'
    assert not r['scenarios']['post_only_candidate_integer']['fixed_one']['trades']
    assert r['orders'][-1]['status']=='expired_unfilled'


def test_missing_next_minute_not_backfilled_and_inputs_immutable():
    fs,rows,outcomes=inputs()
    rows=[q for q in rows if q['timestamp']!=240]
    before=deepcopy((fs,rows,outcomes))
    r=compare(fs,rows,outcomes,as_of=620)
    assert r['coverage']['quote_entries_missed']==1
    assert r['orders'][0]['status']=='cancelled_missing_minute'
    assert (fs,rows,outcomes)==before
