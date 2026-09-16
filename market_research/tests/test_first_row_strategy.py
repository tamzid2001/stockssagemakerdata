import time
from copy import deepcopy
from pathlib import Path
import pytest
import yaml
from market_research.first_row_strategy import VERSION, classify, simulate, from_store
from market_research.recovery_replay import process_game
from market_research.recovery_watchdog import resume_inputs
from market_research.engine import iso
from market_research.local_store import LocalStore
from market_research.p1_worker import MODELS

def pair(origin=60, available=61):
    return [dict(strategy=VERSION, forecast_id=f'{origin}-{s}', origin=origin, available_at=available,
        expected_side_count=2, market_context={'event_id':'g','market_id':'m','contract_id':s},
        rows=[{'timestamp':origin+60,'quantiles':{'0.1':.2,'0.9':.8}},
              {'timestamp':origin+120,'quantiles':{'0.1':.01,'0.9':.99}}]) for s in ('yes','no')]

def tape(t=120, yes=.1, no=.9):
    return [dict(contract_id=s, timestamp=t, ask=a, bid=max(0,a-.01), observed=True)
            for s,a in (('yes',yes),('no',no))]

def run(fs, obs):
    return simulate(fs,obs,{'yes':dict(resolution_status='resolved',selected_side_payout=1,settled_at=3600),
                           'no':dict(resolution_status='resolved',selected_side_payout=0,settled_at=3600)},as_of=4000)

def test_strict_first_row_thresholds():
    assert classify(.2,.2,.8)=='neutral' and classify(.8,.2,.8)=='neutral'
    assert classify(.1,.2,.8)=='buy' and classify(.9,.2,.8)=='sell'
    with pytest.raises(ValueError): classify(float('nan'),.2,.8)

def test_first_row_frozen_next_minute_ask_then_settlement():
    result=run(pair(),tape()+tape(180,yes=.3,no=.7))
    trade=result['trades'][0]
    assert trade['contract_id']=='yes' and trade['entry_at']==180 and trade['entry_price']==.3
    assert trade['exit_price']==1 and len(result['signals'])==2

def test_sell_selects_same_binary_complement_and_does_not_need_opposite_p10():
    result=run(pair(),tape(yes=.85,no=.3)+tape(180,yes=.8,no=.35))
    assert result['trades'][0]['contract_id']=='no'

def test_missing_first_quote_not_replaced_with_later_signal_and_no_execution_without_book():
    assert not run(pair(),tape(180)+tape(240))['trades']
    assert not run(pair(),tape())['trades']

def test_conflicting_targets_are_not_selected_using_winner():
    r=run(pair(),tape(yes=.1,no=.1)+tape(180))
    assert not r['trades'] and r['audit'][0]['status']=='ambiguous_targets'

def test_switch_only_on_next_forecasts_first_quote_recovery_capped_and_no_extra_entries():
    fs=pair()+pair(1860,1861)
    r=run(fs,tape()+tape(180,yes=.5,no=.5)+tape(1920,yes=.9,no=.1)+tape(1980,yes=.2,no=.8))
    assert [t['contract_id'] for t in r['trades']]==['yes','no']
    assert r['trades'][0]['exit_reason']=='opposite_first_row_signal'
    assert r['trades'][1]['quantity']==2 and r['trades'][1]['exit_reason']=='authoritative_settlement'
    assert all(t['quantity']<=100 for t in r['trades'])

class Provider:
    def history(self,side,start,end,**kwargs):
        return [dict(timestamp=iso(t),long_price=.4,short_price=.6,selected_position=side['side']) for t in range(1200,11000,60)]
    def resolution(self,side):
        return dict(contract_id=side['contractId'],resolution_status='resolved',selected_side_payout=int(side['side']=='long'))

def test_30_minute_origins_source_stored_once_flat_allowed_and_resumable(tmp_path):
    store=LocalStore('first','test',tmp_path/'store'); store.claim({'version':VERSION})
    sides=[dict(eventId='g',marketId='m',contractId=s,side=s,providerSymbol='game',eventStart=iso(3600),resolutionTime=iso(11000)) for s in ('long','short')]
    calls=[]
    def forecast(window,horizon,models,quantiles,**kwargs):
        assert horizon==30 and models==MODELS and kwargs['failure_policy']=='fail'
        calls.append(window); origin=window[-1].timestamp
        return dict(forecast_id=f'{origin}-{window[0].ask}',origin=origin,models=[dict(id=m,status='completed') for m in models],
                    rows=[dict(timestamp=origin+i*60,quantiles={str(q):q for q in quantiles}) for i in range(1,31)])
    r=process_game(store,Provider(),sides,12000,30,1,time.monotonic()+60,forecast,first_row=True)
    assert not r['complete'] and len(calls)==2
    count=len(store.values('observations'))
    class NoFetch(Provider):
        def history(self,*a,**k): raise AssertionError('source downloaded twice')
    r=process_game(store,NoFetch(),sides,12000,30,100,time.monotonic()+60,forecast,first_row=True)
    assert r['complete'] and len(store.values('observations'))==count
    fs=store.values('forecasts'); origins=sorted({f['origin'] for f in fs})
    assert all(b-a==1800 for a,b in zip(origins,origins[1:]))
    assert all('input_snapshot' not in f and f['input_reference']['count']<=500 for f in fs)
    assert all(q.timestamp>3600 for w in calls if w[-1].timestamp>=5520 for q in w)
    store.checkpoint('recovery_coverage',{'as_of':12000}); assert from_store(store)['forecast_count']==len(fs)
    store.db.close()

def test_invalid_settlement_window_skips_game_not_campaign(tmp_path):
    store=LocalStore('first','test',tmp_path/'s');store.claim({'version':VERSION})
    s=dict(eventId='g',marketId='m',contractId='yes',eventStart=iso(60000),resolutionTime=iso(60))
    assert process_game(store,Provider(),[s],70000,30,10,time.monotonic()+1,first_row=True)['status']=='invalid_history_range'
    store.db.close()

def test_fresh_workflow_handoff_pins_code_and_never_uses_old_campaign():
    record=dict(id='p90-'+'a'*24,campaign_kind=VERSION,status='paused',configuration=dict(continuous=True,horizon=30,code_sha='b'*40))
    assert resume_inputs(record) is None
    assert resume_inputs(record,first_row=True)['code_ref']=='b'*40
    assert resume_inputs({**record,'status':'archived'},first_row=True) is None
    text=(Path(__file__).resolve().parents[2]/'.github/workflows/sports-first-row-backtest.yml').read_text()
    w=yaml.safe_load(text)
    assert w['jobs']['replay']['timeout-minutes']==360 and not w['concurrency']['cancel-in-progress']
    assert 'upload-artifact' not in text and '--strategy first_row' in text and '--horizon 30' in text
