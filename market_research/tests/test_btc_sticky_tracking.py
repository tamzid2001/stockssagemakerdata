import pytest
from market_research.btc_sticky_tracking import direction_at,simulate,taker_fee,compare


def settlement(market='m',result='yes',close=900,confirmed=910):
    return {'market_id':market,'result':result,'close_at':close,'first_confirmed_at':confirmed,'resolution_status':'resolved'}


def signal(market='m',at=120,end=900):
    return {'market_id':market,'game_id':market,'contract_id':market+':yes',
            'signal_at':at,'signal_received_at':at+5,'market_end':end}


def quote(at,ask,market='m'):
    return {'contract_id':market+':yes','timestamp':at,'received_at':at+5,
            'ask':ask,'bid':max(0,ask-.01),'observed':True,'collection_mode':'live'}


def replay(signals=None,tape=None,settlements=None,policy='fixed_one',as_of=1000):
    return simulate(signals or [signal()],tape or [quote(180,.7)],
        settlements or [settlement()],as_of=as_of,policy=policy,precision='0.0001',multiplier=1)


def test_sticky_state_equivalent_to_opposite_latest_all_settlements():
    rows=[]; sticky='no'
    for i,result in enumerate(('yes','yes','no','yes','no','no')):
        # This updates even if there was no P90 signal, quote or forecast.
        if sticky==result:sticky='no' if result=='yes' else 'yes'
        rows.append(settlement(str(i),result,(i+1)*900,(i+1)*900+10))
        assert direction_at(rows,(i+1)*900+11,'new')['side']==sticky


def test_late_settlement_and_revision_not_backdated():
    rows=[settlement('old','yes',0,10),settlement('new','no',900,1200)]
    assert direction_at(rows,1000,'current')['side']=='no'
    assert direction_at(rows,1201,'current')['side']=='yes'
    rows.append(settlement('new','yes',900,1300))
    assert direction_at(rows,1250,'current')['side']=='yes'
    assert direction_at(rows,1301,'current')['side']=='no'
    assert direction_at(rows,10,'current') is None


@pytest.mark.parametrize('precision,expected',[('0.0001',.0175),('0.01',.02)])
def test_quadratic_fee_rounding(precision,expected):
    assert taker_fee(1,.5,precision)==pytest.approx(expected)
    assert taker_fee(100,.5,precision)==pytest.approx(1.75)


def test_next_minute_ask_not_signal_minute_or_later_substitute():
    result=replay(tape=[quote(120,.4),quote(180,.8),quote(240,.2)])
    assert result['trades'][0]['entry_price']==.8
    missed=replay(tape=[quote(120,.4),quote(240,.2)])
    assert not missed['trades'] and len(missed['missed_entries'])==1


def test_recursive_ladder_only_one_fill_per_minute_and_total_cap():
    tape=[quote(60,.9)]+[quote(t,.15) for t in range(120,900,60)]
    result=replay(signals=[signal(at=0)],tape=tape,policy='recursive_ladder')
    fills=result['trades'][0]['fills']
    assert [f['quantity'] for f in fills]==[1,2,4,8,16,32,37]
    assert sum(f['quantity'] for f in fills)==100
    assert len({f['timestamp'] for f in fills})==len(fills)
    assert fills[1]['timestamp']==180 and fills[2]['timestamp']==300


def test_missing_minute_and_ask_rebound_do_not_fill_ladder():
    result=replay(tape=[quote(180,.7),quote(240,.59),quote(300,.65),quote(420,.55)],policy='recursive_ladder')
    assert len(result['trades'][0]['fills'])==1
    assert result['ladder_orders'][0]['unfilled_attempts']==1
    assert result['ladder_orders'][0]['missing_minutes']>0


def test_recovery_uses_confirmed_losses_cap_and_does_not_reset_on_partial_win():
    signals=[];tape=[];rows=[]
    for i in range(8):
        signals.append(signal(str(i),i*900+120,(i+1)*900))
        tape.append(quote(i*900+180,.8,str(i)))
        rows.append(settlement(str(i),'no' if i!=2 else 'yes',(i+1)*900,(i+1)*900+10))
    result=replay(signals,tape,rows,policy='recover_cycle',as_of=8000)
    assert [r['quantity'] for r in result['trades']]==[1,2,5,5,12,30,75,100]
    assert all(r['fees']==pytest.approx(taker_fee(r['quantity'],.8)) for r in result['trades'])


def test_unknown_outcome_cannot_change_next_trade_size():
    s=[signal('a'),signal('b',1020,1800)]
    t=[quote(180,.8,'a'),quote(1080,.8,'b')]
    rows=[settlement('a','no',900,1500),settlement('b','yes',1800,1810)]
    result=replay(s,t,rows,policy='recover_cycle',as_of=2000)
    assert [r['quantity'] for r in result['trades']]==[1,1]


def test_report_refuses_unknown_fee_and_late_quotes():
    result=compare([], [quote(180,.7)],[],{},as_of=1000)
    assert result['fee_status']=='unavailable_no_financial_results' and not result['scenarios']
    late=quote(180,.7);late['received_at']=999
    assert compare([], [late],[],{},as_of=1000)['coverage']['timely_minute_side_rows']==0
