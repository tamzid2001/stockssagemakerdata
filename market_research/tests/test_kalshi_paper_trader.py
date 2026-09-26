from copy import deepcopy
from decimal import Decimal
import pytest
from market_research.kalshi_execution import Config
from market_research.kalshi_paper_trader import PaperTrader, modeled_fee

class Journal:
    def __init__(self): self.value={'size':2,'active':None};self.closed=[]
    def state(self):return deepcopy(self.value)
    def used(self,ticker):return any(e['ticker']==ticker for e in self.closed)
    def begin(self,e):self.value['active']=deepcopy(e)
    def finish(self,e,net):self.closed.append({**e,'net':net});self.value['active']=None

class Broker:
    enabled=False
    def __init__(self):self.row={'status':'active','market_type':'binary','open_time':'1970-01-01T00:00:00Z','close_time':'1970-01-01T00:15:00Z','exchange_index':0}
    def market(self,t):return dict(self.row)
    def submit(self,*a):raise AssertionError('Order-write path reached')
    def account(self):raise AssertionError('Paper consulted real balance/positions')

def enter():
    b=Broker();j=Journal();trader=PaperTrader(Config(),b,j)
    s={'signal_at':60,'market_end':900,'market_id':'KXBTC15M-26SEP261215-15','contract_id':'KXBTC15M-26SEP261215-15:yes'}
    q={'timestamp':120,'received_at':121,'timely':True,'yes_ask':'.60'}
    trader.enter(s,q,122);return b,j,trader,s,q

def test_paper_holds_and_official_settlement_without_orders():
    b,j,t,s,q=enter();assert t.reconcile()=='paper_held_to_settlement'
    b.row.update(status='settled',result='yes')
    assert t.reconcile()=='paper_settled'
    assert j.closed[0]['net']==Decimal('0.76')
    assert j.closed[0]['paper_only']
    with pytest.raises(RuntimeError,match='DUPLICATE'):t.enter(s,q,123)

def test_paper_stop_uses_later_observed_bid_and_fees():
    b,j,t,_,_=enter();b.row['yes_bid_dollars']='.04'
    assert t.reconcile()=='paper_stopped'
    assert j.closed[0]['net']==Decimal('-1.17')

def test_paper_missing_or_zero_bid_remains_open():
    b,j,t,_,_=enter();b.row['yes_bid_dollars']='0'
    assert t.reconcile()=='paper_held_to_settlement';assert j.state()['active']

def test_paper_rejects_enabled_broker():
    b=Broker();b.enabled=True
    with pytest.raises(RuntimeError,match='READ_ONLY'):PaperTrader(Config(),b,Journal())

def test_paper_fee_assumption():
    assert modeled_fee(2,'.60')==Decimal('.04')
