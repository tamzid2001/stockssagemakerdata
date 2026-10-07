from copy import deepcopy
from decimal import Decimal
import pytest
from market_research.opposite_dollar_trader import DollarTrader, AccountGate, CloudJournal, PREFIX, complete_context, quantity_remaining, approved, market_ask
from market_research.opposite_strategies import DollarConfig, STRATEGIES, VERSION, portfolio_fingerprint
from dataclasses import asdict
from market_research.kalshi_execution import order_payload

CONFIG=DollarConfig("KXSOL15M")
TICKER="KXSOL15M-26OCT071515"
FEE={"fee_type":"quadratic","multiplier":1}

class Journal:
    def __init__(self):self.state={"entries":{},"totals":{}};self.saved=[]
    def save(self):self.saved.append(deepcopy(self.state))
class Gate:
    def __init__(self,*_):pass
    def __enter__(self):return self
    def verify(self):pass
    def __exit__(self,*_):pass
class Broker:
    def __init__(self,journal):self.journal=journal;self.posts=[];self.orders={};self.price="0.2000";self.now=100
    def market(self,ticker):
        return {"ticker":ticker,"market_type":"binary","status":"active","open_time":"1970-01-01T00:00:00Z","close_time":"1970-01-01T00:15:00Z",
            "exchange_index":2,"yes_ask_dollars":"0.8200","no_ask_dollars":self.price,
            "price_ranges":[{"start":"0.0000","end":"1.0000","step":"0.0001"}]}
    def balance(self,*_):return Decimal(100)
    def submit(self,intent):
        # Proves durable intent is present before the exchange write.
        assert self.journal.saved[-1]["entries"][TICKER]["live"]["pending"]["intent"]==intent
        self.posts.append(intent);return {"order_id":"order-identifier-1234"}
    def find_order(self,intent,order_id=None):return self.orders.get(intent["client_order_id"])
    def pages(self,*_,**__):return []

def setup(mode="live"):
    j=Journal();b=Broker(j);t=DollarTrader(CONFIG,b,j,mode,gate_factory=Gate)
    s={"market_id":TICKER,"contract_id":TICKER+":yes","signal_at":60,"signal_received_at":65,"market_end":900,
       "sticky":{"side":"yes","confirmed_at":30}}
    q={"timestamp":120,"received_at":125,"timely":True}
    t.begin(s,q,125)
    return t,b,j

def terminal(intent,filled,cost,fee="0.01"):
    return {"client_order_id":intent["client_order_id"],"ticker":TICKER,"exchange_index":2,
            "subaccount_number":0,"book_side":"ask","outcome_side":"no","fill_count_fp":filled,
            "remaining_count_fp":"0.00","status":"canceled","taker_fill_cost_dollars":cost,
            "maker_fill_cost_dollars":"0","taker_fees_dollars":fee,"maker_fees_dollars":"0",
            "order_id":"order-identifier-1234"}

def test_no_purchase_uses_complementary_yes_book_and_one_dollar_fractional_budget(monkeypatch):
    monkeypatch.setattr("market_research.opposite_dollar_trader.time.time",lambda:126)
    t,b,j=setup();t.reconcile(TICKER,FEE,126)
    assert b.posts[0]["side"]=="ask" and b.posts[0]["price"]=="0.8000"
    assert b.posts[0]["count"]=="5.00"
    assert b.posts[0]["time_in_force"]=="immediate_or_cancel"

def test_ambiguous_or_unseen_order_never_blindly_reposts(monkeypatch):
    monkeypatch.setattr("market_research.opposite_dollar_trader.time.time",lambda:126)
    t,b,j=setup();t.reconcile(TICKER,FEE,126)
    t.reconcile(TICKER,FEE,127);t.reconcile(TICKER,FEE,128)
    assert len(b.posts)==1 and j.state["entries"][TICKER]["live"]["pending"]

def test_partial_fill_retries_current_price_and_only_unspent_budget(monkeypatch):
    monkeypatch.setattr("market_research.opposite_dollar_trader.time.time",lambda:128)
    t,b,j=setup();t.reconcile(TICKER,FEE,126);first=b.posts[0]
    b.orders[first["client_order_id"]]=terminal(first,"3.00","0.60")
    b.price="0.4000";t.reconcile(TICKER,FEE,128)
    assert b.posts[1]["price"]=="0.6000" and b.posts[1]["count"]=="1.00"
    assert b.posts[1]["client_order_id"]!=first["client_order_id"]
    assert j.state["entries"][TICKER]["live"]["cost"]=="0.60"

def test_one_second_retry_cadence_waits_for_reconciled_terminal_order(monkeypatch):
    monkeypatch.setattr("market_research.opposite_dollar_trader.time.time",lambda:127)
    t,b,j=setup();t.reconcile(TICKER,FEE,126);first=b.posts[0]
    b.orders[first["client_order_id"]]=terminal(first,"0.00","0",fee="0")
    t.reconcile(TICKER,FEE,126.5)
    assert len(b.posts)==1
    t.reconcile(TICKER,FEE,127)
    assert len(b.posts)==2

def test_market_close_stops_new_orders_even_without_a_fill(monkeypatch):
    monkeypatch.setattr("market_research.opposite_dollar_trader.time.time",lambda:900)
    t,b,j=setup();t.reconcile(TICKER,FEE,900)
    assert not b.posts

def test_paper_holds_to_settlement_and_has_no_five_cent_stop():
    t,b,j=setup("paper");t.reconcile(TICKER,FEE,126)
    b.price="0.9900";t.reconcile(TICKER,FEE,300)
    assert j.state["entries"][TICKER]["paper"]["status"]=="held" and not b.posts

def test_begin_requires_confirmed_sticky_and_never_uses_late_backfills():
    rows=[{"market_id":TICKER,"timestamp":i*60,"received_at":i*60+31,"timely":False,"collection_mode":"live"} for i in range(1,8)]
    assert complete_context(rows,TICKER,0,7,500) is None
    j=Journal();t=DollarTrader(CONFIG,Broker(j),j,"paper")
    with pytest.raises(RuntimeError,match="OFFICIAL_STICKY"):
        t.begin({"market_id":TICKER,"contract_id":TICKER+":yes","signal_at":60,"signal_received_at":65,"market_end":900},
                {"timestamp":120,"received_at":125,"timely":True},125)

def test_live_gates_pin_entire_five_asset_portfolio_and_exact_code():
    env={"QUANTURA_CODE_SHA":"a"*40,"QUANTURA_KALSHI_DOLLAR_APPROVED_SHA":"a"*40,
         "QUANTURA_KALSHI_DOLLAR_APPROVED_CONFIG":portfolio_fingerprint(),"QUANTURA_KALSHI_DOLLAR_LIVE_ENABLED":"true"}
    assert approved(CONFIG,env)
    assert not approved(CONFIG,{**env,"QUANTURA_CODE_SHA":"b"*40})

def test_fractional_budget_floor_and_dynamic_market_grid():
    assert quantity_remaining({"cost":"0"},".18")==Decimal("5.55")
    assert quantity_remaining({"cost":".999"},".20")==0
    b=Broker(Journal());m=b.market(TICKER)
    assert market_ask(m,"no",120,900)==Decimal(".20")
    m["no_ask_dollars"]=".20005"
    with pytest.raises(RuntimeError,match="GRID"):market_ask(m,"no",120,900)

class SharedJournal:
    def __init__(self):
        self.account="account";self.holder="worker";self.config=CONFIG;self.rows={};self.generations={}
    def read(self,name):return deepcopy(self.rows.get(name,{})),self.generations.get(name,0)
    def write(self,name,value,generation):
        assert generation==self.generations.get(name,0)
        self.rows[name]=deepcopy(value);self.generations[name]=generation+1;return generation+1
    def verify(self):pass

def test_shared_admission_accepts_all_five_owned_positions_and_releases_gate():
    j=SharedJournal();positions=[]
    for s in STRATEGIES:
        ticker=s+"-26OCT071515"
        entry={"ticker":ticker,"status":"held","filled":"5.00","side":"no","market_end":10**12,"pending":None}
        j.rows[PREFIX+j.account+"/"+s+".enc"]={"version":VERSION,"config":asdict(DollarConfig(s)),"entries":{ticker:{"live":entry}}}
        positions.append({"ticker":ticker,"position_fp":"-5.00"})
    class B:
        def account(self):return [],positions
    with AccountGate(j,B()) as gate:gate.verify()
    assert j.rows[PREFIX+j.account+"/order-gate.enc"]["lease"]["expires"]==0

def test_unattributed_account_position_blocks_entry_and_releases_claim():
    j=SharedJournal()
    class B:
        def account(self):return [],[{"ticker":"UNRELATED-MARKET","position_fp":"1.00"}]
    with pytest.raises(RuntimeError,match="ACCOUNT_POSITION_UNVERIFIED"):
        with AccountGate(j,B()):pass
    assert j.rows[PREFIX+j.account+"/order-gate.enc"]["lease"]["expires"]==0

def test_changed_gcs_generation_fences_stale_worker():
    j=object.__new__(CloudJournal);j.name="name";j.holder="worker";j.fence=1;j.generation=4
    j.read=lambda _:({"lease":{"holder":"worker","fence":1,"expires":10**12}},5)
    with pytest.raises(RuntimeError,match="FENCE_LOST"):j.verify()
