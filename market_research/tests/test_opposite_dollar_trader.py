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
        assert self.journal.saved[-1]["entries"][TICKER]["live"]["pending"]["delivery_stage"]=="submitting"
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
    clock=[126]
    monkeypatch.setattr("market_research.opposite_dollar_trader.time.time",lambda:clock[0])
    t,b,j=setup();t.reconcile(TICKER,FEE,126);first=b.posts[0]
    b.orders[first["client_order_id"]]=terminal(first,"3.00","0.60")
    clock[0]=128;b.price="0.4000";t.reconcile(TICKER,FEE,128)
    assert b.posts[1]["price"]=="0.6000" and b.posts[1]["count"]=="1.00"
    assert b.posts[1]["client_order_id"]!=first["client_order_id"]
    assert j.state["entries"][TICKER]["live"]["cost"]=="0.60"

def test_one_second_retry_cadence_waits_for_reconciled_terminal_order(monkeypatch):
    clock=[126]
    monkeypatch.setattr("market_research.opposite_dollar_trader.time.time",lambda:clock[0])
    t,b,j=setup();t.reconcile(TICKER,FEE,126);first=b.posts[0]
    b.orders[first["client_order_id"]]=terminal(first,"0.00","0",fee="0")
    t.reconcile(TICKER,FEE,126.5)
    assert len(b.posts)==1
    clock[0]=127;t.reconcile(TICKER,FEE,127)
    assert len(b.posts)==2

def test_retry_interval_starts_at_actual_admission_completion(monkeypatch):
    monkeypatch.setattr("market_research.opposite_dollar_trader.time.time",lambda:126.8)
    t,b,j=setup();t.reconcile(TICKER,FEE,126)
    b.orders[b.posts[0]["client_order_id"]]=terminal(b.posts[0],"0.00","0",fee="0")
    t.reconcile(TICKER,FEE,127.4)
    assert len(b.posts)==1

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

def test_shared_gate_compare_and_swap_contention_waits_without_submission():
    from google.api_core.exceptions import PreconditionFailed
    j=SharedJournal()
    def conflict(*_):raise PreconditionFailed("generation changed")
    j.write=conflict
    with pytest.raises(RuntimeError,match="LEASE_HELD"):
        with AccountGate(j,None):pytest.fail("Contended gate cannot admit orders")

class ReadBlob:
    def __init__(self,generation,payload=None,*,reload_error=None,download_error=None):
        self.generation=generation;self.payload=payload;self.size=len(payload or b"")
        self.reload_error=reload_error;self.download_error=download_error;self.downloads=[]
    def reload(self):
        if self.reload_error:raise self.reload_error
    def download_to_filename(self,path,**kwargs):
        self.downloads.append(kwargs)
        if self.download_error:raise self.download_error
        path.write_bytes(self.payload)

def reading_journal(monkeypatch,blobs):
    monkeypatch.setattr("market_research.opposite_dollar_trader.time.sleep",lambda _:None)
    class Bucket:
        def __init__(self):self.reads=0
        def blob(self,name):
            index=self.reads;self.reads+=1;return blobs[index]
    j=object.__new__(CloudJournal);j.bucket=Bucket();return j

def encoded_state(monkeypatch,tmp_path,value):
    # Exercise authenticated encrypted decoding with a test-only key.
    from market_research.recovery_cloud import encode_catalog
    monkeypatch.setenv("QUANTURA_RESEARCH_ARTIFACT_KEY","a"*64)
    path=tmp_path/"snapshot.enc";encode_catalog(value,path);return path.read_bytes()

@pytest.mark.parametrize("error_code",[404,412])
def test_read_reloads_new_generation_after_concurrent_replacement(monkeypatch,tmp_path,error_code):
    from google.api_core.exceptions import NotFound,PreconditionFailed
    error=(NotFound if error_code==404 else PreconditionFailed)("old generation replaced")
    value={"lease":{"holder":"other","fence":2},"entries":{"market":{"live":{"pending":{"intent":{"client_order_id":"keep-me"}}}}}}
    blobs=[ReadBlob(4,download_error=error),ReadBlob(5,encoded_state(monkeypatch,tmp_path,value))]
    j=reading_journal(monkeypatch,blobs)
    assert j.read("journal")== (value,5)
    assert j.bucket.reads==2
    assert [b.downloads[0]["if_generation_match"] for b in blobs]==[4,5]

def test_genuinely_absent_journal_can_be_created(monkeypatch):
    from google.api_core.exceptions import NotFound
    j=reading_journal(monkeypatch,[ReadBlob(None,reload_error=NotFound("absent"))])
    assert j.read("journal")==({},0)

def test_observed_journal_disappearance_never_erases_pending_state(monkeypatch):
    from google.api_core.exceptions import NotFound
    blobs=[ReadBlob(4,download_error=NotFound("obsolete generation"))]+[
        ReadBlob(None,reload_error=NotFound("missing after observation")) for _ in range(3)]
    j=reading_journal(monkeypatch,blobs)
    with pytest.raises(RuntimeError,match="TRADER_SNAPSHOT_BUSY"):j.read("journal")
    assert j.bucket.reads==4

def test_continuous_generation_replacement_has_bounded_read_retries(monkeypatch):
    from google.api_core.exceptions import PreconditionFailed
    j=reading_journal(monkeypatch,[ReadBlob(i,download_error=PreconditionFailed("replaced")) for i in range(4)])
    with pytest.raises(RuntimeError,match="TRADER_SNAPSHOT_BUSY"):j.read("journal")
    assert j.bucket.reads==4

def test_refreshed_snapshot_still_fences_out_the_stale_owner(monkeypatch,tmp_path):
    from google.api_core.exceptions import NotFound
    value={"lease":{"holder":"other","fence":2,"expires":10**12}}
    j=reading_journal(monkeypatch,[ReadBlob(4,download_error=NotFound("replaced")),ReadBlob(5,encoded_state(monkeypatch,tmp_path,value))])
    j.name="journal";j.holder="worker";j.fence=1;j.generation=4
    with pytest.raises(RuntimeError,match="TRADER_FENCE_LOST"):j.verify()

def test_invalid_encrypted_snapshot_is_not_retried_or_treated_as_empty(monkeypatch):
    j=reading_journal(monkeypatch,[ReadBlob(4,b"invalid authenticated journal")])
    with pytest.raises(ValueError,match="INVALID_CATALOG_ARCHIVE"):j.read("journal")
    assert j.bucket.reads==1

def test_snapshot_contention_cannot_submit_an_order(monkeypatch):
    monkeypatch.setattr("market_research.opposite_dollar_trader.time.time",lambda:126)
    class BusyGate(Gate):
        def __enter__(self):raise RuntimeError("TRADER_SNAPSHOT_BUSY")
    t,b,j=setup();t.gate_factory=BusyGate
    with pytest.raises(RuntimeError,match="TRADER_SNAPSHOT_BUSY"):t.reconcile(TICKER,FEE,126)
    assert not b.posts and j.state["entries"][TICKER]["live"]["pending"] is None

def test_known_pre_submit_gate_failure_clears_unsent_intent(monkeypatch):
    monkeypatch.setattr("market_research.opposite_dollar_trader.time.time",lambda:126)
    class LostGate(Gate):
        def verify(self):raise RuntimeError("ACCOUNT_ORDER_GATE_LOST")
    t,b,j=setup();t.gate_factory=LostGate
    with pytest.raises(RuntimeError,match="ACCOUNT_ORDER_GATE_LOST"):t.reconcile(TICKER,FEE,126)
    assert not b.posts and j.state["entries"][TICKER]["live"]["pending"] is None
    assert any(s["entries"][TICKER]["live"]["pending"] for s in j.saved)
    assert j.saved[-1]["entries"][TICKER]["live"]["pending"] is None

def test_restart_can_replace_proven_unsent_prepared_intent(monkeypatch):
    monkeypatch.setattr("market_research.opposite_dollar_trader.time.time",lambda:126)
    t,b,j=setup();entry=j.state["entries"][TICKER]["live"]
    entry.update(attempt=1,pending={"intent":order_payload(TICKER,"no",Decimal("5"),".20",2,CONFIG,attempt=1,fractional_remainder=True),"delivery_stage":"prepared"})
    t.reconcile(TICKER,FEE,126)
    assert len(b.posts)==1 and entry["attempt"]==2

@pytest.mark.parametrize("stage",[None,"submitting"])
def test_restart_preserves_uncertain_legacy_and_submitting_intents(monkeypatch,stage):
    monkeypatch.setattr("market_research.opposite_dollar_trader.time.time",lambda:126)
    t,b,j=setup();entry=j.state["entries"][TICKER]["live"]
    pending={"intent":order_payload(TICKER,"no",Decimal("5"),".20",2,CONFIG,attempt=1,fractional_remainder=True)}
    if stage:pending["delivery_stage"]=stage
    entry.update(attempt=1,pending=deepcopy(pending));t.reconcile(TICKER,FEE,126)
    assert not b.posts and entry["pending"]==pending

def test_clock_crossing_market_close_during_admission_never_submits(monkeypatch):
    clock=[126]
    monkeypatch.setattr("market_research.opposite_dollar_trader.time.time",lambda:clock[0])
    class ClosingGate(Gate):
        def verify(self):clock[0]=900
    t,b,j=setup();t.gate_factory=ClosingGate;t.reconcile(TICKER,FEE,126)
    assert not b.posts and j.state["entries"][TICKER]["live"]["pending"] is None

@pytest.mark.parametrize("code",[400,401,403,422])
def test_definitively_rejected_ioc_is_not_left_ambiguous(monkeypatch,code):
    monkeypatch.setattr("market_research.opposite_dollar_trader.time.time",lambda:126)
    t,b,j=setup()
    def reject(_):raise RuntimeError("KALSHI_HTTP_"+str(code))
    b.submit=reject
    with pytest.raises(RuntimeError,match="KALSHI_HTTP"):t.reconcile(TICKER,FEE,126)
    entry=j.state["entries"][TICKER]["live"]
    assert entry["pending"] is None and entry["status"]=="entry_rejected"

def test_rate_limit_rejection_retries_after_delay_with_new_intent(monkeypatch):
    clock=[126]
    monkeypatch.setattr("market_research.opposite_dollar_trader.time.time",lambda:clock[0])
    t,b,j=setup();submit=b.submit
    def throttle(_):raise RuntimeError("KALSHI_HTTP_429")
    b.submit=throttle;t.reconcile(TICKER,FEE,126)
    assert j.state["entries"][TICKER]["live"]["pending"] is None
    b.submit=submit;clock[0]=127;t.reconcile(TICKER,FEE,127)
    assert not b.posts
    clock[0]=130;t.reconcile(TICKER,FEE,130)
    assert len(b.posts)==1 and j.state["entries"][TICKER]["live"]["attempt"]==2

def test_actual_terminal_fill_is_applied_exactly_once_after_recovery(monkeypatch):
    monkeypatch.setattr("market_research.opposite_dollar_trader.time.time",lambda:126)
    t,b,j=setup();t.reconcile(TICKER,FEE,126);intent=b.posts[0]
    b.orders[intent["client_order_id"]]=terminal(intent,"5.00","1.00",".056")
    recovered_j=Journal();recovered_j.state=deepcopy(j.saved[-1])
    recovered= DollarTrader(CONFIG,b,recovered_j,"live",gate_factory=Gate)
    recovered.reconcile(TICKER,FEE,128);recovered.reconcile(TICKER,FEE,129)
    entry=recovered_j.state["entries"][TICKER]["live"]
    assert (entry["filled"],entry["cost"],entry["fees"])==("5.00","1.00","0.056")
    assert len(entry["fills"])==1 and len(b.posts)==1

def test_settlement_updates_totals_exactly_once():
    t,b,j=setup("paper");t.reconcile(TICKER,FEE,126)
    e=j.state["entries"][TICKER]["paper"];market={"status":"settled","result":"no"}
    t.settle(e,"paper",market,1000)
    t.settle(e,"paper",market,1001)
    t.reconcile(TICKER,FEE,1001)
    assert j.state["totals"]["paper"]["trades"]==1
    assert Decimal(j.state["totals"]["paper"]["net"])==Decimal(e["net_pnl"])

def test_stop_request_during_admission_never_sends_a_new_ioc(monkeypatch):
    monkeypatch.setattr("market_research.opposite_dollar_trader.time.time",lambda:126)
    t,b,j=setup()
    class StoppingGate(Gate):
        def verify(self):t.stopping=True
    t.gate_factory=StoppingGate;t.reconcile(TICKER,FEE,126)
    assert not b.posts and j.state["entries"][TICKER]["live"]["pending"] is None
    assert j.state["entries"][TICKER]["live"]["status"]=="entering"

def test_expired_gate_release_never_clears_the_successors_lease():
    from google.api_core.exceptions import PreconditionFailed
    j=SharedJournal();write=j.write
    def cas(name,value,generation):
        if generation!=j.generations.get(name,0):raise PreconditionFailed("successor owns generation")
        return write(name,value,generation)
    j.write=cas
    class B:
        def account(self):return [],[]
    with AccountGate(j,B()) as gate:
        successor={"lease":{"holder":"successor","fence":2,"expires":10**12}}
        j.write(gate.name,successor,j.generations[gate.name])
        with pytest.raises(RuntimeError,match="ACCOUNT_ORDER_GATE_LOST"):gate.verify()
    assert j.rows[gate.name]==successor

def test_live_settlement_requires_exact_authenticated_quantity_cost_and_fees():
    t,b,j=setup();e=j.state["entries"][TICKER]["live"]
    e.update(filled="5.00",cost="1.00",fees=".07",status="held")
    market={"status":"settled","result":"no","exchange_index":2}
    row={"ticker":TICKER,"exchange_index":2,"market_result":"no","no_count_fp":"5.00","yes_count_fp":"0.00",
         "revenue_dollars":"5.00","no_total_cost_dollars":"1.00","fee_cost":".08"}
    b.pages=lambda *_,**__: [row]
    with pytest.raises(RuntimeError,match="SETTLEMENT_RECONCILIATION_MISMATCH"):t.settle(e,"live",market,1000)
    assert not j.state["totals"]
    row["fee_cost"]=".07";t.settle(e,"live",market,1001);t.settle(e,"live",market,1002)
    assert j.state["totals"]["live"]["net"]=="3.93" and j.state["totals"]["live"]["trades"]==1

def test_opposite_no_signal_buys_yes_on_yes_book(monkeypatch):
    monkeypatch.setattr("market_research.opposite_dollar_trader.time.time",lambda:126)
    j=Journal();b=Broker(j);t=DollarTrader(CONFIG,b,j,"live",gate_factory=Gate)
    signal={"market_id":TICKER,"contract_id":TICKER+":no","signal_at":60,"signal_received_at":65,"market_end":900,
            "sticky":{"side":"no","confirmed_at":30}}
    t.begin(signal,{"timestamp":120,"received_at":125,"timely":True},125);t.reconcile(TICKER,FEE,126)
    assert b.posts[0]["side"]=="bid" and b.posts[0]["price"]=="0.8200" and b.posts[0]["count"]=="1.21"

def test_initial_entry_deadline_uses_actual_current_time():
    j=Journal();b=Broker(j);t=DollarTrader(CONFIG,b,j,"live",gate_factory=Gate)
    s={"market_id":TICKER,"contract_id":TICKER+":yes","signal_at":60,"signal_received_at":65,"market_end":900,
       "sticky":{"side":"yes","confirmed_at":30}}
    with pytest.raises(RuntimeError,match="MISSED_OR_DUPLICATE_DOLLAR_ENTRY"):
        t.begin(s,{"timestamp":120,"received_at":125,"timely":True},151)
    assert not j.state["entries"] and not b.posts

def test_startup_waits_for_the_previous_lease_without_overriding_it(monkeypatch):
    clock=[100];saved=[]
    monkeypatch.setattr("market_research.opposite_dollar_trader.time.time",lambda:clock[0])
    monkeypatch.setattr("market_research.opposite_dollar_trader.time.sleep",lambda seconds:clock.__setitem__(0,clock[0]+seconds))
    old={"version":VERSION,"config":asdict(CONFIG),"lease":{"holder":"previous","fence":7,"expires":111},
         "entries":{"existing":{"live":{"pending":{"intent":"preserve"}}}}}
    j=object.__new__(CloudJournal);j.config=CONFIG;j.holder="successor";j.name="journal"
    j.read=lambda _: (deepcopy(old),4)
    def write(name,value,generation):
        assert clock[0]>=111 and generation==4
        saved.append(deepcopy(value));return 5
    j.write=write;j.claim(wait_seconds=20)
    assert j.fence==8 and j.generation==5 and len(saved)==1
    assert j.state["entries"]==old["entries"]

def test_startup_wait_is_bounded_and_a_running_owner_is_never_evicted(monkeypatch):
    clock=[100]
    monkeypatch.setattr("market_research.opposite_dollar_trader.time.time",lambda:clock[0])
    monkeypatch.setattr("market_research.opposite_dollar_trader.time.sleep",lambda seconds:clock.__setitem__(0,clock[0]+seconds))
    j=object.__new__(CloudJournal);j.config=CONFIG;j.holder="successor";j.name="journal"
    j.read=lambda _: ({"version":VERSION,"config":asdict(CONFIG),"lease":{"holder":"running","fence":1,"expires":999}},4)
    j.write=lambda *_: pytest.fail("Cannot replace another owner's valid lease")
    with pytest.raises(RuntimeError,match="STARTUP_LEASE_TIMEOUT"):j.claim(wait_seconds=12)
    assert clock[0]==112
