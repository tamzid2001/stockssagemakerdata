from copy import deepcopy
import pytest

from market_research.engine import Quote
from market_research.p1_oco import EXITS, arm_minute, advance_book, exit_limit, initial_book, price, replace_orders, summary


def q(t,ask,bid=None): return Quote(t,ask,ask if bid is None else bid,True)
def forecasts():
    return {side:{"forecast_id":side,"available_at":100,"rows":[{"timestamp":1900,"quantiles":{"0.01":.2,**{str(x):.2+x/2 for x in EXITS}}}]} for side in ("a","b")}
def armed():
    book=initial_book();replace_orders(book,forecasts(),{"a":q(100,.5),"b":q(100,.5)},100)
    return book


def test_p1_orders_require_price_strictly_above_p1_and_round_down():
    book=initial_book(); replace_orders(book,forecasts(),{"a":q(100,.19),"b":q(100,.2)},100)
    assert all(not l["orders"] for l in book["levels"].values())
    assert price(.123456)==.1234
    assert price(.123456,up=True)==.1235
    for bad in [float('nan'),float('inf'),-1,1.01]:
        with pytest.raises(ValueError): price(bad)


def test_first_fill_cancels_other_side_and_never_sells_same_minute():
    book=armed()
    assert not advance_book(book,{"a":q(100,.19),"b":q(100,.8)},100)
    events=advance_book(book,{"a":q(160,.19),"b":q(160,.8)},160)
    assert len([e for e in events if e["kind"]=="entry"])==6
    assert all(l["position"]["side"]=="a" and not l["orders"] for l in book["levels"].values())
    assert not advance_book(book,{"a":q(160,.99),"b":q(160,.01)},160)
    assert not advance_book(book,{"a":q(220,.21),"b":q(220,.01)},220)
    assert all(l["position"]["side"]=="a" for l in book["levels"].values())


def test_ambiguous_opposing_fills_are_excluded_not_cherry_picked():
    book=armed(); events=advance_book(book,{"a":q(160,.1),"b":q(160,.1)},160)
    assert all(e["kind"]=="ambiguous_fill_excluded" for e in events)
    assert all(not l["position"] and not l["orders"] for l in book["levels"].values())


def test_no_stop_hold_across_horizon_and_exit_then_wait_for_fresh_forecast():
    book=armed();advance_book(book,{"a":q(160,.19),"b":q(160,.8)},160)
    advance_book(book,{"a":q(2000,.01),"b":q(2000,.9)},2000)
    assert all(l["position"] for l in book["levels"].values())
    exits=advance_book(book,{"a":q(2060,.9,.85),"b":q(2060,.1)},2060)
    assert len(exits)==6 and book["needs_forecast"]
    assert all(e["exit"]>e["entry"] and e["fees"]>0 for e in exits)
    assert summary([book])["0.5"]["closed_trades"]==1
    assert summary([initial_book()])["0.5"]["win_rate"] is None


def test_exit_must_exceed_average_cost_and_no_reentry_on_stale_forecast():
    assert exit_limit(.1,.3) is None
    assert exit_limit(.1,1.) is None
    book=armed();advance_book(book,{"a":q(160,.19),"b":q(160,.8)},160)
    advance_book(book,{"a":q(220,.99),"b":q(220,.5)},220)
    replace_orders(book,forecasts(),{"a":q(280,.5),"b":q(280,.5)},280)
    assert all(not l["orders"] for l in book["levels"].values())
    fresh=deepcopy(forecasts());fresh["a"]["forecast_id"]="new"
    replace_orders(book,fresh,{"a":q(280,.5),"b":q(280,.5)},280)
    assert not any(l["orders"] for l in book["levels"].values())  # Already armed in this minute.
    arm_minute(book,{"a":q(340,.5),"b":q(340,.5)},340)
    assert all(len(l["orders"])==2 for l in book["levels"].values())


def test_no_future_quotes_no_unpublished_or_expired_forecasts():
    with pytest.raises(ValueError): replace_orders(initial_book(),forecasts(),{"a":q(200,.5),"b":q(200,.5)},100)
    f=forecasts();f["a"]["available_at"]=150
    with pytest.raises(ValueError): replace_orders(initial_book(),f,{"a":q(100,.5),"b":q(100,.5)},100)
    with pytest.raises(ValueError): replace_orders(initial_book(),forecasts(),{"a":q(2000,.5),"b":q(2000,.5)},2000)


def test_live_worker_uses_500_inputs_both_sides_and_persists_before_arming(monkeypatch):
    from market_research import p1_worker
    monkeypatch.setattr(p1_worker.time,"time",lambda:40_000)
    monkeypatch.setattr(p1_worker,"normalize_quotes",lambda raw,now:raw)
    pair=[{"contractId":s,"marketId":"game","providerSymbol":"test-game","side":side,"live":True} for s,side in [("a","long"),("b","short")]]
    calls=[]
    class Memory:
        def __init__(self): self.values={}
        def load(self,key): return deepcopy(self.values.get(key,{}))
        def save(self,key,state,forecast,events,contract): self.values[key]=deepcopy({"state":state,"forecast":forecast})
        def checkpoint(self,key,value): self.values[key]=deepcopy(value)
    class Provider:
        def history(self,*args): return [q(40_000-(499-i)*60,.5) for i in range(500)]
    class Heartbeat:
        def check(self): pass
    def forecast(window,horizon,models,quantiles):
        calls.append((len(window),horizon,window[-1].timestamp))
        return {"forecast_id":"f"+str(len(calls)),"origin":40_000,"rows":[{"timestamp":40000+i*60,"quantiles":{str(x):.1+x/2 for x in quantiles}} for i in range(1,31)],"models":[{"id":m,"status":"completed"} for m in ["prophet","toto","granite","chronos","timesfm"]]}
    store=Memory()
    book,created,_=p1_worker.process_game(store,Provider(),pair,Heartbeat(),forecaster=forecast)
    assert created==2 and calls==[(500,30,40000)]*2
    assert store.values['a']['forecast'] and store.values['b']['forecast']
    assert all(len(l['orders'])==2 for l in book['levels'].values())
    class ShortProvider:
        def history(self,*args): return [q(39940,.5),q(40000,.5)]
    with pytest.raises(ValueError,match="THIRTY_TWO"):
        p1_worker.process_game(Memory(),ShortProvider(),pair,Heartbeat(),forecaster=forecast)
    assert len(calls)==2


def test_minute_tranches_stack_then_average_and_reserve_share_cap():
    book=armed()
    for t in (160,220,280):
        advance_book(book,{"a":q(t,.5),"b":q(t,.5)},t)
        arm_minute(book,{"a":q(t,.5),"b":q(t,.5)},t,max_quantity=3)
    assert all(len(l["orders"])==6 for l in book["levels"].values())  # 3 per opposing side.
    before=deepcopy(book)
    assert arm_minute(book,{"a":q(280,.5),"b":q(280,.5)},280)==[]
    assert book==before
    events=advance_book(book,{"a":q(340,.19),"b":q(340,.8)},340)
    assert sum(e["kind"]=="average_in" for e in events)==12
    assert all(l["position"]["quantity"]==3 and l["position"]["average_cost"]==pytest.approx(.2) for l in book["levels"].values())
    assert all(not l["orders"] for l in book["levels"].values())
    assert not arm_minute(book,{"a":q(400,.5),"b":q(400,.5)},400,max_quantity=3)


def test_newer_p1_fills_recalculate_weighted_cost_and_exit_fees():
    book=armed();advance_book(book,{"a":q(160,.19),"b":q(160,.8)},160)
    f=deepcopy(forecasts())
    for side in f:
        f[side]["forecast_id"]+="new"
        f[side]["rows"][-1]["quantiles"]["0.01"]=.1
    replace_orders(book,f,{"a":q(220,.22),"b":q(220,.78)},220,quantity=2)
    events=advance_book(book,{"a":q(280,.09),"b":q(280,.91)},280)
    assert all(e["kind"]=="average_in" for e in events)
    p=book["levels"]["0.5"]["position"]
    assert p["quantity"]==3 and p["cost_basis"]==pytest.approx(.4)
    assert p["average_cost"]==pytest.approx(.4/3)
    exits=advance_book(book,{"a":q(340,.99,.95),"b":q(340,.01)},340)
    e=next(e for e in exits if e["kind"]=="exit" and e["level"]=="0.5")
    assert e["gross_pnl"]==pytest.approx(.45*3-.4)
    assert e["fees"]==pytest.approx(.004+.45*3*.01)
    assert not arm_minute(book,{"a":q(400,.5),"b":q(400,.5)},400)


def test_minute_order_only_if_above_p1_no_same_quote_fill_no_expired_buys():
    book=armed()
    assert not advance_book(book,{"a":q(100,.19),"b":q(100,.8)},100)
    before=sum(len(l["orders"]) for l in book["levels"].values())
    arm_minute(book,{"a":q(160,.2),"b":q(160,.1)},160)
    assert sum(len(l["orders"]) for l in book["levels"].values())==before
    events=advance_book(book,{"a":q(1901,.1),"b":q(1901,.9)},1901)
    assert not any(e["kind"]=="entry" for e in events)
    assert not arm_minute(book,{"a":q(1960,.5),"b":q(1960,.5)},1960)


def test_six_sides_cancel_five_other_exposures():
    f={str(i):deepcopy(forecasts()["a"]) for i in range(6)}
    for side in f:f[side]["forecast_id"]=side
    book=initial_book();replace_orders(book,f,{s:q(100,.5) for s in f},100)
    advance_book(book,{s:q(160,.1 if s=="2" else .8) for s in f},160)
    assert all(l["position"]["side"]=="2" and not l["orders"] for l in book["levels"].values())


@pytest.mark.parametrize("count",[32,60,499,500,650])
def test_live_worker_uses_available_rows_with_32_minimum(count,monkeypatch):
    from market_research import p1_worker
    from market_research.local_store import LocalStore
    import tempfile
    monkeypatch.setattr(p1_worker.time,"time",lambda:40000)
    monkeypatch.setattr(p1_worker,"normalize_quotes",lambda raw,now:raw)
    pair=[{"contractId":s,"marketId":"game","providerSymbol":"test","side":side,"live":True} for s,side in [("a","long"),("b","short")]]
    class Provider:
        def history(self,*args):return [q(40000-i*60,.5) for i in reversed(range(count))]
    class Heartbeat:
        def check(self):pass
    calls=[]
    def forecast(window,horizon,models,quantiles):
        calls.append(len(window))
        return {"forecast_id":str(len(calls)),"origin":40000,"models":[{"id":m,"status":"completed"} for m in models],
            "rows":[{"timestamp":40000+i*60,"quantiles":{str(x):.1+x/2 for x in quantiles}} for i in range(1,31)]}
    with tempfile.TemporaryDirectory() as directory:
        store=LocalStore("test","test",directory);store.claim({})
        book,n,_=p1_worker.process_game(store,Provider(),pair,Heartbeat(),forecaster=forecast)
        assert n==2 and calls==[min(count,500)]*2
        assert len(store.values("forecasts"))==2
        assert all(len(l["orders"])==2 for l in book["levels"].values())
        store.release();store.db.close()
