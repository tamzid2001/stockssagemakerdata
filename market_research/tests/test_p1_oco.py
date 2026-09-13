from copy import deepcopy
import pytest

from market_research.engine import Quote
from market_research.p1_oco import EXITS, advance_book, exit_limit, initial_book, price, replace_orders, summary


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
    assert len(events)==6 and all(e["kind"]=="entry" for e in events)
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
    assert exit_limit(.1,.3)==.3001
    assert exit_limit(.1,1.) is None
    book=armed();advance_book(book,{"a":q(160,.19),"b":q(160,.8)},160)
    advance_book(book,{"a":q(220,.99),"b":q(220,.5)},220)
    replace_orders(book,forecasts(),{"a":q(280,.5),"b":q(280,.5)},280)
    assert all(not l["orders"] for l in book["levels"].values())
    fresh=deepcopy(forecasts());fresh["a"]["forecast_id"]="new"
    replace_orders(book,fresh,{"a":q(280,.5),"b":q(280,.5)},280)
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
    class Provider:
        def history(self,*args): return [q(40_000-(499-i)*60,.5) for i in range(500)]
    class Heartbeat:
        def check(self): pass
    def forecast(window,horizon,models,quantiles):
        calls.append((len(window),horizon,window[-1].timestamp))
        return {"forecast_id":"f"+str(len(calls)),"origin":40_000,"rows":[{"timestamp":41800,"quantiles":{str(x):.1+x/2 for x in quantiles}}],"models":[{"id":m,"status":"completed"} for m in ["prophet","toto","granite","chronos","timesfm"]]}
    store=Memory()
    book,created,_=p1_worker.process_game(store,Provider(),pair,Heartbeat(),forecaster=forecast)
    assert created==2 and calls==[(500,30,40000)]*2
    assert store.values['a']['forecast'] and store.values['b']['forecast']
    assert all(len(l['orders'])==2 for l in book['levels'].values())
    class ShortProvider:
        def history(self,*args): return [q(40000,.5),q(40060,.5)]
    with pytest.raises(ValueError,match="FIVE_HUNDRED"):
        p1_worker.process_game(Memory(),ShortProvider(),pair,Heartbeat(),forecaster=forecast)
    assert len(calls)==2
