from copy import deepcopy
import time
import pytest
from market_research.engine import Quote, iso
from market_research.local_store import LocalStore
from market_research.p1_oco import QUANTILES
from market_research.p1_outcomes import cohorts


@pytest.mark.parametrize("horizon",[5,15,30])
def test_historical_rolling_uses_past_only_resumes_and_keeps_curves(tmp_path,monkeypatch,horizon):
    from market_research import p1_historical as engine
    pair=[{"contractId":s,"side":side,"eventId":"g","marketId":"m","providerSymbol":"test",
        "eventStart":iso(100020),"availableFrom":iso(97020)} for s,side in (("a","long"),("b","short"))]
    times=list(range(97020,106021,60));calls=[]
    class Provider:
        def history(self,*args):return [{"timestamp":iso(t)} for t in times]
    monkeypatch.setattr(engine,"historical_range",lambda c,now:(97020,106020))
    monkeypatch.setattr(engine,"normalize_quotes",lambda raw,end:[Quote(t,.4,.38) for t in times])
    def forecast(window,n,models,quantiles):
        calls.append((window[-1].timestamp,len(window),n))
        return {"forecast_id":str(len(calls)),"origin":window[-1].timestamp,
            "models":[{"id":m,"status":"completed"} for m in models],
            "rows":[{"timestamp":window[-1].timestamp+i*60,"quantiles":{str(q):.1+q*.5 for q in quantiles}} for i in range(1,n+1)]}
    store=LocalStore("test","holder",tmp_path);store.claim({})
    result=engine.process_game(store,Provider(),pair,106020,horizon,1,time.monotonic()+30,forecast)
    assert not result["complete"] and len(calls)==2
    assert calls[0]==(100020,51,horizon)
    checkpoint=deepcopy(store.load("p1-game:g"))
    assert checkpoint["state"]["completed_decision"]==100020
    result=engine.process_game(store,Provider(),pair,106020,horizon,1,time.monotonic()+30,forecast)
    assert len(calls)==4 and calls[-1][0]==100020+horizon*60
    assert len(store.values("forecasts"))==4
    assert len(store.values("observations"))>0
    assert all(q["timestamp"]<=f["origin"] for f in store.values("forecasts") for q in f["input_snapshot"])
    store.db.close()


def test_p1_p10_to_p90_p99_outcomes_count_unique_game_sides_not_price_winners():
    f={"forecast_id":"f","available_at":100,"origin":40,
        "market_context":{"contract_id":"a","event_id":"g","market_id":"m","side":"long"},
        "rows":[{"timestamp":160+i*60,"quantiles":{str(q):q for q in QUANTILES}} for i in range(15)]}
    observations=[{"contract_id":"a","timestamp":t,"ask":a,"bid":b,"observed":True}
        for t,a,b in [(100,.001,.001),(160,.005,.004),(220,.99,.98),(280,1.,.99)]]
    r=cohorts([f,{**f,"forecast_id":"second"}],observations,{"a":{"resolution_status":"resolved","selected_side_won":False}})
    c=r["cohorts"]["15m:0.01->0.99"]
    assert c["hit_episodes"]==2 and c["resolved_game_sides"]==1
    assert c["game_side_win_rate_after_hit"]==0  # P99 hit did NOT make this a game winner.
    assert all(e["entry_at"]==160 for e in r["episodes"])  # No pre-publication signal.
    unknown=cohorts([f],observations,{})["cohorts"]["15m:0.1->0.9"]
    assert unknown["game_side_win_rate_after_hit"] is None and unknown["unknown_hit_episodes"]==1
    truncated=cohorts([f],observations[:2],{})["cohorts"]["15m:0.1->0.99"]
    assert truncated["hit_episodes"]==0
