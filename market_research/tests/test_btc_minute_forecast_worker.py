import pytest
from market_research import btc_minute_forecast_worker as worker
from market_research.local_store import LocalStore
from market_research.engine import stamp

def market():
    return {"ticker":"KXBTC15M-26OCT070000","event_ticker":"e",
            "open_time":"2026-10-07T00:00:00Z","close_time":"2026-10-07T00:15:00Z"}

def minutes(n):
    m=market();opened=stamp(m["open_time"])
    return [{"market_id":m["ticker"],"timestamp":opened+i*60,"received_at":opened+i*60+5,
             "collection_mode":"live","yes_ask":.5,"yes_bid":.49,"no_ask":.51,"no_bid":.5}
            for i in range(1,n+1)]

def test_missing_or_not_received_history_is_not_substituted():
    m=market();opened=stamp(m["open_time"])
    with pytest.raises(ValueError,match="MISSING_FIRST_N"):
        worker.minute_window(minutes(2)[1:],m["ticker"],opened,2,opened+130,"yes")
    with pytest.raises(ValueError,match="MISSING_FIRST_N"):
        worker.minute_window(minutes(2),m["ticker"],opened,2,opened+121,"yes")
    assert len(worker.minute_window(minutes(14),m["ticker"],opened,14,opened+850,"yes"))==14

@pytest.mark.parametrize("n,model_count",[(1,3),(2,4),(14,4)])
def test_live_pair_uses_each_origin_remaining_horizon_and_actual_completion(monkeypatch,n,model_count):
    m=market();origin=stamp(m["open_time"])+n*60;times=iter([origin+6,origin+35.1]);calls=[];validations=[]
    monkeypatch.setattr(worker,"validate_pair_member",lambda *args:validations.append(args))
    def forecaster(window,horizon,models,quantiles,**kwargs):
        calls.append((window,horizon,models,kwargs));return {"forecast_id":"f","duration_seconds":1}
    r=worker.forecast_pair(m,n,minutes(n),forecaster,lambda:next(times))
    assert r["published_at"]==origin+36 and r["status"]=="published"
    assert all(f["available_at"]==origin+36 for f in r["forecasts"])
    assert len(calls)==len(validations)==2
    assert all(len(c[0])==n and c[1]==15-n and len(c[2])==model_count for c in calls)
    assert calls[0][3].get("single_point_research",False)==(n==1)

def test_late_completion_never_gets_a_retroactive_publication_clock(monkeypatch):
    m=market();origin=stamp(m["open_time"])+120;times=iter([origin+6,origin+60])
    monkeypatch.setattr(worker,"validate_pair_member",lambda *a:None)
    r=worker.forecast_pair(m,2,minutes(2),lambda *a,**k:{"forecast_id":"f"},lambda:next(times))
    assert r["status"]=="published_late_untradeable"
    assert not any(f["live_origin_usable"] for f in r["forecasts"])
    with pytest.raises(ValueError,match="LIVE_ORIGIN_DEADLINE_MISSED"):
        worker.forecast_pair(m,2,minutes(2),clock=lambda:origin+60)

def test_retirement_requires_durable_archive_and_does_not_double_count(tmp_path):
    store=LocalStore(worker.VERSION,"test",tmp_path)
    m=market();end=stamp(m["close_time"])
    with store.lock,store.db:
        store._put("btc_lifecycle",m["ticker"],{"market_id":m["ticker"],"market":m,"close_at":end,"resolution_status":"resolved"})
    report={"fee_policy":{},"fee_status":"current_schedule_sensitivity","origins":{"1":{"p90_sticky":{"trades":[
        {"market_id":m["ticker"],"status":"closed","net_pnl":.4,"fees":.01,"entry_price":.59,"gross_pnl":.41}]}}}}
    class Cloud:
        def __init__(self):self.fail=True
        def archive(self,*args):
            if self.fail:raise OSError("ARCHIVE_UNAVAILABLE")
    cloud=Cloud()
    with pytest.raises(OSError):worker.finalize(store,cloud,end+300,report)
    assert store.values("btc_lifecycle") and not store._get("checkpoints","archived_totals")
    cloud.fail=False
    assert worker.finalize(store,cloud,end+300,report)==1
    assert worker.finalize(store,cloud,end+300,report)==0
    assert store._get("checkpoints","archived_totals")["1:p90_sticky"]["trades"]==1
    store.db.close()

def test_unknown_fee_never_enters_net_profit_totals(tmp_path):
    store=LocalStore(worker.VERSION,"test",tmp_path);m=market();end=stamp(m["close_time"])
    with store.lock,store.db:
        store._put("btc_lifecycle",m["ticker"],{"market_id":m["ticker"],"market":m,"close_at":end,"resolution_status":"resolved"})
    report={"fee_policy":{},"fee_status":"gross_only_fee_unavailable","origins":{"1":{"p90_sticky":{"trades":[
        {"market_id":m["ticker"],"status":"closed","net_pnl":None,"fees":None,"gross_pnl":.4}]}}}}
    class Cloud:
        def archive(self,*args):pass
    worker.finalize(store,Cloud(),end+300,report)
    total=store._get("checkpoints","archived_totals")["1:p90_sticky"]
    assert total["net_pnl"]==0 and total["trades"]==0 and total["unknown_fee_trades"]==1
    assert total["unknown_fee_gross_pnl"]==.4
    store.db.close()
