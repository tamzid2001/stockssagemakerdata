import time
from types import SimpleNamespace
import pytest

from market_research.engine import Quote, Strategy, iso, normalize_quotes
from market_research.local_store import LocalStore
from market_research.worker import process_historical, paired_contracts
from market_research.historical import evaluate_metrics
from market_research.tests.test_engine import curve


def test_kalshi_two_sides_preserve_spread_and_candle_end_without_imputed_trades():
    row = {"source": "kalshi", "timestamp": iso(120), "yes_ask_close": .6, "yes_bid_close": .57}
    assert normalize_quotes([row], 120) == [Quote(120, .6, .57)]
    no = normalize_quotes([{**row, "selected_position": "no"}], 120)[0]
    assert no.ask == pytest.approx(.43) and no.bid == pytest.approx(.4)
    assert normalize_quotes([{**row, "yes_ask_close": None}], 120) == []
    assert normalize_quotes([row], 119) == []
    assert len(paired_contracts([{"marketId":"m","side":side} for side in ("yes","no")])) == 2


def test_replay_resumes_published_forecast_after_interruption_without_refitting(tmp_path):
    contract={"contractId":"c","providerSymbol":"s","side":"long","eventStart":iso(1000),"resolutionTime":iso(4200)}
    rows=[{"timestamp":iso(t-1),"long_price":.4,"short_price":.62} for t in range(60,4260,60)]
    provider=SimpleNamespace(history=lambda *args:rows)
    heartbeat=SimpleNamespace(check=lambda:None)
    store=LocalStore("session","run",tmp_path);store.claim({})
    calls=[]
    def forecast(window,horizon):
        calls.append(window[-1].timestamp)
        return {**curve(window[-1].timestamp,horizon),"forecast_id":str(window[-1].timestamp)}
    real_save=store.save
    def interrupted(contract_id,state,forecast,trades,contract=None):
        real_save(contract_id,state,forecast,trades,contract)
        if state.get("active_origin"):
            raise OSError("simulated runner interruption after publication")
    store.save=interrupted
    with pytest.raises(OSError):
        process_historical(store,provider,contract,5000,30,Strategy(),forecast,3,[],heartbeat,time.monotonic()+60)
    assert calls == [120]
    restored=LocalStore("session","next-run",tmp_path);restored.claim({})
    result=process_historical(restored,provider,contract,5000,30,Strategy(),forecast,3,[],heartbeat,time.monotonic()+60)
    assert result["complete"] and calls == [120,1920,3720]
    previous=len(restored.values("forecasts"))
    process_historical(restored,provider,contract,5000,30,Strategy(),forecast,3,[],heartbeat,time.monotonic()+60)
    assert len(restored.values("forecasts")) == previous and len(calls)==3


def test_backtest_online_encrypted_checkpoint_retains_replay_state(tmp_path,monkeypatch):
    from market_research.artifact import snapshot_package, restore
    monkeypatch.setenv("QUANTURA_RESEARCH_ARTIFACT_KEY","a"*64)
    store=LocalStore("s","run",tmp_path/"data");store.claim({"code":"immutable"})
    store.checkpoint("head",{"index":3,"cursor":"next"})
    store.save("c",{"completed_origin":120},curve(),[])
    encrypted=tmp_path/"state.enc";snapshot_package(tmp_path/"data",encrypted)
    restore(encrypted,tmp_path/"restored",preserve_results=True)
    new=LocalStore("s","new-run",tmp_path/"restored");new.claim({"code":"immutable"})
    assert new._get("checkpoints","head")["index"]==3
    assert new.load("c")["state"]["completed_origin"]==120
    assert len(new.values("forecasts"))==1
    with pytest.raises(RuntimeError,match="CONFIGURATION"):
        new.claim({"code":"changed"})


def test_ml_metrics_only_match_out_of_sample_timestamps_and_observed_quotes():
    forecast=curve(120);forecast["market_context"]={"symbol":"s","side":"long"}
    rows=[{"timestamp":iso(t-1),"long_price":.4,"short_price":.62} for t in (60,120,180,240)]
    result=evaluate_metrics([forecast],[{"contract":{"providerSymbol":"s"},"rows":rows,"end":240}])
    assert result["observations_scored"]==2
    assert result["mae"]==pytest.approx(.1)
    assert result["quantiles"]["0.1"]["count"]==2
    assert result["quantiles"]["0.1"]["empirical_coverage"]==0
