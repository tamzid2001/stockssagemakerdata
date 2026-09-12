from copy import deepcopy
from dataclasses import asdict
import time
from types import SimpleNamespace

import pytest

from market_research.engine import Quote, Strategy, iso, summarize
from market_research.local_store import LocalStore
from market_research.median import advance_median, initial_median_state, process_median_historical
from market_research.tests.test_engine import curve


def seeded(config=None):
    state = initial_median_state(config or Strategy())
    state.update(previous=asdict(Quote(600, .6, .59)), last_timestamp=600)
    return state


def test_buy_below_and_sell_above_median_require_later_executable_quotes():
    config = Strategy(slippage=.005, fee_per_contract=.01)
    state = seeded(config)
    forecast = curve()
    changes, reason = advance_median(state, Quote(660, .4, .39), forecast, config)
    assert reason == "median_crossing" and changes[0]["direction"] == "cross_below"
    assert state["position"] is None and state["pending_entry"]["limit"] == .5
    advance_median(state, Quote(720, .39, .38), forecast, config)
    assert state["position"]["entry"] == pytest.approx(.395)
    assert state["position"]["entered_at"] > state["position"]["signal_at"]
    changes, reason = advance_median(state, Quote(780, .6, .59), forecast, config)
    assert reason == "median_crossing" and not any(c["kind"] == "trade" for c in changes)
    assert state["pending_exit"]["signal_at"] == 780
    changes, _ = advance_median(state, Quote(840, .59, .58), forecast, config)
    trade = next(c for c in changes if c["kind"] == "trade")
    assert trade["exit_signal_at"] < trade["exited_at"]
    assert trade["net_pnl"] == pytest.approx(.575 - .395 - .02)
    assert state["position"] is None
    assert summarize(changes, ("0.5",))["levels"]["0.5"]["wins"] == 1
    assert advance_median(state, Quote(840, .59, .58), forecast, config) == ([], None)


def test_losses_multiply_then_cap_and_profit_resets_without_same_bar_reentry():
    config = Strategy(slippage=0, fee_per_contract=0, loss_multiplier=2.5, max_shares=4)
    state = seeded(config); forecast = curve(horizon=60)
    t = 600
    for expected_size, next_size in [(1, 2.5), (2.5, 4), (4, 4)]:
        for ask in (.4, .39):
            t += 60; advance_median(state, Quote(t, ask, ask-.01), forecast, config)
        assert state["position"]["size"] == expected_size
        t += 60; events, _ = advance_median(state, Quote(t, .09, .08), forecast, config)
        assert events[-1]["reason"] == "stop" and events[-1]["size"] <= 4
        assert state["size"] == next_size and state["pending_entry"] is None
    for ask in (.4, .39, .7, .72):
        t += 60; advance_median(state, Quote(t, ask, ask-.01), forecast, config)
    assert state["size"] == 1 and state["count"] == 4


def test_gap_or_unpublished_model_does_not_invent_crossings_and_stop_precedes_target():
    state = seeded(); forecast = curve(); config = Strategy()
    _, refresh = advance_median(state, Quote(720, .4, .39), forecast, config)
    assert refresh is None
    fresh = seeded(); delayed = {**forecast, "available_at": 780}
    assert advance_median(fresh, Quote(660, .4, .39), delayed, config)[1] is None
    assert fresh["pending_entry"] is None
    with pytest.raises(ValueError, match="IMPUTED"):
        advance_median(fresh, Quote(720, .4, .39, False), forecast, config)
    state = seeded()
    advance_median(state, Quote(660, .4, .39), forecast, config)
    advance_median(state, Quote(720, .39, .38), forecast, config)
    advance_median(state, Quote(780, .7, .69), forecast, config)
    events, _ = advance_median(state, Quote(840, .09, .08), forecast, config)
    assert events[-1]["reason"] == "stop"


def test_pending_order_keeps_original_median_and_expiry_despite_new_forecast():
    state=seeded(); config=Strategy(slippage=0); original=curve(horizon=3)
    advance_median(state, Quote(660,.4,.39),original,config)
    changed=curve(660,30)
    for row in changed["rows"]:
        row["quantiles"]={q:v+.1 for q,v in row["quantiles"].items()}
    advance_median(state, Quote(720,.55,.54),changed,config)
    assert state["position"] is None and state["pending_entry"]["limit"]==.5
    advance_median(state,Quote(780,.6,.59),changed,config)
    assert state["pending_entry"] is None


def replay_fixture(tmp_path, prices, duration=1):
    contract={"contractId":"c","marketId":"m","providerSymbol":"s","side":"long","eventStart":iso(1000),"resolutionTime":iso(10000)}
    rows=[{"timestamp":iso((i+1)*60-1),"long_price":p,"short_price":1-p+.01} for i,p in enumerate(prices)]
    provider=SimpleNamespace(history=lambda *args:deepcopy(rows)); heartbeat=SimpleNamespace(check=lambda:None)
    store=LocalStore("median","run",tmp_path);store.claim({"experiment":"median_cross"})
    calls=[]
    def forecaster(window,horizon):
        origin=window[-1].timestamp;calls.append([q.timestamp for q in window])
        assert len(window)<=500 and max(q.timestamp for q in window)==origin
        return {**curve(origin,horizon),"forecast_id":str(origin),"duration_seconds":duration}
    return store,provider,contract,heartbeat,forecaster,calls


def run_fixture(parts, max_origins=100):
    store,provider,contract,heartbeat,forecaster,_=parts
    return process_median_historical(store,provider,contract,12000,30,Strategy(),forecaster,max_origins,[],heartbeat,time.monotonic()+30)


def test_crossings_refit_both_directions_and_inputs_exclude_future(tmp_path):
    parts=replay_fixture(tmp_path,[.6,.6,.4,.39,.7,.72,.4,.39,.7,.72])
    result=run_fixture(parts)
    store,_,_,_,_,calls=parts
    assert result["complete"]
    assert [c[-1] for c in calls]==[120,180,300,420,540]
    assert all(c==list(range(60,c[-1]+1,60)) for c in calls)
    assert {f.get("refresh_reason") for f in store.values("forecasts")}=={"initial_or_expired","median_crossing"}
    events=store.values("trades")
    assert sum(e["kind"]=="median_crossing" for e in events)==4
    assert len([e for e in events if e["kind"]=="trade"])==2


def test_serial_inference_latency_prevents_early_entries(tmp_path):
    parts=replay_fixture(tmp_path,[.6,.6,.4,.39,.7,.72,.4,.39,.7,.72],duration=125)
    run_fixture(parts)
    forecasts=sorted(parts[0].values("forecasts"),key=lambda f:f["origin"])
    assert forecasts[0]["available_at"]==300
    for before,after in zip(forecasts,forecasts[1:]):
        assert after["available_at"]>=max(before["available_at"],after["origin"])+180
    assert all(t["signal_at"]>=300 for t in parts[0].values("trades") if t["kind"]=="trade")


def test_resume_does_not_reforecast_published_crossing_or_duplicate_trades(tmp_path):
    parts=replay_fixture(tmp_path,[.6,.6,.4,.39,.7,.72,.4,.39,.7,.72])
    store,provider,contract,heartbeat,forecaster,calls=parts
    save=store.save
    def crash_after_commit(*args):
        save(*args)
        if args[2] is not None: raise OSError("runner crashed after commit")
    store.save=crash_after_commit
    with pytest.raises(OSError): run_fixture(parts)
    assert calls[-1][-1]==120
    restored=LocalStore("median","next",tmp_path);restored.claim({"experiment":"median_cross"})
    resumed=(restored,provider,contract,heartbeat,forecaster,calls)
    run_fixture(resumed)
    assert [c[-1] for c in calls]==[120,180,300,420,540]
    trades=restored.values("trades")
    run_fixture(resumed)
    assert restored.values("trades")==trades and len(calls)==5


def test_refresh_budget_is_explicit_and_failed_models_never_create_fake_positions(tmp_path):
    parts=replay_fixture(tmp_path,[.6,.6,.4,.39,.7,.72])
    result=run_fixture(parts,max_origins=1)
    assert result["completion_reason"]=="forecast_budget" and result["forecasts"]==1
    assert not parts[0].values("trades")
    failed_parts=list(replay_fixture(tmp_path/"fail",[.6,.6,.4,.39,.7,.72]))
    def fail(*args): raise RuntimeError("model unavailable")
    failed_parts[4]=fail
    result=run_fixture(failed_parts,max_origins=2)
    assert result["failed_origins"]==2 and result["forecasts"]==0
    assert failed_parts[0].values("forecasts")==[]
    assert all(t["kind"]=="forecast_failure" for t in failed_parts[0].values("trades"))
