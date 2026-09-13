from copy import deepcopy
import json
import time
import zipfile

from market_research.artifact import decrypt, restore, snapshot_package
from market_research.engine import Quote, iso
from market_research.local_store import LocalStore
from market_research.p1_oco import QUANTILES
from market_research.p1_worker import MODELS
from market_research.recovery_replay import process_game
from market_research.recovery_switch import VERSION, from_store


def pair():
    return [{"eventId": "game", "marketId": "m", "contractId": s, "side": s,
             "providerSymbol": "sports-game", "eventStart": iso(3600), "resolutionTime": iso(7200)}
            for s in ("long", "short")]


class Provider:
    def history(self, contract, start, end, history_phase="both"):
        assert start == 3600 and history_phase == "in_game"
        return [{"timestamp": iso(t), "long_price": .4+(t%120)/1000, "short_price": .6-(t%120)/1000,
                 "selected_position": contract["side"]} for t in range(1200, 7200, 60)]

    def resolution(self, contract):
        return {"contract_id": contract["contractId"], "resolution_status": "resolved",
                "selected_side_payout": int(contract["side"] == "long")}


def forecast(window, horizon, models, quantiles):
    assert models == MODELS and len(window) >= 32
    assert all(q.timestamp > 3600 for q in window)
    origin = window[-1].timestamp
    return {"forecast_id": f"{origin}-{window[-1].ask}", "origin": origin, "history_count": len(window),
            "models": [{"id": m, "status": "completed"} for m in models],
            "rows": [{"timestamp": origin+i*60, "quantiles": {str(q): q for q in quantiles}} for i in range(1, horizon+1)]}


def setup_store(directory):
    store = LocalStore("recovery", "holder", directory)
    store.claim({"version": VERSION, "loss_multiplier": 2.5, "max_shares": 100})
    store.checkpoint("recovery_coverage", {"as_of": 10000})
    return store


def test_first_32_in_game_minutes_and_atomic_pair_idempotent_resume(tmp_path, monkeypatch):
    monkeypatch.setenv("QUANTURA_RESEARCH_ARTIFACT_KEY", "45"*32)
    store = setup_store(tmp_path/"initial")
    calls = []
    def record(*args):
        calls.append(args[0]); return forecast(*args)
    result = process_game(store, Provider(), pair(), 10000, 15, 1, time.monotonic()+60, record)
    assert result["first_decision"] == 3600+32*60 and len(calls) == 2 and not result["complete"]
    assert len(calls[0]) == 32
    initial = store.values("forecasts")
    assert len(initial) == 2 and initial[0]["available_at"] == initial[1]["available_at"]
    assert all(q["timestamp"] <= initial[0]["origin"] for q in initial[0]["input_snapshot"])
    encrypted = tmp_path/"checkpoint.enc"
    snapshot_package(store.directory, encrypted)
    clear = tmp_path/"verified.zip"; decrypt(encrypted, clear)
    with zipfile.ZipFile(clear) as archive:
        assert {"research.sqlite3", "recovery_summary.json", "recovery_trades.csv.gz", "recovery_signals.csv.gz", "recovery_forecast_quantiles.csv.gz"} <= set(archive.namelist())
    restore(encrypted, tmp_path/"resumed", preserve_results=True)
    resumed = setup_store(tmp_path/"resumed")
    before = from_store(resumed)
    assert before == from_store(store)
    process_game(resumed, Provider(), pair(), 10000, 15, 100, time.monotonic()+60, record)
    assert len(calls) == 4  # One later origin, no repeated initial pair.
    assert all(f in resumed.values("forecasts") for f in initial)
    assert process_game(resumed, Provider(), pair(), 10000, 15, 100, time.monotonic()+60, record)["complete"]
    assert len(calls) == 4
    store.db.close(); resumed.db.close()


def test_model_failure_never_publishes_single_side(tmp_path):
    store = setup_store(tmp_path)
    calls = []
    def partial(*args):
        calls.append(1)
        if len(calls) == 2:
            raise RuntimeError("MODEL_FAILED")
        return forecast(*args)
    result = process_game(store, Provider(), pair(), 10000, 15, 1, time.monotonic()+60, partial)
    assert not store.values("forecasts") and result["failed_origins"] == 1
    store.db.close()


def test_missing_start_or_31_in_game_bars_do_not_use_pregame(tmp_path):
    import pytest
    store = setup_store(tmp_path)
    sides = pair(); sides[0]["eventStart"] = None
    with pytest.raises(ValueError, match="GAME_START"):
        process_game(store, Provider(), sides, 10000, 15, 1, time.monotonic()+60, forecast)
    class Short(Provider):
        def history(self, *args, **kwargs):
            return super().history(*args, **kwargs)[:71]  # 40 pregame + only 31 in-game.
    result = process_game(store, Short(), pair(), 10000, 15, 1, time.monotonic()+60, forecast)
    assert result["status"] == "insufficient_in_game_history" and not store.values("forecasts")
    store.db.close()
