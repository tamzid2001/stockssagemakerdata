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
        assert start < 3600 and history_phase == "both"
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


def test_missing_start_rejected_and_pregame_fallback_is_point_in_time(tmp_path):
    import pytest
    store = setup_store(tmp_path)
    sides = pair(); sides[0]["eventStart"] = None
    with pytest.raises(ValueError, match="GAME_START"):
        process_game(store, Provider(), sides, 10000, 15, 1, time.monotonic()+60, forecast)
    class Short(Provider):
        def history(self, *args, **kwargs):
            # Keep pregame; sparse in-game minutes still cannot be filled.
            return [r for r in super().history(*args, **kwargs) if r['timestamp'] < iso(3600) or int(r['timestamp'][14:16]) % 2 == 0]
    def fallback(window, *args):
        assert len(window) >= 32 and any(q.timestamp <= 3600 for q in window)
        assert all(q.timestamp <= window[-1].timestamp for q in window)
        origin = window[-1].timestamp
        return {'forecast_id': str(window[-1].ask), 'origin': origin, 'models': [{'id': m, 'status': 'completed'} for m in MODELS],
                'rows': [{'timestamp': origin+i*60, 'quantiles': {str(q):q for q in QUANTILES}} for i in range(1, args[0]+1)]}
    result = process_game(store, Short(), pair(), 10000, 15, 1, time.monotonic()+60, fallback)
    assert result['pregame_fallback_forecasts'] == 2
    assert all(f['history_phase'] == 'pregame_fallback' for f in store.values('forecasts'))
    store.db.close()


def test_no_history_fabricated_when_total_available_is_under_32(tmp_path):
    class Short(Provider):
        def history(self, *args, **kwargs):
            return [r for r in super().history(*args, **kwargs) if r['timestamp'] >= iso(3600)][::3]
    store = setup_store(tmp_path)
    result = process_game(store, Short(), pair(), 10000, 15, 100, time.monotonic()+60, forecast)
    assert result['failed_origins'] > 0 and not store.values('forecasts')
    store.db.close()
