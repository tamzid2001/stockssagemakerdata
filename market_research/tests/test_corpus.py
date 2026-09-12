import time
from types import SimpleNamespace

import pytest

from market_research.corpus import lagged_window, process_quantile_historical, matching_actuals, export_corpus
from market_research.engine import Quote, iso, Strategy
from market_research.local_store import LocalStore
from market_research.worker import process_live
from market_research.tests.test_engine import curve


def test_lag_selects_500_before_cutoff_not_500_then_drops_latest_30():
    rows = [Quote(i * 60, .4, .38) for i in range(1, 601)]
    window, cutoff = lagged_window(rows, 600 * 60, 30)
    assert len(window) == 500
    assert window[0].timestamp == 71 * 60
    assert window[-1].timestamp == cutoff == 570 * 60
    assert all(q.timestamp <= cutoff for q in window)
    changed_future = rows[:570] + [Quote(i * 60, .99, .98) for i in range(571, 601)]
    assert lagged_window(changed_future, 600 * 60, 30)[0] == window


@pytest.mark.parametrize("lag,horizon", [(15,45), (15,60), (30,45), (30,60)])
def test_all_four_corpus_variants_save_inputs_quantiles_actuals_without_trades(tmp_path, monkeypatch, lag, horizon):
    contract = {"contractId": "c", "providerSymbol": "s", "marketId": "m", "side": "long",
                "eventStart": iso(600 * 60), "resolutionTime": iso(800 * 60), "availableFrom": iso(60)}
    rows = [{"timestamp": iso(i * 60 - 1), "long_price": .4, "short_price": .62} for i in range(1, 801)]
    ranges, calls = [], []
    def history(c, start, end):
        ranges.append((start, end))
        return rows
    def forecast(window, h):
        calls.append(window)
        assert all(q.timestamp <= (600 - lag) * 60 for q in window)
        return {**curve(window[-1].timestamp, h), "history_count": len(window), "models": [],
                "input_snapshot": [{"timestamp": iso(q.timestamp), "target": q.ask} for q in window]}
    store = LocalStore("s", "run", tmp_path); store.claim({})
    args = (store, SimpleNamespace(history=history), contract, 900 * 60, horizon, forecast, 1,
            SimpleNamespace(check=lambda: None), time.monotonic()+60, lag, 30)
    result = process_quantile_historical(*args)
    assert result["complete"] and result["forecasts"] == 1
    assert len(calls[0]) == 500 and len(ranges) == 1
    saved = store.values("forecasts")[0]
    assert len(saved["rows"]) == horizon and saved["strategy"] is None
    assert sum(a["phase"] == "already_observed_at_decision" for a in saved["actuals"]) == lag
    assert all(a["observed"] for a in saved["actuals"])
    assert store.values("trades") == []
    manifest = export_corpus(store, [saved], {"lag": lag, "horizon": horizon})
    assert manifest["prediction_rows"] == horizon and len(manifest["sha256"]) == 64
    import gzip
    csv = gzip.decompress((tmp_path / manifest["data_file"]).read_bytes()).decode()
    assert "p01,p10,p20,p30,p40,p50,p60,p70,p80,p90,p99" in csv
    assert csv.count("already_observed_at_decision") == lag
    from market_research.artifact import snapshot_package, decrypt
    import zipfile
    monkeypatch.setenv("QUANTURA_RESEARCH_ARTIFACT_KEY", "b"*64)
    encrypted = tmp_path / "research.enc"; clear = tmp_path / "verified.zip"
    snapshot_package(tmp_path, encrypted); decrypt(encrypted, clear)
    with zipfile.ZipFile(clear) as archive:
        assert "forecast_quantiles.csv.gz" in archive.namelist()
        assert "quantile_manifest.json" in archive.namelist()
    process_quantile_historical(*args)
    assert len(calls) == 1 and len(store.values("forecasts")) == 1


def test_sparse_history_never_pads_or_uses_future_to_reach_500():
    quotes = [Quote(t * 60, .4, .39) for t in [1, 8, 50, 99, 100]]
    window, cutoff = lagged_window(quotes, 100 * 60, 30)
    assert len(window) == 3 and cutoff == 70 * 60
    f = {**curve(50 * 60, 60), "decision_time": 80 * 60}
    actuals = matching_actuals(f, quotes)
    assert sum(a["observed"] for a in actuals) == 2
    assert sum(a["ask"] is None for a in actuals) == 58
    with pytest.raises(ValueError):
        lagged_window(quotes[:1], 100 * 60, 30)


def test_live_lag_does_not_replay_already_known_overlay_as_trades(tmp_path, monkeypatch):
    now = 600 * 60
    monkeypatch.setattr("market_research.worker.time.time", lambda: now + 65)
    contract = {"source": "kalshi", "contractId": "c", "providerSymbol": "s", "marketId": "m", "side": "yes", "live": True}
    rows = [{"source": "kalshi", "timestamp": iso(i*60), "yes_ask_close": .4, "yes_bid_close": .38} for i in range(1,601)]
    store = LocalStore("s", "run", tmp_path); store.claim({})
    calls = []
    def forecast(window, horizon):
        calls.append(window)
        return {**curve(window[-1].timestamp, horizon), "history_count": len(window)}
    events = []
    args = (store, SimpleNamespace(history=lambda *a: rows), contract, now, 60, Strategy(), forecast, events, SimpleNamespace(check=lambda: None))
    result = process_live(*args, lag_minutes=30, roll_minutes=15, forecast_only=True)
    assert result["forecast"] and len(calls[0]) == 500
    saved = store.load("c")["forecast"]
    assert saved["origin"] == 570*60 and saved["decision_time"] == now
    assert saved["available_at"] > now + 65
    assert sum(a["observed"] for a in saved["actuals"]) == 30
    assert not events and not store.values("trades")
    again = process_live(*args, lag_minutes=30, roll_minutes=15, forecast_only=True)
    assert not again["forecast"] and len(calls) == 1
