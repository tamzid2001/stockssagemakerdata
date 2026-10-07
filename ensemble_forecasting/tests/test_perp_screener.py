from copy import deepcopy
from datetime import datetime, timedelta, timezone
import pytest
from ensemble_forecasting.kalshi_perp_screener import completed_daily, build_snapshot, configuration


def fixture():
    start = datetime(2026, 10, 1, tzinfo=timezone.utc)
    history = [{"timestamp": (start + timedelta(days=i)).isoformat(), "target": 100.0} for i in range(40)]
    config = configuration()
    result = {"runtime": {"mock": False}, "models": [{"id": k, "status": "completed"} for k in config["models"]],
              "result_hash": "result", "dataset_hash": "dataset", "effective_weights_by_quantile": {},
              "predictions": [{"timestamp": (start + timedelta(days=40+i)).isoformat(), "quantiles": {str(q): 90+10*q for q in config["quantiles"]}} for i in range(7)]}
    return history, result


def test_completed_trade_closes_use_underlying_units_without_empty_bar_filling():
    candles = [{"end_period_ts": 86400, "price": {"close": "8.1"}}, {"end_period_ts": 172800, "price": {"close": None}}, {"end_period_ts": 259200, "price": {"close": "8.2"}}]
    rows = completed_daily(candles, {"contract_size": ".0001", "underlying_multiplier": "1"}, 172800)
    assert len(rows) == 1 and rows[0]["target"] == pytest.approx(81000)


def test_publication_requires_real_five_model_ordered_daily_predictions():
    history, result = fixture()
    snapshot = build_snapshot({"ticker": "KXBTCPERP"}, history, result, history[-1]["timestamp"])
    assert len(snapshot["forecast_rows"]) == 7
    assert set(snapshot["quantile_stats"]) == {"p01", "p25", "p50", "p75", "p90", "p99"}
    for change in ("mock", "missing_model", "hour_gap", "crossed_quantiles"):
        broken = deepcopy(result)
        if change == "mock": broken["runtime"]["mock"] = True
        elif change == "missing_model": broken["models"].pop()
        elif change == "hour_gap": broken["predictions"][0]["timestamp"] = broken["predictions"][1]["timestamp"]
        else: broken["predictions"][0]["quantiles"]["0.01"] = 200
        with pytest.raises(ValueError): build_snapshot({"ticker": "KXBTCPERP"}, history, broken, history[-1]["timestamp"])


def test_discovery_detects_paged_new_listing_without_loading_models_or_writing_firestore(monkeypatch, tmp_path):
    import json
    import time
    import requests
    from market_research import public_snapshots
    from ensemble_forecasting.kalshi_perp_screener import run
    cutoff = int(time.time()) // 86400 * 86400
    old = {"ticker": "KXBTCPERP", "history_cutoff_at": datetime.fromtimestamp(cutoff, timezone.utc).isoformat().replace("+00:00", "Z"), "forecast_engine": "quantura_perps_daily_ensemble_v2"}
    stored = {"items": [old], "catalog": [{"ticker": old["ticker"]}]}
    monkeypatch.setattr(public_snapshots, "previous", lambda *_: stored)
    market = {"ticker": old["ticker"], "status": "active", "title": "BTC", "contract_size": "1", "underlying_multiplier": "1"}
    class Response:
        status_code = 200
        def __init__(self, value): self.value = value
        def raise_for_status(self): pass
        def json(self): return self.value
    class Session:
        def get(self, url, params=None, **kwargs):
            if url.endswith('/markets'):
                return Response({"markets": [{**market, "ticker": "KXNEWPERP"}]} if params.get('cursor') else {"markets": [market], "cursor": "page2"})
            symbol = url.split('/')[-2]
            candles = [{"end_period_ts": cutoff-i*86400, "price": {"close": "100"}} for i in range(40 if symbol == old['ticker'] else 2)]
            return Response({"ticker": symbol, "candlesticks": candles})
    monkeypatch.setattr(requests, 'Session', Session)
    monkeypatch.setenv('QUANTURA_PUBLIC_DIR', str(tmp_path/'public'))
    monkeypatch.setenv('GITHUB_OUTPUT', str(tmp_path/'outputs'))
    run(0, 1, True)
    value = public_snapshots.unpack((tmp_path/'public'/'snapshot.json.gz').read_bytes(), 'perps-0')['data']
    assert [r['ticker'] for r in value['catalog']] == ['KXBTCPERP', 'KXNEWPERP']
    assert value['items'] == [old]
    assert value['status']['new_listings'] == ['KXNEWPERP']
    assert value['status']['failures'][0]['reason'] == 'INSUFFICIENT_GENUINE_DAILY_HISTORY'
    assert (tmp_path/'outputs').read_text() == 'needs_models=false\n'


def test_standalone_discovery_import_needs_no_model_environment():
    import subprocess
    import sys
    from pathlib import Path
    root = Path(__file__).resolve().parents[2]
    subprocess.run([sys.executable, "-S", "-c", "import runpy,sys,types;sys.modules['requests']=types.ModuleType('requests');runpy.run_path('ensemble_forecasting/kalshi_perp_screener.py');assert 'numpy' not in sys.modules;assert 'ensemble_forecasting' not in sys.modules"], cwd=root, check=True)
