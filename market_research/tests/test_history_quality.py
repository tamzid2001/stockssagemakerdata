import pytest
from market_research.engine import Quote, history_quality
from market_research.forecast import forecast_window
from market_research.provider import QuanturaProvider


def test_flat_quotes_are_preserved_but_inference_is_blocked_before_models(monkeypatch):
    rows = [Quote(i * 60, .545, .54) for i in range(500)]
    assert history_quality(rows)["forecast_blocked"]
    monkeypatch.setattr("market_research.forecast.execute_job", lambda *a, **k: pytest.fail("No compute on flat history"))
    with pytest.raises(ValueError, match="HISTORY_FLAT_WINDOW"):
        forecast_window(rows, 30, ("prophet",))
    assert len(rows) == 500


def test_pregame_flat_stretch_does_not_discard_changing_ingame_quotes():
    rows = [Quote(i * 60, .545 if i < 450 else .6 + (i - 450) / 1000, .5) for i in range(500)]
    result = history_quality(rows)
    assert not result["forecast_blocked"]
    assert result["longest_unchanged_minutes"] == 449
    assert result["price_changes"] == 50
    stale = [Quote(i * 60, .6 if i < 100 else .545, .5) for i in range(500)]
    assert history_quality(stale)["forecast_blocked"]


def test_research_uses_shared_phase_downloads_with_scope_aware_cache(monkeypatch):
    adapter = QuanturaProvider()
    calls = []
    def request(path, body):
        calls.append(body)
        return {"rows": [{"timestamp": "2026-09-12T20:00:00Z", "long_price": .5, "short_price": .51}]}
    monkeypatch.setattr(adapter, "request", request)
    contract = {"providerSymbol": "fixture", "side": "long"}
    adapter.history(contract, 1, 2)
    adapter.history(contract, 1, 2, "in_game", 60)
    adapter.history(contract, 1, 2, "in_game", 60)
    assert len(calls) == 2
    assert calls[0]["history_phase"] == "both"
    assert calls[1]["history_phase"] == "in_game"
    assert calls[1]["history_lookback_minutes"] == 60
    assert calls[1]["missing"] == "leave"
    with pytest.raises(ValueError): adapter.history(contract, 1, 2, "unknown")
