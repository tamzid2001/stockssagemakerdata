from __future__ import annotations

from fastapi.testclient import TestClient

import app.main as market_main


client = TestClient(market_main.app)


def test_health_endpoint() -> None:
    response = client.get("/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["service"] == "market-data"
    assert payload["source"] == "yfinance"


def test_fx_convert_same_currency() -> None:
    response = client.get("/fx/convert", params={"amount": 10, "from": "USD", "to": "USD"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["rate"] == 1.0
    assert payload["amountOut"] == 10.0
    assert payload["symbolUsed"] == "USDUSD=X"


def test_stocks_quote_endpoint_with_stub(monkeypatch) -> None:
    def fake_resolve_stock_quotes(symbols, mode):
        assert symbols == ["AAPL", "MSFT"]
        assert mode == "fast"
        return {
            "source": "yfinance",
            "mode": "fast",
            "count": 2,
            "asOf": "2026-02-24T00:00:00+00:00",
            "items": [{"symbol": "AAPL"}, {"symbol": "MSFT"}],
        }

    monkeypatch.setattr(market_main, "resolve_stock_quotes", fake_resolve_stock_quotes)

    response = client.get("/stocks/quote", params={"tickers": "AAPL,MSFT", "mode": "fast"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 2
    assert [item["symbol"] for item in payload["items"]] == ["AAPL", "MSFT"]


def test_stocks_screener_endpoint_with_stub(monkeypatch) -> None:
    def fake_run_screener_query(payload):
        assert payload.preset == "most_actives"
        return {
            "source": "yfinance",
            "mode": "preset",
            "preset": "most_actives",
            "offset": 0,
            "size": 5,
            "total": 5,
            "count": 1,
            "items": [{"symbol": "AAPL"}],
        }

    monkeypatch.setattr(market_main, "run_screener_query", fake_run_screener_query)

    response = client.post("/stocks/screener", json={"preset": "most_actives", "size": 5, "offset": 0})
    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "preset"
    assert payload["count"] == 1
    assert payload["items"][0]["symbol"] == "AAPL"
