# Quantura Python SDK and CLI

Market search, probabilistic forecasts, provider history and Scout. Python 3.10 or newer; no runtime dependencies. API access requires Pro (including an active 14-day trial), enterprise or verified administrator access.

## Install

```sh
python -m pip install 'https://github.com/tamzid2001/stockssagemakerdata/releases/download/quantura-sdk-v1.0.1/quantura_sdk-1.0.1-py3-none-any.whl'
quantura login
quantura whoami
quantura search AAPL --source alpaca
quantura models
```

The release wheel is the supported installation source for this version. Browser OAuth uses Clerk, PKCE S256 and a loopback callback at `http://127.0.0.1:8766/callback`. Google and email/password use your existing account. Tokens refresh automatically and are stored privately at `~/.config/quantura/credentials.json`, shared with the npm CLI. `quantura login --no-browser` prints the sign-in URL; `quantura logout` revokes the grant and removes local credentials. Alternatively use the server environment variable `QUANTURA_API_KEY`.

Both packages install a `quantura` command. Install one CLI per environment or use `python -m quantura_sdk` for the Python CLI.

## Python

```python
from quantura_sdk import Quantura
from quantura_sdk.oauth import access_token

q = Quantura(access_token)  # Run quantura login first; refreshes expired tokens.
print(q.search("AAPL", source="alpaca"))
print(q.models())
```

`Quantura()` reads `QUANTURA_API_KEY` or `QUANTURA_ACCESS_TOKEN`. You can supply a token, token callable or HTTPS `base_url`. Responses preserve the API's `data` and `meta` envelopes. `QuanturaError` exposes `status`, `code` and `request_id`. Redirects are refused and mutations are not automatically retried.

## Forecasts and Scout

Inspect `q.models()` for current capabilities and model licensing. Example daily request:

```python
request = {
    "source": {"type": "ticker", "provider": "alpaca", "symbol": "AAPL", "frequency": "1Day", "limit": 500, "field": "close"},
    "prediction_length": 7,
    "horizon_mode": "trading_sessions",
    "quantiles": [0.01, 0.10, 0.25, 0.50, 0.75, 0.90, 0.99],
    "models": {"prophet": {"enabled": True, "weight": 1}},
    "failure_policy": "renormalize",
}
created = q.create_forecast(request, idempotency_key="my-aapl-request-1")
forecast_id = created["data"]["forecast_id"]
completed = q.wait_for_forecast(forecast_id)
with open("forecast.csv", "wb") as output:
    output.write(q.download_forecast(forecast_id))
answer = q.ask_scout({"kind": "ensemble", "id": forecast_id}, "Where is the widest forecast range?")
```

Save and reuse an explicit idempotency key after an ambiguous connection failure. Poll the returned forecast ID instead of creating another job. Historical cutoff fields are forwarded unchanged.

## Paged history

```python
for page in q.history_pages({"source": "dukascopy", "symbol": "XAUUSD", "timeframe": "1Hour", "price_side": "bid", "start": "2025-10-08", "end": "2026-10-07"}):
    print(page)
```

The iterator follows `next_cursor`, preserves settings and detects loops. `history()` returns JSON or raw CSV bytes depending on `format`. The CLI also supports `forecast --file request.json --key retry-key`, `get FORECAST_ID --wait`, `download FORECAST_ID --output forecast.csv`, `history --file history.json --all-pages` and `scout --file question.json`. Run `quantura --help` for details.

## MCP

The live OAuth MCP endpoint is **https://quantura.studio/mcp**. It exposes market search, forecast capabilities, forecast creation/read, history, Scout and account access. Forecast and Scout requests are saved to your account; it does not place trades.

[API guide](https://quantura.studio/developers/api) · [Source](https://github.com/tamzid2001/stockssagemakerdata/tree/main/packages/python)
