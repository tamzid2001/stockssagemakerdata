# Quantura SDK and CLI

Market search, probabilistic forecasts, provider history and Scout from the Quantura API. Node.js 20 or newer; no runtime dependencies. API access requires Pro (including an active 14-day trial), enterprise or verified administrator access. Active 14-day Pro trials include API and MCP access.

## CLI with browser sign-in

```sh
npm install -g 'https://github.com/tamzid2001/stockssagemakerdata/releases/download/quantura-sdk-v1.0.1/quantura-sdk-1.0.1.tgz'
quantura login
quantura whoami
quantura search AAPL --source alpaca
quantura models
```

`quantura login` opens Clerk sign-in and uses authorization code OAuth with PKCE. Google and email/password use your existing Quantura account. Credentials are stored privately at `~/.config/quantura/credentials.json` and refresh automatically. The loopback callback is `http://127.0.0.1:8766/callback`; `--no-browser` prints the sign-in URL. `quantura logout` revokes the grant and removes local credentials. Alternatively set `QUANTURA_API_KEY` in a server environment; do not put a key in command arguments.

The release tarball is the installation source for this version. To add the SDK to an application, use `npm install https://github.com/tamzid2001/stockssagemakerdata/releases/download/quantura-sdk-v1.0.1/quantura-sdk-1.0.1.tgz`. Registry publication is pending npm account 2FA setup.

## JavaScript / TypeScript

```js
import { Quantura } from 'quantura-sdk';
import { accessToken } from 'quantura-sdk/oauth'; // Node only; run quantura login first

const q = new Quantura({ token: accessToken });
console.log(await q.search('AAPL', { source: 'alpaca' }));
console.log(await q.models());
```

`new Quantura()` uses `QUANTURA_API_KEY` or `QUANTURA_ACCESS_TOKEN`. You can supply a token or async token function and an HTTPS `baseUrl`. The client refuses redirects and does not automatically retry mutations.

## Forecasts

Inspect `q.models()` first for current availability, model minimums, licensing and frequencies. Put the complete request in `request.json`:

```json
{
  "source": {"type": "ticker", "provider": "alpaca", "symbol": "AAPL", "frequency": "1Day", "limit": 500, "field": "close"},
  "prediction_length": 7,
  "horizon_mode": "trading_sessions",
  "quantiles": [0.01, 0.10, 0.25, 0.50, 0.75, 0.90, 0.99],
  "models": {"prophet": {"enabled": true, "weight": 1}},
  "failure_policy": "renormalize"
}
```

```sh
quantura forecast --file request.json --key my-aapl-request-1
quantura get FORECAST_ID --wait
quantura download FORECAST_ID --output forecast.csv
```

```js
const created = await q.createForecast(request, { idempotencyKey: 'my-aapl-request-1' });
const job = created.data;
const completed = await q.waitForForecast(job.forecast_id);
const csv = await q.downloadForecast(job.forecast_id);
const answer = await q.askScout({ kind: 'ensemble', id: job.forecast_id }, 'Where is the widest forecast range?');
```

Save and reuse an explicit idempotency key if a job-creation response is lost. Changing a request while reusing its key returns a conflict. Historical cutoffs such as `history_lag_minutes` are forwarded unchanged. A request uses at most the available genuine observations; missing bars are not invented.

## Provider history

```js
for await (const page of q.historyPages({ source: 'dukascopy', symbol: 'XAUUSD', timeframe: '1Hour', price_side: 'bid', start: '2025-10-08', end: '2026-10-07' })) {
  console.log(page);
}
```

The iterator preserves the request settings, follows `next_cursor` and detects cursor loops. `quantura history --file history.json --all-pages --output history.json` saves the page envelopes; it does not silently flatten or resample rows. Use `history()` for one page or a supported CSV response.

Other commands: Resolve market links with `q.resolve(url)`; `quantura scout --file question.json` accepts `{ "context": {"kind":"ensemble","id":"FORECAST_ID"}, "question":"Explain P50" }`. Run `quantura --help` for the CLI command list.

## MCP

Connect ChatGPT or another OAuth MCP client to **https://quantura.studio/mcp**. This live API server exposes search, capabilities, forecast creation/read, history, Scout and account access. Forecast creation and Scout save requests in your account. It does not place trades. Authentication discovery is public; every tool call rechecks identity, current Pro subscription/trial access and resource permissions.

[API and connection guide](https://quantura.studio/developers/api) · [Source](https://github.com/tamzid2001/stockssagemakerdata/tree/main/packages/npm)
