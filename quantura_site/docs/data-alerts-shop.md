# Data, Alerts, and Shop Runbook

## 1) Market Data Endpoints
Service path: `quantura_site/services/market-data`

### Endpoints
- `GET /health`
- `GET /fx/convert?amount=&from=&to=`
- `GET /stocks/quote?tickers=AAPL,MSFT&mode=fast|full`
- `POST /stocks/screener`

### Notes
- `mode=fast`: uses `fast_info` + batch download path.
- `mode=full`: includes `raw` with complete `yfinance.info` payload.
- Quote cache TTL default: 90s.
- Full info cache TTL default: 900s.

## 2) Firestore Schema (Phase 11)

### Watchlists
Path: `users/{uid}/watchlists/{listId}`

Fields:
- `name: string`
- `symbols: string[]`
- `createdAt: timestamp`
- `updatedAt: timestamp`
- `updatedByUid: string`

### Alerts
Path: `users/{uid}/alerts/{alertId}`

Fields:
- `symbol: string`
- `type: "price_above" | "price_below" | "pct_up_day" | "ma_cross"`
- `params: object`
- `isActive: boolean`
- `cooldownMins: number`
- `lastTriggeredAt: timestamp`
- `createdAt: timestamp`
- `updatedAt: timestamp`

### Devices
Path: `users/{uid}/devices/{deviceId}`

Fields:
- `fcmToken: string`
- `platform: string`
- `isActive: boolean`
- `updatedAt: timestamp`
- `createdAt: timestamp`

### Alert events
Path: `users/{uid}/alert_events/{eventId}`

Fields:
- `count: number`
- `alerts: object[]`
- `push: object`
- `createdAt: timestamp`

## 3) Alert Scheduler

Function: `alert_tick_scheduler` in `functions/main.py`

Schedule:
- `every 1 minutes`
- Timezone from `ALERT_SCHEDULER_TIMEZONE` (default `America/New_York`)

Flow:
1. Query active alerts from `collectionGroup("alerts")`.
2. Build unique symbol set.
3. Fetch batched quotes using `MARKET_DATA_SERVICE_URL` (`/stocks/quote`) with fallback to Yahoo batch close.
4. Evaluate:
   - `price_above`
   - `price_below`
   - `pct_up_day`
   - `ma_cross` (SMA fast/slow cross up)
5. Enforce cooldown via `cooldownMins` and `lastTriggeredAt`.
6. Send push to `users/{uid}/devices` tokens and store alert event docs.

## 4) Testing Alerts + Push

### Manual test sequence
1. Register device token with callable `register_device_v2`.
2. Create alert with callable `upsert_alert_v2`.
3. Ensure symbol is moving through threshold or set threshold near live price.
4. Wait for scheduler run (1 minute).
5. Verify:
   - function logs include `alert_tick_scheduler: checked=... triggered=...`
   - device receives push
   - deep link path in data: `/ticker/{symbol}?alert=1`

### Function callables
- `upsert_watchlist_v2`
- `upsert_alert_v2`
- `register_device_v2`
- `list_alerts_v2`

## 5) Screener Presets + Custom Fields

Backend route: `POST /stocks/screener`

### Preset example
```json
{
  "preset": "most_actives",
  "size": 25,
  "offset": 0
}
```

### Custom query example
```json
{
  "query": {
    "operator": "and",
    "operands": [
      { "operator": "eq", "operands": ["region", "us"] },
      { "operator": "gte", "operands": ["dayvolume", 1000000] },
      { "operator": "lte", "operands": ["peratio.lasttwelvemonths", 25] }
    ]
  },
  "size": 25,
  "offset": 0,
  "sortField": "dayvolume",
  "sortAsc": false
}
```

Use `yfinance.EquityQuery.valid_fields` / `valid_values` to extend accepted filter UI.

## 6) Shop Products

Route: `/shop`

### Product source
- Firestore Stripe extension catalog:
  - `products/{productId}`
  - `products/{productId}/prices/{priceId}`

### Checkout lane logic
- Web/PWA: create checkout session in `customers/{uid}/checkout_sessions` and redirect to Stripe URL.
- Native digital goods: dispatch native IAP bridge event (`quantura:iap:open`) and webkit/Android bridge hooks.

### Add a new product
1. Add product + price in Stripe (extension syncs to Firestore).
2. Mark product metadata for digital goods when needed (`isDigital=true` or `productType=digital`).
3. Open `/shop` and verify card render + lane behavior.
