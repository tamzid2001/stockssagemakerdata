# BTC P90 + sticky trader: approval and operations

Status: implemented approval-gated execution path; offline verification is not proof of exchange fills. **Real orders and continuous execution remain disabled until the operator configures approval.** No startup probe order, funding transfer, or real order is part of verification/readiness.

## Exact strategy

- BTC `KXBTC15M` only. Choose 1–12 observed opening one-minute closes; forecast the remaining 14–3 minutes. Missing opening observations are not fabricated; flat observed windows are allowed.
- First one-minute history uses Granite, Chronos-2 and commercially authorized TimesFM. Two or more minutes also use Prophet. No Toto. Strict completion of all requested models; equal central weights and capability-aware tail weights use the existing ensemble implementation.
- Preserve the historical study's quantile grid: P01, P10, P20, P25, P30, P40, P50, P60, P70, P75, P80, P90, P99.
- First unambiguous completed-minute bid at/above its time-aligned P90 must agree with sticky direction. Neutral/ambiguous quotes do not qualify. A first P90 that disagrees is not replaced by a later convenient signal.
- Sticky direction updates from confirmed settlements even when no trade occurs. For binary outcomes, opposite the latest confirmed settled result is equivalent to holding a wrong directional prediction until correct, then flipping.
- Attempt entry on the following completed minute, using that side's observed ask as the maximum economic entry price. A marketable IOC limit provides taker-style execution with a price cap. It is **not** a simulated minus-one-cent maker order and a minute ask is not a guaranteed fill.
- First received bars must be timely (within 30 seconds), and the actual POST must still be inside the entry window. No backdated execution. Partial and zero fills are recorded; the remainder is not chased.
- Hold the filled side until authoritative settlement. No stop, switching, ladder, or rolling exit in this version.
- Start one contract; floor rounding after 2.5× losses, fixed 100-contract cap, reset on recovery-cycle net P&L >= 0. Actual reconciled cost/fees drive recovery. Wait for prior position/accounting reconciliation before another entry; delayed settlement can therefore skip a market.

Defaults subject to approval: two observed minutes, dedicated subaccount 1, maximum 100 contracts, $100 principal/order, $100 realized UTC-day net-loss entry breaker, maximum ask $0.99. Breaking the daily limit blocks new entries; it does not liquidate a settlement-held position. Recovery state does not reset with a new runner or UTC day.

## Reused architecture and source review

Reviewed reference run: https://github.com/tamzid2001/polymarketfuturesbot/actions/runs/34975817928 at `19807a671993db6624d89204dfb575339fbf0e49`. It ran the older v14 opposite ladder and was canceled by its owner after approximately 4h27m; this does not demonstrate a P90 ensemble strategy or uninterrupted operation.

Also inspected current source at `ca41716` (v15 direct 51c engine) so this integration does not restore v14 over newer work. Scope was order transport, direction conversion, routing, recovery, reconciliation and Actions handoff—not an exhaustive audit of its large strategy engine.

Review findings and disposition:

1. **Strategy mismatch:** reference has opposite-side ladder/51c exits; current source also differs from this P90 strategy. Port execution patterns, not legacy entry/exit rules.
2. **Critical YES/NO conversion:** V2 uses a single YES bid/ask book. Buying NO is ASK at `1 - no_price`; never submit NO's economic price as the YES price. Covered by tests.
3. **Shard funding/routing:** use authoritative market `exchange_index` for orders and balance. Do not assume shard zero, infer it from ticker text, or transfer funds automatically.
4. **Ambiguous POST:** a timeout, malformed ACK or crash is not evidence that no order exists. Persist a deterministic client ID and intent before POST, reconcile by that exact ID, and never blindly resubmit.
5. **Different account activity:** the existing bot can trade the same BTC markets. Require a dedicated subaccount and block if unexplained positions/orders exist. Do not use the primary account alongside the existing bot.
6. **Reference startup probe actually trades:** not copied. Readiness performs only authenticated GET requests; any later exchange-order validation is a separate operator action.
7. **State published to Git/artifacts:** not copied. Private small Firestore coordination/journal plus encrypted cloud evidence avoids public account-state commits and the previous Actions artifact quota failure.
8. **Reference dependency ranges:** Quantura uses its existing hashed Python inference/analysis lockfiles; no runtime dependency installation in requests.
9. **Handoff is not zero downtime:** GitHub queues, hosted-runner startup and model loading can miss markets. The new runner reconciles old positions, but only starts new forecasts for markets it witnessed from opening. It never reconstructs a missed first live signal from a late backfill.

Protocol references:

- https://docs.kalshi.com/api-reference/orders/create-order-v2
- https://docs.kalshi.com/api-reference/orders/get-orders
- https://docs.kalshi.com/api-reference/portfolio/get-positions
- https://docs.kalshi.com/api-reference/portfolio/get-settlements
- https://docs.kalshi.com/getting_started/api_keys

## Workflow modes

`kalshi-btc-approved-trader.yml`:

- `verify` (default): offline tests/config digest, no account credentials required, no inference/models downloaded.
- `readiness`: authenticated orders/positions GET checks, no POSTs and no orders.
- `observe`: real minute collection/ensemble/signals, signed read-only account preflight, intent logging only; does not invent fills or update recovery from simulated outcomes.
- `live`: requires all independent approval gates below. Only this mode can submit orders.

The execution worker is isolated from existing all-14-series collectors, historical studies and paper workflows. Those continue to provide multi-origin statistical comparisons. A live worker runs **one approved origin** at a time; it does not place competing bets for all forecast timings.

## Operator approval checklist

1. Review the implementation and exact strategy/risk configuration. Select forecast timing and a dedicated existing subaccount 1–63. Prefer a subaccount-restricted API key; scope/permissions must match this subaccount. Provision/fund it yourself. This code never opens/funds an account.
2. Secrets in the `quantura-kalshi-live` GitHub environment or repository: `KALSHI_PROD_API_KEY`, `KALSHI_PRIVATE_KEY`, `FIREBASE_SERVICE_ACCOUNT_JSON`, `QUANTURA_RESEARCH_ARTIFACT_KEY`, `HF_TOKEN`. Keep these in GitHub secrets, not files or workflow inputs. Existing secret names are reused.
3. Model variables: `TIMESFM_COMMERCIAL_LICENSED=true` and `TIMESFM_HF_ACCESS_APPROVED=true` only when approved. CPU-safe hosted runner; no CUDA assumption or silent reduced ensemble.
4. Run `verify`, then `readiness`, then a bounded `observe`. Inspect missing/late minutes, inference latency, signal selection, actual response field compatibility, flatness and shard funding. Offline mocks are not execution proof.
5. Generate the exact configuration hash: `python -m market_research.kalshi_live_worker --mode config --history-minutes 2 --subaccount 1` (change values to your selections).
6. Only after operator approval, set `QUANTURA_KALSHI_APPROVED_CONFIG` to that hash and `QUANTURA_KALSHI_APPROVED_SHA` to the reviewed full commit SHA. Set `QUANTURA_KALSHI_LIVE_ENABLED=true` yourself and dispatch `mode=live`. No agent has enabled this gate.
7. For automatic continuation, set `QUANTURA_KALSHI_CONTINUOUS=true`, `QUANTURA_KALSHI_WATCHDOG_MODE=live`, `QUANTURA_KALSHI_HISTORY_MINUTES`, and `QUANTURA_KALSHI_SUBACCOUNT`, then select `continuous=true`. Watchdog defaults to observe; invalid configuration fails closed.

The workflow starts only after offline safety tests. Changing history/account/risk invalidates the configuration hash. Changing reviewed code invalidates the SHA gate. Existing durable configurations are immutable: do not create a new namespace/reset recovery to bypass an unresolved position. Configuration migrations require review while flat.

## Restart, persistence and emergency behavior

The worker runs up to five hours with a total-job deadline before the six-hour limit, then publishes evidence and releases its fence. A successful continuous run dispatches a serialized successor pinned to the approved code. A 15-minute watchdog checks queued/running/waiting/pending workers before dispatching. Schedules can be delayed; **24/7 recovery intent is not uninterrupted execution**.

`kalshi_execution_sessions` is explicitly client-denied in Firestore rules. It contains bounded configuration, fenced lease, recovery/current intent and one small audit record per traded market. Raw minute/forecast evidence is AES-GCM encrypted in the existing private Storage bucket under `private-research/kalshi-execution/`. No public URLs, public Git state, or Actions account-data artifacts. Storage/Firestore usage is not free; retention should be reviewed before lengthy operation.

Before any POST, the worker verifies its current lease and the session's `enabled` kill switch. Unknown delivery stops new entries until exact order/account reconciliation. It does not retry with a new client ID. Missing/account-mismatched fields fail closed, not to zero. Authoritative settlement quantities, costs, fees and revenue must agree before recovery changes. An unresolved post requires operator reconciliation, not deleting the journal.

Emergency: set `QUANTURA_KALSHI_CONTINUOUS=false` and `QUANTURA_KALSHI_LIVE_ENABLED=false` to prevent new dispatch/approval, and set the active private session's `enabled=false` to block new entries in the running process. Cancel the running Action if necessary. Already submitted IOC fills remain held to settlement; inspect the exchange directly. **This does not liquidate positions or cancel another bot's orders.**

## Remaining production verification

No real order was sent during implementation. Authenticated readiness, dedicated subaccount funding, actual YES/NO order reconciliation, real partial fills/fees, settlement reconciliation and an extended observe run must be verified before calling this proven live execution. Hosted CPU model initialization may miss short-horizon opportunities; missed signals are reported rather than backfilled as trades. A persistent worker is preferable if uninterrupted coverage becomes mandatory.
