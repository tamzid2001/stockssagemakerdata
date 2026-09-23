# BTC P90 + sticky trader: approval and operations

Status: implemented approval-gated execution path; offline verification is not proof of exchange fills. **Real orders and continuous execution remain disabled until the operator configures approval.** No startup probe order, funding transfer, or real order is part of verification/readiness.

## Exact strategy

- BTC `KXBTC15M` only. Choose 1–12 observed opening one-minute closes; forecast the remaining 14–3 minutes. Missing opening observations are not fabricated; flat observed windows are allowed.
- First one-minute history uses Granite, Chronos-2 and commercially authorized TimesFM. Two or more minutes also use Prophet. No Toto. Strict completion of all requested models; equal central weights and capability-aware tail weights use the existing ensemble implementation.
- Preserve the historical study's quantile grid: P01, P10, P20, P25, P30, P40, P50, P60, P70, P75, P80, P90, P99.
- First unambiguous completed-minute bid at/above its time-aligned P90 must agree with sticky direction. Neutral/ambiguous quotes do not qualify. A first P90 that disagrees is not replaced by a later convenient signal.
- Sticky direction updates even when no trade occurs. The prepared default uses a **provisional near-close bid** for the immediately preceding market when its official outcome is not yet known. See the separate evidence policy below. A known official result always takes precedence.
- Attempt entry on the following completed minute, using that side's observed ask as the maximum economic entry price. A marketable IOC limit provides taker-style execution with a price cap. It is **not** a simulated minus-one-cent maker order and a minute ask is not a guaranteed fill.
- First received bars must be timely (within 30 seconds), and the actual POST must still be inside the entry window. No backdated execution. Partial and zero fills are recorded; the remainder is not chased.
- Hold the filled side until authoritative settlement. No stop, switching, ladder, or rolling exit in this version.
- The dispatch form defaults to one contract and at most three loss-triggered 2.5× increases per recovery cycle, using whole-contract floor rounding. A non-configurable 100-contract safety ceiling remains. Once the increase limit is reached, size stays fixed until cumulative recovery-cycle net P&L reaches zero; then size and the increase count reset. Starting size, multiplier, and maximum increases are explicit bounded inputs in the exact approval hash. A newly approved sizing configuration is adopted only when durable recovery-cycle P&L is exactly zero and no position/intent is active. Mid-cycle or unreconciled sizing changes fail closed. Actual reconciled cost/fees drive recovery. Wait for prior position/accounting reconciliation before another entry; delayed settlement can therefore skip a market.

Selected timing (September 20): **one observed minute → 14-minute forecast**, first P90 + sticky direction, hold to settlement. The default now matches this selection. No live gate was enabled by this change.

Prepared account selection: **primary account (subaccount 0)**, as requested. Other defaults still subject to operator review: maximum 100 contracts, $100 principal/order, $100 realized UTC-day net-loss entry breaker, maximum ask $0.99. Breaking the daily limit blocks new entries; it does not liquidate a settlement-held position. Recovery state does not reset with a new runner or UTC day. Other account positions or resting orders block entry, including activity from another bot; this code does not cancel, close or take ownership of them.

The user plans funding **exchange shard 2**. This does not mean subaccount 2. Orders and balances follow the market's authoritative `exchange_index`; primary account 0 or subaccount 1–63 is independently selected. Authenticated readiness remains outstanding; this preparation does not fund an account or enable orders.

## Provisional near-close direction

- Public GET `/markets/{ticker}` is sampled approximately once per second during the final ten seconds, independently of the minute collector and inference process. Only active, binary, exactly 15-minute BTC markets qualify.
- A side needs a bid at least $0.99 and positive displayed bid size. YES/NO book complements and numeric bounds are checked. A 99c ask, old last trade, crossed book or missing size is insufficient.
- The final usable snapshot must arrive within three seconds before close; its request must start before close and take at most two seconds. A newer neutral snapshot invalidates an earlier qualifying snapshot. Missing evidence does not invent a direction or reuse an older market.
- This is **receipt-timed REST book evidence**, not a verified exchange timestamp or guaranteed settlement. Market `updated_time` is a non-trading metadata timestamp, so it is not used as quote freshness evidence. Kalshi's own `is_provisional` field describes market lifecycle and is not this heuristic.
- Predict the opposite of the inferred prior winner only after that market closes. P90 signals and entry timing still use completed one-minute candles. These second-level snapshots never enter forecast inputs, minute candles or settlement tables.
- Freeze each first-P90 decision with its evidence. When the official outcome arrives, record whether it matched; use official information for later decisions but never retroactively alter a prior signal/fill.
- **P&L, fees, recovery sizing and release of a held position always require official exchange settlement/accounting.** A provisional signal cannot unblock an unreconciled prior position. Delayed settlement may still cause missed entries.
- `--direction-policy confirmed` retains official-only direction. Default `provisional_near_close` changes the immutable approval fingerprint; old approvals do not activate the new configuration. The earlier 74.50% retrospective figure is for confirmed direction, not evidence of this new heuristic's performance.

Completed 291-market retrospective cohort: 149 sticky entries, 111 wins / 38 losses (74.50%); one-contract net −$0.302 after modeled fees. Recovery sizing net +$47.6689 with $20.7013 realized drawdown and maximum 75 contracts. These are simulated next-minute ask entries, not verified live fills; the 100-contract cap is not a loss guarantee.

## Reused architecture and source review

Reviewed reference run: https://github.com/tamzid2001/polymarketfuturesbot/actions/runs/34975817928 at `19807a671993db6624d89204dfb575339fbf0e49`. It ran the older v14 opposite ladder and was canceled by its owner after approximately 4h27m; this does not demonstrate a P90 ensemble strategy or uninterrupted operation.

Also inspected current source at `ca41716` (v15 direct 51c engine) so this integration does not restore v14 over newer work. Scope was order transport, direction conversion, routing, recovery, reconciliation and Actions handoff—not an exhaustive audit of its large strategy engine.

Review findings and disposition:

1. **Strategy mismatch:** reference has opposite-side ladder/51c exits; current source also differs from this P90 strategy. Port execution patterns, not legacy entry/exit rules.
2. **Critical YES/NO conversion:** V2 uses a single YES bid/ask book. Buying NO is ASK at `1 - no_price`; never submit NO's economic price as the YES price. Covered by tests.
3. **Shard funding/routing:** use authoritative market `exchange_index` for orders and balance. Do not assume shard zero, infer it from ticker text, or transfer funds automatically.
4. **Ambiguous POST:** a timeout, malformed ACK or crash is not evidence that no order exists. Persist a deterministic client ID and intent before POST, reconcile by that exact ID, and never blindly resubmit.
   Create Order V2 request prices use the documented 2–4 decimal request format; the six-decimal format is response-only. The worker validates and journals the V2 acknowledgement (`order_id`, fill/remaining counts and matching-engine timestamp), then looks up that exact order before falling back to the deterministic client ID. Kalshi account reads can briefly lag a successful write, so an acknowledged-but-not-yet-readable order is reported separately from truly unknown delivery.
5. **Different account activity:** the existing bot can trade the same BTC markets. Primary-account selection is supported, but unexplained positions/orders block entry. Account reconciliation is not designed to share positions with another bot. Dedicated subaccounts remain supported if independently selected by the operator.
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

`kalshi-btc-live-diagnostics.yml` is a separate manually dispatched, read-only
workflow. It reads the private live journal and performs signed account/order/fill
GET requests without receiving any live-write approval variables. It reports
only bounded counts, status and P&L metadata—never credentials, request headers,
raw response bodies or client/order identifiers. Use it before intervening in an
`unknown_delivery_blocked` state.

`kalshi-btc-stale-intent-recovery.yml` is a separate manual repair for a legacy
unacknowledged intent. It shares the live singleton, has no POST authorization,
requires the exact ticker plus `resolve-no-order`, waits until after market close,
requires Kalshi's user-data read timestamp to advance, and refuses recovery if
any matching order, fill, resting order, or open position exists. It marks that
one failed intent rejected; it never retries it or changes P&L.

The execution worker is isolated from existing all-14-series collectors, historical studies and paper workflows. Those continue to provide multi-origin statistical comparisons. A live worker runs **one approved origin** at a time; it does not place competing bets for all forecast timings.

## Operator approval checklist

1. Review the implementation and exact strategy/risk configuration. Default account 0 is the primary account; optionally select an existing subaccount 1–63. Key scope/permissions must match that selection. Provision/fund it yourself. This code never opens/funds an account. Ensure no other bot/manual activity will share its positions.
2. Secrets in the `quantura-kalshi-live` GitHub environment or repository: `KALSHI_PROD_API_KEY`, `KALSHI_PRIVATE_KEY`, `FIREBASE_SERVICE_ACCOUNT_JSON`, `QUANTURA_RESEARCH_ARTIFACT_KEY`, `HF_TOKEN`. Keep these in GitHub secrets, not files or workflow inputs. Existing secret names are reused.
3. Model variables: `TIMESFM_COMMERCIAL_LICENSED=true` and `TIMESFM_HF_ACCESS_APPROVED=true` only when approved. CPU-safe hosted runner; no CUDA assumption or silent reduced ensemble.
4. Run `verify`, then `readiness`, then a bounded `observe`. Inspect missing/late minutes, inference latency, signal selection, actual response field compatibility, flatness and shard funding. Offline mocks are not execution proof.
5. Generate the exact prepared configuration hash: `python -m market_research.kalshi_live_worker --mode config --history-minutes 1 --subaccount 0 --direction-policy provisional_near_close --starting-contracts 1 --recovery-multiplier 2.5 --max-recovery-increases 3`. Shard 2 is not a substitute for the account selection.
6. Only after operator approval, set `QUANTURA_KALSHI_APPROVED_CONFIG` to that hash and `QUANTURA_KALSHI_APPROVED_SHA` to the reviewed full commit SHA. Set `QUANTURA_KALSHI_LIVE_ENABLED=true` yourself and dispatch `mode=live`. No agent has enabled this gate.
   When `code_ref` is empty, a live dispatch checks out `QUANTURA_KALSHI_APPROVED_SHA` automatically rather than drifting to a newer default-branch commit. An explicit different `code_ref` fails before dependency installation unless it is approved first. Verify/readiness/observe retain their dispatched-commit default.
7. For automatic continuation, the operator must configure `QUANTURA_KALSHI_CONTINUOUS`, `QUANTURA_KALSHI_WATCHDOG_MODE`, `QUANTURA_KALSHI_HISTORY_MINUTES`, `QUANTURA_KALSHI_SUBACCOUNT`, `QUANTURA_KALSHI_DIRECTION_POLICY`, `QUANTURA_KALSHI_STARTING_CONTRACTS`, `QUANTURA_KALSHI_RECOVERY_MULTIPLIER`, and `QUANTURA_KALSHI_MAX_RECOVERY_INCREASES`, then select `continuous=true`. Watchdog defaults to observe and the continuous gate defaults off; invalid configuration fails closed. A legacy v2 session with an open fill must be reconciled against official settlement using the read-only filled-settlement workflow before v3 sizing can be adopted. A losing legacy cycle cannot change risk settings until its recovery-cycle P&L reaches zero.

The workflow starts only after offline safety tests. Changing history/account/risk invalidates the configuration hash. Changing reviewed code invalidates the SHA gate. Existing durable configurations are immutable: do not create a new namespace/reset recovery to bypass an unresolved position. Configuration migrations require review while flat.

## Restart, persistence and emergency behavior

The worker runs up to five hours with a total-job deadline of 345 minutes measured before setup, then publishes evidence and releases its fence. The Action timeout is 355 minutes. A successful continuous run dispatches a serialized successor with the same timing/account/direction and pinned code. A watchdog at minutes 03/18/33/48 checks queued/running/requested/waiting/pending workers before dispatching. A crash leaves the durable intent and lease for reconciliation; unknown delivery never causes blind order retries. The watchdog recovers failed/cancelled/completed runs, not an active duplicate. Schedules, startup and model downloads can be delayed; **24/7 recovery intent is not uninterrupted execution**. Hosted runners have no persistent model cache; no multi-GB Actions cache is created. A persistent compute runner is needed if missed opening minutes are unacceptable.

`kalshi_execution_sessions` is explicitly client-denied in Firestore rules. It contains bounded configuration, fenced lease, recovery/current intent, the last eight immutable provisional snapshots, worker health (updated each minute), and one small audit record per traded market. Near-close snapshots and outcome comparisons use the same encrypted evidence store. Raw minute/forecast evidence is AES-GCM encrypted in the existing private Storage bucket under `private-research/kalshi-execution/`. No public URLs, public Git state, or Actions account-data artifacts. Storage/Firestore usage is not free; retention should be reviewed before lengthy operation. SIGTERM/graceful handoff saves remaining tape; hard termination can lose uncheckpointed minute evidence but does not reset durable trade accounting or invent missed signals.

Before any POST, the worker verifies its current lease and the session's `enabled` kill switch. A definitive HTTP 400/401/403/422 rejection is recorded and releases the rejected intent; it is never reported as successful and is not retried for that market. HTTP 409/429/5xx, transport failures, malformed acknowledgements and crashes remain ambiguous and stop new entries until exact order/account reconciliation. A validated acknowledgement remains blocked from resubmission while Kalshi's read model catches up. Missing/account-mismatched fields fail closed, not to zero. Authoritative settlement quantities, costs, fees and revenue must agree before recovery changes. An unresolved post requires operator reconciliation, not deleting the journal.

Emergency: set `QUANTURA_KALSHI_CONTINUOUS=false` and `QUANTURA_KALSHI_LIVE_ENABLED=false` to prevent new dispatch/approval, and set the active private session's `enabled=false` to block new entries in the running process. Cancel the running Action if necessary. Already submitted IOC fills remain held to settlement; inspect the exchange directly. **This does not liquidate positions or cancel another bot's orders.**

## Remaining production verification

No real order was sent during implementation. Authenticated readiness, account isolation/funding, actual YES/NO order reconciliation, real partial fills/fees, settlement reconciliation, provisional disagreement rates and an extended observe run remain unverified. Offline tests cannot establish these. Hosted CPU model initialization may miss short-horizon opportunities; missed signals are reported rather than backfilled as trades. A persistent worker is preferable if uninterrupted coverage becomes mandatory.
