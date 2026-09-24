# Five approval-gated Kalshi 15-minute coin traders

BNB, DOGE, ETH, NEAR, and ZEC each have their own `kalshi-<coin>-approved-trader.yml` manual workflow. They call one reviewed worker implementation; there are no five divergent strategy copies. Each uses the BTC trader's first-P90/sticky-opposite-prior-direction signal, first completed minute by default, three-model one-point ensemble, next completed minute ask/IOC entry, official settlement accounting, and 2.5× recovery sizing with three increases by default. Toto is excluded from the one-point forecast. A 99¢ near-close quote can inform the next market's provisional direction but never settles P&L.

No coin workflow is live-enabled by this change. All default to `verify`, `continuous: false`, and main subaccount `0`. Each coin has a separate fenced intent/recovery journal. A shared account-admission lease serializes order submissions; before a new order, every nonzero account position must be explained by an exact terminal fill in one of the six strategy journals. Unknown orders/positions block new entries. The existing BTC journal and recovery history are retained. Coin orders remain blocked until a BTC worker running the shared-account protocol has claimed that journal; an older pinned BTC worker cannot safely recognize the coin positions. The live exchange balance, including a fee reserve, bounds each order. No funds are moved and no subaccounts are created.

For an approved coin `COIN`, configure these repository Actions variables only after reviewing its exact configuration hash and commit SHA:

| Variable suffix | Meaning |
| --- | --- |
| `APPROVED_SHA` | Reviewed, immutable 40-character commit SHA |
| `APPROVED_CONFIG` | The 64-character hash displayed by that coin's `verify` run |
| `LIVE_ENABLED` | Must be `true` before any real order path can open |
| `CONTINUOUS` | Must be `true` before successor/watchdog dispatch |
| `WATCHDOG_MODE` | `observe` or `live`; defaults to `observe` |
| `SUBACCOUNT` | Main account `0` for this shared-account deployment |
| `HISTORY_MINUTES`, `DIRECTION_POLICY`, `STARTING_CONTRACTS`, `RECOVERY_MULTIPLIER`, `MAX_RECOVERY_INCREASES` | Parameters for watchdog restarts; must match the reviewed configuration |

The full prefix is `QUANTURA_KALSHI_<COIN>_`, for example `QUANTURA_KALSHI_ETH_LIVE_ENABLED`. Existing `KALSHI_PROD_API_KEY`, `KALSHI_PRIVATE_KEY`, `FIREBASE_SERVICE_ACCOUNT_JSON`, `QUANTURA_RESEARCH_ARTIFACT_KEY`, and `HF_TOKEN` remain GitHub environment secrets; never put their values in workflow inputs or this file. The existing TimesFM commercial-license/access flags still apply.

The worker hands off before GitHub's six-hour limit. A single scheduled watchdog checks each *explicitly continuous-enabled* coin at minutes 03/18/33/48, skips queued/running workers, and redispatches after failures. This is recovery-oriented continuous operation, not a guarantee of zero-second coverage: GitHub scheduling/queueing and model inference can delay or miss the first live signal. Do not describe an unfilled IOC acknowledgement as an exchange position. The worker verifies fills and reconciles official settlement before risk state changes.
