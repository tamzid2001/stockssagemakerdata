"""Read-only Kalshi BTC 15-minute discovery and first-two-minute paper worker.

No account credentials, exchange order routes, Firebase writes, or fabricated
candles. Uses the existing Python ensemble and encrypted SQLite artifact runner.
"""
import argparse
from dataclasses import asdict
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request

from ensemble_forecasting.capabilities import MODEL_REGISTRY
from .engine import Quote, digest, stamp, validate_forecast
from .forecast import default_research_models, forecast_window
from .local_store import LocalStore
from .p1_oco import QUANTILES
from .quantile_paths import from_store

VERSION = "kalshi_btc_first2_next13_v1"
ONE_MINUTE_VERSION = 'kalshi_btc_first1_next14_v1'
SERIES = "KXBTC15M"
API = "https://external-api.kalshi.com/trade-api/v2"


class KalshiBTCProvider:
    """Provider-specific translation; only approved public GET endpoints."""
    def get(self, path, params=None):
        if not re.fullmatch(r"/(markets|historical/markets|historical/cutoff|series/KXBTC15M|markets/KXBTC15M-[A-Z0-9-]+|(?:historical|series/KXBTC15M)/markets/KXBTC15M-[A-Z0-9-]+/candlesticks)", path):
            raise ValueError("UNAPPROVED_KALSHI_READ_ROUTE")
        url = API + path + ("?" + urllib.parse.urlencode(params) if params else "")
        for attempt in range(4):
            try:
                request = urllib.request.Request(url, headers={"User-Agent": "Quantura-Paper-Research/1"})
                with urllib.request.urlopen(request, timeout=20) as response:
                    return json.load(response)
            except urllib.error.HTTPError as error:
                if error.code in {429, 502, 503, 504} and attempt < 3:
                    time.sleep(2**attempt)
                    continue
                raise RuntimeError(f"KALSHI_HTTP_{error.code}") from None
            except (urllib.error.URLError, TimeoutError):
                if attempt < 3:
                    time.sleep(2**attempt)
                    continue
                raise RuntimeError("KALSHI_UNAVAILABLE") from None

    def discover(self, historical=False, limit=10):
        result = self.get("/markets", {"series_ticker": SERIES, "status": "settled" if historical else "open", "limit": limit})
        markets = [m for m in result.get("markets", []) if self.valid_market(m)]
        return markets, {"markets_returned": len(result.get("markets", [])), "valid_15m_markets": len(markets),
                         "next_cursor": result.get("cursor") or None, "historical_tier": False}

    @staticmethod
    def valid_market(m):
        try:
            return bool(re.fullmatch(r"KXBTC15M-[A-Z0-9-]+", m["ticker"])) and m["market_type"] == "binary" and stamp(m["close_time"]) - stamp(m["open_time"]) == 900
        except (KeyError, TypeError, ValueError):
            return False

    def market(self, ticker):
        return self.get("/markets/" + ticker)["market"]

    def candles(self, market, end):
        start, close = stamp(market["open_time"]), stamp(market["close_time"])
        ticker = market["ticker"]
        params = {"start_ts": start + 60, "end_ts": min(end // 60 * 60, close), "period_interval": 1,
                  "include_latest_before_start": "false"}
        if params["end_ts"] < params["start_ts"]:
            return []
        # Official moving cutoff; do not assume live endpoints hold old history.
        cutoff = self.get("/historical/cutoff")
        boundary = stamp(cutoff["market_settled_ts"]) if cutoff.get("market_settled_ts") else 0
        historical = close < boundary
        if historical:
            params.pop("include_latest_before_start")
        path = ("/historical" if historical else "/series/" + SERIES) + "/markets/" + ticker + "/candlesticks"
        result = self.get(path, params)
        if result.get("ticker", ticker) != ticker:
            raise ValueError("KALSHI_CANDLE_IDENTITY_MISMATCH")
        return result.get("candlesticks", [])

    @staticmethod
    def quotes(candles, market, end):
        start, close = stamp(market["open_time"]), stamp(market["close_time"])
        yes, no = {}, {}
        for candle in candles:
            t = candle.get("end_period_ts")
            if type(t) is not int or t % 60 or not start < t <= min(close, end // 60 * 60):
                continue
            try:
                bid_object, ask_object = candle["yes_bid"], candle["yes_ask"]
                # Current fixed-point dollar keys and documented archived dollar
                # keys; never convert a missing value to 0 or forward-fill it.
                bid = float(bid_object.get("close_dollars", bid_object.get("close")))
                ask = float(ask_object.get("close_dollars", ask_object.get("close")))
                y, n = Quote(t, ask, bid), Quote(t, 1 - bid, 1 - ask)
                if t in yes and yes[t] != y:
                    raise RuntimeError("CONFLICTING_KALSHI_CANDLE")
                yes[t], no[t] = y, n
            except (KeyError, TypeError, ValueError):
                continue
        return {"yes": sorted(yes.values(), key=lambda q: q.timestamp),
                "no": sorted(no.values(), key=lambda q: q.timestamp)}

    def resolution(self, market, side):
        m = self.market(market["ticker"])
        if m.get("ticker") != market["ticker"]:
            raise ValueError("KALSHI_RESOLUTION_IDENTITY_MISMATCH")
        result = m.get("result")
        verified = m.get("status") in {"finalized", "settled"} and result in {"yes", "no"}
        return {"contract_id": m["ticker"] + ":" + side, "provider": "kalshi", "checked_at": int(time.time()),
                "source_url": API + "/markets/" + m["ticker"], "result": result,
                "resolution_status": "resolved" if verified else "pending_or_unverified",
                "selected_side_won": side == result if verified else None}


def two_minute_window(quotes, opened):
    by_time = {q.timestamp: q for q in quotes if q.observed}
    if any(t not in by_time for t in (opened + 60, opened + 120)):
        raise ValueError("FIRST_TWO_COMPLETED_MINUTES_REQUIRED")
    return [by_time[opened + 60], by_time[opened + 120]]


def first_minute_window(quotes, opened):
    matches = [q for q in quotes if q.observed and q.timestamp == opened + 60]
    if len(matches) != 1:
        raise ValueError('EXACT_FIRST_COMPLETED_MINUTE_REQUIRED')
    return matches


def short_context_models():
    return tuple(m for m in default_research_models()
                 if MODEL_REGISTRY["models"][m].get("minimumObservedContext", 2) <= 2)


def process_market(store, provider, market, now, historical=False, forecaster=forecast_window, history_minutes=2):
    if history_minutes not in (1, 2):
        raise ValueError('INVALID_BTC_HISTORY_MINUTES')
    version = ONE_MINUTE_VERSION if history_minutes == 1 else VERSION
    horizon = 15 - history_minutes
    if not provider.valid_market(market):
        raise ValueError("INVALID_BTC_15M_MARKET")
    ticker = market["ticker"]
    opened, end = stamp(market["open_time"]), stamp(market["close_time"])
    saved = store._get("checkpoints", "btc:" + ticker) or {"market": market, "status": "waiting"}
    if saved["status"] in {"missed_start", "failed", "missing_first_minutes"}:
        return saved
    origin = opened + history_minutes * 60
    if now < origin:
        store.checkpoint("btc:" + ticker, saved)
        return saved
    if saved["status"] == "waiting" and not historical and now > origin + 60:
        saved.update(status="missed_start", reason="WORKER_NOT_READY_WITHIN_ORIGIN_WINDOW")
        store.checkpoint("btc:" + ticker, saved)
        return saved
    raw = provider.candles(market, now)
    quotes = provider.quotes(raw, market, now)
    if saved["status"] == "waiting":
        try:
            window_builder = first_minute_window if history_minutes == 1 else two_minute_window
            windows = {s: window_builder(q, opened) for s, q in quotes.items()}
        except ValueError:
            if historical or now >= origin + 60:
                saved.update(status="missing_first_minutes", reason="EXACT_FIRST_COMPLETED_MINUTE_REQUIRED" if history_minutes == 1 else "FIRST_TWO_COMPLETED_MINUTES_REQUIRED")
            store.checkpoint("btc:" + ticker, saved)
            return saved
        models = ('granite', 'chronos', 'timesfm') if history_minutes == 1 else short_context_models()
        if len(models) < 2:
            raise RuntimeError("INSUFFICIENT_SHORT_CONTEXT_MODELS")
        # Claim before inference: a crash never reruns an origin as if the
        # delayed forecast were available earlier. Failed/in-flight rows explicit.
        saved.update(status="inference_started", requested_models=list(models), origin=origin)
        store.checkpoint("btc:" + ticker, saved)
        started = time.monotonic()
        pair = []
        try:
            for side in ("yes", "no"):
                options = {'single_point_research': True} if history_minutes == 1 else {}
                f = forecaster(windows[side], horizon, models, QUANTILES, **options)
                validate_forecast(f, origin, horizon)
                f["market_context"] = {"event_id": market["event_ticker"], "market_id": ticker,
                                       "contract_id": ticker + ":" + side, "side": side,
                                       "outcome": "BTC up" if side == "yes" else "BTC not up", "provider": "kalshi"}
                f.update(history_count=history_minutes, strategy=version, input_snapshot=[asdict(q) for q in windows[side]],
                         expected_side_count=2,
                         short_context_exclusions=["toto: requires 32 genuine observations"] +
                             (["prophet: requires at least two observations"] if history_minutes == 1 else []),
                         inference_started_at=now, requested_horizon_minutes=horizon)
                # Same underlying input for different contracts must not collide.
                f["forecast_id"] = digest([f["forecast_id"], ticker, side, version])
                pair.append(f)
        except Exception as error:
            saved.update(status="failed", error_type=type(error).__name__)
            store.checkpoint("btc:" + ticker, saved)
            raise
        duration = time.monotonic() - started
        available = origin + max(1, int(duration) + 1) if historical else max(int(time.time()), now)
        # Atomically publish BOTH sides, then observe quotes. No partial-pair bias.
        with store.lock, store.db:
            for f in pair:
                f.update(available_at=available, group_inference_seconds=duration)
                store._put("forecasts", f["forecast_id"], f, True)
            saved.update(status="published" if available < end else "missed_deadline", available_at=available)
            store._put("checkpoints", "btc:" + ticker, saved)
        if not historical:
            now = int(time.time())
            raw = provider.candles(market, now)
            quotes = provider.quotes(raw, market, now)
    elif saved["status"] == "inference_started":
        # Restored interruption: no unknowable publication time or duplicate fit.
        saved.update(status="failed", reason="INFERENCE_INTERRUPTED_BEFORE_PUBLICATION")
        store.checkpoint("btc:" + ticker, saved)
    if saved["status"] in {"published", "observing", "complete"}:
        with store.lock, store.db:
            for side, rows in quotes.items():
                for q in rows:
                    if q.timestamp > saved["available_at"]:
                        record = {"game_id": market["event_ticker"], "contract_id": ticker + ":" + side, **asdict(q)}
                        store._put("observations", digest([ticker, side, q.timestamp]), record, True)
            saved.update(status="complete" if now >= end else "observing", observed_at=now,
                         observation_counts={s: len(q) for s, q in quotes.items()})
            store._put("checkpoints", "btc:" + ticker, saved)
        if now >= end:
            for side in ("yes", "no"):
                store.checkpoint("resolution:" + ticker + ":" + side, provider.resolution(market, side))
    return saved


def report(store, coverage, failures):
    paths = from_store(store, int(time.time()))
    from .btc_signals import from_store as signal_report
    signals = signal_report(store, int(time.time()))
    single = any(c.get('version') == ONE_MINUTE_VERSION for c in store.values('configuration'))
    records = [r for r in store.values("checkpoints") if isinstance(r, dict) and r.get("market")]
    result = {"version": ONE_MINUTE_VERSION if single else VERSION, "paper_only": True, "coverage": coverage,
              "market_status_counts": {s: sum(r["status"] == s for r in records) for s in sorted({r["status"] for r in records})},
              "forecast_count": len(store.values("forecasts")), "failures": failures,
              "model_participation": paths["model_participation"],
              "experiments": paths["summary"], "generated_at": int(time.time()),
              "p90_buy_p10_sell": {"version": signals['version'], "summary": signals['summary'],
                                   "per_market": signals['per_game'], "configuration": signals['configuration']},
              "model_policy": "First two genuine observations; Toto excluded by minimum context. Actual participants/effective per-quantile weights persisted. Two-point forecasts are unvalidated research.",
              "execution": "Read-only bid/ask paper simulation; 1% entry/exit notional fee assumption, not a verified exchange fee schedule or executable liquidity."}
    if single:
        result.update(model_policy='Exactly one first-minute observation; Granite, Chronos-2 and TimesFM; strict all-three success. Unvalidated research.',
                      execution='Post-only minus-one-cent limit candidate-fill proxy; not actual orders. Zero-fee sensitivity; all unfilled orders recorded.',
                      history_minutes=1,horizon_minutes=14)
    store.report(digest(result), result)
    print(json.dumps({"event": "btc_paper_report", **result}), flush=True)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["live", "historical"], default="live")
    parser.add_argument("--duration-minutes", type=int, default=345)
    parser.add_argument("--max-markets", type=int, default=10)
    parser.add_argument('--history-minutes', type=int, choices=(1, 2), default=2)
    args = parser.parse_args()
    if not 1 <= args.duration_minutes <= 345 or not 1 <= args.max_markets <= 100:
        parser.error("INVALID_WORKER_LIMITS")
    configuration = {"version": ONE_MINUTE_VERSION if args.history_minutes == 1 else VERSION,
                     "mode": args.mode, "code_sha": os.environ.get("QUANTURA_CODE_SHA", "local"),
                     "history_minutes": args.history_minutes, "horizon_minutes": 15-args.history_minutes, "paper_only": True}
    store = LocalStore("btc-" + digest(configuration)[:20], os.environ.get("GITHUB_RUN_ID", "local"))
    if store.values("configuration") and configuration not in store.values("configuration"):
        raise RuntimeError("RESTORE_ORIGINAL_CODE_AND_CONFIGURATION")
    store.claim(configuration)
    provider = KalshiBTCProvider()
    failures, coverage = [], {}
    budget = min(args.duration_minutes * 60, max(0, float(os.environ.get("QUANTURA_JOB_STARTED_AT", time.time())) + 350 * 60 - time.time()))
    deadline = time.monotonic() + budget
    try:
        while time.monotonic() < deadline and not store.at_capacity:
            markets, coverage = provider.discover(args.mode == "historical", args.max_markets)
            # Continue ended markets until official result is available. Selection
            # is fixed by checkpoint metadata, never hidden by current discovery.
            tracked = {m["ticker"]: m for m in markets}
            for record in store.values("checkpoints"):
                if not isinstance(record, dict) or not record.get("market"):
                    continue
                ticker = record["market"]["ticker"]
                resolution = store._get("checkpoints", "resolution:" + ticker + ":yes") or {}
                if record["status"] not in {"failed", "missed_start", "missing_first_minutes", "missed_deadline"} and resolution.get("resolution_status") != "resolved":
                    tracked[ticker] = record["market"]
            for m in tracked.values():
                if time.monotonic() >= deadline or store.at_capacity:
                    break
                try:
                    process_market(store, provider, m, int(time.time()), args.mode == "historical", history_minutes=args.history_minutes)
                except (RuntimeError, ValueError, OSError) as error:
                    failure = {"ticker": m["ticker"], "error_type": type(error).__name__, "at": int(time.time())}
                    store.checkpoint("failure:" + digest(failure), failure)
                    failures.append(failure)
                    failures = failures[-100:]
            # Snapshot reporting is bounded to once per minute; immutable per-
            # quote/checkpoint records still update on each observation poll.
            prior_report = store._get("checkpoints", "last_report") or {}
            if args.mode == "historical" or time.time() - prior_report.get("at",0) >= 60:
                result = report(store, coverage, failures)
                store.checkpoint("last_report", {"at":int(time.time())})
            if args.mode == "historical":
                if not result["forecast_count"]:
                    raise RuntimeError("NO_BTC_FORECASTS_PRODUCED")
                break
            time.sleep(min(15, max(0, deadline - time.monotonic())))
        if store.at_capacity:
            raise RuntimeError("ARTIFACT_CAPACITY_REQUIRES_NEW_SHARD")
        if os.environ.get("GITHUB_OUTPUT"):
            with open(os.environ["GITHUB_OUTPUT"], "a") as out:
                out.write("handoff_ready=true\n")
    finally:
        report(store, coverage, failures)
        store.release()


if __name__ == "__main__":
    main()
