"""Discover Kalshi perpetual listings hourly; publish seven-day daily forecasts.

One latest snapshot per ticker. Failed refreshes preserve the previous snapshot;
no synthetic candles, substitute provider, or reduced-model forecast is published.
"""
from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import gzip
import hashlib
import json
import math
import os
import re
import time

import requests
from scripts.weekly_screener import weekly_configuration

ORIGIN = "https://api.elections.kalshi.com/trade-api/v2"
ENGINE = "quantura_perps_daily_ensemble_v2"
QUANTILES = (.01, .25, .5, .75, .9, .99)
NAMES = ("p01", "p25", "p50", "p75", "p90", "p99")


def configuration():
    config = weekly_configuration()
    return {**config, "prediction_length": 7, "frequency": "1D", "calendar": "NONE",
            "horizon_mode": "frequency_periods", "quantiles": list(QUANTILES), "failure_policy": "fail"}


def completed_daily(candles, market, cutoff):
    units = float(market["contract_size"]) * float(market["underlying_multiplier"])
    if not math.isfinite(units) or units <= 0:
        raise ValueError("INVALID_CONTRACT_EXPOSURE")
    rows = {}
    for candle in candles:
        ts, value = candle.get("end_period_ts"), (candle.get("price") or {}).get("close")
        if not isinstance(ts, int) or ts > cutoff or value is None:
            continue
        close = float(value) / units
        if not math.isfinite(close) or close <= 0:
            continue
        row = {"timestamp": datetime.fromtimestamp(ts, timezone.utc).isoformat().replace("+00:00", "Z"), "target": close}
        if ts in rows and rows[ts] != row:
            raise ValueError("CONFLICTING_DAILY_CLOSE")
        rows[ts] = row
    return [rows[ts] for ts in sorted(rows)][-512:]


def build_snapshot(market, history, result, generated_at):
    if len(history) < 32 or result.get("runtime", {}).get("mock"):
        raise ValueError("PRODUCTION_HISTORY_REQUIRED")
    config = configuration()
    expected = set(config["models"])
    completed = {m["id"] for m in result.get("models", []) if m.get("status") == "completed"}
    if completed != expected or result.get("failures"):
        raise ValueError("ALL_FIVE_MODELS_REQUIRED")
    predictions = result.get("predictions", [])
    if len(predictions) != 7:
        raise ValueError("INVALID_HORIZON")
    rows, prior = [], datetime.fromisoformat(history[-1]["timestamp"].replace("Z", "+00:00")).timestamp()
    for prediction in predictions:
        values = [float(prediction["quantiles"][str(q)]) for q in QUANTILES]
        timestamp = prediction["timestamp"]
        epoch = datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp()
        if epoch != prior + 86400 or not all(math.isfinite(v) and v > 0 for v in values) or values != sorted(values):
            raise ValueError("INVALID_PREDICTIONS")
        rows.append({"timestamp": timestamp, "date": timestamp[:10], **dict(zip(NAMES, values))})
        prior = epoch
    config_hash = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
    identity = hashlib.sha256(json.dumps([market["ticker"], history[-1]["timestamp"], config_hash, result["result_hash"]]).encode()).hexdigest()[:20]
    return {"ticker": market["ticker"], "status": "success", "forecast_engine": ENGINE,
            "scan_id": f"perps-{identity}", "forecast_config": config, "forecast_config_hash": config_hash,
            "forecast_input_gzip": base64.b64encode(gzip.compress(json.dumps([[r["timestamp"], r["target"]] for r in history], separators=(",", ":")).encode(), mtime=0)).decode(),
            "forecast_rows": rows, "forecast_models": result["models"], "effective_weights_by_quantile": result["effective_weights_by_quantile"],
            "forecast_provenance": {key: result.get(key) for key in ("result_hash", "prepared_series_hash", "runtime_seconds", "runtime", "warnings")},
            "dataset_hash": result["dataset_hash"], "history_cutoff_at": history[-1]["timestamp"], "forecast_history_points": len(history),
            "last_forecast_update": generated_at, "forecast_date": rows[-1]["date"],
            "quantile_stats": {q: {"min": min(r[q] for r in rows), "max": max(r[q] for r in rows), "avg": sum(r[q] for r in rows)/len(rows)} for q in NAMES},
            **{q: rows[0][q] for q in NAMES}}


def run(shard=0, shards=1, discovery_only=False):
    from market_research.public_snapshots import previous as previous_public, write as write_public
    feed = f"perps-{shard}"
    stored = previous_public(feed)
    snapshots = {row["ticker"]: row for row in stored["items"]}
    session = requests.Session()
    def get(path, params=None):
        for attempt in range(4):
            response = session.get(ORIGIN + path, params=params, timeout=30)
            if response.status_code == 429 or response.status_code >= 500:
                time.sleep(min(10, 2 ** attempt)); continue
            response.raise_for_status()
            return response.json()
        raise ValueError("PROVIDER_UNAVAILABLE")
    markets, cursor, seen = [], "", set()
    while True:
        body = get("/margin/markets", {"limit": 1000, **({"cursor": cursor} if cursor else {})})
        if not isinstance(body.get("markets"), list) or len(body["markets"]) > 1000:
            raise ValueError("INVALID_PERPETUAL_CATALOG")
        markets.extend(body["markets"])
        cursor = body.get("cursor")
        if not cursor:
            break
        if not isinstance(cursor, str) or cursor in seen or len(seen) >= 20:
            raise ValueError("INVALID_PERPETUAL_CURSOR")
        seen.add(cursor)
    markets = list({m["ticker"]:m for m in markets if isinstance(m,dict) and isinstance(m.get("ticker"),str)}.values())
    selected = [m for m in markets if isinstance(m, dict) and re.fullmatch(r"KX[A-Z0-9]{1,36}PERP", str(m.get("ticker", ""))) and m.get("status") == "active" and int(hashlib.sha256(m["ticker"].encode()).hexdigest()[:8], 16) % shards == shard]
    active = {m["ticker"] for m in selected}
    snapshots = {ticker: row for ticker, row in snapshots.items() if ticker in active}
    report = {"eligible": len(selected), "successful": 0, "failed": 0, "failures": [], "shard": shard, "shards": shards}
    config = configuration()
    needs_models = False
    for market in selected:
        try:
            cutoff = int(time.time())//86400*86400
            candles = get(f"/margin/markets/{market['ticker']}/candlesticks", {"start_ts": cutoff-999*86400, "end_ts": cutoff, "period_interval": 1440, "include_latest_before_start": "false"})
            if candles.get("ticker") != market["ticker"]:
                raise ValueError("TICKER_MISMATCH")
            history = completed_daily(candles["candlesticks"], market, cutoff)
            if len(history) < 32:
                raise ValueError("INSUFFICIENT_GENUINE_DAILY_HISTORY")
            if datetime.fromisoformat(history[-1]["timestamp"].replace("Z", "+00:00")).timestamp() < cutoff-2*86400:
                raise ValueError("DAILY_HISTORY_NOT_CURRENT")
            previous = snapshots.get(market["ticker"], {})
            if previous.get("history_cutoff_at") == history[-1]["timestamp"] and previous.get("forecast_engine") == ENGINE:
                report["successful"] += 1; continue
            needs_models = True
            if discovery_only:
                continue
            from ensemble_forecasting.worker import execute_job
            request = {key: value for key, value in config.items() if key not in {"model_checkpoints", "model_revisions", "history_lag_sessions", "adjustment", "model_failure_policy"}}
            result = execute_job({"request": request, "source": {"type": "kalshi_perp", "symbol": market["ticker"], "provider": "kalshi_perps"}, "runtime_mode": "production",
                                  "model_checkpoints": config["model_checkpoints"], "model_revisions": config["model_revisions"],
                                  "input": {"rows": history, "frequency": "1D", "timezone": "UTC"}}, minimum_history_rows=32)
            snapshot = build_snapshot(market, history, result, datetime.now(timezone.utc).isoformat())
            snapshots[market["ticker"]] = snapshot
            report["successful"] += 1
            print(json.dumps({"ticker": market["ticker"], "status": "published", "history": len(history)}), flush=True)
        except Exception as error:
            report["failed"] += 1
            # Keep credentials/provider bodies out of logs.
            reason = str(error) if isinstance(error, ValueError) and str(error).isupper() else type(error).__name__
            report["failures"].append({"ticker": market["ticker"], "reason": reason})
            print(json.dumps(report["failures"][-1]), flush=True)
    report["updated_at"] = datetime.now(timezone.utc).isoformat()
    # The catalog is rediscovered hourly. Unchanged completed daily cutoffs reuse
    # their five-model forecast, so newly listed contracts are detected cheaply.
    report["new_listings"] = sorted(active - {m.get("ticker") for m in stored.get("catalog", [])})
    report["needs_models"] = needs_models
    report["discovery_only"] = discovery_only
    report["run_status"] = "partial" if report["failed"] or discovery_only and needs_models else "completed"
    # A new listing without 32 genuine daily observations still enters the
    # catalog. Keep its forecast unavailable rather than synthesizing bars.
    catalog = [{k:m.get(k) for k in ("ticker", "title", "status", "contract_size", "underlying_multiplier")} for m in selected]
    write_public(feed, {"items": list(snapshots.values()), "status": report, "catalog": catalog}, os.environ.get("QUANTURA_PUBLIC_DIR", "/tmp/quantura-public-perps"))
    if discovery_only and os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            output.write(f"needs_models={'true' if needs_models else 'false'}\n")
    print(json.dumps(report), flush=True)
    if not discovery_only and needs_models and selected and not report["successful"]:
        raise ValueError("NO_PERPETUAL_FORECASTS_PUBLISHED")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--discovery-only", action="store_true")
    parser.add_argument("--shard", type=int, default=0)
    parser.add_argument("--shards", type=int, default=1)
    args = parser.parse_args()
    if args.shards < 1 or not 0 <= args.shard < args.shards:
        parser.error("invalid shard")
    run(args.shard, args.shards, args.discovery_only)
