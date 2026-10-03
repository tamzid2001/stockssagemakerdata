"""Publish genuine hourly Kalshi perpetual forecasts outside Vercel.

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
import time

import requests
from scripts.weekly_screener import weekly_configuration

ORIGIN = "https://api.elections.kalshi.com/trade-api/v2"
ENGINE = "quantura_perps_ensemble_v1"
QUANTILES = (.01, .25, .5, .75, .9, .99)
NAMES = ("p01", "p25", "p50", "p75", "p90", "p99")


def configuration():
    config = weekly_configuration()
    return {**config, "prediction_length": 24, "frequency": "1h", "calendar": "NONE",
            "horizon_mode": "frequency_periods", "quantiles": list(QUANTILES), "failure_policy": "fail"}


def completed_hourly(candles, market, cutoff):
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
            raise ValueError("CONFLICTING_HOURLY_CLOSE")
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
    if len(predictions) != 24:
        raise ValueError("INVALID_HORIZON")
    rows, prior = [], datetime.fromisoformat(history[-1]["timestamp"].replace("Z", "+00:00")).timestamp()
    for prediction in predictions:
        values = [float(prediction["quantiles"][str(q)]) for q in QUANTILES]
        timestamp = prediction["timestamp"]
        epoch = datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp()
        if epoch != prior + 3600 or not all(math.isfinite(v) and v > 0 for v in values) or values != sorted(values):
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


def run(shard=0, shards=1):
    from ensemble_forecasting.worker import execute_job
    from market_research.store import Store
    db = Store("perps-screener-readonly", "perps").db
    session = requests.Session()
    def get(path, params=None):
        for attempt in range(4):
            response = session.get(ORIGIN + path, params=params, timeout=30)
            if response.status_code == 429 or response.status_code >= 500:
                time.sleep(min(10, 2 ** attempt)); continue
            response.raise_for_status()
            return response.json()
        raise ValueError("PROVIDER_UNAVAILABLE")
    markets = get("/margin/markets")["markets"]
    selected = [m for m in markets if m["status"] == "active" and int(hashlib.sha256(m["ticker"].encode()).hexdigest()[:8], 16) % shards == shard]
    report = {"eligible": len(selected), "successful": 0, "failed": 0, "failures": [], "shard": shard, "shards": shards}
    config = configuration()
    for market in selected:
        try:
            cutoff = int(time.time())//3600*3600
            candles = get(f"/margin/markets/{market['ticker']}/candlesticks", {"start_ts": cutoff-999*3600, "end_ts": cutoff, "period_interval": 60, "include_latest_before_start": "false"})
            if candles.get("ticker") != market["ticker"]:
                raise ValueError("TICKER_MISMATCH")
            history = completed_hourly(candles["candlesticks"], market, cutoff)
            if len(history) < 32:
                raise ValueError("INSUFFICIENT_GENUINE_HOURLY_HISTORY")
            if datetime.fromisoformat(history[-1]["timestamp"].replace("Z", "+00:00")).timestamp() < cutoff-24*3600:
                raise ValueError("HOURLY_HISTORY_NOT_CURRENT")
            ref = db.collection("perp_forecast_catalog").document(market["ticker"])
            previous = ref.get().to_dict() or {}
            if previous.get("history_cutoff_at") == history[-1]["timestamp"] and previous.get("forecast_engine") == ENGINE:
                report["successful"] += 1; continue
            request = {key: value for key, value in config.items() if key not in {"model_checkpoints", "model_revisions", "history_lag_sessions", "adjustment", "model_failure_policy"}}
            result = execute_job({"request": request, "source": {"type": "kalshi_perp", "symbol": market["ticker"], "provider": "kalshi_perps"}, "runtime_mode": "production",
                                  "model_checkpoints": config["model_checkpoints"], "model_revisions": config["model_revisions"],
                                  "input": {"rows": history, "frequency": "1h", "timezone": "UTC"}}, minimum_history_rows=32)
            snapshot = build_snapshot(market, history, result, datetime.now(timezone.utc).isoformat())
            # Monotonic publication protects against delayed/concurrent workers.
            from google.cloud import firestore
            @firestore.transactional
            def publish(transaction):
                existing = ref.get(transaction=transaction).to_dict() or {}
                if str(existing.get("history_cutoff_at", "")) <= snapshot["history_cutoff_at"]:
                    transaction.set(ref, snapshot)
            publish(db.transaction())
            report["successful"] += 1
            print(json.dumps({"ticker": market["ticker"], "status": "published", "history": len(history)}), flush=True)
        except Exception as error:
            report["failed"] += 1
            # Keep credentials/provider bodies out of logs.
            reason = str(error) if isinstance(error, ValueError) and str(error).isupper() else type(error).__name__
            report["failures"].append({"ticker": market["ticker"], "reason": reason})
            print(json.dumps(report["failures"][-1]), flush=True)
    report["updated_at"] = datetime.now(timezone.utc).isoformat()
    db.collection("perp_forecast_status").document(str(shard)).set(report)
    print(json.dumps(report), flush=True)
    if selected and not report["successful"]:
        raise ValueError("NO_PERPETUAL_FORECASTS_PUBLISHED")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard", type=int, default=0)
    parser.add_argument("--shards", type=int, default=1)
    args = parser.parse_args()
    if args.shards < 1 or not 0 <= args.shard < args.shards:
        parser.error("invalid shard")
    run(args.shard, args.shards)
