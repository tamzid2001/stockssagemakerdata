"""Strategy-free rolling forecast corpus shared by historical and live research.

Model inputs end at decision_time - lag. Matching observations AFTER that cutoff
are separate annotations, never arguments to the forecaster. Missing bars are
not synthesized. This archive measures forecasts, not hypothetical trading wins.
"""
import csv
import gzip
import hashlib
import io
import json
import time

from .engine import QUANTILES, digest, history_window, iso, normalize_quotes, stamp, validate_forecast
from .provider import historical_range

CORPUS_VERSION = "rolling_quantile_corpus_v1"


def export_corpus(store, forecasts, configuration):
    """Machine-readable curves in addition to the recoverable SQLite archive.

    These files remain local until the enclosing authenticated encrypted ZIP
    is uploaded. No provider credentials, account tokens or order data enter it.
    """
    path = store.directory / "forecast_quantiles.csv.gz"
    fields = ["forecast_id", "provider", "event_id", "market_id", "contract_id", "side",
              "decision_time", "input_cutoff", "requested_input_cutoff", "history_count",
              "lag_minutes", "horizon_minutes", "timestamp", *[f"p{round(q*100):02d}" for q in QUANTILES],
              "actual_ask", "actual_bid", "actual_observed", "actual_phase", "participating_models"]
    count = 0
    with path.open("wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", filename="", mtime=0) as zipped:
        with io.TextIOWrapper(zipped, encoding="utf-8", newline="") as output:
            writer = csv.writer(output); writer.writerow(fields)
            for f in sorted(forecasts, key=lambda v: v["forecast_id"]):
                c = f["market_context"]
                actuals = {a["timestamp"]: a for a in f["actuals"]}
                for row in f["rows"]:
                    a = actuals[row["timestamp"]]
                    writer.writerow([f["forecast_id"], c["provider"], c.get("event_id"), c["market_id"], c["contract_id"], c["side"],
                        iso(f["decision_time"]), iso(f["origin"]), iso(f["requested_input_cutoff"]), f["history_count"],
                        f["lag_minutes"], len(f["rows"]), iso(row["timestamp"]),
                        *[row["quantiles"][str(q)] for q in QUANTILES], a["ask"], a["bid"], a["observed"], a["phase"],
                        "+".join(m["id"] for m in f.get("models", []) if m.get("status") == "completed")])
                    count += 1
    manifest = {"schema_version": CORPUS_VERSION, "configuration": configuration,
                "forecast_count": len(forecasts), "prediction_rows": count, "fields": fields,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "data_file": path.name,
                "input_snapshots": "research.sqlite3 / forecasts.input_snapshot",
                "redistribution_status": "review_required", "strategy": None}
    (store.directory / "quantile_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def lagged_window(quotes, decision_time, lag_minutes=0):
    if not isinstance(lag_minutes, int) or not 0 <= lag_minutes <= 120:
        raise ValueError("INVALID_HISTORY_LAG")
    cutoff = decision_time - lag_minutes * 60
    eligible = [q for q in quotes if q.observed and q.timestamp <= cutoff]
    if len(eligible) < 2:
        raise ValueError("TWO_OBSERVATIONS_BEFORE_CUTOFF_REQUIRED")
    return history_window(eligible, eligible[-1].timestamp), cutoff


def annotate_forecast(forecast, contract, decision_time, cutoff, lag_minutes):
    forecast["market_context"] = {"provider": contract.get("source", "polymarket_us"),
        "symbol": contract["providerSymbol"], "side": contract["side"],
        "contract_id": contract["contractId"], "event_id": contract.get("eventId"),
        "market_id": contract["marketId"], "outcome": contract.get("outcome")}
    forecast.update(experiment=CORPUS_VERSION, decision_time=decision_time,
                    requested_input_cutoff=cutoff, lag_minutes=lag_minutes,
                    generated_at=int(time.time()), strategy=None)
    forecast["forecast_id"] = digest([forecast["forecast_id"], forecast["market_context"],
                                      decision_time, cutoff, CORPUS_VERSION])
    return forecast


def matching_actuals(forecast, quotes):
    by_time = {q.timestamp: q for q in quotes if q.observed}
    return [{"timestamp": row["timestamp"],
             "ask": by_time[row["timestamp"]].ask if row["timestamp"] in by_time else None,
             "bid": by_time[row["timestamp"]].bid if row["timestamp"] in by_time else None,
             "phase": "already_observed_at_decision" if row["timestamp"] <= forecast["decision_time"] else "after_decision",
             "observed": row["timestamp"] in by_time}
            for row in forecast["rows"]]


def corpus_origins(quotes, contract, lag_minutes, roll_minutes):
    if roll_minutes not in (15, 30, 45, 60):
        raise ValueError("INVALID_ROLL_MINUTES")
    if len(quotes) < 2:
        return
    # Begin at game start where known, with pregame data as context. Without a
    # trustworthy start use the first 500th observation, or the second when the
    # provider has fewer. Do not spend a budget on hundreds of tiny prefixes.
    start = stamp(contract["eventStart"]) if contract.get("eventStart") else quotes[499 if len(quotes) >= 500 else 1].timestamp + lag_minutes * 60
    start = max(start, quotes[1].timestamp + lag_minutes * 60)
    start = (start + 59) // 60 * 60
    yield from range(start, quotes[-1].timestamp, roll_minutes * 60)


def process_quantile_historical(store, provider, contract, now, horizon, forecaster,
                                max_origins, heartbeat, deadline, lag_minutes=30,
                                roll_minutes=30):
    if horizon not in (30, 45, 60) or not 0 <= lag_minutes < horizon:
        raise ValueError("LAG_MUST_BE_LESS_THAN_HORIZON")
    start, end = historical_range(contract, now)
    # Ask for genuine pregame history beyond 500 wall-clock minutes when a
    # sparse feed needs it. The common download adapter paginates upstream.
    available = stamp(contract["availableFrom"]) if contract.get("availableFrom") else start - 7 * 86400
    start = max(available, start - 7 * 86400, end - 90 * 86400)
    raw = store.archived_history(contract, start, end)
    cache_hit = raw is not None
    if raw is None:
        raw = provider.history(contract, start, end)
        store.archive_history(contract, start, end, raw)
    quotes = normalize_quotes(raw, end)
    if len(quotes) < 2:
        raise ValueError("MISSING_HISTORY")
    state = store.load(contract["contractId"]).get("state") or {"origin_count": 0, "completed_decision": 0, "failed_origins": 0}
    origins = list(corpus_origins(quotes, contract, lag_minutes, roll_minutes))
    for decision in origins:
        if decision <= state["completed_decision"]:
            continue
        if state["origin_count"] >= max_origins or store.at_capacity or time.monotonic() >= deadline:
            break
        heartbeat.check()
        forecast = None
        try:
            window, cutoff = lagged_window(quotes, decision, lag_minutes)
            # A long data gap can consume the entire future portion. Do not
            # mislabel a fully expired curve as a forecast available to trade.
            if window[-1].timestamp + horizon * 60 <= decision:
                raise ValueError("FORECAST_EXPIRES_BEFORE_DECISION")
            forecast = forecaster(window, horizon)
            validate_forecast(forecast, window[-1].timestamp, horizon)
            annotate_forecast(forecast, contract, decision, cutoff, lag_minutes)
            forecast.update(mode="historical_replay_not_prospective_publication",
                            available_at=decision, actuals=matching_actuals(forecast, quotes))
        except (ValueError, RuntimeError) as error:
            # Failure metadata is non-secret and retained independently. No
            # random fallback, padding or zero-filled quantiles enter the corpus.
            forecast = None
            state["failed_origins"] += 1
            store.checkpoint("failed:" + digest([contract["contractId"], decision]),
                             {"contract_id": contract["contractId"], "decision_time": decision,
                              "status": "failed_origin", "error_type": type(error).__name__})
        state.update(completed_decision=decision, origin_count=state["origin_count"] + 1)
        # Forecast + its cursor committed atomically. Restart never refits a
        # completed origin, even if interruption follows immediately after save.
        store.save(contract["contractId"], state, forecast, [], contract)
    return {"forecasts": state["origin_count"] - state["failed_origins"],
            "failed_origins": state["failed_origins"], "observations_downloaded": len(quotes),
            "archive_cache_hit": cache_hit, "strategy": None,
            "complete": state["origin_count"] >= max_origins or all(o <= state["completed_decision"] for o in origins)}
