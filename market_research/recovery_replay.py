"""In-game-only replay on the existing provider, ensemble and artifact worker."""
from dataclasses import asdict
import math
import time

from .corpus import annotate_forecast
from .engine import digest, history_window, normalize_quotes, stamp, validate_forecast
from .forecast import forecast_window
from .p1_oco import QUANTILES
from .p1_worker import MODELS, game_groups
from .provider import historical_range
from .recovery_switch import VERSION


def process_game(store, provider, pair, as_of, horizon, max_origins, deadline, forecaster=forecast_window):
    game = pair[0].get("eventId", pair[0]["marketId"])
    starts = {stamp(s["eventStart"]) for s in pair if s.get("eventStart")}
    if len(starts) != 1 or any(not s.get("eventStart") for s in pair):
        raise ValueError("AUTHORITATIVE_GAME_START_REQUIRED")
    start = starts.pop()
    data = {}
    for side in pair:
        _, end = historical_range(side, as_of)
        if side.get("resolutionTime"):
            end = min(end, stamp(side["resolutionTime"]))
        raw = store.archived_history(side, start, end)
        if raw is None:
            raw = provider.history(side, start, end, history_phase="in_game")
            # Independently enforce the source boundary, before minute bucketing.
            raw = [r for r in raw if start <= stamp(r["timestamp"]) < end]
            store.archive_history(side, start, end, raw)
        data[side["contractId"]] = [q for q in normalize_quotes(raw, end) if q.observed and start < q.timestamp <= end]
    counts = {s: len(rows) for s, rows in data.items()}
    if any(n < 32 for n in counts.values()):
        return {"status": "insufficient_in_game_history", "complete": True, "observations": counts}
    common = sorted(set.intersection(*({q.timestamp for q in rows} for rows in data.values())))
    if not common:
        return {"status": "unaligned_in_game_history", "complete": True, "observations": counts}
    first = max(start + 32 * 60, max(rows[31].timestamp for rows in data.values()))
    first = (first + 59) // 60 * 60
    end = min(rows[-1].timestamp for rows in data.values())
    decisions = list(range(first, end, horizon * 60))
    # Quote tape is immutable and separate from training windows.
    with store.lock, store.db:
        for side, rows in data.items():
            for q in rows:
                store._put("observations", digest([game, side, q.timestamp]),
                           {"game_id": game, "contract_id": side, **asdict(q)}, True)
    attempted = 0
    for decision in decisions:
        key = "recovery-origin:" + digest([game, decision])
        previous = store._get("checkpoints", key)
        if previous:
            if previous["status"] == "inference_started":
                store.checkpoint(key, {**previous, "status": "failed", "code": "INTERRUPTED_BEFORE_ATOMIC_PUBLICATION"})
            continue
        if attempted >= max_origins or time.monotonic() >= deadline or store.at_capacity:
            break
        attempted += 1
        state = {"status": "inference_started", "game_id": game, "decision": decision}
        store.checkpoint(key, state)
        try:
            prior = [t for t in common if t <= decision]
            if not prior or decision - prior[-1] > 120:
                raise ValueError("STALE_ALIGNED_HISTORY")
            origin = prior[-1]
            windows = {side: history_window(rows, origin, minimum=32) for side, rows in data.items()}
            started = time.monotonic()
            forecasts = []
            for side in pair:
                window = windows[side["contractId"]]
                f = forecaster(window, horizon, MODELS, QUANTILES)
                if {m["id"] for m in f["models"] if m["status"] == "completed"} != set(MODELS):
                    raise ValueError("FIVE_MODEL_ENSEMBLE_REQUIRED")
                validate_forecast(f, origin, horizon)
                annotate_forecast(f, side, decision, origin, 0)
                f.update(strategy=VERSION, game_start=start, expected_side_count=len(pair),
                         history_phase="in_game", history_count=len(window),
                         input_snapshot=[asdict(q) for q in window])
                forecasts.append(f)
            latency = time.monotonic() - started
            available = decision + max(1, math.ceil(latency / 60)) * 60
            for f in forecasts:
                f.update(available_at=available, group_inference_seconds=latency)
            # Either every side and the publication marker commit, or none do.
            with store.lock, store.db:
                for f in forecasts:
                    store._put("forecasts", f["forecast_id"], f, True)
                store._put("checkpoints", key, {**state, "status": "published", "available_at": available,
                    "forecast_ids": [f["forecast_id"] for f in forecasts]})
        except (ValueError, RuntimeError) as error:
            code = str(error) if str(error).isupper() and len(str(error)) <= 100 else "MODEL_OR_DATA_UNAVAILABLE"
            store.checkpoint(key, {**state, "status": "failed", "code": code})
    records = [store._get("checkpoints", "recovery-origin:" + digest([game, d])) for d in decisions]
    complete = all(records)
    if complete:
        for side in pair:
            if not side.get("resolutionTime"):
                continue
            try:
                result = provider.resolution(side)
                result["settled_at"] = stamp(side["resolutionTime"])
                store.checkpoint("resolution:" + side["contractId"], result)
            except (ValueError, RuntimeError, OSError):
                pass  # Unknown settlement is an open position, not a loss.
    return {"status": "processed", "complete": complete, "observations": counts,
            "first_decision": first, "total_origins": len(decisions),
            "published_origins": sum(bool(r and r["status"] == "published") for r in records),
            "failed_origins": sum(bool(r and r["status"] == "failed") for r in records)}
