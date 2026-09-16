"""Completed-minute replay, preferring in-game history with disclosed fallback."""
from dataclasses import asdict
import math
import time

from .corpus import annotate_forecast
from .engine import Quote, digest, history_window, normalize_quotes, stamp, validate_forecast
from .forecast import forecast_window
from .p1_oco import QUANTILES
from .p1_worker import MODELS, game_groups
from .provider import historical_range
from .recovery_switch import VERSION


def process_game(store, provider, pair, as_of, horizon, max_origins, deadline, forecaster=forecast_window, *, first_row=False):
    if first_row and horizon != 30:
        raise ValueError('FIRST_ROW_REQUIRES_THIRTY_MINUTE_ORIGINS')
    game = pair[0].get("eventId", pair[0]["marketId"])
    starts = {stamp(s["eventStart"]) for s in pair if s.get("eventStart")}
    if len(starts) != 1 or any(not s.get("eventStart") for s in pair):
        if first_row:
            return {'status': 'missing_authoritative_game_start', 'complete': True, 'published_origins': 0}
        raise ValueError("AUTHORITATIVE_GAME_START_REQUIRED")
    start = starts.pop()
    data, history = {}, {}
    for side in pair:
        try:
            history_start, end = historical_range(side, as_of)
        except ValueError:
            if first_row:
                return {'status': 'invalid_history_range', 'complete': True, 'published_origins': 0}
            raise
        history_start = min(history_start, start - 500 * 60)
        if side.get("resolutionTime"):
            end = min(end, stamp(side["resolutionTime"]))
        if first_row:
            if history_start >= end:
                return {'status': 'invalid_history_range', 'complete': True, 'published_origins': 0}
            cache_key = 'first-row-source:' + side['contractId']
            if store._get('checkpoints', cache_key):
                history[side['contractId']] = [Quote(r['timestamp'], r['ask'], r['bid'], r['observed'])
                    for r in store.values('observations') if r['contract_id'] == side['contractId']]
            else:
                raw = provider.history(side, history_start, end, history_phase='both')
                raw = [r for r in raw if history_start <= stamp(r['timestamp']) < end]
                history[side['contractId']] = normalize_quotes(raw, end)
                with store.lock, store.db:
                    for q in history[side['contractId']]:
                        store._put('observations', digest([game, side['contractId'], q.timestamp]),
                                   dict(game_id=game, contract_id=side['contractId'], **asdict(q)), True)
                    store._put('checkpoints', cache_key, {'source_hash': digest(raw), 'start': history_start, 'end': end})
            history[side['contractId']].sort(key=lambda q: q.timestamp)
            data[side['contractId']] = [q for q in history[side['contractId']] if q.observed and start < q.timestamp <= end]
            continue
        raw = store.archived_history(side, history_start, end)
        if raw is None:
            raw = provider.history(side, history_start, end, history_phase="both")
            raw = [r for r in raw if history_start <= stamp(r["timestamp"]) < end]
            store.archive_history(side, history_start, end, raw)
        # Boundary filtering precedes minute aggregation: a pregame tick cannot
        # masquerade as an in-game observation merely by rounding its timestamp.
        history[side["contractId"]] = normalize_quotes(raw, end)
        in_game = [r for r in raw if stamp(r["timestamp"]) >= start]
        data[side["contractId"]] = [q for q in normalize_quotes(in_game, end) if q.observed and start < q.timestamp <= end]
    counts = {s: len(rows) for s, rows in data.items()}
    if not first_row and any(n == 0 for n in counts.values()):
        return {"status": "insufficient_in_game_history", "complete": True, "observations": counts}
    origin_data = history if first_row else data
    common = sorted(set.intersection(*({q.timestamp for q in rows} for rows in origin_data.values())))
    if not common:
        return {"status": "unaligned_in_game_history", "complete": True, "observations": counts}
    if first_row and len(common) < 32:
        return {'status': 'insufficient_observed_history', 'complete': True, 'observations': counts, 'published_origins': 0}
    first = common[31] if first_row else max(start + 32 * 60, max(rows[0].timestamp for rows in data.values()))
    first = (first + 59) // 60 * 60
    end = min(rows[-1].timestamp for rows in origin_data.values())
    decisions = list(range(first, end, horizon * 60))
    # Quote tape is immutable and separate from training windows.
    with store.lock, store.db:
        for side, rows in ({} if first_row else data).items():
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
            windows, phases = {}, {}
            for side, rows in data.items():
                past = [q for q in rows if q.timestamp <= origin]
                phases[side] = ("in_game" if origin >= start+32*60 else "pregame_and_in_game") if first_row else ("in_game" if len(past) >= 32 else "pregame_fallback")
                selected = rows if phases[side] == "in_game" else history[side]
                windows[side] = history_window(selected, origin, minimum=32)
            started = time.monotonic()
            forecasts = []
            for side in pair:
                window = windows[side["contractId"]]
                f = forecaster(window, horizon, MODELS, QUANTILES, **({'failure_policy':'fail'} if first_row else {}))
                if {m["id"] for m in f["models"] if m["status"] == "completed"} != set(MODELS):
                    raise ValueError("FIVE_MODEL_ENSEMBLE_REQUIRED")
                validate_forecast(f, origin, horizon)
                annotate_forecast(f, side, decision, origin, 0)
                f.update(strategy=VERSION, game_start=start, expected_side_count=len(pair),
                         history_phase=phases[side["contractId"]], history_count=len(window),
                         in_game_history_count=sum(q.timestamp > start for q in window),
                         quote_basis="completed_one_minute_bid_ask",
                         input_snapshot=[asdict(q) for q in window])
                if first_row:
                    from .first_row_strategy import VERSION as FIRST_ROW_VERSION
                    f.update(strategy=FIRST_ROW_VERSION, input_reference={'kind':'observations',
                        'contract_id':side['contractId'], 'start':window[0].timestamp,
                        'end':origin, 'count':len(window), 'sha256':digest([asdict(q) for q in window])})
                    f.pop('input_snapshot', None)
                forecasts.append(f)
            latency = time.monotonic() - started
            available = decision + max(1, math.ceil(latency)) if first_row else decision + max(1, math.ceil(latency / 60)) * 60
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
                result.setdefault("settled_at", stamp(side["resolutionTime"]))
                store.checkpoint("resolution:" + side["contractId"], result)
            except (ValueError, RuntimeError, OSError):
                pass  # Unknown settlement is an open position, not a loss.
    return {"status": "processed", "complete": complete, "observations": counts,
            "first_decision": first, "total_origins": len(decisions),
            "published_origins": sum(bool(r and r["status"] == "published") for r in records),
            "failed_origins": sum(bool(r and r["status"] == "failed") for r in records),
            "pregame_fallback_forecasts": sum(f.get("history_phase") == "pregame_fallback" for f in store.values("forecasts"))}
