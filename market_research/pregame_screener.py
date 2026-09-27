"""Today-only public game forecasts from genuine completed pregame hours.

No exchange credentials or order endpoints. One latest document per outcome;
expired documents are deleted in bounded batches on subsequent hourly runs.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import json
import math
import os
import signal
from pathlib import Path
import time
from zoneinfo import ZoneInfo
from .engine import digest, iso, stamp
from .provider import KalshiProvider, QuanturaProvider

NY = ZoneInfo("America/New_York")
COLLECTION = "game_forecast_catalog"
REPORT_FILENAME = "report-pregame-summary.json"
GAME_QUANTILES = (.01, .25, .5, .75, .9, .99)


def game_models(observations, timesfm_available=None):
    from ensemble_forecasting.capabilities import MODEL_REGISTRY, timesfm_availability
    available = timesfm_availability("production")[0] if timesfm_available is None else timesfm_available
    models = ["prophet", "granite", "chronos"]
    if available:
        models.append("timesfm")
    if observations >= MODEL_REGISTRY["models"]["toto"]["minimumObservedContext"]:
        models.append("toto")
    if len(models) < 4:
        raise ValueError("INSUFFICIENT_HISTORY_FOR_FOUR_AVAILABLE_MODELS")
    return tuple(models)


def game_date(seconds):
    return datetime.fromtimestamp(seconds, NY).date().isoformat()


def eligible(contract, now):
    try:
        start = stamp(contract["eventStart"])
    except (KeyError, TypeError, ValueError):
        return False
    return (contract.get("status") in {"open", "upcoming"}
            and not contract.get("live") and game_date(start) == game_date(now)
            and now < start // 3600 * 3600)


def through_deadline(rows, origin, price, end):
    """Interpolate only the final model point when kickoff is a partial hour."""
    result = [r for r in rows if origin < r["timestamp"] <= end]
    if result and result[-1]["timestamp"] == end:
        return result
    after = next((r for r in rows if r["timestamp"] > end), None)
    before = result[-1] if result else {"timestamp": origin, "quantiles": {q: price for q in after["quantiles"]}} if after else None
    if not after or not before or end <= origin:
        raise ValueError("FORECAST_DOES_NOT_REACH_GAME_END")
    weight = (end - before["timestamp"]) / (after["timestamp"] - before["timestamp"])
    result.append({"timestamp": end, "quantiles": {q: before["quantiles"][q] + weight * (v - before["quantiles"][q]) for q, v in after["quantiles"].items()}, "interpolated_model_point": True})
    return result


def document(contract, forecast, now, original=None):
    if not original and not eligible(contract, now):
        raise ValueError("GAME_START_HOUR_REACHED")
    start = stamp(contract["eventStart"])
    original_generated = stamp(original.get("original_generated_at") or original["generated_at"]) if original else None
    if original and (game_date(start) != original.get("game_date", game_date(now)) or original_generated >= start // 3600 * 3600 or forecast["origin"] > original_generated):
        raise ValueError("ORIGINAL_CUTOFF_NOT_PREGAME")
    end = start + 4 * 3600
    rows = through_deadline(forecast["rows"], forecast["origin"], forecast["input_snapshot"][-1]["target"], end)
    identity = digest({"source": contract["source"], "contract_id": contract["contractId"]})[:32]
    return {
        "id": identity, "provider": contract["source"], "contract_id": contract["contractId"],
        "schema_version": 2, "symbol": contract["providerSymbol"], "event_id": contract["eventId"],
        "event_slug": contract.get("eventSlug"), "side": contract.get("side"), "market_id": contract.get("marketId"),
        "event_title": contract["eventTitle"], "outcome": contract["outcome"],
        "market_title": contract.get("marketTitle"), "market_type": contract.get("marketType"),
        "sport": contract.get("sport"), "league": contract.get("league"),
        "home_team": contract.get("homeTeam"), "away_team": contract.get("awayTeam"),
        "game_date": game_date(start), "game_start": iso(start), "forecast_end": iso(end),
        "generated_at": iso(now), "input_cutoff": iso(forecast["origin"]),
        **({"original_generated_at": iso(original_generated), "recomputed_at": iso(now)} if original else {}),
        "schedule_source": contract.get("scheduleSource"), "schedule_verified_at": contract.get("scheduleVerifiedAt"),
        "history_start": iso(forecast["history_start"]), "history_count": forecast["history_count"],
        "history_gap_count": forecast["history_gap_count"], "frequency": "1h",
        "models": [m["id"] for m in forecast["models"] if m["status"] == "completed"],
        "effective_weights_by_quantile": forecast["weights"], "model_versions": forecast["model_versions"],
        "warnings": forecast["warnings"], "predictions": [{**r, "timestamp": iso(r["timestamp"])} for r in rows],
        "expires_at": datetime.fromtimestamp(end + 2 * 86400, timezone.utc),
        "method": "Ten days of genuine completed hourly pregame observations. Four models for 2–31 observations with approved TimesFM; five from 32 observations (or four without TimesFM). Equal weights renormalized separately for each supported quantile; Toto/TimesFM tails are never extrapolated. Logit probability ensemble; missing hours stay missing. Final partial-hour model point is interpolated.",
    }


def run(source, maximum, refresh_published=False, shard=0, shards=1, refresh_date=None):
    from .forecast import forecast_window
    from .store import Store
    provider = KalshiProvider() if source == "kalshi" else QuanturaProvider()
    db = Store("pregame-screener-readonly", "pregame").db
    # This collection contains only public game forecast outputs, never user data.
    old = db.collection(COLLECTION).where("game_date", "<", game_date(time.time() - 3 * 86400)).limit(500).stream()
    batch = db.batch()
    removed = 0
    for doc in old:
        batch.delete(doc.reference)
        removed += 1
    if removed:
        batch.commit()
    deadline = time.time() + 48 * 60
    target_date = refresh_date or game_date(time.time())
    originals = {}
    if refresh_published:
        for saved in db.collection(COLLECTION).where("game_date", "==", target_date).limit(1000).stream():
            row = saved.to_dict()
            if row.get("provider") == source:
                originals[row["contract_id"]] = row
    contracts, cursor, seen, coverage = {}, "0", set(), {}
    while (not refresh_date or refresh_date == game_date(time.time())) and cursor not in seen and time.time() < deadline:
        seen.add(cursor)
        rows, coverage = provider.discover("premarket", 20, cursor)
        contracts.update({c["contractId"]: c for c in rows})
        cursor = coverage.get("next_cursor")
        if not cursor:
            break
    selected = sorted((c for c in contracts.values() if eligible(c, time.time())), key=lambda c: (c["eventStart"], c["contractId"]))
    # Recalculate older saved snapshots using only their immutable pregame cutoff.
    for saved in originals.values():
        if saved["contract_id"] not in {c["contractId"] for c in selected}:
            selected.append({"source": source, "contractId": saved["contract_id"], "providerSymbol": saved["symbol"],
                             "eventId": saved["event_id"], "eventSlug": saved.get("event_slug"), "eventTitle": saved["event_title"],
                             "outcome": saved["outcome"], "side": saved.get("side") or (saved["contract_id"].split(":")[-1] if source == "kalshi" else "long"),
                             "eventStart": saved["game_start"], "status": "open"})
    if refresh_date and refresh_date != game_date(time.time()):cursor=None
    total_eligible=len(selected)
    # Refresh every existing published snapshot before acquiring new outcomes.
    selected.sort(key=lambda c:(c["contractId"] not in originals,c["eventStart"],c["contractId"]))
    selected = [c for c in selected[:maximum] if int(digest(c["contractId"])[:8],16) % shards == shard]
    report = {"provider": source, "game_date": target_date, "eligible": len(selected), "successful": 0,
              "skipped": 0, "failed": 0, "partial": bool(cursor) or total_eligible > maximum,
              "shard": shard, "shards": shards, "refresh_published":refresh_published,
              "discovery": coverage, "removed_expired": removed, "failures": []}
    try:
        for contract in selected:
            if time.time() >= deadline:
                report["partial"] = True
                break
            original = originals.get(contract["contractId"])
            if not original and not eligible(contract, time.time()):
                report["skipped"] += 1
                continue
            try:
                contract = provider.verify_schedule(contract)
                if game_date(stamp(contract["eventStart"])) != (target_date if original else game_date(time.time())):
                    if original:
                        db.collection(COLLECTION).document(original["id"]).delete()
                    report["skipped"] += 1
                    continue
                cutoff = int(stamp(original["input_cutoff"])) if original else int(time.time()) // 3600 * 3600
                if original and (stamp(original.get("original_generated_at") or original["generated_at"]) >= stamp(contract["eventStart"]) // 3600 * 3600):
                    db.collection(COLLECTION).document(original["id"]).delete()
                    raise ValueError("ORIGINAL_CUTOFF_NOT_PREGAME")
                quotes = provider.hourly_history(contract, cutoff - 10 * 86400, cutoff)
                if len(quotes) < 2:
                    raise ValueError("TWO_GENUINE_HOURLY_OBSERVATIONS_REQUIRED")
                if not original and not eligible(contract, time.time()):
                    report["skipped"] += 1
                    continue
                end = stamp(contract["eventStart"]) + 4 * 3600
                horizon = math.ceil((end - quotes[-1].timestamp) / 3600)
                if not 1 <= horizon <= 512:
                    raise ValueError("HISTORY_TOO_STALE")
                # GitHub's Linux runner interrupts inference at the start hour,
                # including a model that began before the boundary.
                seconds_left = deadline - time.time() if original else min(deadline - time.time(), stamp(contract["eventStart"]) // 3600 * 3600 - time.time())
                if seconds_left <= 0:
                    report["skipped"] += 1
                    continue
                def stop_at_start_hour(_signum, _frame):
                    raise TimeoutError("GAME_START_HOUR_REACHED")
                previous_handler = signal.signal(signal.SIGALRM, stop_at_start_hour)
                signal.setitimer(signal.ITIMER_REAL, seconds_left)
                try:
                    forecast = forecast_window(quotes, horizon, models=game_models(len(quotes)), quantiles=GAME_QUANTILES, frequency="1h", failure_policy="fail")
                finally:
                    signal.setitimer(signal.ITIMER_REAL, 0)
                    signal.signal(signal.SIGALRM, previous_handler)
                # Recheck schedule after inference, so a rescheduled kickoff never
                # publishes under the time used to acquire the input.
                verified = provider.verify_schedule(contract)
                if verified["eventStart"] != contract["eventStart"]:
                    if original:
                        db.collection(COLLECTION).document(original["id"]).delete()
                    raise ValueError("SCHEDULE_CHANGED_DURING_INFERENCE")
                doc = document(verified, forecast, int(time.time()), original)
                db.collection(COLLECTION).document(doc["id"]).set(doc)
                report["successful"] += 1
                print(json.dumps({"event": "pregame_published", "provider": source, "id": doc["id"], "hours": len(quotes), "models":doc["models"], "quantiles": list(doc["predictions"][0]["quantiles"]), "retrospective": bool(original)}), flush=True)
            except (ValueError, RuntimeError, TimeoutError) as error:
                if str(error) == "GAME_START_HOUR_REACHED" or (not original and not eligible(contract, time.time())):
                    report["skipped"] += 1
                    continue
                report["failed"] += 1
                report["failures"].append({"contract_id": contract["contractId"], "code": str(error) if str(error).isupper() else "HISTORY_OR_MODEL_UNAVAILABLE"})
                print(json.dumps({"event": "pregame_skipped", **report["failures"][-1]}), flush=True)
    finally:
        report["updated_at"] = iso(int(time.time()))
        db.collection("game_forecast_status").document(source if shards==1 else f"{source}-{shard}").set(report)
        output = Path(os.environ.get("QUANTURA_RESEARCH_DIR", "/tmp/quantura-pregame"))
        output.mkdir(parents=True, exist_ok=True)
        (output / REPORT_FILENAME).write_text(json.dumps(report))
        print(json.dumps({"event": "pregame_summary", **report}), flush=True)
        if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
            with open(summary, "a") as stream:
                stream.write(f"## {source}: today's pregame forecasts\n\n10 days of completed hourly observations. Forecast end: game start + 4 hours. No inference/publication from the start hour onward.\n\n`{json.dumps(report)}`\n")
    if report["eligible"] and not report["successful"] and report["failed"]:
        raise RuntimeError("NO_QUALIFYING_PREGAME_FORECASTS")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", required=True, choices=["kalshi", "polymarket_us"])
    parser.add_argument("--max-contracts", type=int, default=500)
    parser.add_argument("--refresh-published", action="store_true")
    parser.add_argument("--refresh-date", help="Recalculate saved snapshots for a recent date, with their original pregame cutoff")
    parser.add_argument("--shard", type=int, default=0)
    parser.add_argument("--shards", type=int, default=1)
    args = parser.parse_args()
    if not 1 <= args.max_contracts <= 500:
        parser.error("Maximum must be between 1 and 500")
    if not 1 <= args.shards <= 4 or not 0 <= args.shard < args.shards:
        parser.error("Choose a valid shard")
    if args.refresh_date:
        try:
            selected_date = datetime.strptime(args.refresh_date, "%Y-%m-%d").date()
            today = datetime.now(NY).date()
            if not args.refresh_published or not 0 <= (today - selected_date).days <= 3:
                raise ValueError()
        except ValueError:
            parser.error("Refresh date must be today or one of the last three days, with --refresh-published")
    run(args.provider, args.max_contracts, args.refresh_published, args.shard, args.shards, args.refresh_date)
