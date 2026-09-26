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


def document(contract, forecast, now):
    if not eligible(contract, now):
        raise ValueError("GAME_START_HOUR_REACHED")
    start = stamp(contract["eventStart"])
    end = start + 4 * 3600
    rows = through_deadline(forecast["rows"], forecast["origin"], forecast["input_snapshot"][-1]["target"], end)
    identity = digest({"source": contract["source"], "contract_id": contract["contractId"]})[:32]
    return {
        "id": identity, "provider": contract["source"], "contract_id": contract["contractId"],
        "symbol": contract["providerSymbol"], "event_id": contract["eventId"],
        "event_title": contract["eventTitle"], "outcome": contract["outcome"],
        "game_date": game_date(start), "game_start": iso(start), "forecast_end": iso(end),
        "generated_at": iso(now), "input_cutoff": iso(forecast["origin"]),
        "history_start": iso(forecast["history_start"]), "history_count": forecast["history_count"],
        "history_gap_count": forecast["history_gap_count"], "frequency": "1h",
        "models": [m["id"] for m in forecast["models"] if m["status"] == "completed"],
        "warnings": forecast["warnings"], "predictions": [{**r, "timestamp": iso(r["timestamp"])} for r in rows],
        "expires_at": datetime.fromtimestamp(end + 2 * 86400, timezone.utc),
        "method": "10-day genuine hourly pregame history; price probabilities; logit ensemble. Missing hours are left missing. Final partial-hour model point is interpolated.",
    }


def run(source, maximum):
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
    contracts, cursor, seen, coverage = {}, "0", set(), {}
    while cursor not in seen and time.time() < deadline:
        seen.add(cursor)
        rows, coverage = provider.discover("premarket", 20, cursor)
        contracts.update({c["contractId"]: c for c in rows})
        cursor = coverage.get("next_cursor")
        if not cursor:
            break
    selected = sorted((c for c in contracts.values() if eligible(c, time.time())), key=lambda c: (c["eventStart"], c["contractId"]))
    report = {"provider": source, "game_date": game_date(time.time()), "eligible": len(selected), "successful": 0,
              "skipped": 0, "failed": 0, "partial": bool(cursor) or len(selected) > maximum,
              "discovery": coverage, "removed_expired": removed, "failures": []}
    try:
        for contract in selected[:maximum]:
            if time.time() >= deadline:
                report["partial"] = True
                break
            if not eligible(contract, time.time()):
                report["skipped"] += 1
                continue
            try:
                cutoff = int(time.time()) // 3600 * 3600
                quotes = provider.hourly_history(contract, cutoff - 10 * 86400, cutoff)
                if len(quotes) < 2:
                    raise ValueError("TWO_GENUINE_HOURLY_OBSERVATIONS_REQUIRED")
                if not eligible(contract, time.time()):
                    report["skipped"] += 1
                    continue
                end = stamp(contract["eventStart"]) + 4 * 3600
                horizon = math.ceil((end - quotes[-1].timestamp) / 3600)
                if not 1 <= horizon <= 512:
                    raise ValueError("HISTORY_TOO_STALE")
                # A CPU-friendly ensemble makes hourly full-game scans practical.
                # GitHub's Linux runner interrupts inference at the start hour,
                # including a model that began before the boundary.
                seconds_left = stamp(contract["eventStart"]) // 3600 * 3600 - time.time()
                if seconds_left <= 0:
                    report["skipped"] += 1
                    continue
                def stop_at_start_hour(_signum, _frame):
                    raise TimeoutError("GAME_START_HOUR_REACHED")
                previous_handler = signal.signal(signal.SIGALRM, stop_at_start_hour)
                signal.setitimer(signal.ITIMER_REAL, seconds_left)
                try:
                    forecast = forecast_window(quotes, horizon, models=("prophet", "chronos"), frequency="1h", failure_policy="fail")
                finally:
                    signal.setitimer(signal.ITIMER_REAL, 0)
                    signal.signal(signal.SIGALRM, previous_handler)
                doc = document(contract, forecast, int(time.time()))
                db.collection(COLLECTION).document(doc["id"]).set(doc)
                report["successful"] += 1
                print(json.dumps({"event": "pregame_published", "provider": source, "id": doc["id"], "hours": len(quotes)}), flush=True)
            except (ValueError, RuntimeError, TimeoutError) as error:
                if str(error) == "GAME_START_HOUR_REACHED" or not eligible(contract, time.time()):
                    report["skipped"] += 1
                    continue
                report["failed"] += 1
                report["failures"].append({"contract_id": contract["contractId"], "code": str(error) if str(error).isupper() else "HISTORY_OR_MODEL_UNAVAILABLE"})
                print(json.dumps({"event": "pregame_skipped", **report["failures"][-1]}), flush=True)
    finally:
        report["updated_at"] = iso(int(time.time()))
        db.collection("game_forecast_status").document(source).set(report)
        output = Path(os.environ.get("QUANTURA_RESEARCH_DIR", "/tmp/quantura-pregame"))
        output.mkdir(parents=True, exist_ok=True)
        (output / "pregame-summary.json").write_text(json.dumps(report))
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
    args = parser.parse_args()
    if not 1 <= args.max_contracts <= 500:
        parser.error("Maximum must be between 1 and 500")
    run(args.provider, args.max_contracts)
