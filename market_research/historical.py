"""Resumable multi-provider replay orchestration; the shared engine does the math.

Checkpoints freeze discovery time/configuration/code and each completed origin.
No Firebase or order-placement calls. A continuation restores the complete SQLite
checkpoint, not the catalog-only archive sharding format.
"""
import argparse
from dataclasses import asdict
import json
import os
import time

from .engine import Strategy, VERSION, digest, summarize
from .local_store import LocalStore
from .provider import KalshiProvider, QuanturaProvider
from .worker import Heartbeat, paired_contracts, process_historical


def evaluate_metrics(forecasts, snapshots):
    """Score only timestamp-matched observed FUTURE asks, never fitted history."""
    from .engine import normalize_quotes
    by_contract = {}
    for snapshot in snapshots:
        contract = snapshot["contract"]
        # Stored raw two-sided prices can score either side without assuming
        # that NO ask equals 1-YES ask (the bid/ask spread is retained).
        for side in (("yes", "no") if contract.get("source") == "kalshi" else ("long", "short")):
            rows = [{**row, "selected_position": side} for row in snapshot["rows"]]
            by_contract[(contract["providerSymbol"], side)] = {q.timestamp: q.ask for q in normalize_quotes(rows, snapshot["end"])}
    errors, absolute, quantiles = [], [], {}
    for forecast in forecasts:
        context = forecast.get("market_context", {})
        actuals = by_contract.get((context.get("symbol"), context.get("side")), {})
        for row in forecast["rows"]:
            timestamp = row["timestamp"]
            if timestamp <= forecast["origin"] or timestamp not in actuals:
                continue
            actual = actuals[timestamp]
            error = row["quantiles"]["0.5"] - actual
            errors.append(error)
            absolute.append(abs(error))
            for level, prediction in row["quantiles"].items():
                q = float(level)
                residual = actual - prediction
                bucket = quantiles.setdefault(level, {"count": 0, "pinball_sum": 0, "covered": 0})
                bucket["count"] += 1
                bucket["pinball_sum"] += max(q * residual, (q - 1) * residual)
                bucket["covered"] += int(actual <= prediction)
    return {"scope": "timestamp_matched_out_of_sample_asks", "observations_scored": len(errors),
            "mae": sum(absolute) / len(errors) if errors else None,
            "rmse": (sum(e * e for e in errors) / len(errors)) ** .5 if errors else None,
            "bias": sum(errors) / len(errors) if errors else None,
            "quantiles": {q: {"count": b["count"], "pinball_loss": b["pinball_sum"] / b["count"], "empirical_coverage": b["covered"] / b["count"], "nominal": float(q)} for q, b in quantiles.items()}}


def run(args):
    from .forecast import default_research_models, forecast_window
    models = default_research_models()
    code = os.environ.get("QUANTURA_CODE_SHA", "local")
    config = {"version": VERSION, "provider": args.provider, "series": args.series, "horizon": args.horizon,
              "models": models, "max_contracts": args.max_contracts, "max_origins": args.max_origins,
              "strategy": asdict(Strategy()), "code": code}
    # JSON-normalized so tuple/list distinctions cannot break a valid resume.
    config = json.loads(json.dumps(config))
    run_id = os.environ.get("GITHUB_RUN_ID", str(time.time_ns())) + "-" + os.environ.get("GITHUB_RUN_ATTEMPT", "1")
    store = LocalStore("historical-v5", run_id)
    store.claim(config)
    checkpoint = store._get("checkpoints", "head") or {"as_of": int(time.time()), "cursor": "0", "queue": [], "index": 0, "attempted": 0, "pages": 0, "done": False, "failures": []}
    store.checkpoint("head", checkpoint)
    provider = KalshiProvider(args.series) if args.provider == "kalshi" else QuanturaProvider()
    heartbeat = Heartbeat(store)
    heartbeat.thread.start()
    started = time.monotonic()
    job_started = float(os.environ.get("QUANTURA_JOB_STARTED_AT", time.time()))
    deadline = min(started + args.duration_minutes * 60, started + max(0, job_started + 345 * 60 - time.time()))
    events = []
    try:
        while time.monotonic() < deadline - 180 and not store.at_capacity and not checkpoint["done"]:
            heartbeat.check()
            if checkpoint["index"] >= len(checkpoint["queue"]):
                if checkpoint["cursor"] is None or checkpoint["attempted"] >= args.max_contracts:
                    checkpoint["done"] = True
                    break
                contracts, coverage = provider.discover("historical", 1, checkpoint["cursor"])
                if coverage.get("next_cursor") is not None and coverage["next_cursor"] == checkpoint["cursor"]:
                    raise RuntimeError("REPEATED_DISCOVERY_CURSOR")
                contracts = [c for c in contracts if c["contractId"] not in checkpoint.get("seen_contracts", [])]
                checkpoint.update(queue=paired_contracts(contracts), index=0, cursor=coverage.get("next_cursor"), pages=checkpoint["pages"] + 1)
                store.checkpoint("head", checkpoint)
                if not checkpoint["queue"]:
                    continue
            contract = checkpoint["queue"][checkpoint["index"]]
            if checkpoint["attempted"] >= args.max_contracts:
                checkpoint["done"] = True
                break

            def forecaster(window, horizon):
                result = forecast_window(window, horizon, models)
                result["market_context"] = {"symbol": contract["providerSymbol"], "side": contract["side"], "contract_id": contract["contractId"]}
                result["forecast_id"] = digest([result["forecast_id"], result["market_context"]])
                return result

            try:
                result = process_historical(store, provider, contract, checkpoint["as_of"], args.horizon, Strategy(), forecaster, args.max_origins, events, heartbeat, deadline - 180)
            except (ValueError, RuntimeError) as error:
                checkpoint["failures"].append({"contract_id": contract["contractId"], "code": "HISTORY_UNAVAILABLE", "type": type(error).__name__})
                result = {"complete": True}
            if result["complete"]:
                checkpoint.setdefault("seen_contracts", []).append(contract["contractId"])
                checkpoint["index"] += 1
                checkpoint["attempted"] += 1
            store.checkpoint("head", checkpoint)
            print(json.dumps({"event": "historical_checkpoint", "provider": args.provider, "contracts_attempted": checkpoint["attempted"], "pages": checkpoint["pages"], "paper_only": True}), flush=True)
            if not result["complete"]:
                break
        if checkpoint["attempted"] >= args.max_contracts:
            checkpoint["done"] = True
        store.checkpoint("head", checkpoint)
        forecasts = store.values("forecasts")
        trades = store.values("trades")
        report = {"configuration": config, "statistics_scope": "entire_resumed_checkpoint", "registered_models": 5,
                  "forecast_count": len(forecasts), "checkpoint": checkpoint, "summary": summarize(trades),
                  "metrics": evaluate_metrics(forecasts, store.values("snapshots")),
                  "model_participation": {m: sum(any(r.get("id") == m and r.get("status") == "completed" for r in f.get("models", [])) for f in forecasts) for m in models},
                  "resume_required": not checkpoint["done"], "storage_limit_reached": store.at_capacity,
                  "limitations": ["Simulated quote fills, not executed orders. Independent target experiments must not be summed as a portfolio.", "Missing minutes are not fabricated. No crossing is inferred across a gap.", "Short/irregular history is disclosed; five registered models do not imply five successful models at every origin.", "Coverage is bounded by the configured contract/origin budget, not all historical sports."]}
        store.report(run_id, report)
        if output := os.environ.get("GITHUB_OUTPUT"):
            with open(output, "a") as handle:
                handle.write(f"resume_required={str(not checkpoint['done'] and not store.at_capacity).lower()}\n")
        if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
            with open(summary, "a") as handle:
                handle.write(f"## {args.provider} historical P10 research\n\nSimulated, out-of-sample; no orders. {checkpoint['attempted']} contract sides processed; {len(forecasts)} stored forecasts. Resume required: {not checkpoint['done']}.\n\nDetailed win rates, ML metrics, inputs and checkpoints are in the encrypted artifact. No Firestore backtest storage.\n")
        return report
    finally:
        heartbeat.close()
        store.release()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", choices=("kalshi", "polymarket_us"), required=True)
    parser.add_argument("--series", default="")
    parser.add_argument("--horizon", choices=(30, 60), type=int, default=30)
    parser.add_argument("--max-contracts", type=int, default=100)
    parser.add_argument("--max-origins", type=int, default=1000)
    parser.add_argument("--duration-minutes", type=int, default=345)
    args = parser.parse_args()
    if not 2 <= args.max_contracts <= 500 or args.max_contracts % 2 or not 1 <= args.max_origins <= 1000 or not 3 <= args.duration_minutes <= 345:
        parser.error("Bounded duration/origins and even contract count required")
    try:
        run(args)
    except Exception as error:
        print(json.dumps({"event": "backtest_failed", "type": type(error).__name__, "checkpoint_retained": True}), flush=True)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
