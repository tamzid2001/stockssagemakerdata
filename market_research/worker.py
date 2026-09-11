"""Durable PAPER research workers. No exchange account/order client is imported."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import os
import threading
import time

from .engine import (
    Strategy,
    advance,
    digest,
    history_window,
    initial_state,
    normalize_quotes,
    rolling_origins,
    summarize,
    validate_forecast,
)
from .provider import QuanturaProvider, historical_range
from .store import Store
from .local_store import LocalStore


class Heartbeat:
    def __init__(self, store):
        self.store = store
        self.stop = threading.Event()
        self.failed = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        while not self.stop.wait(25):
            try:
                self.store.renew()
            except Exception:
                self.failed.set()
                return

    def check(self):
        if self.failed.is_set():
            raise RuntimeError("LEASE_HEARTBEAT_FAILED")

    def close(self):
        self.stop.set()
        self.thread.join(timeout=30)


def paired_contracts(contracts):
    groups = {}
    for contract in contracts:
        groups.setdefault(contract["marketId"], []).append(contract)
    return [
        c
        for group in groups.values()
        if len(group) == 2 and {c["side"] for c in group} == {"long", "short"}
        for c in group
    ]


def replay_quotes(store, contract, state, quotes, forecast, strategy, events):
    fresh = [q for q in quotes if q.timestamp > state["last_timestamp"]]
    for index, quote in enumerate(fresh):
        changes = advance(state, quote, forecast or {"rows": []}, strategy)
        tagged = [
            {
                **e,
                "contract_id": contract["contractId"],
                "market_id": contract["marketId"],
            }
            for e in changes
        ]
        # Commit each transition's outcomes and its checkpoint atomically. A
        # successor can replay duplicate quotes, but cannot count them twice.
        if tagged or index == len(fresh) - 1:
            store.save(contract["contractId"], state, forecast, tagged, contract)
        events.extend(tagged)


def process_live(
    store, provider, contract, now, horizon, strategy, forecaster, events, heartbeat
):
    saved = store.load(contract["contractId"])
    state = saved.get("state") or initial_state()
    old_forecast = saved.get("forecast")
    # Recover enough history for pending/position exits after a handoff gap.
    start = max(now - 48 * 3600, min(now - 501 * 60, state["last_timestamp"] or now))
    quotes = normalize_quotes(provider.history(contract, start, now), now)
    gap = max(0, now - state["last_timestamp"] - 60) if state["last_timestamp"] else 0
    fresh = [q for q in quotes if q.timestamp > state["last_timestamp"]]
    if old_forecast:
        replay_quotes(store, contract, state, fresh, old_forecast, strategy, events)
    if not contract.get("live"):
        return {"forecast": False, "gap_seconds": gap, "status": "closed_followup"}
    origin = now // 60 * 60
    if old_forecast and origin - old_forecast["origin"] < horizon * 60:
        return {"forecast": False, "gap_seconds": gap, "status": "monitoring"}
    window = history_window(quotes, origin)
    new_forecast = forecaster(window, horizon)
    heartbeat.check()
    validate_forecast(new_forecast, origin, horizon)
    # ceil to the next minute: no historical quote preceding publication may
    # trigger a prospective signal, including time consumed by model inference.
    new_forecast["available_at"] = (int(time.time()) // 60 + 1) * 60
    new_forecast["mode"] = "prospective_paper"
    if not old_forecast:
        state["previous"] = asdict(window[-1])
        state["last_timestamp"] = origin
    store.save(contract["contractId"], state, new_forecast, [], contract)
    return {
        "forecast": True,
        "gap_seconds": gap,
        "history_count": sum(q.observed for q in window),
        "model_context_steps": len(window),
        "status": "forecast_persisted",
    }


def process_historical(
    store,
    provider,
    contract,
    now,
    horizon,
    strategy,
    forecaster,
    max_origins,
    events,
    heartbeat,
    deadline,
):
    start, end = historical_range(contract, now)
    raw = store.archived_history(contract, start, end)
    cache_hit = raw is not None
    if raw is None:
        raw = provider.history(contract, start, end)
        store.archive_history(contract, start, end, raw)
    quotes = normalize_quotes(raw, end)
    if not quotes:
        raise ValueError("MISSING_HISTORY")
    saved = store.load(contract["contractId"])
    state = saved.get("state") or initial_state()
    successful = failed = 0
    for origin in rolling_origins(quotes, horizon):
        if getattr(store, "at_capacity", False):
            break
        if (
            origin < state["last_timestamp"]
            or successful + failed >= max_origins
            or time.monotonic() >= deadline
        ):
            continue
        heartbeat.check()
        try:
            window = history_window(quotes, origin)
            forecast = forecaster(window, horizon)
            validate_forecast(forecast, origin, horizon)
        except (ValueError, RuntimeError):
            failed += 1
            replay_quotes(
                store,
                contract,
                state,
                [q for q in quotes if origin < q.timestamp <= origin + horizon * 60],
                None,
                strategy,
                events,
            )
            continue
        forecast["mode"] = "out_of_sample_historical_simulation"
        if not state["last_timestamp"]:
            state["previous"] = asdict(window[-1])
            state["last_timestamp"] = origin
        # Immutable forecast is saved before its future observations are scored.
        store.save(contract["contractId"], state, forecast, [], contract)
        replay_quotes(
            store,
            contract,
            state,
            [q for q in quotes if origin < q.timestamp <= origin + horizon * 60],
            forecast,
            strategy,
            events,
        )
        successful += 1
    return {
        "forecasts": successful,
        "failed_origins": failed,
        "observations": len(quotes),
        "archive_cache_hit": cache_hit,
        "open_positions": sum(bool(l["position"]) for l in state["levels"].values()),
        "unresolved_triggers": len(state.get("observations", [])),
    }


def run(args):
    from .forecast import forecast_window

    strategy = Strategy()
    configuration = {
        "version": 4,
        "mode": args.mode,
        "horizon": args.horizon,
        "models": args.models,
        "strategy": asdict(strategy),
    }
    run_id = (
        os.environ.get("GITHUB_RUN_ID", str(time.time_ns()))
        + "-"
        + os.environ.get("GITHUB_RUN_ATTEMPT", "1")
    )
    session = (
        ("polymarket-paper-" + digest(configuration)[:20])
        if args.mode == "live"
        else "polymarket-history-" + run_id
    )
    store = (
        Store(session, run_id) if args.mode == "live" else LocalStore(session, run_id)
    )
    store.claim(configuration)
    heartbeat = Heartbeat(store)
    heartbeat.thread.start()
    provider = QuanturaProvider()
    events = []
    failures = []
    cycles = []
    started = time.monotonic()
    deadline = started + args.duration_minutes * 60
    coverage = {
        "selected_contracts": 0,
        "successful": 0,
        "failed": 0,
        "coverage_pct": None,
    }
    # Leave time for checkpoint + dispatch before GitHub's six-hour hard limit.
    job_started = float(os.environ.get("QUANTURA_JOB_STARTED_AT", time.time()))
    deadline = min(deadline, started + max(0, job_started + 358 * 60 - time.time()))
    participation = {model: {"completed": 0, "failed": 0} for model in args.models}

    def forecaster(window, horizon):
        forecast = forecast_window(window, horizon, tuple(args.models))
        for model in forecast.get("models", []):
            participation[model["id"]][
                "completed" if model["status"] == "completed" else "failed"
            ] += 1
        return forecast

    try:
        while time.monotonic() < deadline:
            heartbeat.check()
            contracts, coverage = provider.discover(args.mode, args.max_pages)
            eligible = paired_contracts(contracts)
            selected = eligible[: args.max_contracts]
            # Keep unresolved simulated positions from games that stopped being live.
            current_ids = {c["contractId"] for c in contracts}
            if args.mode == "live":
                selected += [
                    {**c, "live": False}
                    for c in store.tracked()
                    if c["contractId"] not in current_ids
                ]
            coverage.update(
                {
                    "eligible_contracts": len(eligible),
                    "selected_contracts": min(len(eligible), args.max_contracts),
                    "bounded_selection": len(eligible) > args.max_contracts,
                    "successful": 0,
                    "failed": 0,
                    "forecasts_generated": 0,
                }
            )
            for contract in selected:
                heartbeat.check()
                if time.monotonic() >= deadline - 120 or getattr(
                    store, "at_capacity", False
                ):
                    coverage["stopped_at_storage_or_runtime_limit"] = True
                    break
                try:
                    if args.mode == "live":
                        result = process_live(
                            store,
                            provider,
                            contract,
                            int(time.time()) // 60 * 60,
                            args.horizon,
                            strategy,
                            forecaster,
                            events,
                            heartbeat,
                        )
                    else:
                        result = process_historical(
                            store,
                            provider,
                            contract,
                            int(time.time()),
                            args.horizon,
                            strategy,
                            forecaster,
                            args.max_origins,
                            events,
                            heartbeat,
                            deadline,
                        )
                    produced = result.get(
                        "forecasts", int(result.get("forecast", False))
                    )
                    coverage["forecasts_generated"] += produced
                    coverage[
                        "successful" if args.mode == "live" or produced else "failed"
                    ] += 1
                    # Per-contract provenance is private, not printed to public logs.
                    cycles.append({"contract_id": contract["contractId"], **result})
                except (ValueError, RuntimeError) as error:
                    heartbeat.check()
                    coverage["failed"] += 1
                    failures.append(
                        {
                            "contract_id": contract["contractId"],
                            "code": "HISTORY_OR_FORECAST_UNAVAILABLE",
                            "type": type(error).__name__,
                        }
                    )
            coverage["coverage_pct"] = (
                100 * coverage["successful"] / len(eligible) if eligible else None
            )
            print(
                json.dumps(
                    {
                        "event": "research_cycle",
                        "mode": args.mode,
                        "paper_only": True,
                        **coverage,
                    }
                ),
                flush=True,
            )
            if args.mode == "historical" or args.once:
                break
            heartbeat.stop.wait(min(60, max(0, deadline - time.monotonic())))
        heartbeat.check()
        summary_stats = summarize(events)
        for level in summary_stats["levels"].values():
            curve = level["equity_curve"]
            level["equity_curve"] = curve[:: max(1, len(curve) // 200 + 1)]
            level["equity_curve_downsampled"] = len(curve) > 200
        code_sha = os.environ.get(
            "QUANTURA_CODE_SHA", os.environ.get("GITHUB_SHA", "local")
        )
        report = {
            "configuration": configuration,
            "model_participation_in_returned_ensembles": participation,
            "registered_model_count": 5,
            "requested_model_count": len(args.models),
            "commit": code_sha,
            "coverage": coverage,
            "cycles": cycles[-1000:],
            "failures": failures[-1000:],
            "summary": summary_stats,
            "statistics_scope": "this_worker_run_only",
            "duration_seconds": time.monotonic() - started,
            "limitations": [
                "Paper quote simulation, not executed orders or verified fills.",
                "Display quotes lack order-book depth.",
                "Historical discovery and compute budgets bound coverage.",
                "Forecasts may be delayed; publication cutoff is enforced.",
                "GitHub scheduling cannot guarantee continuous uptime.",
            ],
        }
        store.report(run_id, report)
        heartbeat.close()
        store.release()
        if args.mode == "historical" and not coverage.get("forecasts_generated"):
            raise RuntimeError("NO_VALID_HISTORICAL_FORECASTS")
        # Successor dispatch is only enabled after durable completion and release.
        if output := os.environ.get("GITHUB_OUTPUT"):
            with open(output, "a", encoding="utf-8") as handle:
                handle.write("handoff_ready=true\n")
        print(
            json.dumps(
                {
                    "event": "research_completed",
                    "paper_only": True,
                    "run_id": run_id,
                    "storage": getattr(store, "storage_name", "private_firestore"),
                    "coverage": coverage,
                    "trade_events": len(events),
                }
            )
        )
        if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
            with open(summary, "a", encoding="utf-8") as handle:
                handle.write(
                    f"## Polymarket US {args.mode} paper research\n\nCommit: `{code_sha}`\n\n"
                    f"Selected contracts: {coverage['selected_contracts']}; successful: {coverage['successful']}; unavailable: {coverage['failed']}.\n\n"
                    f"Registered models: 5. Requested for this run: {', '.join(args.models)}. Actual participation counts: `{json.dumps(participation)}`. TimesFM is not included unless explicitly selected and commercially licensed.\n\n"
                    f"Detailed results storage: {getattr(store, 'storage_name', 'private_firestore')}. No exchange orders were submitted.\n"
                )
    finally:
        heartbeat.close()


def main():
    from .forecast import default_research_models
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("live", "historical"), required=True)
    parser.add_argument("--horizon", type=int, choices=(30, 60), default=30)
    parser.add_argument("--duration-minutes", type=int, default=345)
    parser.add_argument("--max-contracts", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=20)
    parser.add_argument("--max-origins", type=int, default=100)
    parser.add_argument(
        "--models",
        nargs="+",
        choices=("prophet", "toto", "granite", "chronos", "timesfm"),
        default=list(default_research_models()),
    )
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if (
        not 3 <= args.duration_minutes <= 345
        or not 2 <= args.max_contracts <= 500
        or args.max_contracts % 2
        or not 1 <= args.max_pages <= 100
        or not 1 <= args.max_origins <= 1000
    ):
        parser.error(
            "Bounded duration, even contract count, discovery pages and origin count required."
        )
    try:
        run(args)
    except Exception as error:
        print(
            json.dumps(
                {
                    "event": "research_failed",
                    "code": "WORKER_FAILED",
                    "type": type(error).__name__,
                    "paper_only": True,
                }
            ),
            flush=True,
        )
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
