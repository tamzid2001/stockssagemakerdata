"""Resumable private replay collection, independent of expensive inference."""

import argparse
import json
import os
import re
import time
from copy import copy

from .engine import normalize_quotes
from .provider import QuanturaProvider, KalshiProvider, historical_range
from .local_store import LocalStore
from .worker import Heartbeat, paired_contracts


def unique_games(contracts):
    if contracts and contracts[0].get("source") == "kalshi":
        # A game's team YES markets are independent instruments. NO is not
        # automatically the other team (draw/three-way markets exist).
        return list(
            {c["providerSymbol"]: c for c in contracts if c["side"] == "yes"}.values()
        )
    # Each moneyline's two sides share one immutable upstream quote history.
    return list(
        {
            c["providerSymbol"]: c
            for c in paired_contracts(contracts)
            if c["side"] == "long"
        }.values()
    )


def collect(args, store, provider, heartbeat):
    started = time.monotonic()
    deadline = started + args.duration_minutes * 60
    job_started = float(os.environ.get("QUANTURA_JOB_STARTED_AT", time.time()))
    deadline = min(deadline, started + max(0, job_started + 350 * 60 - time.time()))
    if args.catalog_id:
        contracts, metadata = store.load_catalog(args.catalog_id)
        catalog_id, as_of, discovery = (
            args.catalog_id,
            metadata["as_of"],
            metadata["coverage"],
        )
    else:
        as_of = int(time.time())
        contracts, discovery = provider.discover(
            "historical", args.max_pages, args.start_cursor
        )
        catalog_id = store.save_catalog(contracts, discovery, as_of)
    games = unique_games(contracts)
    if args.market_offset > len(games):
        raise ValueError("INVALID_MARKET_OFFSET")
    report = {
        "source": getattr(provider, "source", "polymarket_us"),
        "catalog_id": catalog_id,
        "as_of": as_of,
        "discovery": discovery,
        "games_discovered": len(games),
        "unique_events": len(
            {c.get("eventId", c.get("marketId", c["providerSymbol"])) for c in games}
        ),
        "market_count": len(games),
        "start_market_offset": args.market_offset,
        "attempted": 0,
        "downloaded": 0,
        "cache_hits": 0,
        "missing": 0,
        "failed": 0,
        "raw_rows": 0,
        "observed_minutes": 0,
        "at_least_two_observations": 0,
        "next_market_offset": args.market_offset,
        "storage": "encrypted_github_artifact",
        "paper_only": True,
    }
    # Publish an initial resume point even before the first history request.
    store.archive_progress(
        catalog_id, args.market_offset, {}, {"status": "catalog_ready"}, report
    )
    for index in range(args.market_offset, len(games)):
        if time.monotonic() >= deadline - 60 or getattr(store, "at_capacity", False):
            break
        heartbeat.check()
        contract = games[index]
        report["attempted"] += 1
        try:
            start, end = historical_range(contract, as_of)
            rows = store.archived_history(contract, start, end)
            if rows is None:
                rows = provider.history(contract, start, end)
                snapshot = store.archive_history(contract, start, end, rows)
                report["downloaded"] += 1
            else:
                report["cache_hits"] += 1
                snapshot = {"row_count": len(rows)}
            observations = (
                rows
                if getattr(provider, "source", "polymarket_us") == "kalshi"
                else normalize_quotes(rows, end)
            )
            report["raw_rows"] += len(rows)
            report["observed_minutes"] += len(observations)
            report["at_least_two_observations"] += int(len(observations) >= 2)
            report["missing"] += int(not rows)
            result = {
                "status": "downloaded" if rows else "missing_history",
                **snapshot,
                "observed_minutes": len(observations),
            }
        except (ValueError, RuntimeError) as error:
            report["failed"] += 1
            # Never expose arbitrary upstream payloads or credential-bearing errors.
            result = {
                "status": "failed",
                "code": "HISTORY_DOWNLOAD_FAILED",
                "error_type": type(error).__name__,
            }
        report["next_market_offset"] = index + 1
        # Immediate per-game checkpoint and immutable outcome for retry/audit.
        store.archive_progress(catalog_id, index, contract, result, report)
        if report["attempted"] % 25 == 0:
            print(
                json.dumps({"event": "replay_archive_progress", **report}), flush=True
            )
        time.sleep(0.1)
    report["catalog_complete"] = report["next_market_offset"] == len(games)
    report["all_discovered_ranges_available"] = (
        report["catalog_complete"]
        and not report["missing"]
        and not report["failed"]
        and args.market_offset == 0
    )
    report["coverage_pct_this_run"] = (
        (100 * report["at_least_two_observations"] / len(games)) if games else None
    )
    report["duration_seconds"] = time.monotonic() - started
    report["limitations"] = [
        "No fabricated observations. Empty upstream history is recorded, not a successful replay.",
        "Collection window is 501 pregame minutes plus resolution, or up to 36h after scheduled start; capped at 48h. Kalshi without a verified game start uses actual market open/settlement, bounded to 90 days.",
        "Kalshi counts independent market tickers; YES and NO are not assumed to be opposite teams. Use unique_events for game grouping.",
        "Discovery may require additional cursor batches; provider retention and metadata completeness are not guaranteed.",
        "At least two observations does not imply contiguous context or a successful ensemble forecast.",
        "Archive availability is not evidence of predictive performance or redistribution rights.",
    ]
    return report


def collect_batches(args, store, provider, heartbeat):
    """Discover, persist and download ONE page before advancing its cursor.

    Never hold thousands of uncheckpointed pages or lose earlier completed pages
    when the provider fails. Incomplete pages resume by catalog ID + market offset.
    """
    started = time.monotonic()
    batch = copy(args)
    batch.max_pages = 1
    totals = {
        key: 0
        for key in (
            "pages",
            "attempted",
            "downloaded",
            "cache_hits",
            "missing",
            "failed",
            "raw_rows",
            "observed_minutes",
            "at_least_two_observations",
        )
    }
    last = None
    while totals["pages"] < args.max_pages:
        remaining = args.duration_minutes * 60 - (time.monotonic() - started)
        if remaining < 180:
            break
        batch.duration_minutes = remaining / 60
        last = collect(batch, store, provider, heartbeat)
        totals["pages"] += 1
        for key in totals.keys() - {"pages"}:
            totals[key] += last[key]
        # Persist a global pointer after EVERY page; no raw records in GH logs.
        summary = {
            **totals,
            "source": getattr(provider, "source", "polymarket_us"),
            "last_page": last,
            "completed": last["catalog_complete"]
            and last["discovery"].get("next_cursor") is None,
        }
        store.report(f"{store.holder}-page-{totals['pages']}", summary)
        print(
            json.dumps(
                {
                    "event": "archive_page_completed",
                    **totals,
                    "catalog_id": last["catalog_id"],
                    "next_market_offset": last["next_market_offset"],
                    "next_cursor": last["discovery"].get("next_cursor"),
                }
            ),
            flush=True,
        )
        if not last["catalog_complete"] or summary["completed"]:
            return summary
        batch.catalog_id, batch.market_offset = "", 0
        batch.start_cursor = last["discovery"]["next_cursor"]
    if last is None:
        raise RuntimeError("ARCHIVE_DEADLINE_EXHAUSTED")
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-pages", type=int, default=100)
    parser.add_argument(
        "--source", choices=["polymarket_us", "kalshi"], default="polymarket_us"
    )
    parser.add_argument("--series-ticker", default="")
    parser.add_argument("--start-cursor", default="0")
    parser.add_argument("--catalog-id", default="")
    parser.add_argument("--market-offset", type=int, default=0)
    parser.add_argument("--duration-minutes", type=int, default=345)
    args = parser.parse_args()
    if (
        not 1 <= args.max_pages <= 1000
        or len(args.start_cursor) > 4096
        or (
            args.series_ticker
            and not re.fullmatch(r"[A-Z0-9._-]{1,120}", args.series_ticker)
        )
        or not 0 <= args.market_offset <= 200000
        or not 3 <= args.duration_minutes <= 345
        or (args.catalog_id and not re.fullmatch(r"[a-f0-9]{64}", args.catalog_id))
        or (args.market_offset and not args.catalog_id)
    ):
        parser.error(
            "Bounded arguments and a stored catalog ID are required for resumption."
        )
    run_id = (
        os.environ.get("GITHUB_RUN_ID", str(time.time_ns()))
        + "-archive-"
        + os.environ.get("GITHUB_RUN_ATTEMPT", "1")
    )
    prefix = "kalshi" if args.source == "kalshi" else "polymarket"
    store = LocalStore(prefix + "-replay-archive-v1", run_id)
    store.claim({"version": 1, "purpose": "private_replay_archive", "paper_only": True})
    heartbeat = Heartbeat(store)
    heartbeat.thread.start()
    try:
        provider = (
            KalshiProvider(args.series_ticker)
            if args.source == "kalshi"
            else QuanturaProvider()
        )
        report = collect_batches(args, store, provider, heartbeat)
        heartbeat.check()
        store.report(run_id, report)
        heartbeat.close()
        store.release()
        print(json.dumps({"event": "replay_archive_completed", **report}), flush=True)
        if path := os.environ.get("GITHUB_STEP_SUMMARY"):
            last = report["last_page"]
            with open(path, "a", encoding="utf-8") as handle:
                handle.write(
                    f"## Private {args.source} replay collection\n\nPages: {report['pages']}; markets attempted: {report['attempted']}; with ≥2 observations: {report['at_least_two_observations']}; empty: {report['missing']}; failed: {report['failed']}.\n\n"
                    f"Resume catalog `{last['catalog_id']}` at market offset `{last['next_market_offset']}`. Next discovery cursor: `{last['discovery'].get('next_cursor')}`.\n\n"
                    "Encrypted GitHub artifact; no Firestore writes, model execution or exchange orders. Coverage is not a backtest result.\n"
                )
    finally:
        heartbeat.close()


if __name__ == "__main__":
    main()
