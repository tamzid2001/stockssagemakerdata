"""Resumable, game-sharded historical campaign; small Firestore metadata only."""
import argparse
import json
import os
from pathlib import Path
import re
import tempfile
import threading
import time

from .artifact import key_bytes, restore, snapshot_package
from .engine import digest, stamp
from .local_store import LocalStore
from .p1_worker import MODELS, game_groups
from .provider import QuanturaProvider, KalshiProvider
from .recovery_cloud import Campaign, MAX_GAME_ARCHIVE, decode_catalog, encode_catalog, validate_campaign
from .recovery_replay import process_game
from .recovery_switch import VERSION, from_store
from .worker import Heartbeat


def totals(records):
    summary = {"games_archived": len(records), "forecast_count": sum(r["forecast_count"] for r in records),
               "games_with_forecasts": sum(r["forecast_count"] > 0 for r in records)}
    rows = [next(iter(r["summary"].values())) for r in records]
    for key in ("closed_trades", "wins", "losses", "breakeven", "gross_pnl", "net_pnl", "fees",
                "closed_entry_notional", "open_positions", "open_mark_net_pnl", "open_entry_notional",
                "recovery_signals", "multiplier_increases"):
        summary[key] = sum(r[key] for r in rows)
    for key in ("longest_winning_streak", "longest_losing_streak", "max_quantity_used"):
        summary["max_within_game_" + key] = max((r[key] for r in rows), default=0)
    summary["win_rate"] = summary["wins"] / summary["closed_trades"] if summary["closed_trades"] else None
    cost = summary["closed_entry_notional"]
    summary["net_return_on_closed_entry_notional"] = summary["net_pnl"] / cost if cost else None
    return summary


class Checkpoints:
    def __init__(self, cloud, local, game, heartbeat):
        self.cloud, self.local, self.game, self.heartbeat = cloud, local, game, heartbeat
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.last_error = None

    def publish(self):
        self.heartbeat.check()
        with tempfile.TemporaryDirectory(prefix="quantura-game-checkpoint-") as directory:
            target = Path(directory) / "game.enc"
            snapshot_package(self.local.directory, target, max_bytes=MAX_GAME_ARCHIVE)
            pointer = self.cloud.upload(target, "checkpoint")
            self.cloud.update({"active_game": self.game, "active_checkpoint": pointer, "checkpoint_at": time.time()})
            print(json.dumps({"event": "durable_game_checkpoint", "game_id": self.game, "bytes": pointer["bytes"], "sha256": pointer["sha256"]}), flush=True)
        self.last_error = None

    def run(self):
        while not self.stop.wait(300):
            try:
                self.publish()
            except Exception as error:
                self.last_error = type(error).__name__
                print(json.dumps({"event": "durable_checkpoint_error", "error_type": self.last_error}), flush=True)

    def close(self):
        self.stop.set(); self.thread.join(timeout=180)
        if self.thread.is_alive():
            raise RuntimeError("CHECKPOINT_THREAD_DID_NOT_STOP")


def run(args):
    key_bytes()
    sha = os.environ["QUANTURA_CODE_SHA"]
    if not re.fullmatch(r"[a-f0-9]{40}", sha):
        raise ValueError("PINNED_CODE_REQUIRED")
    first_row = getattr(args, 'strategy', 'legacy') == 'first_row'
    version = VERSION
    reporter = from_store
    group_games = game_groups
    if first_row:
        from .first_row_strategy import VERSION as version, from_store as reporter
        from .first_row_strategy import game_groups as group_games
        if args.horizon != 30:
            raise ValueError('FIRST_ROW_REQUIRES_THIRTY_MINUTE_ORIGINS')
    config = {"version": version, "provider": args.provider, "horizon": args.horizon, "roll_minutes": args.horizon,
              "history_phase": "in_game_with_point_in_time_pregame_fallback", "minimum_elapsed_minutes": 32, "minimum_history": 32,
              "quote_basis": "completed_one_minute_bid_ask",
              "maximum_history": 500, "models": list(MODELS), "base_shares": 1,
              "loss_multiplier": 2.5, "max_shares": 100, "fee_rate_assumption": .01,
              "reset_policy": "cumulative_game_net_recovery",
              "code_sha": sha, "paper_only": True, "continuous": args.continuous}
    if first_row:
        config.update(strategy='first_row', history_phase='pregame_and_in_game_then_in_game_after_32_elapsed_minutes',
                      storage_policy='one_minute_tape_per_game_with_hashed_input_references',
                      signal_policy='first_post_publication_minute_ask_vs_frozen_first_row',
                      execution_policy='next_minute_book_hold_or_signal_switch', fee_status='assumption_not_verified')
    identifier = ("p90-" + digest([config, os.environ.get("GITHUB_RUN_ID", time.time_ns())])[:24]
                  if args.campaign_id == "new" else validate_campaign(args.campaign_id))
    cloud = Campaign(identifier, os.environ.get("GITHUB_RUN_ID", "local"))
    cloud.lease.claim(config)
    heartbeat = Heartbeat(cloud.lease); heartbeat.thread.start()
    state = cloud.load()
    state.setdefault("as_of", int(time.time())); state.setdefault("page_cursor", "0"); state.setdefault("page_index", 0)
    cloud.update({"campaign_kind": version, "status": "running", "as_of": state["as_of"],
                  "page_cursor": state["page_cursor"], "page_index": state["page_index"], "current_run_id": os.environ.get("GITHUB_RUN_ID")})
    provider = KalshiProvider() if args.provider == 'kalshi' else QuanturaProvider()
    deadline = time.monotonic() + args.duration_minutes * 60
    complete = False
    try:
        while time.monotonic() < deadline:
            heartbeat.check()
            state = cloud.load()
            if state.get("catalog"):
                with tempfile.TemporaryDirectory(prefix="quantura-catalog-") as directory:
                    file = Path(directory) / "catalog.enc"
                    cloud.download(state["catalog"], file)
                    catalog = decode_catalog(file)
            else:
                contracts, coverage = provider.discover("historical", 1 if args.provider == 'kalshi' else 10, str(state["page_cursor"]))
                # Campaign time is frozen. Newly finished games are not added retrospectively.
                contracts = [c for c in contracts if c.get("eventStart") and stamp(c["eventStart"]) < state["as_of"]]
                catalog = {"contracts": contracts, "coverage": coverage}
                with tempfile.TemporaryDirectory(prefix="quantura-catalog-") as directory:
                    file = Path(directory) / "catalog.enc"; encode_catalog(catalog, file)
                    pointer = cloud.upload(file, "catalog")
                cloud.update({"catalog": pointer})
            games = list(group_games(catalog["contracts"]).items())
            index = int(state["page_index"])
            while index < len(games) and time.monotonic() < deadline:
                game, pair = games[index]
                heartbeat.check()
                if cloud.game(game):
                    index += 1; cloud.update({"page_index": index}); continue
                with tempfile.TemporaryDirectory(prefix="quantura-active-game-") as directory:
                    root = Path(directory) / "store"
                    state = cloud.load()
                    if state.get("active_game") == game and state.get("active_checkpoint"):
                        file = Path(directory) / "restore.enc"
                        cloud.download(state["active_checkpoint"], file)
                        restore(file, root, preserve_results=True, max_bytes=MAX_GAME_ARCHIVE,
                                max_database_bytes=2 * MAX_GAME_ARCHIVE)
                    local = LocalStore(identifier + "-" + digest(game)[:16], cloud.lease.holder, root,
                                       capacity_bytes=MAX_GAME_ARCHIVE)
                    local.claim(config)
                    local.checkpoint("recovery_coverage", {"as_of": state["as_of"], "game_id": game})
                    checkpoint = Checkpoints(cloud, local, game, heartbeat)
                    checkpoint.thread.start()
                    try:
                        result = process_game(local, provider, pair, state["as_of"], args.horizon, 10000, deadline, first_row=first_row)
                        checkpoint.close()
                        if not result["complete"]:
                            checkpoint.publish()
                            if local.at_capacity:
                                raise RuntimeError("SINGLE_GAME_EXCEEDS_ONE_GIB_SAFETY_LIMIT")
                            break
                        local.checkpoint("recovery_coverage", {"as_of": state["as_of"], "game_results": {game: result}, "complete": True})
                        report = reporter(local)
                        if first_row:
                            local.checkpoint('first_row_report', report)
                        file = Path(directory) / "completed.enc"
                        snapshot_package(root, file, max_bytes=MAX_GAME_ARCHIVE)
                        pointer = cloud.upload(file, "game")
                        record = {"game_id": game, "archive": pointer, "result": result,
                                  "forecast_count": report["forecast_count"], "summary": report["summary"],
                                  "code_sha": sha, "completed_at": time.time()}
                        cloud.finish_game(game, record, {"page_index": index + 1, "active_game": None,
                                                       "active_checkpoint": None, "last_game_id": game})
                        print(json.dumps({"event": "research_game_archived", "campaign_id": identifier, **record}), flush=True)
                    except (ValueError, RuntimeError) as error:
                        checkpoint.close()
                        checkpoint.publish()
                        cloud.update({"status": "retry_required", "error_type": type(error).__name__})
                        raise
                    finally:
                        checkpoint.close(); local.release(); local.db.close()
                index += 1
            if index < len(games):
                break
            cursor = catalog["coverage"].get("next_cursor")
            if cursor is None:
                complete = True; break
            if str(cursor) == str(state["page_cursor"]):
                raise RuntimeError("REPEATED_CATALOG_CURSOR")
            cloud.update({"page_cursor": str(cursor), "page_index": 0, "catalog": None})
        records = [r.to_dict() for r in cloud.lease.ref.collection("games").stream()]
        summary = totals(records)
        cloud.update({"status": "completed" if complete else "paused", "summary": summary,
                      "resume_required": not complete, "updated_at": time.time()})
        print(json.dumps({"event": "research_campaign_report", "campaign_id": identifier,
                          "horizon": args.horizon, "complete": complete, "summary": summary}), flush=True)
        return identifier, not complete
    finally:
        heartbeat.close(); cloud.lease.release()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--horizon", type=int, choices=[15, 30], required=True)
    parser.add_argument("--provider", choices=['polymarket_us', 'kalshi'], default='polymarket_us')
    parser.add_argument("--campaign-id", default="new")
    parser.add_argument("--duration-minutes", type=int, default=330)
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument('--strategy', choices=['legacy', 'first_row'], default='legacy')
    args = parser.parse_args()
    if not 1 <= args.duration_minutes <= 330:
        parser.error("duration must be 1–330 minutes")
    identifier, needed = run(args)
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as out:
            out.write(f"campaign_id={identifier}\nresume_required={str(needed).lower()}\n")


if __name__ == "__main__":
    main()
