"""Checkpointed comparison sidecar: no inference, upstream calls or orders.

Reads existing private sports game shards and downloaded live/BTC checkpoints.
Each input hash gets an immutable encrypted report. Mutable small pointers never
replace a source forecast, strategy book or historical report.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
import time
import zipfile

from .artifact import package, restore, MAX_ARTIFACT_BYTES
from .engine import digest
from .exit_comparison import VERSION, export, from_store
from .local_store import LocalStore
from .recovery_cloud import Campaign, MAX_GAME_ARCHIVE
from .worker import Heartbeat


def compare_archive(source, metadata, cloud, record_id, maximum=MAX_GAME_ARCHIVE):
    with open(source, "rb") as data:
        checksum = hashlib.file_digest(data, "sha256").hexdigest()
    ref = cloud.lease.ref.collection("comparisons").document(record_id)
    existing = ref.get().to_dict() or {}
    if existing.get("source_sha256") == checksum:
        return False
    with tempfile.TemporaryDirectory(prefix="quantura-exit-analysis-") as temporary:
        root = Path(temporary)
        restore(source, root / "input", preserve_results=True, max_bytes=maximum, max_database_bytes=2 * maximum)
        store = LocalStore("exit-observer", "read-only-source-copy", root / "input", capacity_bytes=2 * maximum)
        try:
            result = from_store(store, as_of=metadata["as_of"])
        finally:
            store.db.close()
        result.update(source=metadata, source_sha256=checksum, analysis_code_sha=os.environ["QUANTURA_CODE_SHA"])
        output = root / "report"; output.mkdir(mode=0o700)
        export(result, output)
        encrypted = root / "report.enc"
        package(output, encrypted, max_bytes=MAX_GAME_ARCHIVE)
        pointer = cloud.upload(encrypted, "game")
        record = {"version": VERSION, "source": metadata, "source_sha256": checksum, "report": pointer,
                  "summary": result["summary"], "game_count": result["game_count"],
                  "signal_target_study": result["signal_target_study"]["summary"],
                  "forecast_count": result["forecast_count"], "updated_at": time.time()}
        if result.get('btc_minute_policy'):
            record['btc_minute_policy'] = {name: report['summary']
                for name, report in result['btc_minute_policy']['scenarios'].items()}
            record['btc_limit_proxy'] = result['btc_minute_policy']['post_only_limit_proxy']['summary']
        if result.get('btc_hold_tracking'):
            holding = result['btc_hold_tracking']
            record['btc_hold_tracking'] = {key: holding[key] for key in ('version', 'history_minutes', 'horizon_minutes', 'coverage', 'live_readiness')}
            record['btc_hold_tracking']['scenarios'] = {execution: {policy: scenario['summary']
                for policy, scenario in policies.items()} for execution, policies in holding['scenarios'].items()}
        if len(json.dumps(record)) > 900000:
            raise ValueError("COMPARISON_METADATA_TOO_LARGE")
        @cloud.lease.fs.transactional
        def publish(tx):
            cloud.lease.check(tx)
            tx.set(ref, record)
        cloud.lease.transact(publish)
        print(json.dumps({"event": "exit_comparison_saved", "source": metadata,
                          "comparison_id": cloud.id, "record_id": record_id,
                          "game_count": result["game_count"], "forecast_count": result["forecast_count"],
                          "variants": len(result["summary"]), "archive_bytes": pointer["bytes"],
                          "baseline": result["summary"]["p90_cross_p10_opposite"]}), flush=True)
        return True


def extract_checkpoint(archive, output):
    with zipfile.ZipFile(archive) as z:
        names = z.namelist()
        if names != ["research.qra.enc"]:
            raise ValueError("UNAPPROVED_CHECKPOINT_ZIP_CONTENTS")
        info = z.getinfo(names[0])
        if not 32 < info.file_size <= MAX_ARTIFACT_BYTES:
            raise ValueError("CHECKPOINT_TOO_LARGE")
        with output.open("xb") as dest:
            os.chmod(output, 0o600)
            dest.write(z.read(info))


def run(checkpoint_dir, max_games=25):
    from google.cloud.firestore_v1.base_query import FieldFilter
    sha = os.environ["QUANTURA_CODE_SHA"]
    if not re.fullmatch(r"[a-f0-9]{40}", sha):
        raise ValueError("PINNED_ANALYSIS_CODE_REQUIRED")
    cloud = Campaign("p90-" + digest([VERSION, sha])[:24], os.environ.get("GITHUB_RUN_ID", "observer"))
    cloud.lease.claim({"version": VERSION, "analysis_code_sha": sha, "paper_only": True})
    heartbeat = Heartbeat(cloud.lease); heartbeat.thread.start()
    done = 0
    try:
        # Analyze latest live snapshots first; sports history must not starve it.
        for path in sorted(checkpoint_dir.glob("*/source.json")):
            heartbeat.check()
            metadata = json.loads(path.read_text())
            if metadata.get("kind") not in ("btc_live", "btc_historical", "polymarket_live") or not str(metadata.get("artifact_id", "")).isdigit():
                raise ValueError("INVALID_CHECKPOINT_SOURCE")
            with tempfile.TemporaryDirectory(prefix="quantura-exit-source-") as temporary:
                source = Path(temporary) / "research.enc"
                extract_checkpoint(path.parent / "checkpoint.zip", source)
                # Latest snapshot for this run replaces the pointer only. Prior
                # reports remain immutable; overlapping snapshots are NOT summed.
                compare_archive(source, metadata, cloud, digest([metadata["kind"], metadata["run_id"]]), MAX_ARTIFACT_BYTES)
        sessions = cloud.lease.db.collection("market_research_sessions").where(
            filter=FieldFilter("campaign_kind", "==", "in_game_p90_switch_v1"))
        # Bounded batch per campaign avoids starving other providers/horizons.
        for snapshot in sessions.stream():
            state = snapshot.to_dict()
            source_cloud = Campaign(snapshot.id, "read-only-source")
            count = 0
            for game in snapshot.reference.collection("games").stream():
                heartbeat.check()
                if count >= max_games:
                    break
                record = game.to_dict()
                key = digest([snapshot.id, game.id])
                prior = cloud.lease.ref.collection("comparisons").document(key).get().to_dict() or {}
                if prior.get("source_sha256") == record["archive"]["sha256"]:
                    continue
                c = state["configuration"]
                metadata = {"kind": "sports_historical", "campaign_id": snapshot.id, "game_id": record["game_id"],
                            "provider": c["provider"], "horizon": c["horizon"], "as_of": state["as_of"],
                            "source_code_sha": c["code_sha"]}
                with tempfile.TemporaryDirectory(prefix="quantura-exit-game-") as temporary:
                    source = Path(temporary) / "game.enc"
                    source_cloud.download(record["archive"], source)
                    done += compare_archive(source, metadata, cloud, key)
                count += 1
        cloud.update({"comparison_kind": VERSION, "updated_at": time.time(), "last_run_id": os.environ.get("GITHUB_RUN_ID")})
        print(json.dumps({"event": "exit_observer_complete", "comparison_id": cloud.id, "new_sports_reports": done,
                          "scope": "available_completed_game_shards_and_latest_verified_paper_checkpoints_not_full_coverage"}))
    finally:
        heartbeat.close(); cloud.lease.release()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--checkpoint-directory", type=Path, required=True)
    args = parser.parse_args()
    run(args.checkpoint_directory)
