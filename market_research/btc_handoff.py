"""Resume only the latest verified BTC paper lineage, never place real orders."""
import os
import re
from .handoff import api

WORKFLOW = "kalshi-btc-paper.yml"


def latest_checkpoint(artifacts):
    # Artifact IDs are identifiers, not a reliable creation-time ordering.
    from .engine import stamp
    return max(artifacts, key=lambda a: (stamp(a['created_at']), int(a['id'])))


def main():
    if os.environ.get("KALSHI_BTC_PAPER_ENABLED") != "true":
        print("BTC paper continuation disabled.")
        return
    current = os.environ.get("GITHUB_RUN_ID")
    runs = api(f"/actions/workflows/{WORKFLOW}/runs?per_page=100")["workflow_runs"]
    live = [r for r in runs if r.get("head_branch") == "main" and r.get("display_title", "").startswith("Kalshi BTC paper · live")]
    if any(str(r["id"]) != current and r["status"] != "completed" for r in live):
        print("BTC live paper worker already active or queued.")
        return
    checkpoint, code = os.environ.get("PAPER_CHECKPOINT"), os.environ.get("PAPER_CODE_REF")
    if not checkpoint:
        for run in live:
            artifacts = api(f"/actions/runs/{run['id']}/artifacts?per_page=100")["artifacts"]
            verified = [a for a in artifacts if not a["expired"] and re.match(r"btc-paper-checkpoint-[a-f0-9]{40}-", a["name"])]
            if verified:
                chosen = latest_checkpoint(verified)
                checkpoint = str(chosen["id"])
                code = re.match(r"btc-paper-checkpoint-([a-f0-9]{40})-", chosen["name"])[1]
                break
    if not checkpoint:
        print("No verified BTC checkpoint; start a new lineage explicitly. State will not be silently reset.")
        return
    if not str(checkpoint).isdigit() or not re.fullmatch(r"[a-f0-9]{40}", code or ""):
        raise ValueError("VERIFIED_BTC_CHECKPOINT_REQUIRED")
    api(f"/actions/workflows/{WORKFLOW}/dispatches", {"ref": "main", "inputs": {
        "mode": "live", "smoke_mode": "real", "duration_minutes": "345", "max_markets": "10",
        "continuous": "true", "resume_artifact_id": str(checkpoint), "code_ref": code}})
    print("Checkpoint-pinned BTC paper successor dispatched.")


if __name__ == "__main__":
    main()
