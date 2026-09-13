"""Resume existing fenced campaigns only; never reset positions or create one."""
import argparse
import os
import time

from .handoff import api
from .recovery_cloud import Campaign, validate_campaign

WORKFLOW = "polymarket-p90-ingame-backtest.yml"
VERSION = "in_game_p90_switch_v1"


def resume_inputs(record):
    c = record.get("configuration", {})
    if record.get("campaign_kind") != VERSION or not c.get("continuous") or record.get("status") == "completed":
        return None
    if record.get("lease", {}).get("expires", 0) > time.time():
        return None
    import re
    if c.get("horizon") not in (15, 30) or not re.fullmatch(r"[a-f0-9]{40}", c.get("code_sha", "")):
        raise ValueError("INVALID_STORED_CAMPAIGN_CONFIGURATION")
    provider = c.get('provider', 'polymarket_us')
    if provider not in ('polymarket_us', 'kalshi'):
        raise ValueError('INVALID_STORED_PROVIDER')
    return {"provider": provider, "horizon": str(c["horizon"]), "campaign_id": validate_campaign(record["id"]),
            "code_ref": c["code_sha"], "continuous": "true", "smoke_mode": "real"}


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--campaign-id")
    args = parser.parse_args()
    cloud = Campaign(args.campaign_id or "p90-" + "0" * 24, os.environ.get("GITHUB_RUN_ID", "watchdog"))
    if args.campaign_id:
        records = [{**cloud.load(), "id": args.campaign_id}]
    else:
        from google.cloud.firestore_v1.base_query import FieldFilter
        records = [{**r.to_dict(), "id": r.id} for r in cloud.lease.db.collection("market_research_sessions")
                   .where(filter=FieldFilter("campaign_kind", "==", VERSION)).stream()]
    runs = api(f"/actions/workflows/{WORKFLOW}/runs?per_page=100")["workflow_runs"]
    current = os.environ.get("GITHUB_RUN_ID")
    occupied = {(p,h) for p in ('polymarket_us','kalshi') for h in ("15", "30") if any(str(r["id"]) != current and r["status"] != "completed"
                and r.get('display_title','').startswith(p+' ') and f"· {h}m ·" in r.get("display_title", "") for r in runs)}
    for record in records:
        inputs = resume_inputs(record)
        if not inputs or (inputs['provider'],inputs["horizon"]) in occupied:
            continue
        api(f"/actions/workflows/{WORKFLOW}/dispatches", {"ref": "main", "inputs": inputs})
        occupied.add((inputs['provider'],inputs["horizon"]))
        print(f"Resumed {inputs['campaign_id']} on its immutable code and existing checkpoint.")


if __name__ == "__main__":
    main()
