"""Serialize paper-worker handoffs; never dispatch a financial execution bot."""

import json
import os
import re
import urllib.request

WORKFLOW = "polymarket-live-paper.yml"
REPOSITORY = "tamzid2001/stockssagemakerdata"


def api(path, body=None):
    request = urllib.request.Request(
        "https://api.github.com/repos/" + REPOSITORY + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers={
            "Authorization": "Bearer " + os.environ["GH_TOKEN"],
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response) if response.status != 204 else None


def main():
    provider = os.environ.get("PAPER_PROVIDER", "polymarket_us")
    if provider not in {"polymarket_us", "kalshi"}:
        raise ValueError("invalid_provider")
    enabled = "KALSHI_PAPER_ENABLED" if provider == "kalshi" else "POLYMARKET_PAPER_ENABLED"
    if os.environ.get(enabled) != "true":
        print("Paper worker disabled; no handoff dispatched.")
        return
    current = os.environ.get("GITHUB_RUN_ID")
    strategy = os.environ.get("PAPER_STRATEGY", "rolling")
    horizon = os.environ.get("PAPER_HORIZON", "30")
    def matching(run):
        title=run.get("display_title", "")
        return title.startswith(provider + " paper") and (strategy!='p1_oco' or ('p1_oco' in title and f'horizon {horizon}m' in title))
    # Include waiting/queued runs; the Firestore fence is the final race guard.
    for status in ("in_progress", "queued", "waiting", "pending", "requested"):
        result = api(f"/actions/workflows/{WORKFLOW}/runs?status={status}&per_page=100")
        if any(str(run["id"]) != current and matching(run) for run in result["workflow_runs"]):
            print("A paper-worker run is already active or queued.")
            return
    horizon = os.environ.get("PAPER_HORIZON", "30")
    lag = os.environ.get("PAPER_LAG_MINUTES", "0")
    roll = os.environ.get("PAPER_ROLL_MINUTES", "30")
    only = os.environ.get("PAPER_FORECAST_ONLY", "false")
    strategy = os.environ.get("PAPER_STRATEGY", "rolling")
    if strategy not in {"rolling","p1_oco"} or (strategy=="p1_oco" and (provider!="polymarket_us" or horizon not in {"5","15","30"} or lag!="0")):
        raise ValueError("invalid_paper_strategy")
    if horizon not in ({"5","15","30"} if strategy=="p1_oco" else {"30","45","60"}) or lag not in {"0", "15", "30"} or int(lag) >= int(horizon) or roll not in {"15", "30", "45", "60"} or only not in {"true", "false"}:
        raise ValueError("invalid_horizon")
    continuation = {}
    if strategy == "p1_oco":
        checkpoint, code = os.environ.get("PAPER_CHECKPOINT"), os.environ.get("PAPER_CODE_REF")
        if not checkpoint:
            # Watchdog resumes the newest private P1 lineage instead of silently
            # resetting open positions after a failed runner. Inspect our own
            # workflow only; no arbitrary artifact URLs or credentials.
            runs = api(f"/actions/workflows/{WORKFLOW}/runs?per_page=30")["workflow_runs"]
            for run in runs:
                if not matching(run) or run.get('head_branch') != 'main':
                    continue
                artifacts = api(f"/actions/runs/{run['id']}/artifacts?per_page=100")["artifacts"]
                matches = [a for a in artifacts if a['name'].startswith('p1-paper-checkpoint-') and not a['expired']]
                if matches:
                    artifact = max(matches,key=lambda a:a['id'])
                    checkpoint = str(artifact['id'])
                    match = re.match(r'p1-paper-checkpoint-([a-f0-9]{40})-',artifact['name'])
                    code = match[1] if match else None
                    break
            if not checkpoint:
                print('P1 requires a verified checkpoint. Start and validate a new lineage manually; watchdog will not reset it.')
                return
        if not str(checkpoint).isdigit() or not re.fullmatch(r'[a-f0-9]{40}',code or ''):
            raise ValueError('VERIFIED_P1_CHECKPOINT_AND_CODE_REQUIRED')
        continuation = {"resume_artifact_id":str(checkpoint),"code_ref":code}
    api(
        f"/actions/workflows/{WORKFLOW}/dispatches",
        {
            "ref": "main",
            "inputs": {"provider": provider, "horizon": horizon, "lag_minutes": lag,
                       "roll_minutes": roll, "forecast_only": only, "strategy": strategy, "run_once": "false", "smoke_mode": "real", **continuation},
        },
    )
    print("Successor paper worker dispatched from current main.")


if __name__ == "__main__":
    main()
