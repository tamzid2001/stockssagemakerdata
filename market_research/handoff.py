"""Serialize paper-worker handoffs; never dispatch a financial execution bot."""

import json
import os
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
    # Include waiting/queued runs; the Firestore fence is the final race guard.
    for status in ("in_progress", "queued", "waiting", "pending", "requested"):
        result = api(f"/actions/workflows/{WORKFLOW}/runs?status={status}&per_page=100")
        if any(str(run["id"]) != current and (
            run.get("display_title", "").startswith(provider + " paper") or
            (provider == "polymarket_us" and run.get("display_title", "").startswith("Polymarket US"))
        ) for run in result["workflow_runs"]):
            print("A paper-worker run is already active or queued.")
            return
    horizon = os.environ.get("PAPER_HORIZON", "30")
    lag = os.environ.get("PAPER_LAG_MINUTES", "0")
    roll = os.environ.get("PAPER_ROLL_MINUTES", "30")
    only = os.environ.get("PAPER_FORECAST_ONLY", "false")
    if horizon not in {"30", "45", "60"} or lag not in {"0", "15", "30"} or int(lag) >= int(horizon) or roll not in {"15", "30", "45", "60"} or only not in {"true", "false"}:
        raise ValueError("invalid_horizon")
    api(
        f"/actions/workflows/{WORKFLOW}/dispatches",
        {
            "ref": "main",
            "inputs": {"provider": provider, "horizon": horizon, "lag_minutes": lag,
                       "roll_minutes": roll, "forecast_only": only, "run_once": "false", "smoke_mode": "real"},
        },
    )
    print("Successor paper worker dispatched from current main.")


if __name__ == "__main__":
    main()
