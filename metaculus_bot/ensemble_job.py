"""Run the existing production Quantura adapters, outside the bot's SDK venv."""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path


def run(job: dict) -> dict:
    import numpy as np
    from ensemble_forecasting.worker import execute_job
    from market_research.forecast import default_research_models
    from .time_series import QUANTILES

    models = default_research_models()
    seed = int(hashlib.sha256(json.dumps(job["rows"], sort_keys=True).encode()).hexdigest()[:8], 16)
    np.random.seed(seed)
    request = {"prediction_length": job["horizon"], "frequency": job["frequency"],
               "horizon_mode": "frequency_periods", "calendar": job["calendar"], "context_length": 500,
               "transform": "none", "quantiles": QUANTILES, "failure_policy": "renormalize",
               "models": {m: {"enabled": True, "weight": 1} for m in models}}
    result = execute_job({"request": request, "input": {"rows": job["rows"], "frequency": job["frequency"]},
                          "runtime_mode": "production"})
    completed = [m["model"] for m in result["models"] if m["status"] == "completed"]
    if len(completed) < 2:
        raise RuntimeError("MULTIMODEL_ENSEMBLE_REQUIRED")
    matching = [r for r in result["predictions"] if r["timestamp"][:10] == job["target_date"]]
    if len(matching) != 1:
        raise ValueError("EXACT_RESOLUTION_TARGET_NOT_FORECASTED")
    return {"quantiles": matching[0]["quantiles"], "models": completed,
            "provenance": {m["model"]: m.get("quantile_provenance") for m in result["models"] if m["status"] == "completed"}}


if __name__ == "__main__":
    try:
        Path(sys.argv[2]).write_text(json.dumps(run(json.loads(Path(sys.argv[1]).read_text()))))
    except Exception as exc:
        # No full model traceback/request payload, headers, or credentials in logs.
        print(json.dumps({"event": "metaculus_ensemble_failed", "error_type": type(exc).__name__}))
        raise SystemExit(1)
