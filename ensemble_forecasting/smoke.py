from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import numpy as np

from .adapters.toto import TotoAdapter
from .preprocessing import prepare_series
from .schemas import APPROVED_MODELS, ForecastRequest


def run_toto_smoke() -> dict[str, object]:
    """Run the gated real-model smoke without any customer data."""
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = [
        {"timestamp": (start + timedelta(days=index)).isoformat(), "target": 100.0 + index * 0.1}
        for index in range(96)
    ]
    prepared = prepare_series(rows, transform="log", frequency="1D")
    payload = {
        "prediction_length": 2,
        "horizon_mode": "frequency_periods",
        "frequency": "1D",
        "quantiles": [0.1, 0.25, 0.5, 0.75, 0.9],
        "transform": "log",
        "context_length": 64,
        "runtime_mode": "production",
        "failure_policy": "fail",
        "models": {
            model_id: {"enabled": model_id == "toto", "weight": 1.0 if model_id == "toto" else 0.0}
            for model_id in APPROVED_MODELS
        },
    }
    request = ForecastRequest.from_dict(payload)
    timestamps = ("2026-04-07T00:00:00Z", "2026-04-08T00:00:00Z")
    result = TotoAdapter().forecast(prepared, timestamps, request)
    result.validate()
    if result.quantile_matrix.shape != (5, 2) or not np.isfinite(result.quantile_matrix).all():
        raise RuntimeError("Toto real-model smoke returned an invalid quantile matrix")
    if np.any(np.diff(result.quantile_matrix, axis=0) < 0):
        raise RuntimeError("Toto real-model smoke returned crossed quantiles")
    return {
        "model": result.model_id,
        "checkpoint": result.checkpoint,
        "device": result.device,
        "shape": list(result.quantile_matrix.shape),
        "quantiles": list(result.available_quantiles),
        "status": "passed",
    }


def main() -> int:
    print(json.dumps(run_toto_smoke(), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
