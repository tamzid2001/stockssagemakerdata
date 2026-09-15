"""Explicit opt-in actual CPU inference; never downloads weights in normal CI."""

import os
import json
import resource
import sys
import numpy as np
import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("QUANTURA_REAL_TOTO_SMOKE") != "1",
    reason="Explicit real-model smoke opt-in required",
)


@pytest.mark.parametrize("count", [32, 50, 500])
def test_real_toto_cpu_patch_alignment(count):
    import torch
    from ensemble_forecasting.adapters.toto import TotoAdapter
    from ensemble_forecasting.capabilities import MODEL_REGISTRY
    from ensemble_forecasting.preprocessing import prepare_series
    from ensemble_forecasting.schemas import ForecastRequest
    from market_research.engine import iso

    torch.set_num_threads(min(4, os.cpu_count() or 2))
    request = ForecastRequest.from_dict(
        {
            "models": {"toto": {"enabled": True, "weight": 1}},
            "prediction_length": 30,
            "quantiles": [0.1, 0.25, 0.5, 0.75, 0.9],
            "horizon_mode": "frequency_periods",
            "frequency": "1min",
            "calendar": "NONE",
            "context_length": 500,
            "transform": "none",
        }
    )
    rows = [
        {
            "timestamp": iso(1789000000 + i * 60),
            "target": float(np.sin(i / 10) * 0.05 + 0.5),
        }
        for i in range(count)
    ]
    series = prepare_series(rows, minimum_rows=2, frequency="1min", transform="none")
    result = TotoAdapter().forecast(series, (), request)
    result.validate()
    assert result.quantile_matrix.shape == (5, 30)
    assert np.isfinite(result.quantile_matrix).all()
    assert np.all(np.diff(result.quantile_matrix, axis=0) >= 0)
    assert result.checkpoint == MODEL_REGISTRY['models']['toto']['checkpoint']
    assert result.checkpoint_revision == MODEL_REGISTRY['models']['toto']['checkpointRevision']
    report = {'checkpoint':result.checkpoint,'revision':result.checkpoint_revision,
              'observations':count,'horizon':30,'device':result.device,
              'load_and_inference_seconds':result.duration_seconds,
              'process_peak_rss_mib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss /
                  (1024*1024 if sys.platform=='darwin' else 1024),
              'status':'passed','quality_benchmark':False}
    print(json.dumps(report),flush=True)
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'],'a') as summary:
            summary.write(f"\n- {result.checkpoint}, {count} observations → 30 steps: {result.duration_seconds:.2f}s including loading; process peak {report['process_peak_rss_mib']:.0f} MiB. Synthetic smoke only.\n")
