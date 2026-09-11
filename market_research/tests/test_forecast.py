from contextlib import nullcontext
from types import SimpleNamespace
import sys
import numpy as np
import pytest

from ensemble_forecasting.adapters.chronos import ChronosAdapter
from ensemble_forecasting.adapters.toto import aligned_toto_context
from ensemble_forecasting.adapters.base import ModelExecutionError
from ensemble_forecasting.preprocessing import prepare_series
from ensemble_forecasting.schemas import ForecastRequest
from market_research.engine import QUANTILES, Quote, iso, validate_forecast
from market_research import forecast


def test_research_defaults_include_fifth_model_only_with_both_production_gates(monkeypatch):
    monkeypatch.setenv("TIMESFM_HF_ACCESS_APPROVED", "true")
    monkeypatch.setenv("TIMESFM_COMMERCIAL_LICENSED", "false")
    assert len(forecast.default_research_models()) == 4
    monkeypatch.setenv("TIMESFM_COMMERCIAL_LICENSED", "true")
    assert forecast.default_research_models() == ("prophet", "toto", "granite", "chronos", "timesfm")
    monkeypatch.setenv("TIMESFM_HF_ACCESS_APPROVED", "false")
    assert len(forecast.default_research_models()) == 4


@pytest.mark.parametrize("count", [32, 40, 50, 500, 512])
def test_toto_patch_alignment_preserves_observations_and_masks_padding(count):
    values = np.arange(count, dtype=np.float32)
    padded, mask = aligned_toto_context(values, 32)
    assert len(padded) % 32 == 0
    assert int(mask.sum()) == count and mask[-32:].all()
    np.testing.assert_array_equal(padded[mask], values)
    assert not mask[: len(padded) - count].any()


@pytest.mark.parametrize("count", [2, 14, 31])
def test_toto_short_context_never_marks_padding_as_observed(count):
    with pytest.raises(ModelExecutionError) as error:
        aligned_toto_context(np.arange(count, dtype=np.float32), 32)
    assert error.value.code == "MODEL_CONTEXT_TOO_SHORT"


def test_equal_weight_ensemble_and_short_history(monkeypatch):
    seen = {}

    def execute(job, *, minimum_history_rows, progress):
        assert minimum_history_rows == 2
        seen.update(job)
        request = job["request"]
        assert {m["weight"] for m in request["models"].values()} == {1}
        return {
            "predictions": [
                {
                    "timestamp": iso(3000 + i * 60),
                    "quantiles": {str(q): float(q) for q in QUANTILES},
                }
                for i in range(1, 31)
            ],
            "models": [
                {"id": "prophet", "status": "completed"},
                {"id": "chronos", "status": "completed"},
            ],
            "effective_weights_by_quantile": {},
            "model_runs": [],
        }

    monkeypatch.setattr(forecast, "execute_job", execute)
    output = forecast.forecast_window(
        [Quote(i * 60, 0.5, 0.48) for i in range(1, 51)], 30
    )
    assert output["history_count"] == 50 and len(seen["input"]["rows"]) == 50
    assert output["input_snapshot"][-1]["timestamp"] == iso(3000)
    validate_forecast(output, 3000, 30)


def test_two_value_real_worker_path_with_mock_adapters(monkeypatch):
    from ensemble_forecasting.worker import execute_job

    monkeypatch.setattr(
        forecast, "execute_job", lambda job, **kw: execute_job(job, mock=True, **kw)
    )
    output = forecast.forecast_window(
        [Quote(60, 0.4, 0.38), Quote(120, 0.42, 0.40)], 30
    )
    assert output["history_count"] == 2
    assert output["imputed_context_steps"] == 0
    assert any("SHORT_HISTORY" in warning for warning in output["warnings"])
    validate_forecast(output, 120, 30)
    job = {
        "request": output["configuration"],
        "input": {"rows": output["input_snapshot"]},
    }
    with pytest.raises(ValueError, match="40"):
        execute_job(job, mock=True)


def test_chronos_partial_native_range_never_requests_clamped_tails(monkeypatch):
    requested = []

    class Pipeline:
        quantiles = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
        model_context_length = 512

        @classmethod
        def from_pretrained(cls, *args, **kwargs):
            return cls()

        def predict_quantiles(self, **kwargs):
            requested.extend(kwargs["quantile_levels"])
            assert isinstance(kwargs["inputs"], list) and kwargs["inputs"][0].ndim == 1
            return [np.tile(np.array(kwargs["quantile_levels"]), (3, 1))], None

    torch = SimpleNamespace(
        cuda=SimpleNamespace(is_available=lambda: False),
        tensor=lambda values, **kw: np.array(values),
        float32=np.float32,
        inference_mode=nullcontext,
        is_tensor=lambda v: False,
    )
    monkeypatch.setitem(sys.modules, "torch", torch)
    monkeypatch.setitem(
        sys.modules, "chronos", SimpleNamespace(Chronos2Pipeline=Pipeline)
    )
    source = prepare_series(
        [{"timestamp": iso(i * 60), "target": 0.5} for i in range(1, 51)],
        frequency="1min",
        transform="none",
    )
    request = ForecastRequest.from_dict(
        {
            "models": {"chronos": {"enabled": True, "weight": 1}},
            "prediction_length": 3,
            "quantiles": [0.01, 0.1, 0.25, 0.5, 0.99],
            "horizon_mode": "frequency_periods",
        }
    )
    output = ChronosAdapter().forecast(source, (), request)
    assert requested == [0.1, 0.25, 0.5]
    assert np.isnan(output.quantile_matrix[[0, 4]]).all()
    assert output.quantile_provenance["0.01"] == "unavailable"
    assert output.available_quantiles == (0.1, 0.25, 0.5)
