from contextlib import nullcontext
from types import SimpleNamespace
import sys
import numpy as np

from ensemble_forecasting.adapters.chronos import ChronosAdapter
from ensemble_forecasting.preprocessing import prepare_series
from ensemble_forecasting.schemas import ForecastRequest
from market_research.engine import QUANTILES, Quote, iso, validate_forecast
from market_research import forecast


def test_equal_weight_ensemble_and_short_history(monkeypatch):
    seen = {}

    def execute(job):
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
