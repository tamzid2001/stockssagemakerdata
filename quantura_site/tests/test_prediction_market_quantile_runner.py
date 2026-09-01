import importlib.util
import sys
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "prediction_market_quantile_runner.py"
SPEC = importlib.util.spec_from_file_location("prediction_market_quantile_runner", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def test_quantiles_are_bounded_and_ordered():
    values = [0.41, 0.42, 0.40, 0.43, 0.45, 0.44, 0.47, 0.48, 0.46, 0.50]
    result = MODULE.forecast_quantiles(values, 12)
    ordered = [result[name] for name in MODULE.QUANTILE_NAMES]
    assert ordered == sorted(ordered)
    assert all(0 <= value <= 1 for value in ordered)


def test_direction_labels_do_not_change_mathematics():
    falling = [0.80, 0.78, 0.74, 0.70, 0.67, 0.63, 0.60, 0.56, 0.52, 0.49]
    result = MODULE.forecast_quantiles(falling, 6)
    assert result["p99"] >= result["p75"] >= result["p50"]
    assert result["p50"] < falling[-1]


def test_invalid_or_short_history_is_rejected():
    try:
        MODULE.forecast_quantiles([0.5, 0.6, None, "bad"], 1)
    except MODULE.RunnerError as error:
        assert "8 valid" in str(error)
    else:
        raise AssertionError("Short history must not produce invented quantiles")
