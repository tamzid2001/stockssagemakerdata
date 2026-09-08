import importlib.util
from pathlib import Path


spec = importlib.util.spec_from_file_location(
    "verify_telemetry", Path(__file__).resolve().parents[2] / "scripts/verify_gitlab_telemetry.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_queries_are_bounded_and_service_scoped():
    for signal in ("logs", "traces", "metrics"):
        payload = module.query_payload(signal, "quantura-api", 2000000)
        assert payload["end"] - payload["start"] == 1800000
        query = payload["compositeQuery"]["queries"][0]["spec"]
        assert query["signal"] == signal
        assert query["filter"]["expression"] == "service.name = 'quantura-api'"
        assert "selectFields" not in query


def test_verification_does_not_mistake_timestamps_for_observations():
    assert list(module.numeric_values({"timestamp": 1000000, "value": 0})) == [0]
    assert list(module.numeric_values({"series": [{"value": 5}]})) == [5]
