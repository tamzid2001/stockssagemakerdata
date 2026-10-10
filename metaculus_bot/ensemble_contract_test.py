"""Check the real worker contract in the separate production inference venv."""
from datetime import datetime, timedelta, timezone
import math

from .ensemble_job import run
from .time_series import QUANTILES


def main():
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    rows = [{"timestamp": (start + timedelta(days=i)).isoformat(), "target": 40 + math.sin(i / 20)} for i in range(500)]
    target = (start + timedelta(days=500 + 22)).date().isoformat()
    result = run({"rows": rows, "horizon": 23, "frequency": "1D", "calendar": "NONE", "target_date": target}, mock=True)
    assert len(result["models"]) >= 4
    assert set(result["provenance"]) == set(result["models"])
    assert all(result["provenance"].values())
    assert set(result["quantiles"]) == {str(q) for q in QUANTILES}
    assert all(math.isfinite(result["quantiles"][str(q)]) for q in QUANTILES)
    assert list(result["quantiles"][str(q)] for q in QUANTILES) == sorted(result["quantiles"][str(q)] for q in QUANTILES)
    print("Production worker contract passed: exact target, seven quantiles, model IDs and provenance")


if __name__ == "__main__":
    main()
