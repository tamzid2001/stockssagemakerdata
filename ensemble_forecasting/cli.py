from __future__ import annotations

import argparse
import json
import logging

from .capabilities import public_capabilities
from .worker import run_remote_job


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a claimed Quantura ensemble forecast job")
    parser.add_argument("--forecast-job-id")
    parser.add_argument("--mock", action="store_true")
    parser.add_argument("--capabilities", action="store_true")
    arguments = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    if arguments.capabilities:
        print(json.dumps(public_capabilities(), indent=2, sort_keys=True))
        return 0
    if not arguments.forecast_job_id:
        parser.error("--forecast-job-id is required unless --capabilities is used")
    result = run_remote_job(arguments.forecast_job_id, mock=arguments.mock)
    print(json.dumps({"forecast_id": arguments.forecast_job_id, "status": "completed", "result_hash": result["result_hash"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
