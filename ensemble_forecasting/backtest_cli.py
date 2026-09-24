"""Authenticated, claim-once Quantura quantile backtest worker entrypoint."""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import time
from typing import Any

import httpx

from .adapters.base import ModelExecutionError
from .quantile_backtest import run_quantile_replay

LOGGER = logging.getLogger("quantura.quantile_backtest")


class BacktestWorkerApi:
    def __init__(self, job_id: str):
        if not re.fullmatch(r"bt_[a-f0-9]{32}", job_id):
            raise ValueError("BACKTEST_JOB_ID_INVALID")
        token = os.environ.get("QUANTURA_ENSEMBLE_WORKER_TOKEN", "")
        if len(token) < 32:
            raise RuntimeError("BACKTEST_WORKER_AUTH_MISSING")
        self.job_id = job_id
        self.client = httpx.Client(
            base_url=os.environ.get("QUANTURA_WORKER_API_BASE", "https://quantura.studio").rstrip("/"),
            headers={"Authorization": f"Bearer {token}", "User-Agent": "Quantura-Quantile-Backtest-Worker/1.0"},
            timeout=60.0,
        )

    def claim(self) -> dict[str, Any]:
        response = self.client.post(f"/api/internal/backtests/{self.job_id}/claim")
        response.raise_for_status()
        return response.json()["data"]

    def callback(self, action: str, payload: dict[str, Any]) -> None:
        for attempt in range(4):
            try:
                response = self.client.post(f"/api/internal/backtests/{self.job_id}/{action}", json=payload)
                response.raise_for_status()
                return
            except (httpx.TransportError, httpx.HTTPStatusError) as exc:
                transient = isinstance(exc, httpx.TransportError) or exc.response.status_code in {408, 429, 500, 502, 503, 504}
                if not transient or attempt == 3:
                    raise
                LOGGER.warning("backtest callback retry: backtest_id=%s action=%s attempt=%s", self.job_id, action, attempt + 1)
                time.sleep(2 ** attempt)


def run_remote_backtest(job_id: str, *, mock: bool = False) -> dict[str, Any]:
    api = BacktestWorkerApi(job_id)
    job = api.claim()
    try:
        if mock:
            from .worker import execute_job
            result = run_quantile_replay(job, progress=lambda row: api.callback("progress", dict(row)),
                                         forecast_fn=lambda candidate: execute_job(candidate, mock=True))
        else:
            result = run_quantile_replay(job, progress=lambda row: api.callback("progress", dict(row)))
        api.callback("complete", result)
        return {"backtest_id": job_id, "status": "completed", "metrics": result["metrics"],
                "result_hash": result["result_hash"]}
    except Exception as exc:
        code = "BACKTEST_MODEL_FAILED" if isinstance(exc, ModelExecutionError) else "BACKTEST_WORKER_FAILED"
        try:
            api.callback("fail", {"code": code, "retryable": isinstance(exc, ModelExecutionError) and bool(exc.retryable)})
        except Exception:
            LOGGER.exception("backtest failure callback unavailable", extra={"backtest_id": job_id})
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a claimed quantile backtest job")
    parser.add_argument("--backtest-job-id", required=True)
    parser.add_argument("--mock", action="store_true", help="Test environment only; never dispatch a mock public result")
    args = parser.parse_args()
    if args.mock and os.environ.get("QUANTURA_WORKER_API_BASE", "https://quantura.studio").rstrip("/") == "https://quantura.studio":
        raise SystemExit("Mock inference cannot write production backtest results")
    print(json.dumps(run_remote_backtest(args.backtest_job_id, mock=args.mock), sort_keys=True))


if __name__ == "__main__":
    main()
