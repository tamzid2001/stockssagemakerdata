#!/usr/bin/env python3
"""Generate bounded prediction-market quantile forecasts from Quantura data.

The runner deliberately reuses Quantura's provider adapters and normalized export
schema. Forecasts are produced in log-odds space from point-in-time observations,
then transformed back to probability space. The five outputs are distribution
quantiles, not guarantees or trade recommendations.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable


QUANTILES = (0.01, 0.25, 0.50, 0.75, 0.99)
QUANTILE_NAMES = ("p1", "p25", "p50", "p75", "p99")
METHOD_VERSION = "quantura_logit_random_walk_v1"
EPSILON = 1e-6


class RunnerError(RuntimeError):
    """A safe, user-facing runner failure."""


@dataclass(frozen=True)
class QuantileForecast:
    provider: str
    event_id: str
    market_id: str
    contract_id: str
    item_id: str
    event: str
    market: str
    outcome: str
    forecast_as_of: str
    target_timestamp: str
    horizon_steps: int
    frequency: str
    observations: int
    p1: float
    p25: float
    p50: float
    p75: float
    p99: float
    model: str
    quantile_methodology: str
    validation_status: str


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def logit(value: float) -> float:
    bounded = min(1.0 - EPSILON, max(EPSILON, value))
    return math.log(bounded / (1.0 - bounded))


def logistic(value: float) -> float:
    if value >= 0:
        exp_value = math.exp(-value)
        return 1.0 / (1.0 + exp_value)
    exp_value = math.exp(value)
    return exp_value / (1.0 + exp_value)


def _finite_probabilities(values: Iterable[Any]) -> list[float]:
    result: list[float] = []
    for raw in values:
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        if math.isfinite(value) and 0.0 <= value <= 1.0:
            result.append(value)
    return result


def forecast_quantiles(values: Iterable[Any], horizon_steps: int) -> dict[str, float]:
    """Forecast final bounded quantiles using a robust logit random walk.

    Historical decimal probabilities are transformed to log odds. The median
    one-step log-odds change estimates drift, while 1.4826 * MAD estimates step
    volatility robustly. The horizon distribution is Normal with drift scaled by
    H and volatility by sqrt(H). Normal quantiles are inverse-logit transformed.
    """

    probabilities = _finite_probabilities(values)
    if len(probabilities) < 8:
        raise RunnerError("At least 8 valid normalized observations are required.")
    horizon = max(1, min(10_000, int(horizon_steps)))
    odds = [logit(value) for value in probabilities]
    changes = [current - previous for previous, current in zip(odds, odds[1:])]
    drift = statistics.median(changes)
    center = statistics.median(changes)
    mad = statistics.median(abs(value - center) for value in changes)
    sigma = max(1e-6, 1.4826 * mad)
    location = odds[-1] + drift * horizon
    scale = sigma * math.sqrt(horizon)
    normal = statistics.NormalDist()
    output = {
        name: round(logistic(location + normal.inv_cdf(quantile) * scale), 8)
        for name, quantile in zip(QUANTILE_NAMES, QUANTILES)
    }
    ordered = [output[name] for name in QUANTILE_NAMES]
    if not all(0.0 <= value <= 1.0 for value in ordered):
        raise RunnerError("Forecast probability escaped the valid 0–1 range.")
    if ordered != sorted(ordered):
        raise RunnerError("Forecast quantile ordering validation failed.")
    return output


class QuanturaApi:
    def __init__(self, base_url: str, timeout: int = 45) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=body,
            method=method,
            headers={"Accept": "application/json", "Content-Type": "application/json", "User-Agent": "quantura-prediction-forecast-runner/1"},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            try:
                detail = json.loads(error.read().decode("utf-8")).get("message", "Provider request failed.")
            except Exception:
                detail = "Provider request failed."
            raise RunnerError(f"Quantura API returned HTTP {error.code}: {detail}") from error
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
            raise RunnerError("Quantura API is unavailable or returned an invalid response.") from error

    def categories(self, provider: str) -> list[dict[str, Any]]:
        query = urllib.parse.urlencode({"source": provider})
        response = self.request("GET", f"/api/sports/prediction-markets/categories?{query}")
        return list(response.get("categories") or [])

    def markets(self, provider: str, category: str, limit: int) -> list[dict[str, Any]]:
        query = urllib.parse.urlencode({"source": provider, "category": category, "status": "any", "page": 1, "pageSize": min(limit, 100)})
        response = self.request("GET", f"/api/sports/prediction-markets/markets?{query}")
        return list(response.get("items") or [])

    def history(self, provider: str, contract: dict[str, Any], start: datetime, end: datetime, frequency: str) -> list[dict[str, Any]]:
        response = self.request("POST", "/api/sports/prediction-markets/export", {
            "source": provider,
            "contracts": [contract],
            "start": start.isoformat(),
            "end": end.isoformat(),
            "frequency": frequency,
            "mode": "normalized",
            "target": "price",
            "missing": "leave",
            "pregameOnly": False,
            "format": "json",
        })
        return list(response.get("rows") or [])


def frequency_delta(frequency: str) -> timedelta:
    return {"1m": timedelta(minutes=1), "5m": timedelta(minutes=5), "15m": timedelta(minutes=15), "30m": timedelta(minutes=30), "1h": timedelta(hours=1), "1d": timedelta(days=1)}[frequency]


def safe_id(value: Any) -> str:
    return str(value or "").strip()[:300]


def build_forecast(provider: str, contract: dict[str, Any], rows: list[dict[str, Any]], horizon_steps: int, frequency: str) -> QuantileForecast:
    clean = sorted(
        (row for row in rows if row.get("item_id") and row.get("timestamp")),
        key=lambda row: (str(row["item_id"]), parse_iso(str(row["timestamp"]))),
    )
    if not clean:
        raise RunnerError("No normalized observations were returned.")
    quantiles = forecast_quantiles((row.get("price") for row in clean), horizon_steps)
    forecast_as_of = parse_iso(str(clean[-1]["timestamp"]))
    target_timestamp = forecast_as_of + frequency_delta(frequency) * horizon_steps
    return QuantileForecast(
        provider=provider,
        event_id=safe_id(clean[-1].get("event_id") or contract.get("eventId")),
        market_id=safe_id(clean[-1].get("market_id") or contract.get("marketId")),
        contract_id=safe_id(clean[-1].get("contract_id") or contract.get("contractId")),
        item_id=safe_id(clean[-1].get("item_id")),
        event=safe_id(contract.get("eventTitle")),
        market=safe_id(contract.get("marketTitle")),
        outcome=safe_id(contract.get("outcome")),
        forecast_as_of=forecast_as_of.isoformat(),
        target_timestamp=target_timestamp.isoformat(),
        horizon_steps=horizon_steps,
        frequency=frequency,
        observations=len(clean),
        **quantiles,
        model=METHOD_VERSION,
        quantile_methodology="robust Normal distribution of log-odds changes; inverse-logit bounded output",
        validation_status="valid",
    )


def write_outputs(output_dir: Path, forecasts: list[QuantileForecast], manifest: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = output_dir / "forecasts.jsonl"
    jsonl_path.write_text("".join(json.dumps(asdict(row), sort_keys=True) + "\n" for row in forecasts), encoding="utf-8")
    csv_path = output_dir / "forecasts.csv"
    if forecasts:
        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(asdict(forecasts[0]).keys()))
            writer.writeheader()
            writer.writerows(asdict(row) for row in forecasts)
    else:
        csv_path.write_text("provider,event_id,market_id,contract_id,item_id,forecast_as_of,p1,p25,p50,p75,p99,validation_status\n", encoding="utf-8")
    manifest["artifacts"] = {
        "forecasts_jsonl": jsonl_path.name,
        "forecasts_csv": csv_path.name,
        "sha256": hashlib.sha256(jsonl_path.read_bytes()).hexdigest(),
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    started = time.monotonic()
    provider = args.provider
    api = QuanturaApi(args.api_origin)
    categories = api.categories(provider)
    selected = [category for category in categories if not args.category or str(category.get("id", "")).lower() == args.category.lower()]
    selected = selected[: args.max_categories]
    contracts: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    for category in selected:
        category_id = str(category.get("id") or "").strip()
        if not category_id:
            continue
        try:
            contracts.extend(api.markets(provider, category_id, args.max_markets))
        except RunnerError as error:
            failures.append({"stage": "discovery", "category": category_id, "error": str(error)})
        if len(contracts) >= args.max_markets:
            break
    deduplicated = {safe_id(contract.get("contractId")): contract for contract in contracts if contract.get("contractId")}
    contracts = list(deduplicated.values())[: args.max_markets]
    forecasts: list[QuantileForecast] = []
    missing_history = 0
    invalid_series = 0
    end = utc_now()
    start = end - timedelta(days=args.lookback_days)
    for contract in contracts:
        try:
            rows = api.history(provider, contract, start, end, args.frequency)
            if not rows:
                missing_history += 1
                continue
            forecasts.append(build_forecast(provider, contract, rows, args.horizon_steps, args.frequency))
        except RunnerError as error:
            invalid_series += 1
            failures.append({"stage": "forecast", "contract_id": safe_id(contract.get("contractId")), "error": str(error)})
    eligible = len(contracts)
    manifest = {
        "provider": provider,
        "generated_at": utc_now().isoformat(),
        "forecast_version": METHOD_VERSION,
        "commit_sha": os.environ.get("GITHUB_SHA", "local"),
        "api_origin": args.api_origin,
        "categories_discovered": len(categories),
        "categories_scanned": len(selected),
        "markets_discovered": len(deduplicated),
        "markets_eligible": eligible,
        "markets_processed": len(forecasts) + invalid_series + missing_history,
        "forecasts_successful": len(forecasts),
        "forecasts_failed": invalid_series,
        "missing_history": missing_history,
        "invalid_series": invalid_series,
        "coverage_pct": round((len(forecasts) / eligible * 100.0), 2) if eligible else 0.0,
        "full_provider_coverage": False,
        "run_limits": {"max_categories": args.max_categories, "max_markets": args.max_markets, "lookback_days": args.lookback_days},
        "runtime_seconds": round(time.monotonic() - started, 3),
        "failures": failures[:100],
    }
    write_outputs(Path(args.output_dir), forecasts, manifest)
    return manifest


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--provider", required=True, choices=("polymarket_us", "kalshi"))
    value.add_argument("--api-origin", default="https://quantura.studio")
    value.add_argument("--category", default="")
    value.add_argument("--max-categories", type=int, default=3)
    value.add_argument("--max-markets", type=int, default=12)
    value.add_argument("--lookback-days", type=int, default=30)
    value.add_argument("--frequency", choices=("1m", "5m", "15m", "30m", "1h", "1d"), default="1h")
    value.add_argument("--horizon-steps", type=int, default=24)
    value.add_argument("--output-dir", required=True)
    return value


def main() -> int:
    args = parser().parse_args()
    if not 1 <= args.max_categories <= 25 or not 1 <= args.max_markets <= 100:
        raise SystemExit("Category and market limits must be within safe bounds.")
    if not 1 <= args.lookback_days <= 90 or not 1 <= args.horizon_steps <= 10_000:
        raise SystemExit("Lookback and horizon must be within safe bounds.")
    try:
        manifest = run(args)
    except RunnerError as error:
        print(json.dumps({"ok": False, "error": str(error)}), file=sys.stderr)
        return 1
    print(json.dumps({"ok": True, "manifest": manifest}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
