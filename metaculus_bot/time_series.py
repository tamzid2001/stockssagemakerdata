from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .api import date, utcnow
from .research import public_get

QUANTILES = [0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99]
SILVER_PAGE = "https://www.natesilver.net/p/trump-approval-ratings-nate-silver-bulletin"


def definition(question, registry: list[dict]) -> dict | None:
    """Only reviewed mappings; never turn an arbitrary title into an asset price."""
    criteria = question.resolution_criteria or ""
    criteria_hash = hashlib.sha256(criteria.encode()).hexdigest()
    for spec in registry:
        if spec["question_id"] == question.id_of_question and spec["resolution_sha256"] == criteria_hash:
            return dict(spec)
    # This public resolution-source adapter is reusable across tournaments and
    # dates. It does not use a human's opinion or a preview of a bot prediction.
    if question.question_type not in {"numeric", "discrete"} or SILVER_PAGE not in criteria:
        return None
    title = question.question_text.lower()
    if "trump" not in title or "approval" not in title or "disapproval" in title:
        return None
    dates = re.findall(r'(?:on|for)\s+([A-Z][a-z]+\s+\d{1,2},?\s+20\d{2})', criteria)
    if len(dates) != 1:
        return None
    target = datetime.strptime(dates[0].replace(",", ""), "%B %d %Y").replace(tzinfo=timezone.utc)
    return {"kind": "silver_bulletin", "target_date": target.date().isoformat(), "frequency": "1D",
            "calendar": "NONE", "unit": "percentage points", "value_column": "net" if "net approval" in title else "approve"}


def history(spec: dict) -> tuple[list[dict], dict]:
    if spec["kind"] == "silver_bulletin":
        # Datawrapper's versionless page points to the current publicly available
        # chart; do not hard-code an outdated chart version or access paid data.
        page, _ = public_get("https://datawrapper.dwcdn.net/kSCt4/")
        versions = re.findall(rb'https://datawrapper\.dwcdn\.net/kSCt4/(\d+)/', page)
        if not versions:
            raise ValueError("PUBLIC_CHART_VERSION_UNAVAILABLE")
        url = f"https://datawrapper.dwcdn.net/kSCt4/{versions[0].decode()}/dataset.csv"
        raw, final = public_get(url)
        records = list(csv.DictReader(io.StringIO(raw.decode("utf-8-sig"))))
        rows = []
        for record in records:
            timestamp = datetime.strptime(record["modeldate"], "%m/%d/%Y").replace(tzinfo=timezone.utc)
            value = float(record["approve"])
            if spec["value_column"] == "net":
                value -= float(record["disapprove"])
            rows.append({"timestamp": timestamp.isoformat(), "target": value})
        source_url = SILVER_PAGE
    elif spec["kind"] == "public_csv":
        # Registry changes require reviewing exact units, period, source, and the
        # resolution-criteria hash. Public HTTP only; no Cloud Storage/database.
        raw, final = public_get(spec["url"])
        records = list(csv.DictReader(io.StringIO(raw.decode("utf-8-sig"))))
        rows = [{"timestamp": date(r[spec["time_column"]]).isoformat(), "target": float(r[spec["value_column"]])}
                for r in records]
        source_url = spec["url"]
    else:
        raise ValueError("UNSUPPORTED_REVIEWED_SOURCE")
    now = utcnow()
    rows = [r for r in rows if date(r["timestamp"]) <= now]
    if any(date(b["timestamp"]) <= date(a["timestamp"]) for a, b in zip(rows, rows[1:])):
        raise ValueError("CHRONOLOGICAL_UNIQUE_HISTORY_REQUIRED")
    if len(rows) < 40:
        raise ValueError("GENUINE_HISTORY_TOO_SHORT")
    max_age = int(spec.get("max_history_age_days", 3))
    if now - date(rows[-1]["timestamp"]) > timedelta(days=max_age):
        raise ValueError("STALE_RESOLUTION_SOURCE_HISTORY")
    return rows[-500:], {"url": source_url, "data_url": final, "sha256": hashlib.sha256(raw).hexdigest(),
                         "last_observation": rows[-1]["timestamp"], "observed_rows": min(len(rows), 500), "unit": spec["unit"]}


def forecast(question, spec: dict, python_executable: str) -> dict:
    rows, source = history(spec)
    target = date(spec["target_date"])
    if target <= utcnow():
        raise ValueError("FUTURE_RESOLUTION_TARGET_REQUIRED")
    # The worker verifies the exact returned target date. A scheduled resolution
    # date is not substituted for a date stated in the question's criteria.
    start = date(rows[-1]["timestamp"])
    frequency = spec.get("frequency", "1D")
    horizon = (target.date() - start.date()).days
    if frequency == "1M":
        horizon = (target.year - start.year) * 12 + target.month - start.month
    if not 1 <= horizon <= 1024:
        raise ValueError("SUPPORTED_TARGET_HORIZON_REQUIRED")
    job = {"rows": rows, "horizon": horizon, "frequency": frequency, "calendar": spec.get("calendar", "NONE"),
           "target_date": target.date().isoformat()}
    with tempfile.TemporaryDirectory(prefix="quantura-metaculus-") as temp:
        input_file, output_file = Path(temp, "input.json"), Path(temp, "output.json")
        input_file.write_text(json.dumps(job))
        # Separate inference environment keeps the production model lock intact.
        result = subprocess.run([python_executable, "-m", "metaculus_bot.ensemble_job", str(input_file), str(output_file)],
                                timeout=1800, check=False)
        if result.returncode != 0 or not output_file.exists():
            raise RuntimeError("ENSEMBLE_INFERENCE_FAILED")
        data = json.loads(output_file.read_text())
    points = [{"percentile": q, "value": data["quantiles"][str(q)]} for q in QUANTILES]
    models = data["models"]
    reasoning = (f"Quantura time-series estimate for {target.date().isoformat()} in {spec['unit']}. "
                 f"The resolution source is {source['url']} (public data snapshot {source['data_url']}, "
                 f"SHA256 {source['sha256']}). It uses {source['observed_rows']} genuine observations through "
                 f"{source['last_observation']}; no later observations or filled gaps are used. "
                 f"Completed models: {', '.join(models)}. Per-quantile weights exclude models that do not support that quantile. "
                 "The predictive quantiles represent model uncertainty in the observed series, not guaranteed outcomes. "
                 "Structural changes and the resolution source's own measurement uncertainty may not be fully captured. "
                 "No LLM was used to invent historical observations or to change the numerical ensemble result. "
                 f"Target quantiles: {json.dumps(points)}. Model quantile provenance: {json.dumps(data['provenance'])}.")
    return {"percentiles": points, "reasoning": reasoning, "engine": "quantura_time_series",
            "models": models, "source_ids": ["S1"], "sources": [{"id": "S1", **source}], "cost_usd": 0}
