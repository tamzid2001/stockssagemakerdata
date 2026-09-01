from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

from .schemas import PreparedSeries


def prepare_series(
    rows: Iterable[Mapping[str, Any]],
    *,
    timestamp_column: str = "timestamp",
    target_column: str = "target",
    transform: str = "auto",
    frequency: str = "infer",
    timezone: str = "UTC",
    minimum_rows: int = 40,
    maximum_rows: int = 10_000,
) -> PreparedSeries:
    frame = pd.DataFrame(list(rows))
    if timestamp_column not in frame.columns or target_column not in frame.columns:
        raise ValueError("timestamp and target columns are required")
    if len(frame) > maximum_rows:
        raise ValueError(f"history exceeds the {maximum_rows} row limit")
    timestamps = pd.to_datetime(frame[timestamp_column], utc=True, errors="coerce")
    targets = pd.to_numeric(frame[target_column], errors="coerce")
    data = pd.DataFrame({"timestamp": timestamps, "target": targets})
    data = data.dropna().sort_values("timestamp").drop_duplicates("timestamp", keep="last")
    if len(data) < minimum_rows:
        raise ValueError(f"at least {minimum_rows} valid history rows are required")
    values = data["target"].to_numpy(dtype=np.float32)
    if not np.isfinite(values).all():
        raise ValueError("target contains NaN/inf")
    requested_transform = str(transform).lower()
    if requested_transform not in {"auto", "log", "none"}:
        raise ValueError("transform must be auto, log, or none")
    chosen = "log" if requested_transform == "auto" and bool(np.all(values > 0)) else requested_transform
    if chosen == "auto":
        chosen = "none"
    if chosen == "log":
        if bool(np.any(values <= 0)):
            raise ValueError("log transform requires strictly positive target values")
        transformed = np.log(values).astype(np.float32)
    else:
        transformed = values.copy()
    inferred = _infer_frequency(data["timestamp"]) if frequency in {"", "infer"} else frequency
    timestamp_values = tuple(value.isoformat().replace("+00:00", "Z") for value in data["timestamp"])
    digest_payload = {
        "timestamps": timestamp_values,
        "values": [float(value) for value in values],
        "frequency": inferred,
        "timezone": timezone,
        "transform": chosen,
    }
    dataset_hash = hashlib.sha256(
        json.dumps(digest_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return PreparedSeries(
        timestamps=timestamp_values,
        values=values,
        transformed_values=transformed,
        transform=chosen,  # type: ignore[arg-type]
        frequency=inferred,
        timezone=timezone,
        dataset_hash=dataset_hash,
    )


def _infer_frequency(values: pd.Series) -> str:
    inferred = pd.infer_freq(pd.DatetimeIndex(values))
    if inferred:
        return inferred
    deltas = values.sort_values().diff().dropna()
    if deltas.empty:
        raise ValueError("frequency could not be inferred")
    seconds = int(deltas.dt.total_seconds().median())
    if seconds <= 0:
        raise ValueError("frequency could not be inferred")
    return pd.tseries.frequencies.to_offset(pd.Timedelta(seconds=seconds)).freqstr
