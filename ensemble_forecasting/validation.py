from __future__ import annotations

import numpy as np


def coerce_quantile_horizon(
    value: object,
    *,
    quantile_count: int,
    prediction_length: int,
    model_name: str,
) -> np.ndarray:
    matrix = np.squeeze(np.asarray(value))
    if matrix.ndim == 1 and prediction_length == 1 and matrix.shape[0] == quantile_count:
        return matrix.astype(np.float64)[:, None]
    if matrix.ndim == 2:
        if matrix.shape == (quantile_count, prediction_length):
            return matrix.astype(np.float64)
        if matrix.shape == (prediction_length, quantile_count):
            return matrix.T.astype(np.float64)
    raise ValueError(
        f"{model_name}: unexpected quantile shape {matrix.shape}; expected "
        f"({quantile_count}, {prediction_length}) or ({prediction_length}, {quantile_count})"
    )


def monotonic_rearrangement(matrix: np.ndarray) -> np.ndarray:
    value = np.asarray(matrix, dtype=np.float64)
    if value.ndim != 2 or not np.isfinite(value).all():
        raise ValueError("ensemble quantile matrix must be finite and two-dimensional")
    return np.sort(value, axis=0)


def validate_monotonic(matrix: np.ndarray, tolerance: float = 1e-10) -> None:
    value = np.asarray(matrix, dtype=np.float64)
    if value.ndim != 2 or not np.isfinite(value).all():
        raise ValueError("quantile matrix must be finite and two-dimensional")
    if bool(np.any(np.diff(value, axis=0) < -tolerance)):
        raise ValueError("forecast quantiles cross")


def interpolate_native_quantiles(
    native_matrix: np.ndarray,
    *,
    native_levels: tuple[float, ...],
    requested_levels: tuple[float, ...],
) -> tuple[np.ndarray, tuple[float, ...], dict[str, str]]:
    native = np.asarray(native_matrix, dtype=np.float64)
    if native.shape[0] != len(native_levels):
        raise ValueError("native quantile matrix does not match native levels")
    native = np.sort(native, axis=0)
    result = np.full((len(requested_levels), native.shape[1]), np.nan, dtype=np.float64)
    available: list[float] = []
    provenance: dict[str, str] = {}
    for index, quantile in enumerate(requested_levels):
        if quantile < native_levels[0] or quantile > native_levels[-1]:
            provenance[str(quantile)] = "unavailable_outside_native_range"
            continue
        for step in range(native.shape[1]):
            result[index, step] = np.interp(quantile, native_levels, native[:, step])
        available.append(quantile)
        provenance[str(quantile)] = "native" if quantile in native_levels else "interpolated_inside_native_range"
    central = [index for index, value in enumerate(requested_levels) if value in available]
    if central:
        result[central] = np.sort(result[central], axis=0)
    return result, tuple(available), provenance
