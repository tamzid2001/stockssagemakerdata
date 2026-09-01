from __future__ import annotations

import ctypes
import gc
import importlib.metadata
import os
from abc import ABC, abstractmethod
from typing import Iterable

from ..schemas import ForecastRequest, ModelForecast, PreparedSeries


class ModelExecutionError(RuntimeError):
    def __init__(self, model: str, code: str, *, retryable: bool = False):
        super().__init__(f"{model}:{code}")
        self.model = model
        self.code = code
        self.retryable = retryable


class ForecastAdapter(ABC):
    model_id: str

    @abstractmethod
    def forecast(
        self,
        series: PreparedSeries,
        timestamps: tuple[str, ...],
        request: ForecastRequest,
    ) -> ModelForecast:
        raise NotImplementedError


def cleanup_memory() -> None:
    gc.collect()
    try:
        import torch

        if torch.cuda.is_available():
            torch.cuda.empty_cache()
            try:
                torch.cuda.ipc_collect()
            except Exception:
                pass
    except ImportError:
        pass
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        pass


def package_versions(names: Iterable[str]) -> dict[str, str]:
    versions = {}
    for name in names:
        try:
            versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            versions[name] = "unknown"
    return versions


def device_name(preference: str = "auto") -> str:
    if preference == "cpu":
        return "cpu"
    try:
        import torch

        if torch.cuda.is_available():
            return "cuda"
    except ImportError:
        pass
    return "cpu"


def is_cuda_oom(error: BaseException) -> bool:
    value = str(error).lower()
    return "out of memory" in value or "cublas_status_alloc_failed" in value


def hf_token() -> str | None:
    value = os.environ.get("HF_TOKEN", "").strip()
    return value or None
