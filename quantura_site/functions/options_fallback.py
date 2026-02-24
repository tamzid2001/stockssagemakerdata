from __future__ import annotations

from typing import Any


def should_use_massive_fallback(
    *,
    yfinance_expirations: list[str] | None,
    calls: list[dict[str, Any]] | None,
    puts: list[dict[str, Any]] | None,
    yfinance_error: bool = False,
) -> bool:
    expirations = [str(item or "").strip() for item in (yfinance_expirations or []) if str(item or "").strip()]
    calls_rows = calls if isinstance(calls, list) else []
    puts_rows = puts if isinstance(puts, list) else []
    if yfinance_error:
        return True
    if not expirations:
        return True
    return not calls_rows and not puts_rows
