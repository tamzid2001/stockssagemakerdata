from __future__ import annotations


def classify_capability_status(status_code: int) -> str:
    code = int(status_code or 0)
    if code == 200:
        return "AVAILABLE"
    if code == 401:
        return "UNAUTHORIZED"
    if code in {402, 403}:
        return "FORBIDDEN_OR_NOT_IN_PLAN"
    return "ERROR"
