"""Legacy Python backend shim.

This module is intentionally minimal.
Active production APIs are deployed from:
- quantura_site/functions_explore (Node.js/TypeScript, Gen2)

The legacy Python backend was retired to prevent drift and duplicate providers.
"""

from __future__ import annotations

from typing import Any


def get_legacy_backend_notice() -> dict[str, Any]:
    """Return a stable machine-readable notice for deprecated callers."""
    return {
        "ok": False,
        "status": "deprecated",
        "message": "Legacy Python backend is retired. Use functions_explore endpoints under /api/*."
    }
