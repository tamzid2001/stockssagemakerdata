from __future__ import annotations

import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIG_ROOT = REPO_ROOT / "scripts" / "lib" / "social" / "config"
CHANNEL_ROOT = CONFIG_ROOT / "channels"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_channel_config(channel: str) -> dict[str, Any]:
    clean = str(channel or "").strip().lower()
    if not clean:
        raise ValueError("Channel is required.")
    path = CHANNEL_ROOT / f"{clean}.json"
    if not path.exists():
        raise FileNotFoundError(f"Missing channel config for '{clean}' at {path}.")
    payload = load_json(path)
    required = ("channel", "provider", "maxBodyLength", "retry", "rateLimit", "defaults")
    missing = [key for key in required if key not in payload]
    if missing:
        raise ValueError(f"Channel config '{clean}' is missing required keys: {', '.join(missing)}")
    return payload


def all_channel_configs() -> list[dict[str, Any]]:
    configs: list[dict[str, Any]] = []
    for path in sorted(CHANNEL_ROOT.glob("*.json")):
        configs.append(load_json(path))
    return configs
