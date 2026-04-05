#!/usr/bin/env python3
"""Sync social publishing secrets from local env files into Google Cloud Secret Manager."""

from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path


ENV_FILES = [Path(".env"), Path("quantura_site/functions/.env")]
SUPPORTED = (
    "OPENAI_API_KEY",
    "TWITTER_API_KEY",
    "TWITTER_API_SECRET",
    "TWITTER_ACCESS_TOKEN",
    "TWITTER_ACCESS_TOKEN_SECRET",
    "X_USER_OAUTH2_TOKEN",
    "LINKEDIN_ACCESS_TOKEN",
    "LINKEDIN_AUTHOR_URN",
    "FACEBOOK_PAGE_ID",
    "FACEBOOK_PAGE_ACCESS_TOKEN",
    "INSTAGRAM_BUSINESS_ACCOUNT_ID",
    "INSTAGRAM_ACCESS_TOKEN",
    "INSTAGRAM_DEFAULT_IMAGE_URL",
    "TIKTOK_ACCESS_TOKEN",
    "TIKTOK_OPEN_ID",
    "TIKTOK_DEFAULT_MEDIA_URL",
    "TIKTOK_PRIVACY_LEVEL",
)


def load_env_files() -> None:
    for path in ENV_FILES:
        if not path.exists():
            continue
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and value and not os.environ.get(key):
                os.environ[key] = value


def secret_exists(project_id: str, name: str) -> bool:
    result = subprocess.run(
        ["gcloud", "secrets", "describe", name, "--project", project_id],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
        text=True,
    )
    return result.returncode == 0


def write_secret(project_id: str, name: str, value: str) -> None:
    if not secret_exists(project_id, name):
        subprocess.run(
            ["gcloud", "secrets", "create", name, "--replication-policy", "automatic", "--project", project_id],
            check=True,
            text=True,
        )
    subprocess.run(
        ["gcloud", "secrets", "versions", "add", name, "--data-file=-", "--project", project_id],
        input=value,
        check=True,
        text=True,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync local social secrets into Google Cloud Secret Manager.")
    parser.add_argument("--project-id", default=os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT_ID") or "quantura-e2e3d")
    parser.add_argument("--only", default="", help="Optional comma-separated subset of secret names.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_env_files()
    wanted = [item.strip() for item in args.only.split(",") if item.strip()] or list(SUPPORTED)
    updated = 0
    missing = 0
    for name in wanted:
        value = str(os.environ.get(name) or "").strip()
        if not value:
            missing += 1
            continue
        write_secret(args.project_id, name, value)
        updated += 1
    print(f"Updated {updated} secret(s) in project {args.project_id}. Missing locally: {missing}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
