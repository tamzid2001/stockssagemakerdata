#!/usr/bin/env python3
"""Sync local social automation credentials into GitHub Actions secrets.

This script reads values from the process environment plus local `.env` files and
writes matching GitHub repository secrets via `gh secret set`.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path


ENV_FILES = [Path(".env"), Path("quantura_site/functions/.env")]

SECRET_SOURCES: dict[str, tuple[str, ...]] = {
    "OPENAI_API_KEY": ("OPENAI_API_KEY",),
    "TWITTER_BEARER_TOKEN": ("TWITTER_BEARER_TOKEN", "X_BEARER_TOKEN"),
    "TWITTER_API_KEY": ("TWITTER_API_KEY", "X_API_KEY"),
    "TWITTER_API_SECRET": ("TWITTER_API_SECRET", "X_API_SECRET"),
    "TWITTER_ACCESS_TOKEN": ("TWITTER_ACCESS_TOKEN", "X_ACCESS_TOKEN"),
    "TWITTER_ACCESS_TOKEN_SECRET": ("TWITTER_ACCESS_TOKEN_SECRET", "X_ACCESS_TOKEN_SECRET"),
    "LINKEDIN_ACCESS_TOKEN": ("LINKEDIN_ACCESS_TOKEN",),
    "LINKEDIN_AUTHOR_URN": ("LINKEDIN_AUTHOR_URN", "LINKEDIN_ORGANIZATION_URN"),
    "FACEBOOK_PAGE_ID": ("FACEBOOK_PAGE_ID",),
    "FACEBOOK_PAGE_ACCESS_TOKEN": ("FACEBOOK_PAGE_ACCESS_TOKEN",),
    "INSTAGRAM_BUSINESS_ACCOUNT_ID": ("INSTAGRAM_BUSINESS_ACCOUNT_ID",),
    "INSTAGRAM_ACCESS_TOKEN": ("INSTAGRAM_ACCESS_TOKEN",),
    "INSTAGRAM_DEFAULT_IMAGE_URL": ("INSTAGRAM_DEFAULT_IMAGE_URL",),
    "TIKTOK_ACCESS_TOKEN": ("TIKTOK_ACCESS_TOKEN",),
    "TIKTOK_OPEN_ID": ("TIKTOK_OPEN_ID",),
    "SOCIAL_WEBHOOK_X": ("SOCIAL_WEBHOOK_X",),
    "SOCIAL_WEBHOOK_LINKEDIN": ("SOCIAL_WEBHOOK_LINKEDIN",),
    "SOCIAL_WEBHOOK_FACEBOOK": ("SOCIAL_WEBHOOK_FACEBOOK",),
    "SOCIAL_WEBHOOK_INSTAGRAM": ("SOCIAL_WEBHOOK_INSTAGRAM",),
    "SOCIAL_WEBHOOK_TIKTOK": ("SOCIAL_WEBHOOK_TIKTOK",),
    "FIREBASE_SERVICE_ACCOUNT_JSON": ("FIREBASE_SERVICE_ACCOUNT_JSON",),
}


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


def detect_repo() -> str:
    proc = subprocess.run(
        ["git", "remote", "get-url", "origin"],
        check=True,
        capture_output=True,
        text=True,
    )
    url = proc.stdout.strip()
    if url.startswith("https://github.com/"):
        repo = url.removeprefix("https://github.com/").removesuffix(".git")
        if repo:
            return repo
    if url.startswith("git@github.com:"):
        repo = url.removeprefix("git@github.com:").removesuffix(".git")
        if repo:
            return repo
    raise RuntimeError(f"Unsupported origin URL: {url}")


def read_firebase_service_account_json() -> str:
    path_candidates = [
        Path(str(os.environ.get("FIREBASE_SERVICE_ACCOUNT_PATH") or "").strip()),
        Path(str(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()),
        Path("quantura_site/functions/serviceAccountKey.local.json"),
        Path("quantura_site/functions/serviceAccountKey.json"),
    ]
    for service_account_path in path_candidates:
        if not str(service_account_path):
            continue
        if not service_account_path.exists():
            continue
        text = service_account_path.read_text(encoding="utf-8").strip()
        if not text:
            continue
        try:
            parsed = json.loads(text)
        except Exception:
            continue
        private_key = str(parsed.get("private_key") or "")
        client_email = str(parsed.get("client_email") or "")
        if "BEGIN PRIVATE KEY" not in private_key or not client_email:
            continue
        try:
            from cryptography.hazmat.primitives import serialization  # type: ignore
            serialization.load_pem_private_key(private_key.encode("utf-8"), password=None)
        except Exception:
            continue
        return text
    return ""


def resolve_secret_value(secret_name: str) -> str:
    for source_key in SECRET_SOURCES.get(secret_name, (secret_name,)):
        value = str(os.environ.get(source_key) or "").strip()
        if value:
            return value
    if secret_name == "FIREBASE_SERVICE_ACCOUNT_JSON":
        return read_firebase_service_account_json()
    return ""


def set_secret(repo: str, name: str, value: str) -> None:
    subprocess.run(
        ["gh", "secret", "set", name, "--repo", repo],
        check=True,
        input=value,
        text=True,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync local social credentials to GitHub Actions secrets.")
    parser.add_argument("--repo", help="GitHub repo slug (owner/repo). Defaults to git origin.")
    parser.add_argument(
        "--only",
        default="",
        help="Comma-separated list of secret names to sync. Defaults to the full supported set.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_env_files()
    repo = args.repo or detect_repo()
    wanted = [
        item.strip()
        for item in args.only.split(",")
        if item.strip()
    ] or list(SECRET_SOURCES.keys())

    print(f"Syncing GitHub secrets for {repo}")
    updated: list[str] = []
    missing: list[str] = []
    for name in wanted:
        value = resolve_secret_value(name)
        if not value:
            missing.append(name)
            continue
        set_secret(repo, name, value)
        updated.append(name)
        print(f"  set {name}")

    print(f"\nUpdated: {len(updated)}")
    print(f"Missing: {len(missing)}")
    if missing:
        print("Missing keys:")
        for key in missing:
            print(f"  - {key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
