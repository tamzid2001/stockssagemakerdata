#!/usr/bin/env python3
"""Deprecated GitHub secret sync helper for social automation.

Social publishing now uses Google Cloud Secret Manager as the source of truth.
Use `scripts/sync_social_secrets_to_secret_manager.py` instead.
"""

from __future__ import annotations

import argparse


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deprecated helper for social secret sync.")
    parser.add_argument("--only", default="", help="Ignored. Use sync_social_secrets_to_secret_manager.py instead.")
    return parser.parse_args()


def main() -> int:
    _ = parse_args()
    print("This script is deprecated.")
    print("Social publishing now reads credentials from Google Cloud Secret Manager, not GitHub Actions secrets.")
    print("Use: python scripts/sync_social_secrets_to_secret_manager.py")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
