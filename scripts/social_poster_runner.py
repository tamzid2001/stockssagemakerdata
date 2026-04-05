#!/usr/bin/env python3
"""Legacy-friendly wrapper around the GitHub Actions social publishing runtime."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.social.models import RunContext
from scripts.lib.social.pipeline import run_social_channel


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Quantura social poster runner")
    parser.add_argument("--channels", default="x,linkedin,facebook,instagram,tiktok", help="Comma-separated channel list.")
    parser.add_argument("--topic", default="", help="Optional topic override.")
    parser.add_argument("--objective", default="", help="Optional objective override.")
    parser.add_argument("--audience", default="", help="Optional audience override.")
    parser.add_argument("--tone", default="", help="Optional tone override.")
    parser.add_argument("--cta-url", default=os.environ.get("SOCIAL_DEFAULT_CTA_URL", "https://quantura.studio"))
    parser.add_argument("--user-id", default=os.environ.get("SOCIAL_AUTOPILOT_USER_ID", "quantura_system"))
    parser.add_argument("--user-email", default=os.environ.get("SOCIAL_AUTOPILOT_USER_EMAIL", "system@quantura.ai"))
    parser.add_argument("--send-now", action="store_true", help="Publish immediately instead of previewing.")
    parser.add_argument("--dry-run", action="store_true", help="Force preview mode even when --send-now is present.")
    parser.add_argument("--force", action="store_true", help="Bypass duplicate-post protection.")
    parser.add_argument("--source-mode", default="auto", help="Source mode: auto, manual, latest_blog.")
    parser.add_argument("--project-id", default=os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT_ID") or "")
    parser.add_argument("--queue-strategic", action="store_true", help="Deprecated. GitHub Actions schedules now manage timing.")
    parser.add_argument("--dispatch-due", action="store_true", help="Deprecated. GitHub Actions schedules now manage dispatch.")
    parser.add_argument("--dispatch-limit", type=int, default=30, help="Deprecated.")
    parser.add_argument("--publish-now-first", action="store_true", help="Deprecated.")
    parser.add_argument("--publish-all-now", action="store_true", help="Deprecated.")
    parser.add_argument("--schedule-autopilot", action="store_true", help="Deprecated. Use scheduled workflows instead.")
    return parser.parse_args()


def _normalize_channels(value: str) -> list[str]:
    return [item.strip().lower() for item in str(value or "").split(",") if item.strip()]


def main() -> int:
    args = parse_args()
    if args.queue_strategic or args.dispatch_due or args.publish_now_first or args.publish_all_now or args.schedule_autopilot:
        print("Note: queue/autopilot flags are deprecated. Scheduled GitHub Actions now own social timing and dispatch.")

    dry_run = bool(args.dry_run or not args.send_now)
    statuses: list[str] = []
    for channel in _normalize_channels(args.channels):
        ctx = RunContext(
            channel=channel,
            dry_run=dry_run,
            force=bool(args.force),
            source_mode=args.source_mode,
            topic=args.topic,
            objective=args.objective,
            audience=args.audience,
            tone=args.tone,
            cta_url=args.cta_url,
            output_dir=str(REPO_ROOT / "artifacts" / "social-cli"),
            github_run_id="local-cli",
            github_run_attempt="1",
            github_workflow="local-social-poster-runner",
            github_repository=os.environ.get("GITHUB_REPOSITORY", "tamzid2001/stockssagemakerdata"),
            github_actor=args.user_id,
            triggered_at="local_cli",
            project_id=args.project_id,
        )
        summary = run_social_channel(ctx)
        statuses.append(summary.status)
        print(f"{channel}: {summary.status} - {summary.message}")

    return 0 if all(status in {"success", "dry_run", "skipped"} for status in statuses) else 1


if __name__ == "__main__":
    raise SystemExit(main())
