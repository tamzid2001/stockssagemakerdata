#!/usr/bin/env python3
"""GitHub Actions entrypoint for Quantura social channel publishing."""

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
    parser = argparse.ArgumentParser(description="Run a Quantura social channel publish workflow.")
    parser.add_argument("--channel", required=True, help="Social channel key (x, linkedin, facebook, instagram, tiktok).")
    parser.add_argument("--dry-run", action="store_true", help="Preview the post without publishing.")
    parser.add_argument("--force", action="store_true", help="Bypass duplicate-post protection.")
    parser.add_argument("--source-mode", default="auto", help="Source mode: auto, manual, latest_blog.")
    parser.add_argument("--topic", default="", help="Optional manual topic override.")
    parser.add_argument("--objective", default="", help="Optional objective override.")
    parser.add_argument("--audience", default="", help="Optional audience override.")
    parser.add_argument("--tone", default="", help="Optional tone override.")
    parser.add_argument("--cta-url", default="", help="Optional CTA URL override.")
    parser.add_argument("--output-dir", default="artifacts/social", help="Artifact directory.")
    parser.add_argument("--project-id", default=os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT_ID") or "", help="Google Cloud project id.")
    return parser.parse_args()


def write_github_summary(summary_path: str, payload_path: str, summary) -> None:
    if not summary_path:
        return
    with open(summary_path, "a", encoding="utf-8") as handle:
        handle.write(f"## Social publish: {summary.channel}\n\n")
        handle.write(f"- Status: `{summary.status}`\n")
        handle.write(f"- Dry run: `{summary.dry_run}`\n")
        handle.write(f"- Source: `{summary.source_kind}` / `{summary.source_id}`\n")
        handle.write(f"- Used AI: `{summary.used_ai}`\n")
        if summary.model_used:
            handle.write(f"- Model: `{summary.model_used}`\n")
        if summary.duplicate_skip:
            handle.write("- Duplicate protection: skipped existing publication\n")
        if summary.external_id:
            handle.write(f"- External ID: `{summary.external_id}`\n")
        if summary.external_url:
            handle.write(f"- External URL: {summary.external_url}\n")
        handle.write(f"- Artifact: `{payload_path}`\n")
        handle.write(f"- Message: {summary.message}\n")


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    ctx = RunContext(
        channel=args.channel,
        dry_run=bool(args.dry_run),
        force=bool(args.force),
        source_mode=args.source_mode,
        topic=args.topic,
        objective=args.objective,
        audience=args.audience,
        tone=args.tone,
        cta_url=args.cta_url,
        output_dir=str(output_dir),
        github_run_id=str(os.environ.get("GITHUB_RUN_ID") or "").strip(),
        github_run_attempt=str(os.environ.get("GITHUB_RUN_ATTEMPT") or "").strip(),
        github_workflow=str(os.environ.get("GITHUB_WORKFLOW") or "").strip(),
        github_repository=str(os.environ.get("GITHUB_REPOSITORY") or "").strip(),
        github_actor=str(os.environ.get("GITHUB_ACTOR") or "").strip(),
        triggered_at=str(os.environ.get("GITHUB_EVENT_NAME") or "").strip(),
        project_id=args.project_id,
    )
    summary = run_social_channel(ctx)
    write_github_summary(
        os.environ.get("GITHUB_STEP_SUMMARY") or "",
        summary.artifact_path,
        summary,
    )
    print(f"Social channel run finished: {summary.channel} -> {summary.status}")
    return 0 if summary.status in {"success", "dry_run", "skipped"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
