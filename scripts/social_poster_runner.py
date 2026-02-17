#!/usr/bin/env python3
"""Run Quantura social poster generation, send smoke-test posts, and queue strategic schedules."""

from __future__ import annotations

import argparse
import importlib.util
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and (key not in os.environ or not str(os.environ.get(key) or "").strip()):
            os.environ[key] = value


def load_functions_module() -> Any:
    module_path = Path("quantura_site/functions/main.py").resolve()
    spec = importlib.util.spec_from_file_location("quantura_functions_main", module_path)
    if not spec or not spec.loader:
        raise RuntimeError("Unable to load quantura functions module.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[attr-defined]
    return module


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Quantura social poster runner")
    parser.add_argument(
        "--channels",
        default="x,linkedin,facebook,instagram,tiktok",
        help="Comma-separated channel list.",
    )
    parser.add_argument(
        "--topic",
        default="Quantura market pulse: setup quality, risk posture, and catalyst map",
        help="Campaign topic.",
    )
    parser.add_argument(
        "--objective",
        default="Drive qualified users into Quantura forecasting workflows",
        help="Campaign objective.",
    )
    parser.add_argument(
        "--audience",
        default="active investors and portfolio operators",
        help="Audience description.",
    )
    parser.add_argument(
        "--tone",
        default="institutional, concise, practical",
        help="Content tone.",
    )
    parser.add_argument("--posts-per-channel", type=int, default=3, help="Draft count per channel.")
    parser.add_argument("--cta-url", default=os.environ.get("SOCIAL_DEFAULT_CTA_URL", "https://quantura-e2e3d.web.app"))
    parser.add_argument("--user-id", default=os.environ.get("SOCIAL_AUTOPILOT_USER_ID", "quantura_system"))
    parser.add_argument("--user-email", default=os.environ.get("SOCIAL_AUTOPILOT_USER_EMAIL", "system@quantura.ai"))
    parser.add_argument("--send-now", action="store_true", help="Send one immediate smoke-test post per channel.")
    parser.add_argument(
        "--queue-strategic",
        action="store_true",
        help="Queue generated posts using strategic suggested times.",
    )
    parser.add_argument(
        "--dispatch-due",
        action="store_true",
        help="Dispatch due queued posts now using backend dispatcher.",
    )
    parser.add_argument("--dispatch-limit", type=int, default=30)
    parser.add_argument(
        "--publish-now-first",
        action="store_true",
        help="When queueing posts, force the first post per channel to publish immediately.",
    )
    parser.add_argument(
        "--publish-all-now",
        action="store_true",
        help="When queueing posts, force all generated posts to publish immediately.",
    )
    parser.add_argument(
        "--auto-filter-channels",
        dest="auto_filter_channels",
        action="store_true",
        default=True,
        help="Automatically skip channels without direct credentials or webhook route.",
    )
    parser.add_argument(
        "--no-auto-filter-channels",
        dest="auto_filter_channels",
        action="store_false",
        help="Do not auto-filter channels by posting route.",
    )
    parser.add_argument(
        "--schedule-autopilot",
        action="store_true",
        help="Run daily autopilot planner once (same path as scheduler).",
    )
    return parser.parse_args()


def main() -> int:
    load_env_file(Path(".env").resolve())
    load_env_file(Path("quantura_site/functions/.env").resolve())
    args = parse_args()
    fx = load_functions_module()

    channels = fx._normalize_social_channels(args.channels)
    if args.auto_filter_channels and hasattr(fx, "_configured_social_posting_channels"):
        routable = fx._configured_social_posting_channels(channels)
        skipped = [channel for channel in channels if channel not in routable]
        if skipped:
            print(f"Skipping channels without posting route: {', '.join(skipped)}")
        channels = routable
    if not channels:
        print("No routable channels selected. Configure direct API credentials or SOCIAL_WEBHOOK_* variables.")
        return 0
    if hasattr(fx, "_social_channel_posting_route"):
        print("Posting routes:")
        for channel in channels:
            route = str(fx._social_channel_posting_route(channel))
            print(f"  - {channel}: {route}")

    drafts, model_used = fx._generate_social_copy_with_openai(
        topic=args.topic,
        objective=args.objective,
        audience=args.audience,
        tone=args.tone,
        channels=channels,
        posts_per_channel=max(1, min(int(args.posts_per_channel), 8)),
        cta_url=args.cta_url,
    )
    print(f"Generated drafts with model: {model_used}")

    if args.publish_now_first or args.publish_all_now:
        now_iso = datetime.now(timezone.utc).replace(second=0, microsecond=0).isoformat().replace("+00:00", "Z")
        for channel in channels:
            posts = drafts.get(channel) if isinstance(drafts, dict) else None
            if not isinstance(posts, list):
                continue
            for idx, post in enumerate(posts):
                if not isinstance(post, dict):
                    continue
                if args.publish_all_now or idx == 0:
                    post["suggestedPostTime"] = now_iso
        if args.publish_all_now:
            print("Queued posts will publish immediately for all generated drafts.")
        else:
            print("Queued posts will publish immediately for the first draft in each channel.")

    if args.send_now:
        print("\nImmediate smoke-test dispatch:")
        for channel in channels:
            posts = drafts.get(channel) if isinstance(drafts, dict) else None
            if not posts:
                print(f"  - {channel}: skipped (no draft)")
                continue
            first = posts[0] if isinstance(posts[0], dict) else {}
            result = fx._post_to_social_channel(
                platform=channel,
                body=str(first.get("body") or ""),
                headline=str(first.get("headline") or ""),
                hashtags=first.get("hashtags") if isinstance(first.get("hashtags"), list) else [],
                cta=str(first.get("cta") or ""),
                cta_url=str(first.get("ctaUrl") or args.cta_url),
                campaign_id=f"cli_smoke_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}",
                queue_id=f"manual_{channel}_{int(datetime.now(timezone.utc).timestamp())}",
            )
            ok = bool(result.get("ok"))
            print(f"  - {channel}: {'OK' if ok else 'FAILED'} | {result.get('error', '')[:180]}")

    if args.queue_strategic:
        campaign_id = f"cli_campaign_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        enqueue = fx._enqueue_social_posts(
            campaign_id=campaign_id,
            user_id=args.user_id,
            user_email=args.user_email,
            channels=channels,
            drafts=drafts,
            scheduled_for=datetime.now(timezone.utc),
            meta={"source": "cli_runner", "strategy": "daypart"},
        )
        print(f"\nQueued strategic posts: campaign={campaign_id}, queued={enqueue.get('count')}")

    if args.dispatch_due:
        summary = fx._dispatch_due_social_posts(
            max_posts=max(1, min(int(args.dispatch_limit), 100)),
            trigger="cli_runner",
            actor_uid=args.user_id,
        )
        print(f"\nDispatch summary: {summary}")

    if args.schedule_autopilot:
        summary = fx._run_social_autopilot_plan(trigger="cli_runner", actor_uid=args.user_id)
        print(f"\nAutopilot schedule summary: {summary}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
