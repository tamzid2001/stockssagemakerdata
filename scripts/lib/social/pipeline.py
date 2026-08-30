from __future__ import annotations

import json
import os
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import load_channel_config
from .content import DEFAULT_VISUAL_URL, choose_social_source
from .drafting import build_social_draft
from .models import RunContext, WorkflowSummary
from .providers import ProviderError, get_provider
from .providers.base import ProviderContext, execute_with_retry
from .secrets import SecretResolver
from .store import FirestoreStore


def _event(name: str, **fields: Any) -> None:
    payload = {"event": name, **fields}
    print(json.dumps(payload, ensure_ascii=True))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    if ".." in str(path):
        raise Exception("Invalid file path")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def _summary_to_dict(summary: WorkflowSummary, draft: dict[str, Any], source: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    return {
        "channel": summary.channel,
        "dryRun": summary.dry_run,
        "sourceKind": summary.source_kind,
        "sourceId": summary.source_id,
        "status": summary.status,
        "message": summary.message,
        "usedAi": summary.used_ai,
        "modelUsed": summary.model_used,
        "duplicateSkip": summary.duplicate_skip,
        "externalId": summary.external_id,
        "externalUrl": summary.external_url,
        "artifactPath": summary.artifact_path,
        "startedAt": summary.started_at,
        "draft": draft,
        "source": source,
        "config": {
            "channel": config.get("channel"),
            "provider": config.get("provider"),
            "schedule": config.get("schedule"),
            "maxBodyLength": config.get("maxBodyLength"),
            "notes": config.get("notes") or [],
        },
    }


def _validate_secret_requirements(resolver: SecretResolver, channel_config: dict[str, Any]) -> tuple[list[str], list[str]]:
    missing: list[str] = []
    found: list[str] = []

    for secret_name in channel_config.get("secretNames") or []:
        if resolver.get(secret_name, ""):
            found.append(secret_name)
        else:
            missing.append(secret_name)

    secret_sets = channel_config.get("secretSets") or []
    if secret_sets:
        set_matched = False
        normalized_groups: list[list[str]] = []
        for secret_group in secret_sets:
            group = [str(item).strip() for item in secret_group if str(item).strip()]
            if not group:
                continue
            normalized_groups.append(group)
            if all(resolver.get(secret_name, "") for secret_name in group):
                found.extend(group)
                set_matched = True
                break
        if normalized_groups and not set_matched:
            missing.append(" or ".join(" + ".join(group) for group in normalized_groups))

    return missing, found


def run_social_channel(ctx: RunContext) -> WorkflowSummary:
    started_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    channel_config = load_channel_config(ctx.channel)
    output_dir = Path(ctx.output_dir or "artifacts/social")
    output_dir.mkdir(parents=True, exist_ok=True)
    resolver = SecretResolver(ctx.project_id)
    store = FirestoreStore(ctx.project_id)

    _event("social_run_started", channel=ctx.channel, dryRun=ctx.dry_run, sourceMode=ctx.source_mode, projectId=ctx.project_id)

    if not ctx.dry_run:
        missing, _ = _validate_secret_requirements(resolver, channel_config)
        if missing:
            raise RuntimeError(
                f"Missing required secrets for {ctx.channel}: {', '.join(missing)}. "
                f"Populate Google Cloud Secret Manager before running this workflow."
            )

    default_media_url = str(channel_config.get("defaultMediaUrl") or resolver.get("INSTAGRAM_DEFAULT_IMAGE_URL", "") or resolver.get("TIKTOK_DEFAULT_MEDIA_URL", "") or DEFAULT_VISUAL_URL)
    default_cta_url = str(ctx.cta_url or channel_config.get("defaults", {}).get("ctaUrl") or "https://quantura.studio/forecasting")
    source = choose_social_source(
        channel=ctx.channel,
        source_mode=ctx.source_mode,
        store=store,
        topic=ctx.topic,
        cta_url=default_cta_url,
        default_media_url=default_media_url,
    )
    draft = build_social_draft(
        channel_config=channel_config,
        source=source,
        resolver=resolver,
        objective=ctx.objective or str(channel_config.get("defaults", {}).get("objective") or ""),
        audience=ctx.audience or str(channel_config.get("defaults", {}).get("audience") or ""),
        tone=ctx.tone or str(channel_config.get("defaults", {}).get("tone") or ""),
        cta_url=default_cta_url,
    )

    run_id = f"{ctx.github_run_id or 'local'}-{ctx.github_run_attempt or '1'}-{ctx.channel}"
    campaign_id = f"campaign-{run_id}"
    queue_id = f"queue-{run_id}"
    store.record_campaign(
        campaign_id,
        {
            "channel": ctx.channel,
            "sourceId": source.source_id,
            "sourceKind": source.kind,
            "dryRun": ctx.dry_run,
            "workflow": ctx.github_workflow,
            "repository": ctx.github_repository,
            "actor": ctx.github_actor,
            "createdAt": datetime.now(timezone.utc),
            "draft": {
                "headline": draft.headline,
                "body": draft.body,
                "hashtags": list(draft.hashtags),
                "cta": draft.cta,
                "ctaUrl": draft.cta_url,
                "mediaUrl": draft.media_url,
                "usedAi": draft.used_ai,
                "modelUsed": draft.model_used,
            },
            "source": {
                "title": source.title,
                "summary": source.summary,
                "canonicalUrl": source.canonical_url,
                "tags": list(source.tags),
                "publishedAt": source.published_at,
            },
        },
    )
    store.record_queue(
        queue_id,
        {
            "channel": ctx.channel,
            "campaignId": campaign_id,
            "sourceId": source.source_id,
            "status": "queued" if not ctx.dry_run else "dry_run",
            "createdAt": datetime.now(timezone.utc),
            "workflowRunId": ctx.github_run_id,
        },
    )

    reserved, reserve_reason = store.reserve_publication(
        channel=ctx.channel,
        source_id=source.source_id,
        run_id=run_id,
        workflow=ctx.github_workflow,
        dry_run=ctx.dry_run,
        force=ctx.force,
        metadata={"campaignId": campaign_id, "queueId": queue_id, "repository": ctx.github_repository},
    )
    if not reserved:
        message = f"Skipped duplicate publication: {reserve_reason}"
        summary = WorkflowSummary(
            channel=ctx.channel,
            dry_run=ctx.dry_run,
            source_kind=source.kind,
            source_id=source.source_id,
            status="skipped",
            message=message,
            used_ai=draft.used_ai,
            model_used=draft.model_used,
            duplicate_skip=True,
            artifact_path=str(output_dir / f"{ctx.channel}-summary.json"),
            started_at=started_at,
        )
        payload = _summary_to_dict(summary, asdict(draft), asdict(source), channel_config)
        _write_json(Path(summary.artifact_path), payload)
        store.record_dispatch_log(
            run_id,
            {
                "channel": ctx.channel,
                "status": "skipped",
                "message": message,
                "duplicateSkip": True,
                "createdAt": datetime.now(timezone.utc),
            },
        )
        return summary

    provider = get_provider(channel_config.get("provider"), ProviderContext(channel_config=channel_config, resolver=resolver, dry_run=ctx.dry_run))
    retry_config = channel_config.get("retry") or {}
    result = execute_with_retry(
        action=lambda: provider.publish(draft),
        provider_name=ctx.channel,
        max_attempts=max(1, int(retry_config.get("maxAttempts") or 1)),
        base_delay_seconds=max(1, int(retry_config.get("baseDelaySeconds") or 1)),
    )

    status = "success" if result.ok and not ctx.dry_run else "dry_run" if result.ok else "failed"
    message = result.message or ("Published successfully." if result.ok else "Publish failed.")
    summary = WorkflowSummary(
        channel=ctx.channel,
        dry_run=ctx.dry_run,
        source_kind=source.kind,
        source_id=source.source_id,
        status=status,
        message=message,
        used_ai=draft.used_ai,
        model_used=draft.model_used,
        external_id=result.external_id,
        external_url=result.external_url,
        artifact_path=str(output_dir / f"{ctx.channel}-summary.json"),
        started_at=started_at,
    )

    store.mark_publication(
        channel=ctx.channel,
        source_id=source.source_id,
        status=status,
        run_id=run_id,
        external_id=result.external_id,
        external_url=result.external_url,
        message=message,
        metadata={"result": result.raw, "dryRun": ctx.dry_run},
    )
    store.record_queue(
        queue_id,
        {
            "status": status,
            "updatedAt": datetime.now(timezone.utc),
            "externalId": result.external_id,
            "externalUrl": result.external_url,
            "message": message,
        },
    )
    store.record_dispatch_log(
        run_id,
        {
            "channel": ctx.channel,
            "status": status,
            "message": message,
            "externalId": result.external_id,
            "externalUrl": result.external_url,
            "createdAt": datetime.now(timezone.utc),
            "draftUsedAi": draft.used_ai,
            "draftModel": draft.model_used,
            "sourceId": source.source_id,
            "sourceKind": source.kind,
            "raw": result.raw,
        },
    )

    payload = _summary_to_dict(summary, asdict(draft), asdict(source), channel_config)
    _write_json(Path(summary.artifact_path), payload)
    _event(
        "social_run_finished",
        channel=ctx.channel,
        status=status,
        sourceId=source.source_id,
        dryRun=ctx.dry_run,
        externalId=result.external_id,
    )
    return summary
