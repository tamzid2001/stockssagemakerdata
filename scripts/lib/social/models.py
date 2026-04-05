from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class SocialSource:
    kind: str
    source_id: str
    title: str
    summary: str
    canonical_url: str = ""
    tags: tuple[str, ...] = ()
    topic: str = ""
    published_at: str = ""
    media_url: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SocialDraft:
    channel: str
    source: SocialSource
    headline: str
    body: str
    hashtags: tuple[str, ...] = ()
    cta: str = ""
    cta_url: str = ""
    media_url: str = ""
    used_ai: bool = False
    model_used: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PublishResult:
    ok: bool
    channel: str
    external_id: str = ""
    external_url: str = ""
    status: str = ""
    message: str = ""
    retryable: bool = False
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RunContext:
    channel: str
    dry_run: bool
    force: bool
    source_mode: str
    topic: str = ""
    objective: str = ""
    audience: str = ""
    tone: str = ""
    cta_url: str = ""
    output_dir: str = ""
    github_run_id: str = ""
    github_run_attempt: str = ""
    github_workflow: str = ""
    github_repository: str = ""
    github_actor: str = ""
    triggered_at: str = ""
    project_id: str = ""


@dataclass(frozen=True)
class WorkflowSummary:
    channel: str
    dry_run: bool
    source_kind: str
    source_id: str
    status: str
    message: str
    used_ai: bool
    model_used: str
    duplicate_skip: bool = False
    external_id: str = ""
    external_url: str = ""
    artifact_path: str = ""
    started_at: str = field(default_factory=lambda: datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
