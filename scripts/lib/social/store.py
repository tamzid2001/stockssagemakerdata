from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any


class FirestoreStore:
    def __init__(self, project_id: str = "") -> None:
        self.project_id = str(project_id or "").strip()
        self.available = False
        self._client = None
        self._firestore = None
        if not self.project_id:
            return
        try:
            from google.cloud import firestore  # type: ignore

            self._firestore = firestore
            self._client = firestore.Client(project=self.project_id)
            self.available = True
        except Exception:
            self.available = False
            self._client = None
            self._firestore = None

    def publication_doc_id(self, channel: str, source_id: str) -> str:
        digest = hashlib.sha256(f"{channel}:{source_id}".encode("utf-8")).hexdigest()[:40]
        return f"{channel}-{digest}"

    def _doc(self, collection: str, document_id: str):
        if not self.available or not self._client:
            return None
        return self._client.collection(collection).document(document_id)

    def was_published(self, channel: str, source_id: str) -> bool:
        ref = self._doc("social_publications", self.publication_doc_id(channel, source_id))
        if ref is None:
            return False
        snap = ref.get()
        if not snap.exists:
            return False
        data = snap.to_dict() or {}
        return str(data.get("status") or "").lower() == "success"

    def reserve_publication(
        self,
        *,
        channel: str,
        source_id: str,
        run_id: str,
        workflow: str,
        dry_run: bool,
        force: bool,
        metadata: dict[str, Any],
    ) -> tuple[bool, str]:
        if not self.available or not self._client or not self._firestore:
            return True, ""

        ref = self._doc("social_publications", self.publication_doc_id(channel, source_id))
        if ref is None:
            return True, ""
        transaction = self._client.transaction()
        now = datetime.now(timezone.utc)

        @self._firestore.transactional
        def _reserve(txn):
            snap = ref.get(transaction=txn)
            if snap.exists:
                data = snap.to_dict() or {}
                status = str(data.get("status") or "").lower()
                if not force and status == "success":
                    return False, "duplicate_success"
                if not force and status == "in_progress":
                    updated_at = data.get("updatedAt")
                    if hasattr(updated_at, "replace"):
                        if updated_at.tzinfo is None:
                            updated_at = updated_at.replace(tzinfo=timezone.utc)
                        if now - updated_at <= timedelta(minutes=20):
                            return False, "already_in_progress"
            txn.set(
                ref,
                {
                    "channel": channel,
                    "sourceId": source_id,
                    "status": "dry_run" if dry_run else "in_progress",
                    "runId": run_id,
                    "workflow": workflow,
                    "updatedAt": now,
                    "createdAt": snap.to_dict().get("createdAt") if snap.exists else now,
                    "metadata": metadata,
                },
                merge=True,
            )
            return True, ""

        return _reserve(transaction)

    def mark_publication(
        self,
        *,
        channel: str,
        source_id: str,
        status: str,
        run_id: str,
        external_id: str = "",
        external_url: str = "",
        message: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        ref = self._doc("social_publications", self.publication_doc_id(channel, source_id))
        if ref is None:
            return
        ref.set(
            {
                "channel": channel,
                "sourceId": source_id,
                "status": status,
                "runId": run_id,
                "externalId": external_id,
                "externalUrl": external_url,
                "message": message,
                "updatedAt": datetime.now(timezone.utc),
                "postedAt": datetime.now(timezone.utc) if status == "success" else None,
                "metadata": metadata or {},
            },
            merge=True,
        )

    def record_campaign(self, campaign_id: str, payload: dict[str, Any]) -> None:
        ref = self._doc("social_campaigns", campaign_id)
        if ref is None:
            return
        ref.set(payload, merge=True)

    def record_queue(self, queue_id: str, payload: dict[str, Any]) -> None:
        ref = self._doc("social_queue", queue_id)
        if ref is None:
            return
        ref.set(payload, merge=True)

    def record_dispatch_log(self, run_id: str, payload: dict[str, Any]) -> None:
        ref = self._doc("social_dispatch_logs", run_id)
        if ref is None:
            return
        ref.set(payload, merge=True)
