"""Private Firestore checkpoints with expiring, fenced worker ownership.

Uses the repository's existing Firebase worker identity. No raw credentials are
written to disk or artifacts; client Firestore rules deny these collections.
"""

from __future__ import annotations

import json
import os
import time
from .engine import digest


def claim_transition(current: dict, holder: str, now: float, ttl: int = 120) -> dict:
    if current.get("expires", 0) > now and current.get("holder") != holder:
        raise RuntimeError("LEASE_HELD")
    fence = current.get("fence", 0) + (
        current.get("holder") != holder or current.get("expires", 0) <= now
    )
    return {"holder": holder, "fence": fence, "expires": now + ttl, "updated_at": now}


class Store:
    def __init__(self, session: str, holder: str):
        import firebase_admin
        from firebase_admin import credentials, firestore

        self.fs = firestore
        if not firebase_admin._apps:
            raw = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")
            credential = (
                credentials.Certificate(json.loads(raw))
                if raw
                else credentials.ApplicationDefault()
            )
            firebase_admin.initialize_app(
                credential,
                {"projectId": os.environ.get("FIREBASE_PROJECT_ID", "quantura-e2e3d")},
            )
        self.db = firestore.client()
        self.session = session
        self.holder = holder
        self.fence = None
        self.ref = self.db.collection("market_research_sessions").document(session)

    def claim(self, configuration=None):
        @self.fs.transactional
        def update(tx):
            old = self.ref.get(transaction=tx).to_dict() or {}
            if (
                configuration is not None
                and old.get("configuration", configuration) != configuration
            ):
                raise RuntimeError("SESSION_CONFIGURATION_CHANGED")
            lease = claim_transition(old.get("lease", {}), self.holder, time.time())
            tx.set(
                self.ref,
                {
                    "lease": lease,
                    "paper_only": True,
                    **(
                        {"configuration": configuration}
                        if configuration is not None
                        else {}
                    ),
                },
                merge=True,
            )
            return lease["fence"]

        self.fence = update(self.db.transaction())

    def check(self, tx):
        current = (self.ref.get(transaction=tx).to_dict() or {}).get("lease", {})
        if (
            current.get("holder") != self.holder
            or current.get("fence") != self.fence
            or current.get("expires", 0) <= time.time()
        ):
            raise RuntimeError("LEASE_LOST")

    def renew(self):
        @self.fs.transactional
        def update(tx):
            self.check(tx)
            tx.set(
                self.ref,
                {
                    "lease": {
                        "holder": self.holder,
                        "fence": self.fence,
                        "expires": time.time() + 120,
                        "updated_at": time.time(),
                    }
                },
                merge=True,
            )

        update(self.db.transaction())

    def release(self):
        @self.fs.transactional
        def update(tx):
            self.check(tx)
            tx.set(
                self.ref,
                {
                    "lease": {"holder": self.holder, "fence": self.fence, "expires": 0},
                    "handoff_ready": True,
                },
                merge=True,
            )

        update(self.db.transaction())

    def load(self, contract_id: str):
        return (
            self.ref.collection("contracts")
            .document(digest(contract_id))
            .get()
            .to_dict()
            or {}
        )

    def tracked(self):
        return [
            s.to_dict().get("contract")
            for s in self.ref.collection("contracts").stream()
            if s.to_dict().get("contract")
        ]

    def save(
        self,
        contract_id: str,
        state: dict,
        forecast: dict | None,
        trades: list[dict],
        contract=None,
    ):
        if len(trades) > 400:
            raise ValueError("CHECKPOINT_BATCH_TOO_LARGE")

        @self.fs.transactional
        def update(tx):
            self.check(tx)
            ref = self.ref.collection("contracts").document(digest(contract_id))
            snapshot_ref = (
                self.db.collection("market_research_forecasts").document(
                    forecast["forecast_id"]
                )
                if forecast
                else None
            )
            existing = (
                snapshot_ref.get(transaction=tx).exists if snapshot_ref else False
            )
            tx.set(
                ref,
                {
                    "contract_id": contract_id,
                    "state": state,
                    "forecast": forecast,
                    "updated_at": time.time(),
                    **({"contract": contract} if contract else {}),
                },
                merge=True,
            )
            if snapshot_ref and not existing:
                tx.create(
                    snapshot_ref,
                    {
                        "session": self.session,
                        "contract_id": contract_id,
                        "forecast": forecast,
                        "published_at": time.time(),
                        "paper_only": True,
                    },
                )
            for trade in trades:
                # Identical replay writes have identical IDs and identical bytes.
                tx.set(
                    self.db.collection("market_research_trades").document(
                        digest({"session": self.session, "trade": trade})
                    ),
                    {"session": self.session, **trade},
                )

        update(self.db.transaction())

    def report(self, run_id: str, report: dict):
        ref = self.db.collection("market_research_runs").document(digest(run_id))
        ref.create(
            {
                "run_id": run_id,
                "session": self.session,
                "paper_only": True,
                "created_at": time.time(),
                "report": report,
            }
        )
