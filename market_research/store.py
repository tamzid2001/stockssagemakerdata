"""Private Firestore checkpoints with expiring, fenced worker ownership.

Uses the repository's existing Firebase worker identity. No raw credentials are
written to disk or artifacts; client Firestore rules deny these collections.
"""

from __future__ import annotations

import json
import os
import time
import gzip
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

    @property
    def archive_ref(self):
        # Already covered by recursive deny-all client rules. Not public data licensing.
        return self.db.collection("market_research_sessions").document(
            "polymarket-replay-archive-v1"
        )

    def _create_once(self, ref, payload):
        @self.fs.transactional
        def update(tx):
            self.check(tx)
            existing = ref.get(transaction=tx)
            if not existing.exists:
                tx.create(ref, payload)

        update(self.db.transaction())

    def save_catalog(self, contracts, coverage, as_of):
        identifier = digest(
            {"contracts": contracts, "coverage": coverage, "as_of": as_of}
        )
        ref = self.archive_ref.collection("catalogs").document(identifier)
        chunks = [contracts[i : i + 100] for i in range(0, len(contracts), 100)]
        for index, chunk in enumerate(chunks):
            self._create_once(
                ref.collection("chunks").document(str(index)), {"contracts": chunk}
            )
        self._create_once(
            ref,
            {
                "chunk_count": len(chunks),
                "coverage": coverage,
                "as_of": as_of,
                "contract_count": len(contracts),
            },
        )
        return identifier

    def load_catalog(self, identifier):
        ref = self.archive_ref.collection("catalogs").document(identifier)
        metadata = ref.get().to_dict()
        if not metadata:
            raise ValueError("CATALOG_NOT_FOUND")
        contracts = []
        for index in range(metadata["chunk_count"]):
            chunk = ref.collection("chunks").document(str(index)).get().to_dict()
            if not chunk:
                raise RuntimeError("INCOMPLETE_CATALOG")
            contracts.extend(chunk["contracts"])
        if len(contracts) != metadata["contract_count"]:
            raise RuntimeError("CATALOG_COUNT_MISMATCH")
        return contracts, metadata

    def archive_history(self, contract, start, end, rows):
        if len(rows) > 100_000:
            raise ValueError("ARCHIVE_ROW_LIMIT")
        checksum = digest(rows)
        identifier = digest(
            {
                "symbol": contract["providerSymbol"],
                "start": start,
                "end": end,
                "checksum": checksum,
            }
        )
        ref = self.archive_ref.collection("snapshots").document(identifier)
        chunks = [rows[i : i + 500] for i in range(0, len(rows), 500)]
        for index, chunk in enumerate(chunks):
            compressed = gzip.compress(
                json.dumps(chunk, allow_nan=False).encode(), mtime=0
            )
            if len(compressed) > 600_000:
                raise ValueError("ARCHIVE_CHUNK_LIMIT")
            self._create_once(
                ref.collection("chunks").document(str(index)), {"gzip_json": compressed}
            )
        metadata = {
            "snapshot_id": identifier,
            "provider": "polymarket_us",
            "start": start,
            "end": end,
            "checksum": checksum,
            "row_count": len(rows),
            "chunk_count": len(chunks),
            "contract": contract,
            "retrieved_at": time.time(),
            "redistribution_status": "review_required",
        }
        self._create_once(ref, metadata)
        key = digest({"symbol": contract["providerSymbol"], "start": start, "end": end})

        @self.fs.transactional
        def publish(tx):
            self.check(tx)
            tx.set(
                self.archive_ref.collection("ranges").document(key),
                {"snapshot_id": identifier},
            )

        publish(self.db.transaction())
        return {"snapshot_id": identifier, "row_count": len(rows), "checksum": checksum}

    def archived_history(self, contract, start, end):
        key = digest({"symbol": contract["providerSymbol"], "start": start, "end": end})
        pointer = self.archive_ref.collection("ranges").document(key).get().to_dict()
        if not pointer:
            return None
        ref = self.archive_ref.collection("snapshots").document(pointer["snapshot_id"])
        meta = ref.get().to_dict()
        if not meta:
            raise RuntimeError("ARCHIVE_MANIFEST_MISSING")
        rows = []
        for index in range(meta["chunk_count"]):
            chunk = ref.collection("chunks").document(str(index)).get().to_dict()
            if not chunk:
                raise RuntimeError("ARCHIVE_CHUNK_MISSING")
            rows.extend(json.loads(gzip.decompress(chunk["gzip_json"])))
        if len(rows) != meta["row_count"] or digest(rows) != meta["checksum"]:
            raise RuntimeError("ARCHIVE_CHECKSUM_MISMATCH")
        for row in rows:
            row["selected_position"] = contract["side"]
            row.setdefault("raw", {})["selected_position"] = contract["side"]
        return rows

    def archive_progress(self, catalog_id, index, contract, result, report):
        entry = {
            "catalog_id": catalog_id,
            "index": index,
            "contract": contract,
            "result": result,
        }
        self._create_once(
            self.archive_ref.collection("attempts").document(digest(entry)), entry
        )

        @self.fs.transactional
        def update(tx):
            self.check(tx)
            tx.set(self.archive_ref.collection("progress").document(catalog_id), report)

        update(self.db.transaction())
