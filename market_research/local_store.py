"""Runner-local research storage. Historical jobs never initialize Firebase.

The completed SQLite database is compressed/encrypted by the artifact step.
Content-addressed snapshots survive retries without overwriting earlier bytes.
"""

import gzip
import json
import os
from pathlib import Path
import sqlite3
import threading
import time

from .engine import digest


class LocalStore:
    storage_name = "encrypted_github_artifact"

    def __init__(self, session, holder, directory=None):
        self.session, self.holder = session, holder
        self.directory = Path(
            directory or os.environ.get("QUANTURA_RESEARCH_DIR", ".research-output")
        )
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.path = self.directory / "research.sqlite3"
        if self.path.is_symlink():
            raise ValueError("UNSAFE_DATABASE_PATH")
        self.lock = threading.RLock()
        self.db = sqlite3.connect(self.path, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=DELETE")
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS records (kind TEXT, id TEXT, data BLOB NOT NULL, PRIMARY KEY(kind,id))"
        )
        self.db.commit()
        os.chmod(self.path, 0o600)
        self.active = False

    @property
    def at_capacity(self):
        # Leave space under the artifact's 25 MiB upload cap for reports/manifests.
        return self.path.stat().st_size >= 20 * 1024 * 1024

    def _get(self, kind, identifier):
        with self.lock:
            row = self.db.execute(
                "SELECT data FROM records WHERE kind=? AND id=?", (kind, identifier)
            ).fetchone()
            return json.loads(gzip.decompress(row[0])) if row else None

    def _put(self, kind, identifier, value, immutable=False):
        data = gzip.compress(
            json.dumps(value, allow_nan=False, separators=(",", ":")).encode(), mtime=0
        )
        self.db.execute(
            "INSERT OR IGNORE INTO records VALUES (?,?,?)"
            if immutable
            else "INSERT INTO records VALUES (?,?,?) ON CONFLICT(kind,id) DO UPDATE SET data=excluded.data",
            (kind, identifier, data),
        )

    def values(self, kind):
        with self.lock:
            return [json.loads(gzip.decompress(row[0])) for row in self.db.execute("SELECT data FROM records WHERE kind=?", (kind,)).fetchall()]

    def checkpoint(self, identifier, value):
        with self.lock, self.db:
            self._put("checkpoints", identifier, value)

    def claim(self, configuration=None):
        with self.lock, self.db:
            old = self._get("configuration", self.session)
            if old is not None and old != configuration:
                raise RuntimeError("SESSION_CONFIGURATION_CHANGED")
            self._put("configuration", self.session, configuration, True)
            self.active = True

    def renew(self):
        if not self.active:
            raise RuntimeError("LOCAL_SESSION_CLOSED")

    def release(self):
        with self.lock:
            self.db.commit()
            self.active = False

    def load(self, contract_id):
        return self._get("contracts", digest([self.session, contract_id])) or {}

    def tracked(self):
        return []  # Local storage is never used for live handoffs.

    def save(self, contract_id, state, forecast, trades, contract=None):
        self.renew()
        with self.lock, self.db:
            self._put(
                "contracts",
                digest([self.session, contract_id]),
                {
                    "contract_id": contract_id,
                    "state": state,
                    "forecast": forecast,
                    "contract": contract,
                },
            )
            if forecast:
                self._put("forecasts", forecast["forecast_id"], forecast, True)
            for trade in trades:
                self._put(
                    "trades",
                    digest([self.session, trade]),
                    {"session": self.session, **trade},
                    True,
                )

    def report(self, run_id, report):
        with self.lock, self.db:
            self._put(
                "reports",
                run_id,
                {
                    "run_id": run_id,
                    "session": self.session,
                    "created_at": time.time(),
                    "report": report,
                },
                True,
            )
        # A human-readable report lives only inside the encrypted artifact.
        filename = self.directory / ("report-" + digest(run_id)[:16] + ".json")
        if not filename.exists():
            with filename.open("x", encoding="utf-8") as handle:
                json.dump(report, handle, allow_nan=False, indent=2)

    def save_catalog(self, contracts, coverage, as_of):
        value = {"contracts": contracts, "coverage": coverage, "as_of": as_of}
        identifier = digest(value)
        with self.lock, self.db:
            self._put("catalogs", identifier, value, True)
        return identifier

    def load_catalog(self, identifier):
        value = self._get("catalogs", identifier)
        if value is None:
            raise ValueError("CATALOG_NOT_FOUND_RESTORE_PRIOR_ARTIFACT")
        return value["contracts"], {k: v for k, v in value.items() if k != "contracts"}

    def _range_key(self, contract, start, end):
        return digest(
            [
                contract.get("source", "polymarket_us"),
                contract["providerSymbol"],
                start,
                end,
            ]
        )

    def archive_history(self, contract, start, end, rows):
        checksum = digest(rows)
        key = self._range_key(contract, start, end)
        identifier = digest([key, checksum])
        with self.lock, self.db:
            self._put(
                "snapshots",
                identifier,
                {
                    "contract": contract,
                    "start": start,
                    "end": end,
                    "rows": rows,
                    "checksum": checksum,
                    "retrieved_at": time.time(),
                },
                True,
            )
            self._put("ranges", key, {"snapshot_id": identifier})
        return {"snapshot_id": identifier, "row_count": len(rows), "checksum": checksum}

    def archived_history(self, contract, start, end):
        pointer = self._get("ranges", self._range_key(contract, start, end))
        if pointer is None:
            return None
        data = self._get("snapshots", pointer["snapshot_id"])
        if data is None or digest(data["rows"]) != data["checksum"]:
            raise RuntimeError("ARCHIVE_CHECKSUM_MISMATCH")
        for row in data["rows"]:
            row["selected_position"] = contract["side"]
            row.setdefault("raw", {})["selected_position"] = contract["side"]
        return data["rows"]

    def archive_progress(self, catalog_id, index, contract, result, report):
        entry = {
            "catalog_id": catalog_id,
            "index": index,
            "contract": contract,
            "result": result,
            "report": report,
        }
        with self.lock, self.db:
            self._put("attempts", digest(entry), entry, True)
            self._put("progress", catalog_id, report)
