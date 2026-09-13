"""Small fenced Firestore coordinator; immutable encrypted files in Firebase Storage.

Forecast arrays and quote histories never enter Firestore. Finished games leave
the runner only AFTER their content-addressed archive and manifest are durable.
"""
import gzip
import hashlib
import json
import os
import re

from .artifact import key_bytes
from .engine import digest
from .store import Store

BUCKET = "quantura-e2e3d.firebasestorage.app"
MAX_GAME_ARCHIVE = 1024 * 1024 * 1024


def validate_campaign(value):
    if not re.fullmatch(r"p90-[a-f0-9]{24}", value):
        raise ValueError("INVALID_CAMPAIGN_ID")
    return value


class Campaign:
    def __init__(self, identifier, holder):
        from firebase_admin import storage
        self.id = validate_campaign(identifier)
        self.lease = Store(self.id, holder)
        self.bucket = storage.bucket(BUCKET)
        self.prefix = f"private-research/p90-campaigns/{self.id}/"

    def load(self):
        return self.lease.ref.get().to_dict() or {}

    def update(self, fields):
        @self.lease.fs.transactional
        def change(tx):
            self.lease.check(tx)
            tx.set(self.lease.ref, fields, merge=True)
        self.lease.transact(change)

    def upload(self, path, kind="game"):
        from google.api_core.exceptions import PreconditionFailed
        with open(path, "rb") as source:
            checksum = hashlib.file_digest(source, "sha256").hexdigest()
        size = os.path.getsize(path)
        if size > MAX_GAME_ARCHIVE or kind not in ("game", "catalog", "checkpoint"):
            raise ValueError("INVALID_CLOUD_ARCHIVE")
        name = self.prefix + kind + "/" + checksum + ".enc"
        blob = self.bucket.blob(name)
        blob.metadata = {"sha256": checksum, "paper_only": "true", "redistribution_status": "review_required"}
        try:
            blob.upload_from_filename(path, content_type="application/octet-stream", if_generation_match=0, checksum="auto")
        except PreconditionFailed:
            blob.reload()
            if blob.metadata.get("sha256") != checksum or blob.size != size:
                raise RuntimeError("IMMUTABLE_ARCHIVE_CONFLICT")
        return {"object": name, "sha256": checksum, "bytes": size, "generation": str(blob.generation)}

    def download(self, pointer, path):
        if not pointer["object"].startswith(self.prefix) or not re.fullmatch(r"[a-f0-9]{64}", pointer["sha256"]):
            raise ValueError("INVALID_CAMPAIGN_ARCHIVE_REFERENCE")
        blob = self.bucket.blob(pointer["object"], generation=int(pointer["generation"]))
        blob.download_to_filename(path, checksum="auto")
        with open(path, "rb") as source:
            if hashlib.file_digest(source, "sha256").hexdigest() != pointer["sha256"]:
                raise RuntimeError("CLOUD_ARCHIVE_CHECKSUM_MISMATCH")

    def game(self, identifier):
        return self.lease.ref.collection("games").document(digest(identifier)).get().to_dict()

    def finish_game(self, identifier, record, next_state):
        if set(record) - {"game_id", "archive", "result", "forecast_count", "summary", "code_sha", "completed_at"} or len(json.dumps(record)) > 60000:
            raise ValueError("SMALL_GAME_METADATA_ONLY")
        ref = self.lease.ref.collection("games").document(digest(identifier))
        @self.lease.fs.transactional
        def finish(tx):
            self.lease.check(tx)
            state = self.lease.ref.get(transaction=tx).to_dict() or {}
            existing = ref.get(transaction=tx)
            if not existing.exists:
                tx.create(ref, record)
                summary = add_summary(state.get("summary", {}), record)
            elif existing.to_dict()["archive"] != record["archive"]:
                raise RuntimeError("GAME_ALREADY_ARCHIVED")
            else:
                summary = state.get("summary", {})
            tx.set(self.lease.ref, {**next_state, "summary": summary}, merge=True)
        self.lease.transact(finish)


def add_summary(previous, record):
    out = dict(previous)
    row = next(iter(record["summary"].values()))
    for key in ("closed_trades", "wins", "losses", "breakeven", "gross_pnl", "net_pnl", "fees",
                "closed_entry_notional", "open_positions", "open_mark_net_pnl", "open_entry_notional",
                "recovery_signals", "multiplier_increases"):
        out[key] = out.get(key, 0) + row[key]
    for key in ("longest_winning_streak", "longest_losing_streak", "max_quantity_used"):
        field = "max_within_game_" + key
        out[field] = max(out.get(field, 0), row[key])
    out["games_archived"] = out.get("games_archived", 0) + 1
    out["forecast_count"] = out.get("forecast_count", 0) + record["forecast_count"]
    out["games_with_forecasts"] = out.get("games_with_forecasts", 0) + (record["forecast_count"] > 0)
    out["win_rate"] = out["wins"] / out["closed_trades"] if out["closed_trades"] else None
    out["net_return_on_closed_entry_notional"] = out["net_pnl"] / out["closed_entry_notional"] if out["closed_entry_notional"] else None
    return out


def encode_catalog(value, path):
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    nonce = os.urandom(12)
    clear = gzip.compress(json.dumps(value, allow_nan=False).encode(), mtime=0)
    data = b"QRC1" + nonce + AESGCM(key_bytes()).encrypt(nonce, clear, b"QRC1")
    with open(path, "xb") as out:
        os.chmod(path, 0o600); out.write(data)


def decode_catalog(path):
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    with open(path, "rb") as source:
        data = source.read(16 * 1024 * 1024 + 1)
    if data[:4] != b"QRC1" or len(data) > 16 * 1024 * 1024:
        raise ValueError("INVALID_CATALOG_ARCHIVE")
    return json.loads(gzip.decompress(AESGCM(key_bytes()).decrypt(data[4:16], data[16:], b"QRC1")))
