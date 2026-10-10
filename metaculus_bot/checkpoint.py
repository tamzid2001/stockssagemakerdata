"""Encrypted recovery for API cooldowns and generations not yet saved as notes."""
from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import os
import re
import subprocess
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from cryptography.fernet import Fernet


class PrivateNoteLimit(RuntimeError):
    pass


class Checkpoint:
    def __init__(self, path=None, token=None):
        self.path = Path(path) if path else None
        self.state = {"pending": {}, "not_before": 0}
        self.cipher = None
        if self.path:
            key = hashlib.sha256(b"quantura-metaculus-checkpoint-v1:" + token.encode()).digest()
            self.cipher = Fernet(base64.urlsafe_b64encode(key))
            if self.path.exists():
                self.state = json.loads(self.cipher.decrypt(self.path.read_bytes()))

    def save(self):
        if self.path:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temp = self.path.with_suffix(".tmp")
            temp.write_bytes(self.cipher.encrypt(json.dumps(self.state, allow_nan=False).encode()))
            temp.chmod(0o600)
            temp.replace(self.path)

    def get(self, question_id):
        return self.state["pending"].get(str(question_id))

    def put(self, record):
        self.state["pending"][str(record["question_id"])] = record
        self.save()

    def remove(self, question_id):
        self.state["pending"].pop(str(question_id), None)
        self.save()

    def defer(self, seconds):
        self.state["not_before"] = max(self.state["not_before"], time.time() + seconds)
        self.save()

    def remaining(self):
        return max(0, self.state["not_before"] - time.time())

    def reserve_note_attempt(self, now=None):
        day = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).date().isoformat()
        budget = self.state.get("private_notes", {})
        count = budget.get("attempts", 0) if budget.get("day") == day else 0
        if count >= 12:
            raise PrivateNoteLimit("PRIVATE_NOTE_DAILY_LIMIT")
        # Reserve before the network call. An ambiguous response still consumes
        # capacity; retries cannot cause a burst after a worker restart.
        self.state["private_notes"] = {"day": day, "attempts": count + 1}
        self.save()


def artifact_prefix():
    namespace = os.environ.get("METACULUS_CHECKPOINT_NAMESPACE")
    if namespace:
        if not re.fullmatch(r"bot-[1-9][0-9]*", namespace):
            raise RuntimeError("INVALID_CHECKPOINT_NAMESPACE")
        return f"metaculus-checkpoint-{namespace}-"
    return "metaculus-checkpoint-"


def restore(path: Path):
    # gh handles the authenticated archive download and its storage redirect.
    # Never extract arbitrary artifact paths or print archive/private contents.
    repo = os.environ["GITHUB_REPOSITORY"]
    prefix = artifact_prefix()
    for page in range(1, 11):
        result = subprocess.run(["gh", "api", f"repos/{repo}/actions/artifacts?per_page=100&page={page}"],
                                capture_output=True, check=True)
        artifacts = json.loads(result.stdout)["artifacts"]
        candidates = [a for a in artifacts if a["name"].startswith(prefix) and not a["expired"]]
        if candidates:
            latest = max(candidates, key=lambda a: a["id"])
            archive = subprocess.run(["gh", "api", f"repos/{repo}/actions/artifacts/{latest['id']}/zip"],
                                     capture_output=True, check=True).stdout
            with zipfile.ZipFile(io.BytesIO(archive)) as zipped:
                members = [i for i in zipped.infolist() if i.filename == "metaculus-checkpoint.enc"]
                if len(members) != 1 or members[0].file_size > 2_000_000:
                    raise RuntimeError("INVALID_CHECKPOINT_ARTIFACT")
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(zipped.read(members[0]))
                path.chmod(0o600)
            print(json.dumps({"event": "metaculus_checkpoint_restored", "artifact_id": latest["id"]}), flush=True)
            return
        if len(artifacts) < 100:
            return
    # Avoid silently dropping recovery state when an unrelated artifact backlog
    # exceeds the search bound. A failed restore must stop new generations.
    raise RuntimeError("CHECKPOINT_ARTIFACT_SEARCH_LIMIT")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--restore", type=Path, required=True)
    restore(parser.parse_args().restore)
