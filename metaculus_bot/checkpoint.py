"""Encrypted recovery for API cooldowns and generations not yet saved as notes."""
from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import os
import subprocess
import time
import zipfile
from pathlib import Path

from cryptography.fernet import Fernet


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


def restore(path: Path):
    # gh handles the authenticated archive download and its storage redirect.
    # Never extract arbitrary artifact paths or print archive/private contents.
    repo = os.environ["GITHUB_REPOSITORY"]
    for page in range(1, 11):
        result = subprocess.run(["gh", "api", f"repos/{repo}/actions/artifacts?per_page=100&page={page}"],
                                capture_output=True, check=True)
        artifacts = json.loads(result.stdout)["artifacts"]
        candidates = [a for a in artifacts if a["name"].startswith("metaculus-checkpoint-") and not a["expired"]]
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
