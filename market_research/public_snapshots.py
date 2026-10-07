"""Public screener snapshots in retained Actions artifacts; no database writes.

Only curated screener output belongs here. Private requests, uploaded datasets,
encrypted research reports and credentials are never accepted as a feed.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import zipfile
import requests

SCHEMA = "quantura-public-screener-v1"
REPOSITORY = "tamzid2001/stockssagemakerdata"
FEED = re.compile(r"^(stocks|games-(kalshi|polymarket_us)-[01]|perps-[0-3])$")
WORKFLOWS = {"stocks": "stock-screener.yml", "games": "hourly-game-screener.yml", "perps": "hourly-perpetual-screener.yml"}
MAX_ARCHIVE = 24 * 1024 * 1024
MAX_JSON = 96 * 1024 * 1024

def validate(value, feed):
    if not FEED.fullmatch(feed) or value.get("schema_version") != SCHEMA or value.get("feed") != feed:
        raise ValueError("PUBLIC_SNAPSHOT_INVALID")
    data = value.get("data")
    if not isinstance(data, dict) or not isinstance(data.get("items"), list) or len(data["items"]) > 10000:
        raise ValueError("PUBLIC_SNAPSHOT_INVALID")
    datetime.fromisoformat(value["published_at"].replace("Z", "+00:00"))
    return value

def write(feed, data, output):
    value = validate({"schema_version": SCHEMA, "feed": feed,
                      "published_at": datetime.now(timezone.utc).isoformat(), "data": data}, feed)
    raw = json.dumps(value, default=lambda v: v.isoformat(), separators=(",", ":"), allow_nan=False).encode()
    if len(raw) > MAX_JSON:
        raise ValueError("PUBLIC_SNAPSHOT_TOO_LARGE")
    packed = gzip.compress(raw, compresslevel=9, mtime=0)
    if len(packed) > MAX_ARCHIVE:
        raise ValueError("PUBLIC_SNAPSHOT_TOO_LARGE")
    path = Path(output) / "snapshot.json.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_bytes(packed)
    temporary.replace(path)
    return path

def unpack(packed, feed):
    # A bounded streaming read also guards a small compressed decompression bomb.
    with gzip.GzipFile(fileobj=io.BytesIO(packed)) as stream:
        raw = stream.read(MAX_JSON + 1)
    if len(raw) > MAX_JSON:
        raise ValueError("PUBLIC_SNAPSHOT_TOO_LARGE")
    return validate(json.loads(raw), feed)

def previous(feed):
    if not FEED.fullmatch(feed):
        raise ValueError("PUBLIC_FEED_INVALID")
    session = requests.Session()
    headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2026-03-10"}
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GITHUB_ACTIONS_TOKEN")
    if token:
        headers["Authorization"] = "Bearer " + token
    root = "https://api.github.com/repos/" + REPOSITORY
    response = session.get(root + "/actions/artifacts", params={"name": "quantura-public-" + feed, "per_page": 100}, headers=headers, timeout=30)
    response.raise_for_status()
    # GitHub IDs are partitioned; sort uploads by timestamp, never numeric ID.
    artifacts = sorted(response.json()["artifacts"], key=lambda a: (a.get("created_at", ""), a["id"]), reverse=True)
    for artifact in artifacts:
        run = artifact.get("workflow_run", {})
        if artifact["expired"] or run.get("head_branch") != "main" or run.get("head_repository_id") != run.get("repository_id"):
            continue
        response = session.get(root + f"/actions/runs/{run['id']}", headers=headers, timeout=30)
        response.raise_for_status()
        info = response.json()
        if info.get("path") not in {".github/workflows/" + WORKFLOWS[feed.split("-")[0]], ".github/workflows/screener-artifact-bootstrap.yml"} or info.get("head_sha") != run.get("head_sha"):
            continue
        redirect = session.get(root + f"/actions/artifacts/{artifact['id']}/zip", headers=headers, timeout=30, allow_redirects=False)
        if redirect.status_code != 302:
            continue
        from urllib.parse import urlparse
        url = redirect.headers["Location"]
        parsed = urlparse(url)
        if parsed.scheme != "https" or not any((parsed.hostname or "").endswith(d) for d in (".blob.core.windows.net", ".githubusercontent.com")):
            raise ValueError("PUBLIC_ARTIFACT_REDIRECT_INVALID")
        response = requests.get(url, timeout=60, stream=True)  # no GitHub credentials on the redirect
        response.raise_for_status()
        content = bytearray()
        for chunk in response.iter_content(1024 * 1024):
            content.extend(chunk)
            if len(content) > MAX_ARCHIVE:
                raise ValueError("PUBLIC_SNAPSHOT_TOO_LARGE")
        digest = artifact.get("digest")
        if digest and digest != "sha256:" + hashlib.sha256(content).hexdigest():
            raise ValueError("PUBLIC_ARTIFACT_DIGEST_INVALID")
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            entries = archive.infolist()
            if len(entries) != 1 or entries[0].filename != "snapshot.json.gz" or entries[0].file_size > MAX_ARCHIVE:
                raise ValueError("PUBLIC_ARTIFACT_PATH_INVALID")
            return unpack(archive.read(entries[0]), feed)["data"]
    # Never silently replace missing publication history with an empty catalog.
    raise ValueError("PUBLIC_SNAPSHOT_NOT_PUBLISHED")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--feed", required=True)
    parser.add_argument("--seed", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    packed = Path(args.seed).read_bytes()
    value = unpack(packed, args.feed)
    write(args.feed, value["data"], args.output)
