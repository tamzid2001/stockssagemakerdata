"""Compressed, authenticated-encrypted research artifacts; no plaintext uploads.

QUANTURA_RESEARCH_ARTIFACT_KEY is a 32-byte hex key from repository secrets.
The key, credentials and model cache are never included in the ZIP or manifest.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import tempfile
import zipfile

MAGIC = b"QRA1"
MAX_ARTIFACT_BYTES = 25 * 1024 * 1024


def key_bytes():
    raw = os.environ.get("QUANTURA_RESEARCH_ARTIFACT_KEY", "")
    if len(raw) != 64:
        raise ValueError("ARTIFACT_ENCRYPTION_KEY_REQUIRED")
    try:
        decoded = bytes.fromhex(raw)
        if len(decoded) != 32:
            raise ValueError("invalid length")
        return decoded
    except ValueError:
        raise ValueError("ARTIFACT_ENCRYPTION_KEY_INVALID") from None


def package(directory, destination):
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    key = key_bytes()  # Fail before collecting/uploading confidential output.
    root, target = Path(directory), Path(destination)
    if root.is_symlink() or not root.is_dir() or target.exists():
        raise ValueError("INVALID_ARTIFACT_PATH")
    files = sorted(
        p
        for p in root.iterdir()
        if p.name in {"research.sqlite3", "forecast_quantiles.csv.gz", "quantile_manifest.json"}
        or (p.name.startswith("report-") and p.suffix == ".json")
    )
    if not files or any(p.is_symlink() or not p.is_file() for p in files):
        raise ValueError("UNSAFE_OR_EMPTY_ARTIFACT")
    manifest = {
        "schema_version": 1,
        "paper_only": True,
        "commit": os.environ.get("QUANTURA_CODE_SHA", os.environ.get("GITHUB_SHA")),
        "run_id": os.environ.get("GITHUB_RUN_ID"),
        "files": [],
    }
    with tempfile.TemporaryFile() as compressed:
        with zipfile.ZipFile(
            compressed, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
        ) as archive:
            for path in files:
                with path.open("rb") as handle:
                    checksum = hashlib.file_digest(handle, "sha256").hexdigest()
                manifest["files"].append(
                    {
                        "name": path.name,
                        "sha256": checksum,
                        "bytes": path.stat().st_size,
                    }
                )
                archive.write(path, path.name)
            archive.writestr("manifest.json", json.dumps(manifest, indent=2))
        if compressed.tell() + 32 > MAX_ARTIFACT_BYTES:
            raise ValueError("ARTIFACT_EXCEEDS_25_MIB_BUDGET")
        compressed.seek(0)
        nonce = os.urandom(12)
        encryptor = Cipher(algorithms.AES(key), modes.GCM(nonce)).encryptor()
        encryptor.authenticate_additional_data(MAGIC)
        with target.open("xb") as handle:
            os.chmod(target, 0o600)
            handle.write(MAGIC + nonce)
            while block := compressed.read(1024 * 1024):
                handle.write(encryptor.update(block))
            handle.write(encryptor.finalize())
            handle.write(encryptor.tag)
    with target.open("rb") as handle:
        checksum = hashlib.file_digest(handle, "sha256").hexdigest()
    return {
        "format": "AES-256-GCM encrypted ZIP",
        "bytes": target.stat().st_size,
        "sha256": checksum,
    }


def decrypt(source, destination):
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    target = Path(destination)
    if target.exists():
        raise ValueError("OUTPUT_ALREADY_EXISTS")
    # Authenticate completely before publishing any plaintext output.
    with Path(source).open("rb") as handle, tempfile.TemporaryFile() as clear:
        header = handle.read(16)
        if len(header) != 16 or header[:4] != MAGIC:
            raise ValueError("INVALID_ENCRYPTED_ARTIFACT")
        size = handle.seek(0, 2)
        if not 32 < size <= MAX_ARTIFACT_BYTES:
            raise ValueError("INVALID_ARTIFACT_SIZE")
        handle.seek(-16, 2)
        decryptor = Cipher(
            algorithms.AES(key_bytes()), modes.GCM(header[4:], handle.read(16))
        ).decryptor()
        decryptor.authenticate_additional_data(MAGIC)
        handle.seek(16)
        remaining = size - 32
        while remaining:
            block = handle.read(min(1024 * 1024, remaining))
            remaining -= len(block)
            clear.write(decryptor.update(block))
        clear.write(decryptor.finalize())
        clear.seek(0)
        with target.open("xb") as output:
            os.chmod(target, 0o600)
            while block := clear.read(1024 * 1024):
                output.write(block)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "operation", choices=["check-key", "package", "decrypt", "restore", "restore-backtest", "checkpoint"]
    )
    parser.add_argument("--source")
    parser.add_argument("--output")
    args = parser.parse_args()
    if args.operation == "check-key":
        key_bytes()
        print("Artifact encryption configured; key not displayed.")
    elif args.operation == "package":
        print(json.dumps(package(args.source, args.output)))
    elif args.operation == "checkpoint":
        snapshot_package(args.source, args.output)
    elif args.operation in {"restore", "restore-backtest"}:
        restore(args.source, args.output, preserve_results=args.operation == "restore-backtest")
        print("Authenticated checkpoint restored to runner-local storage.")
    else:
        decrypt(args.source, args.output)
        print("Authenticated archive decrypted locally.")


def snapshot_package(directory, destination):
    """SQLite online backup gives a consistent checkpoint even during inference."""
    import sqlite3
    import shutil
    root = Path(directory)
    with tempfile.TemporaryDirectory() as temporary:
        with sqlite3.connect(f"file:{root / 'research.sqlite3'}?mode=ro", uri=True) as source:
            with sqlite3.connect(Path(temporary) / "research.sqlite3") as target:
                source.backup(target)
        # Explicit safe exports only, never environment files/model caches.
        # The manifest is written after the CSV closes. Intermediate snapshots
        # still recover all forecast records from SQLite when no export exists.
        manifest = root / "quantile_manifest.json"
        csv = root / "forecast_quantiles.csv.gz"
        if manifest.exists():
            if manifest.is_symlink() or csv.is_symlink():
                raise ValueError("UNSAFE_EXPORT_PATH")
            expected = json.loads(manifest.read_text())["sha256"]
            with csv.open("rb") as data:
                if hashlib.file_digest(data, "sha256").hexdigest() != expected:
                    raise ValueError("EXPORT_CHECKSUM_MISMATCH")
            for path in (manifest, csv):
                shutil.copyfile(path, Path(temporary) / path.name)
        return package(temporary, destination)


def restore(source, directory, preserve_results=False):
    import sqlite3

    root = Path(directory)
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    target = root / "research.sqlite3"
    if root.is_symlink() or target.exists():
        raise ValueError("RESTORE_REQUIRES_EMPTY_OUTPUT")
    with tempfile.TemporaryDirectory() as temporary:
        clear = Path(temporary) / "verified.zip"
        decrypt(source, clear)
        with zipfile.ZipFile(clear) as archive:
            manifest = json.loads(archive.read("manifest.json"))
            entry = next(
                f for f in manifest["files"] if f["name"] == "research.sqlite3"
            )
            info = archive.getinfo("research.sqlite3")
            if info.file_size > 32 * 1024 * 1024:
                raise ValueError("RESTORE_DATABASE_TOO_LARGE")
            data = archive.read(info)
            if hashlib.sha256(data).hexdigest() != entry["sha256"]:
                raise ValueError("RESTORE_CHECKSUM_MISMATCH")
            with target.open("xb") as handle:
                os.chmod(target, 0o600)
                handle.write(data)
        with sqlite3.connect(f"file:{target}?mode=ro", uri=True) as db:
            if db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise ValueError("RESTORE_DATABASE_INVALID")
        # A continuation artifact is a new bounded shard, not a growing copy of
        # every prior dataset. Original results remain in the source artifact.
        # Retain immutable catalogs/configuration required by catalog+offset.
        if not preserve_results:
            with sqlite3.connect(target) as db:
                db.execute("DELETE FROM records WHERE kind NOT IN ('catalogs','configuration')")
                db.commit()
                db.execute("VACUUM")
        with Path(source).open("rb") as handle:
            source_hash = hashlib.file_digest(handle, "sha256").hexdigest()
        with (root / "report-restored-source.json").open("x") as handle:
            json.dump(
                {
                    "source_artifact_sha256": source_hash,
                    "restoration": "complete_backtest_checkpoint" if preserve_results else "catalog_only_new_shard",
                    "prior_results": "retained_in_original_encrypted_artifact",
                },
                handle,
            )


if __name__ == "__main__":
    main()
