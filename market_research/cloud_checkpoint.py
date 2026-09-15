"""Encrypted cloud continuation snapshots with tiny authenticated GH pointers.

No records or open books are deleted. Uses the existing private research bucket,
generation-pinned immutable objects and a strict allowlist, never a supplied URL.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import tempfile

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from .artifact import key_bytes
from .engine import digest
from .recovery_cloud import Campaign, MAX_GAME_ARCHIVE

MAGIC = b'QCP1'
MAX_DATABASE_BYTES = 512 * 1024 * 1024


def enabled():
    return os.environ.get('QUANTURA_CLOUD_PAPER_CHECKPOINTS') == 'true'


def write_pointer(pointer, output):
    clear=json.dumps(pointer,sort_keys=True,allow_nan=False).encode()
    if len(clear)>4096:raise ValueError('CLOUD_POINTER_TOO_LARGE')
    nonce=os.urandom(12)
    with Path(output).open('xb') as out:
        os.chmod(output,0o600)
        out.write(MAGIC+nonce+AESGCM(key_bytes()).encrypt(nonce,clear,MAGIC))


def read_pointer(path):
    with Path(path).open('rb') as source:data=source.read(8193)
    if not 32<len(data)<=8192 or data[:4]!=MAGIC:raise ValueError('INVALID_CLOUD_POINTER')
    pointer=json.loads(AESGCM(key_bytes()).decrypt(data[4:16],data[16:],MAGIC))
    if set(pointer)!={'version','campaign_id','archive'} or pointer['version']!=1:
        raise ValueError('INVALID_CLOUD_POINTER_SCHEMA')
    if not re.fullmatch(r'p90-[a-f0-9]{24}',pointer['campaign_id']):raise ValueError('INVALID_CLOUD_CAMPAIGN')
    a=pointer['archive'];prefix=f"private-research/p90-campaigns/{pointer['campaign_id']}/checkpoint/"
    if set(a)!={'object','sha256','bytes','generation'} or not re.fullmatch(r'[a-f0-9]{64}',a['sha256']):
        raise ValueError('INVALID_CLOUD_ARCHIVE')
    if a['object']!=prefix+a['sha256']+'.enc' or not str(a['generation']).isdigit() or not 32<a['bytes']<=MAX_GAME_ARCHIVE:
        raise ValueError('INVALID_CLOUD_ARCHIVE_REFERENCE')
    return pointer


def snapshot(directory, output):
    from .artifact import snapshot_package
    from .local_store import LocalStore
    store=LocalStore('snapshot','read-only-copy',directory)
    configs=store.values('configuration');store.db.close()
    if len(configs)!=1:raise ValueError('ONE_IMMUTABLE_LINEAGE_REQUIRED')
    campaign=Campaign('p90-'+digest(['cloud-paper-checkpoint-v1',configs[0]])[:24],'snapshot-upload')
    with tempfile.TemporaryDirectory(prefix='quantura-cloud-snapshot-') as folder:
        archive=Path(folder)/'snapshot.enc'
        snapshot_package(directory,archive,max_bytes=MAX_GAME_ARCHIVE)
        pointer=campaign.upload(archive,'checkpoint')
        # Confirm the generation exists before publishing its recovery pointer.
        blob=campaign.bucket.blob(pointer['object'],generation=int(pointer['generation']))
        blob.reload()
        if blob.size!=pointer['bytes']:raise RuntimeError('CLOUD_CHECKPOINT_VERIFICATION_FAILED')
        write_pointer({'version':1,'campaign_id':campaign.id,'archive':pointer},output)
        print(json.dumps({'event':'cloud_checkpoint_saved','campaign_id':campaign.id,
            'encrypted_bytes':pointer['bytes'],'github_pointer_bytes':Path(output).stat().st_size}),flush=True)


def restore_pointer(source,directory,preserve_results):
    from .artifact import restore
    pointer=read_pointer(source)
    campaign=Campaign(pointer['campaign_id'],'read-only-restore')
    with tempfile.TemporaryDirectory(prefix='quantura-cloud-restore-') as folder:
        archive=Path(folder)/'snapshot.enc'
        campaign.download(pointer['archive'],archive)
        with archive.open('rb') as data:
            if data.read(4)!=b'QRA1':raise ValueError('NESTED_CLOUD_POINTER_FORBIDDEN')
        restore(archive,directory,preserve_results=preserve_results,max_bytes=MAX_GAME_ARCHIVE,
                max_database_bytes=MAX_DATABASE_BYTES)


def compatible_configuration(existing, requested):
    """Explicit storage-code migration preserves original identity and records."""
    if not existing:return requested
    if len(existing)!=1:raise RuntimeError('MULTIPLE_LINEAGES_FORBIDDEN')
    prior=existing[0]
    if prior==requested:return prior
    if not enabled() or {k:v for k,v in prior.items() if k!='code_sha'}!={k:v for k,v in requested.items() if k!='code_sha'}:
        raise RuntimeError('RESTORE_ORIGINAL_CODE_AND_CONFIGURATION')
    return prior
