"""Independent all-series minute collection; no model or exchange order calls.

Per-series fenced ownership, five-minute encrypted recovery snapshots, immutable
settled-market files. Only small generation-pinned pointers enter Firestore.
Verified durable market files are removed from the bounded local working set,
not deleted from storage. No GitHub artifact storage quota is involved.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import gzip
import json
import os
from pathlib import Path
import tempfile
import threading
import time

from .btc_horizons import Checkpoints
from .btc_minute_archive import MinuteCollector
from .engine import digest
from .interval_markets import KalshiIntervalProvider, discover_series
from .local_store import LocalStore
from .recovery_cloud import encode_catalog, decode_catalog

VERSION = 'interval_paired_minute_collection_v1'
KINDS = ('btc_minutes', 'btc_minute_revisions', 'btc_minute_coverage',
         'btc_lifecycle', 'btc_settlements', 'checkpoints')


def snapshot_rows(store):
    with store.lock:
        return [{'kind':kind, 'id':key, 'value':json.loads(gzip.decompress(data))}
                for kind, key, data in store.db.execute('SELECT kind,id,data FROM records').fetchall()
                if kind in KINDS]


def restore_rows(store, rows):
    with store.lock, store.db:
        for row in rows:
            if row['kind'] not in KINDS:
                raise ValueError('INVALID_INTERVAL_SNAPSHOT_KIND')
            store._put(row['kind'], row['id'], row['value'], True)


def finalize_markets(store, archive, now):
    """Never discard pending settlements or data before verified remote upload."""
    finalized = 0
    for life in store.values('btc_lifecycle'):
        if life.get('resolution_status') != 'resolved' or now < life['close_at']+300:
            continue
        ticker = life['market_id']
        rows = {kind:[r for r in store.values(kind) if r.get('market_id') == ticker]
                for kind in KINDS if kind != 'checkpoints'}
        record = {'market':life['market'], 'lifecycle':life, 'records':rows,
                  'fee_policy':store._get('checkpoints', 'btc_fee_policy'),
                  'redistribution_status':'review_required', 'version':VERSION}
        if archive.get('source', ticker) is None:
            archive.put('source', ticker, record)
        # Checkpoints.put verifies storage generation and persists the hash/pointer.
        # Keep late receipts/revisions that differ from a finalized snapshot too.
        durable = archive.get('source', ticker)
        if digest(durable) != digest(record):
            archive.put('source', ticker+'-'+digest(record)[:12], record)
        with store.lock, store.db:
            for kind in rows:
                for key, raw in store.db.execute('SELECT id,data FROM records WHERE kind=?', (kind,)).fetchall():
                    if json.loads(gzip.decompress(raw)).get('market_id') == ticker:
                        store.db.execute('DELETE FROM records WHERE kind=? AND id=?', (kind,key))
            store._put('checkpoints', 'finalized:'+ticker, {'market_id':ticker, 'close_at':life['close_at']})
        finalized += 1
    # Keep the newest 128 tombstones, including overnight closures. This covers
    # the collector's 20-settlement overlap without growing an endless local DB.
    with store.lock, store.db:
        retired = [(key, json.loads(gzip.decompress(raw))) for key, raw in
                   store.db.execute("SELECT id,data FROM records WHERE kind='checkpoints'").fetchall()
                   if key.startswith('finalized:')]
        for key, _ in sorted(retired, key=lambda r:r[1]['close_at'], reverse=True)[128:]:
            store.db.execute("DELETE FROM records WHERE kind='checkpoints' AND id=?", (key,))
    return finalized


class SeriesSession:
    def __init__(self, series, directory):
        self.series = series
        self.config = {'version':VERSION, 'series':series, 'code_sha':'collector-schema-v1'}
        self.archive = Checkpoints(self.config)
        self.cloud = self.archive.campaign
        self.cloud.lease.claim(self.config)
        self.store = LocalStore(self.archive.id, self.cloud.lease.holder, directory, capacity_bytes=64*1024*1024)
        self.pointer = self.cloud.lease.ref.collection('collector').document('latest')
        previous = self.pointer.get()
        if previous.exists:
            with tempfile.TemporaryDirectory(prefix='interval-restore-') as folder:
                path = Path(folder)/'state.enc'
                self.cloud.download(previous.to_dict()['archive'], path)
                saved = decode_catalog(path)
                if saved['configuration'] != self.config:
                    raise ValueError('COLLECTOR_CONFIGURATION_CONFLICT')
                restore_rows(self.store, saved['rows'])
        self.collector = MinuteCollector(self.store, KalshiIntervalProvider(series, timeout=5, attempts=2))
        self.saved_at = 0

    def save(self, now):
        value = {'configuration':self.config, 'rows':snapshot_rows(self.store), 'at':now}
        with tempfile.TemporaryDirectory(prefix='interval-save-') as folder:
            path = Path(folder)/'state.enc'; encode_catalog(value, path)
            pointer = self.cloud.upload(path, 'checkpoint')
            blob = self.cloud.bucket.blob(pointer['object'], generation=int(pointer['generation']))
            blob.reload()
            if blob.size != pointer['bytes']:
                raise ValueError('CHECKPOINT_GENERATION_SIZE_MISMATCH')
        @self.cloud.lease.fs.transactional
        def publish(tx):
            self.cloud.lease.check(tx)
            tx.set(self.pointer, {'archive':pointer, 'at':now, 'series':self.series,
                                 'code_sha':os.environ.get('QUANTURA_CODE_SHA', 'local')})
        self.cloud.lease.transact(publish)
        self.saved_at = now

    def tick(self, now):
        self.cloud.lease.renew()
        self.collector.tick(now)
        completed = finalize_markets(self.store, self.archive, now)
        if completed or now-self.saved_at >= 300:
            self.save(now)
            health = self.store._get('checkpoints','btc_collector_health') or {}
            print(json.dumps({'event':'interval_minutes_checkpoint', 'series':self.series,
                'campaign_id':self.archive.id, 'buffered_minutes':len(self.store.values('btc_minutes')),
                'finalized_markets':completed, 'health':health, 'at':now}), flush=True)

    def close(self):
        try:
            self.cloud.lease.renew(); self.save(int(time.time()))
        finally:
            self.cloud.lease.release(); self.store.db.close()


def main():
    p = argparse.ArgumentParser(); p.add_argument('--duration-minutes', type=int, default=330)
    args = p.parse_args()
    if not 1 <= args.duration_minutes <= 330: p.error('INVALID_DURATION')
    discovery = discover_series()
    print(json.dumps({'event':'interval_discovery', **discovery}), flush=True)
    if not discovery['series']: raise RuntimeError('NO_VERIFIED_INTERVAL_MARKETS')
    # Initialize Firebase once before per-series threads (Admin SDK app creation
    # is not an atomic check-then-create in the existing Store constructor).
    from .store import Store
    Store('interval-collector-bootstrap', 'initialize')
    deadline = time.monotonic()+args.duration_minutes*60
    with tempfile.TemporaryDirectory(prefix='interval-collection-') as folder:
        def collect(row):
            session = SeriesSession(row['ticker'], Path(folder)/row['ticker'])
            try:
                while time.monotonic() < deadline:
                    try:
                        session.tick(int(time.time()))
                    except (RuntimeError, ValueError, OSError) as error:
                        print(json.dumps({'event':'interval_collection_error', 'series':row['ticker'],
                                          'error_type':type(error).__name__}), flush=True)
                        # Lease is authoritative: never continue after another worker takes over.
                        if str(error) in ('LEASE_HELD', 'LEASE_LOST'): raise
                    threading.Event().wait(min(15, max(0, deadline-time.monotonic())))
            finally:
                session.close()
        with ThreadPoolExecutor(max_workers=20) as pool:
            list(pool.map(collect, discovery['series']))


if __name__ == '__main__':
    main()
