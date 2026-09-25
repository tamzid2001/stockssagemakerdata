"""BTC P90 + sticky hold worker. Real orders require operator-approved gates.

Collector/lease/execution keep running while an isolated process performs the
existing sequential ensemble. Only minute closes trigger signals, never ticks.
"""
import argparse
from dataclasses import asdict
import json
import multiprocessing
import os
from pathlib import Path
import queue
import re
import signal
import tempfile
import time

from .kalshi_execution import Config, CoinConfig, KalshiExecution, money
from .kalshi_live_state import LiveJournal, Trader, session_statistics


def forecast_pair(market, minutes, rows, output):
    # This process does numerical work only; it has no exchange credentials.
    os.environ.pop('KALSHI_PRIVATE_KEY', None)
    os.environ.pop('KALSHI_API_KEY_ID', None)
    from .engine import Quote, digest, stamp, validate_forecast
    from .forecast import forecast_window
    from .p1_oco import QUANTILES
    try:
        models = ('granite', 'chronos', 'timesfm') if minutes == 1 else ('prophet', 'granite', 'chronos', 'timesfm')
        quantiles = QUANTILES  # Preserve the exact historical study's quantile grid.
        pair = []
        for side in ('yes', 'no'):
            window = [Quote(r['timestamp'], r[side + '_ask'], r[side + '_bid']) for r in rows]
            f = forecast_window(window, 15 - minutes, models, quantiles,
                single_point_research=minutes == 1, failure_policy='fail')
            validate_forecast(f, stamp(market['open_time']) + 60 * minutes, 15 - minutes)
            f.update(history_count=minutes, market_context={'market_id': market['ticker'],
                'event_id': market['event_ticker'], 'contract_id': market['ticker'] + ':' + side})
            f['forecast_id'] = digest([f['forecast_id'], market['ticker'], side])
            pair.append(f)
        published = int(time.time())
        for f in pair:
            f['available_at'] = published
        output.put({'forecasts': pair, 'input_rows': rows, 'published_at': published})
    except Exception as exc:
        output.put({'error': 'ENSEMBLE_FAILED', 'error_type': type(exc).__name__})


def observations(rows):
    return [dict(contract_id=r['market_id'] + ':' + side, timestamp=r['timestamp'],
                 received_at=r['received_at'], bid=r[side + '_bid'], ask=r[side + '_ask'],
                 collection_mode=r['collection_mode'], observed=True)
            for r in rows for side in ('yes', 'no')]


def persist_evidence(journal, ticker, value):
    """Encrypted immutable market-sized evidence, not billable Actions artifacts."""
    from firebase_admin import storage
    from .engine import digest
    from .recovery_cloud import encode_catalog, BUCKET
    from google.api_core.exceptions import PreconditionFailed
    checksum = digest(value)
    name = f'private-research/kalshi-execution/{journal.session}/{ticker}/{checksum}.enc'
    blob = storage.bucket(BUCKET).blob(name)
    with tempfile.TemporaryDirectory(prefix='kalshi-execution-') as directory:
        path = Path(directory) / 'evidence.enc'
        encode_catalog(value, path)
        try:
            blob.upload_from_filename(path, if_generation_match=0, content_type='application/octet-stream')
        except PreconditionFailed:
            blob.reload()
    # Pointer only. Server/Admin access; no public URLs or bearer download tokens.
    journal.ref.collection('evidence').document(checksum).set({
        'object': name, 'generation': str(blob.generation), 'sha256_content': checksum,
        'ticker': ticker, 'kind': value['kind'], 'created_at': time.time()})
    return checksum


def reconcile_status(trader):
    """Wait for authenticated read models without repeating an uncertain IOC.

    The shared-account verifier remains fail-closed for new exposure. A
    pending stop in another series must not terminate this worker's lease.
    """
    try:
        return trader.reconcile()
    except RuntimeError as exc:
        if str(exc) == 'LEASE_HELD':
            return 'account_order_gate_wait'
        if str(exc) == 'ACCOUNT_STOP_INTENT_UNRESOLVED':
            return 'account_stop_intent_read_lag'
        raise


def run(config, mode, duration):
    from .engine import stamp
    from .interval_markets import KalshiIntervalProvider
    from .local_store import LocalStore
    from .btc_minute_archive import MinuteCollector
    from .btc_hold_tracking import first_signals, prospective_observations
    from .kalshi_provisional_direction import NearCloseCollector, direction_at, reconciliation
    broker = KalshiExecution(config, requested_live=mode == 'live')
    if mode == 'readiness':
        orders, positions = broker.account()
        print(json.dumps({'mode': mode, 'configuration': asdict(config), 'config_hash': config.fingerprint,
              'authenticated_reads': True, 'resting_order_count': len(orders),
              'position_count': len(positions), 'orders_sent': 0}))
        return
    journal = LiveJournal(config, broker.key_id, os.environ.get('GITHUB_RUN_ID', 'local'),
        broker.enabled, shared_protocol=broker.enabled)
    journal.claim()
    session_baseline = journal.public_summary()
    from .kalshi_shared_account import SharedAccountCoordinator
    coordinator = SharedAccountCoordinator(journal, broker) if broker.enabled else None
    trader = Trader(config, broker, journal, coordinator=coordinator)
    stopped = False
    def stop(*_):
        nonlocal stopped
        stopped = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    context = multiprocessing.get_context('spawn')
    worker = result_queue = None
    started = time.monotonic()
    boot_at = int(time.time())
    # Leave setup/shutdown margin under Actions' six-hour ceiling.
    job_started = float(os.environ.get('QUANTURA_JOB_STARTED_AT', time.time()))
    deadline = min(time.time() + duration * 60, job_started + 345 * 60)
    attempts, pairs, pending, archived = {}, {}, {}, set()
    snapshots = {r['market_id']: r for r in journal.direction_snapshots()}
    reconciled_directions = set()
    last_renew = last_reconcile = last_health = last_heartbeat = 0
    status = 'starting'
    phase = 'worker_setup'
    try:
        with tempfile.TemporaryDirectory(prefix='kalshi-minute-live-') as directory:
            store = LocalStore(journal.session, journal.holder, directory, capacity_bytes=100*1024*1024)
            collector = MinuteCollector(store, KalshiIntervalProvider(config.series_ticker))
            near_close = NearCloseCollector(store,
                KalshiIntervalProvider(config.series_ticker, timeout=1.5, attempts=1))
            collector.start()
            if config.direction_policy == 'provisional_near_close':
                near_close.start()
            try:
                while not stopped and time.time() < deadline:
                    now = int(time.time())
                    if now - last_renew >= 25:
                        phase = 'lease_renewal'
                        journal.renew()
                        last_renew = now
                    # Check every active entry/stop at roughly one-second cadence.
                    # Retry only the reconciled remainder with a fresh quote and
                    # client ID; never blindly repeat an ambiguous delivery.
                    active_position = bool(journal.state().get('active')) if broker.enabled else False
                    interval = 1 if active_position else 20
                    if now - last_reconcile >= interval:
                        phase = 'position_reconciliation'
                        previous_status = status
                        status = reconcile_status(trader)
                        if now - last_heartbeat >= 20 or status != previous_status:
                            summary = journal.public_summary()
                            print(json.dumps({'event': 'execution_heartbeat', 'mode': mode,
                                'status': status, 'worker_started_at': boot_at,
                                'session_stats': session_statistics(summary, session_baseline),
                                **summary}), flush=True)
                            last_heartbeat = now
                        last_reconcile = now
                    phase = 'collector_snapshot'
                    lifecycle = store.values('btc_lifecycle')
                    minute_rows = store.values('btc_minutes')
                    settlements = store.values('btc_settlements')
                    for row in store.values('btc_provisional_direction'):
                        if row['close_at'] <= now and row['market_id'] not in snapshots:
                            # Encrypt first, then checkpoint the small final snapshot. A
                            # retry reuses the content-addressed object after a crash.
                            persist_evidence(journal, row['market_id'], {'kind': 'provisional_direction', **row})
                            journal.remember_direction(row)
                            snapshots[row['market_id']] = row
                    for official in settlements:
                        ticker = official['market_id']
                        identity = (ticker, official['first_confirmed_at'])
                        if ticker in snapshots and identity not in reconciled_directions:
                            persist_evidence(journal, ticker, reconciliation(snapshots[ticker], official))
                            reconciled_directions.add(identity)
                    if now - last_health >= 60:
                        phase = 'health_checkpoint'
                        journal.health({'mode': mode, 'orders_enabled': broker.enabled,
                            'latest_minute_at': max((r['timestamp'] for r in minute_rows), default=None),
                            'minute_collector': store._get('checkpoints', 'btc_collector_health'),
                            'direction_collector': store._get('checkpoints', 'btc_provisional_health'),
                            'forecast_attempts': len(attempts), 'completed_forecast_pairs': len(pairs),
                            'entry_reconciliation': status, 'boot_at': boot_at})
                        last_health = now
                    phase = 'forecast_processing'
                    if worker:
                        try:
                            result = result_queue.get_nowait()
                        except queue.Empty:
                            result = None
                        if result is not None:
                            worker.join(timeout=1)
                            if worker.is_alive():
                                worker.terminate(); worker.join()
                            worker = None
                            result['kind'] = 'forecast' if 'forecasts' in result else 'forecast_failure'
                            persist_evidence(journal, computing, result)
                            if 'forecasts' in result:
                                pairs[computing] = result['forecasts']
                            print(json.dumps({'event': result['kind'], 'ticker': computing}), flush=True)
                        elif not worker.is_alive() or now >= compute_end - 60 or time.monotonic() - compute_started > 480:
                            failure = 'INFERENCE_DEADLINE' if worker.is_alive() else 'INFERENCE_PROCESS_EXIT'
                            worker.terminate(); worker.join(); worker = None
                            persist_evidence(journal, computing, {'kind': 'forecast_failure', 'code': failure})
                    for row in lifecycle:
                        phase = 'market_processing'
                        market, ticker = row['market'], row['market_id']
                        opened, end = row['open_at'], row['close_at']
                        rows = sorted((r for r in minute_rows if r['market_id'] == ticker), key=lambda r:r['timestamp'])
                        # A restart cannot reconstruct a missed FIRST live signal from
                        # late backfills. Reconcile held positions, but start new entries
                        # only in markets witnessed from opening by this worker.
                        if (not worker and ticker not in attempts and opened >= boot_at
                                and opened + config.history_minutes*60 <= now < end - 90):
                            by_time = {r['timestamp']:r for r in rows}
                            timestamps = [opened + 60*i for i in range(1, config.history_minutes+1)]
                            if all(t in by_time for t in timestamps):
                                attempts[ticker] = True
                                computing, compute_end, compute_started = ticker, end, time.monotonic()
                                result_queue = context.Queue()
                                worker = context.Process(target=forecast_pair,
                                    args=(market, config.history_minutes, [by_time[t] for t in timestamps], result_queue))
                                worker.start()
                        if ticker in pairs and ticker not in pending:
                            signals, _ = first_signals(pairs[ticker], prospective_observations(observations(rows), now), now)
                            if signals:
                                first = signals[0]
                                direction = direction_at(settlements, list(snapshots.values()), lifecycle,
                                    first['signal_received_at'], ticker, opened, config.direction_policy,
                                    config.series_ticker)
                                agrees = direction and first['contract_id'].endswith(':' + direction['side'])
                                pending[ticker] = {**first, 'direction': direction, 'agrees': bool(agrees), 'done': False}
                                persist_evidence(journal, ticker, {'kind': 'first_p90', **pending[ticker]})
                                print(json.dumps({'event': 'signal_decision', 'ticker': ticker,
                                    'forecast_side': first['contract_id'].rsplit(':', 1)[1],
                                    'sticky_side': direction.get('side') if direction else None,
                                    'agrees': bool(agrees), 'signal_at': first['signal_at'],
                                    'entry_minute': first['signal_at'] + 60}), flush=True)
                        s = pending.get(ticker)
                        if s and not s['done'] and s['agrees']:
                            entry_at = s['signal_at'] + 60
                            quote = next((r for r in rows if r['timestamp'] == entry_at), None)
                            if quote and now <= entry_at + 30:
                                try:
                                    entry = trader.enter(s, quote, now)
                                    s['done'] = True
                                    acknowledgement = entry.get('acknowledgement', {})
                                    acknowledged_fill = acknowledgement.get('acknowledged_fill')
                                    print(json.dumps({'event': 'entry_order_acknowledged' if acknowledgement else 'entry_observed',
                                        'ticker': ticker, 'side': entry['side'],
                                        'requested_contracts': entry['intent']['count'],
                                        'attempt': entry.get('attempt', 0),
                                        'acknowledgement_state': ('zero_fill_pending_reconciliation'
                                            if acknowledged_fill is not None and money(acknowledged_fill) == 0
                                            else 'fill_reported'),
                                        'acknowledged_fill': acknowledged_fill,
                                        'acknowledged_remaining': acknowledgement.get('acknowledged_remaining')}), flush=True)
                                except (RuntimeError, ValueError, KeyError) as exc:
                                    code = str(exc) if re.fullmatch(r'[A-Z][A-Z0-9_]{3,80}', str(exc)) else 'EXECUTION_BLOCKED'
                                    # A competing strategy's short account-admission
                                    # lease may clear within this quote's 30s window.
                                    # All other preflight/rejection outcomes remain
                                    # terminal for the first signal.
                                    if code != 'LEASE_HELD':
                                        s['done'] = True
                                    persist_evidence(journal, ticker, {'kind': 'entry_blocked', 'error_type': type(exc).__name__,
                                        'code': code})
                                    print(json.dumps({'event': 'entry_blocked', 'ticker': ticker,
                                        'code': code, **journal.public_summary()}), flush=True)
                            elif now > entry_at + 30:
                                s['done'] = True
                                persist_evidence(journal, ticker, {'kind': 'missed_entry', 'expected_minute': entry_at})
                        if row['resolution_status'] == 'resolved' and ticker not in archived:
                            persist_evidence(journal, ticker, {'kind': 'minute_tape', 'lifecycle': row, 'minutes': rows})
                            archived.add(ticker)
                    if store.at_capacity:
                        raise RuntimeError('LOCAL_EVIDENCE_CAPACITY_REACHED')
                    phase = 'sleep'
                    time.sleep(1 if broker.enabled and active_position else 3)
            finally:
                near_close.stop()
                collector.stop()
                if worker:
                    worker.terminate(); worker.join(timeout=10)
                # Persist in-flight tape before yielding; execution intents already durable.
                for ticker in {r['market_id'] for r in store.values('btc_minutes')} - archived:
                    persist_evidence(journal, ticker, {'kind': 'handoff_minutes',
                        'minutes': [r for r in store.values('btc_minutes') if r['market_id'] == ticker]})
                store.release()
        journal.renew()
        journal.release()
        print(json.dumps({'event': 'durable_handoff_ready', 'elapsed_seconds': time.monotonic()-started}), flush=True)
    except Exception as exc:
        # A safe phase label makes SDK errors (for example InvalidArgument)
        # diagnosable without logging authenticated response bodies or keys.
        print(json.dumps({'event': 'worker_phase_failed', 'phase': phase,
            'error_type': type(exc).__name__}), flush=True)
        raise
    finally:
        broker.client.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['config', 'readiness', 'observe', 'live'], default='config')
    parser.add_argument('--series', choices=['KXBTC15M', 'KXBNB15M', 'KXDOGE15M',
                        'KXETH15M', 'KXNEAR15M', 'KXZEC15M'], default='KXBTC15M')
    parser.add_argument('--history-minutes', type=int, default=1)
    parser.add_argument('--subaccount', type=int, default=0)
    parser.add_argument('--direction-policy', choices=['confirmed', 'provisional_near_close'],
                        default='provisional_near_close')
    parser.add_argument('--starting-contracts', type=int, default=1)
    parser.add_argument('--recovery-multiplier', default='2.5')
    parser.add_argument('--max-recovery-increases', type=int, default=3)
    parser.add_argument('--duration-minutes', type=int, default=300)
    args = parser.parse_args()
    config_type = Config if args.series == 'KXBTC15M' else CoinConfig
    config = config_type(**({'series_ticker': args.series} if config_type is CoinConfig else {}),
                    history_minutes=args.history_minutes, subaccount=args.subaccount,
                    direction_policy=args.direction_policy, starting_contracts=args.starting_contracts,
                    recovery_multiplier=args.recovery_multiplier,
                    max_recovery_increases=args.max_recovery_increases)
    if not 1 <= args.duration_minutes <= 300:
        parser.error('Duration must be 1..300 minutes')
    if args.mode == 'config':
        print(json.dumps({'configuration': asdict(config), 'config_hash': config.fingerprint, 'orders_sent': 0}))
    else:
        run(config, args.mode, args.duration_minutes)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        # Never print request headers, credentials or raw authenticated response bodies.
        print(json.dumps({'event': 'worker_failed', 'error_type': type(exc).__name__,
            'code': str(exc) if re.fullmatch(r'[A-Z][A-Z0-9_]{3,80}', str(exc)) else 'LIVE_WORKER_FAILED'}), flush=True)
        raise SystemExit(1) from None
