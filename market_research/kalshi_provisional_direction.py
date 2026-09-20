"""Near-close quote evidence for direction ONLY, never settlement or accounting.

Minute candles remain the sole P90/entry clock. These separate public REST
snapshots are deliberately not inserted into the minute tape or outcome table.
"""
from decimal import Decimal, InvalidOperation
import math
import re
import threading
import time

from .engine import stamp

VERSION = 'btc-near-close-direction-v1'
WINDOW_SECONDS = 10
FINAL_FRESHNESS_SECONDS = 3
MAX_REQUEST_SECONDS = 2


def snapshot(market, ticker, started_at, received_at):
    if (not re.fullmatch(r'KXBTC15M-[A-Z0-9-]+', ticker)
            or market.get('ticker') != ticker or market.get('market_type') != 'binary'):
        raise ValueError('PROVISIONAL_MARKET_IDENTITY_INVALID')
    opened, close = stamp(market['open_time']), stamp(market['close_time'])
    if (close - opened != 900 or market.get('status') != 'active'
            or not all(type(t) in (float, int) and math.isfinite(t) for t in (started_at, received_at))
            or not close - WINDOW_SECONDS <= started_at <= received_at < close
            or received_at - started_at > MAX_REQUEST_SECONDS):
        raise ValueError('PROVISIONAL_QUOTE_OUTSIDE_WINDOW')
    try:
        bid, ask, no_bid, no_ask = [Decimal(str(market[k])) for k in (
            'yes_bid_dollars', 'yes_ask_dollars', 'no_bid_dollars', 'no_ask_dollars')]
        bid_size, ask_size = [Decimal(str(market[k])) for k in ('yes_bid_size_fp', 'yes_ask_size_fp')]
        if (not all(v.is_finite() for v in (bid, ask, no_bid, no_ask, bid_size, ask_size))
                or not 0 <= bid <= ask <= 1 or not 0 <= no_bid <= no_ask <= 1
                or bid + no_ask != 1 or ask + no_bid != 1 or min(bid_size, ask_size) < 0):
            raise ValueError('PROVISIONAL_BOOK_INVALID')
    except (KeyError, InvalidOperation, TypeError):
        raise ValueError('PROVISIONAL_BOOK_INVALID') from None
    # A 99c ask or old last trade alone is NOT evidence of a 99c bid.
    candidates = [s for s, p, size in (('yes', bid, bid_size), ('no', no_bid, ask_size))
                  if p >= Decimal('.99') and size > 0]
    winner = candidates[0] if len(candidates) == 1 else None
    return dict(version=VERSION, market_id=ticker, open_at=opened, close_at=close,
        request_started_at=started_at, received_at=received_at,
        inferred_winner=winner, status='provisional' if winner else 'neutral',
        yes_bid=str(bid), yes_ask=str(ask), no_bid=str(no_bid), no_ask=str(no_ask),
        yes_bid_size=str(bid_size), yes_ask_size=str(ask_size),
        source='kalshi_public_market_rest_book', clock='http_request_receipt',
        exchange_quote_timestamp_verified=False, settlement_confirmed=False)


def direction_at(settlements, snapshots, lifecycle, timestamp, current_market, current_open,
                 policy='provisional_near_close'):
    """Use the immediately preceding known market; never jump past an unknown one.

Official receipt-timed outcomes supersede quote inference for that market. An
immutable first-P90 decision is not rewritten when a later settlement disagrees.
"""
    if policy not in ('confirmed', 'provisional_near_close'):
        raise ValueError('INVALID_DIRECTION_POLICY')
    prior = [r for r in [*lifecycle, *settlements, *snapshots]
             if r['market_id'] != current_market and r['market_id'].startswith('KXBTC15M-')
             and r['close_at'] <= current_open and r['close_at'] < timestamp]
    latest = max(prior, key=lambda r: (r['close_at'], r['market_id']), default=None)
    # Do not substitute yesterday's last known outcome after a collection gap.
    if latest is None or latest['close_at'] != current_open:
        return None
    ticker = latest['market_id']
    official = max((r for r in settlements if r['market_id'] == ticker
        and r.get('resolution_status') == 'resolved' and r.get('result') in ('yes', 'no')
        and r['close_at'] <= r['first_confirmed_at'] < timestamp),
        key=lambda r: r['first_confirmed_at'], default=None)
    if official:
        return dict(side='no' if official['result'] == 'yes' else 'yes',
            prior_market_id=ticker, prior_result=official['result'],
            source='official_settlement', provisional=False,
            confirmed_at=official['first_confirmed_at'])
    if policy == 'confirmed':
        return None
    row = max((r for r in snapshots if r['market_id'] == ticker and r['received_at'] < timestamp),
              key=lambda r: r['received_at'], default=None)
    if (not row or row.get('version') != VERSION or row.get('inferred_winner') not in ('yes', 'no')
            or not row['close_at'] - FINAL_FRESHNESS_SECONDS <= row['received_at'] < row['close_at']
            or row.get('settlement_confirmed') is not False):
        return None
    winner = row['inferred_winner']
    return dict(side='no' if winner == 'yes' else 'yes', prior_market_id=ticker,
        inferred_prior_winner=winner, source=row['source'], provisional=True,
        observed_at=row['received_at'], threshold='0.99',
        exchange_quote_timestamp_verified=False)


def reconciliation(row, official):
    if (official['market_id'] != row['market_id'] or official.get('resolution_status') != 'resolved'
            or official.get('result') not in ('yes', 'no')):
        raise ValueError('OFFICIAL_OUTCOME_REQUIRED')
    return dict(kind='provisional_direction_reconciliation', market_id=row['market_id'],
        snapshot_at=row['received_at'], inferred_winner=row['inferred_winner'],
        official_result=official['result'], confirmed_at=official['first_confirmed_at'],
        matches=None if row['inferred_winner'] is None else row['inferred_winner'] == official['result'],
        accounting_source='official_settlement_only')


class NearCloseCollector:
    """Independent 1 Hz polling in the final 10 seconds; no keys/order API."""
    def __init__(self, store, provider):
        self.store, self.provider = store, provider
        self.stop_event = threading.Event()
        self.thread = None
        self.last_poll = {}

    def tick(self, now):
        for row in self.store.values('btc_lifecycle'):
            ticker, close = row['market_id'], row['close_at']
            if (not close - WINDOW_SECONDS <= now < close
                    or now - self.last_poll.get(ticker, 0) < 1):
                continue
            self.last_poll[ticker] = now
            started = time.time()
            try:
                value = snapshot(self.provider.market(ticker), ticker, started, time.time())
                with self.store.lock, self.store.db:
                    # Latest neutral snapshot invalidates an earlier 99c snapshot.
                    self.store._put('btc_provisional_direction', ticker, value)
            except (RuntimeError, ValueError, OSError, KeyError, TypeError) as exc:
                self.store.checkpoint('btc_provisional_health', dict(at=time.time(),
                    status='degraded', error_type=type(exc).__name__))
                continue
            self.store.checkpoint('btc_provisional_health', dict(at=time.time(), status='ok'))

    def start(self):
        def collect():
            while not self.stop_event.is_set():
                self.tick(time.time())
                self.stop_event.wait(.25)
        self.thread = threading.Thread(target=collect, name='btc-near-close-direction', daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=5)
