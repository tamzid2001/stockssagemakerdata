"""Fenced order admission and position attribution for shared Kalshi accounts.

Every live strategy keeps its own immutable intent/recovery journal.  The
account gate serializes order submissions; exchange positions are accepted only
when an exact terminal fill in one of those journals explains them.  A missing
or ambiguous exchange read blocks new orders rather than assuming an account
is flat.
"""

from __future__ import annotations

import hashlib
import time
from decimal import Decimal

from .kalshi_execution import COIN_SERIES, money, reconciled_order
from .store import claim_transition

SERIES = ('KXBTC15M', *sorted(COIN_SERIES))


def account_session(key_id: str, subaccount: int, live: bool) -> str:
    return hashlib.sha256(f'{key_id}:{subaccount}:{live}'.encode()).hexdigest()


def strategy_session(key_id: str, subaccount: int, live: bool, series: str) -> str:
    if series not in SERIES:
        raise ValueError('UNSUPPORTED_ACCOUNT_SERIES')
    base = account_session(key_id, subaccount, live)
    # Preserve the running BTC journal and all its recovery/position history.
    return base if series == 'KXBTC15M' else hashlib.sha256(
        f'{base}:{series}'.encode()).hexdigest()


def current_btc_protocol(row: dict) -> bool:
    lease = row.get('lease', {})
    return (row.get('shared_account_protocol') == 1 and
        row.get('shared_account_fence') is not None and
        row.get('shared_account_fence') == lease.get('fence'))


def expected_positions(entries: list[tuple[str, dict]], actual: dict[str, Decimal], now: float) -> dict[str, Decimal]:
    """Require every live position to match one durable, exact exchange fill."""
    expected = {}
    for ticker, entry in entries:
        fill = money(entry['filled'])
        if fill <= 0:
            continue
        sign = 1 if entry['side'] == 'yes' else -1
        end = entry.get('signal', {}).get('market_end')
        if not isinstance(end, (int, float)):
            raise RuntimeError('ACCOUNT_MARKET_END_UNVERIFIED')
        # Kalshi may clear the position before its settlement read model is
        # ready.  Until close, however, an exact position must be present.
        if now < end or ticker in actual:
            if ticker in expected:
                raise RuntimeError('DUPLICATE_ACCOUNT_POSITION_OWNER')
            expected[ticker] = fill * sign
    return expected


class SharedAccountCoordinator:
    def __init__(self, journal, broker):
        self.journal, self.broker = journal, broker
        self.root_id = account_session(broker.key_id, journal.config.subaccount, True)
        root = journal.db.collection('kalshi_execution_sessions').document(self.root_id)
        self.root = root
        self.gate = root.collection('coordination').document('order_admission')
        self.fence = None

    def claim(self):
        @self.journal.fs.transactional
        def update(tx):
            # A pinned, older BTC worker cannot recognize coin positions. A
            # new BTC claim records its current fence; an old-code claim later
            # increments that fence without updating this protocol marker.
            root = self.root.get(transaction=tx).to_dict() or {}
            if (self.journal.config.subaccount == 0 or root.get('state')) and not current_btc_protocol(root):
                raise RuntimeError('BTC_SHARED_ACCOUNT_UPGRADE_REQUIRED')
            if root and root.get('enabled') is not True:
                raise RuntimeError('LIVE_KILL_SWITCH_DISABLED')
            old = self.gate.get(transaction=tx).to_dict() or {}
            lease = claim_transition(old.get('lease', {}), self.journal.holder, time.time(), ttl=90)
            tx.set(self.gate, {'lease': lease}, merge=True)
            return lease['fence']

        self.fence = self.journal.transact(update)

    def release(self):
        if self.fence is None:
            return

        @self.journal.fs.transactional
        def update(tx):
            old = self.gate.get(transaction=tx).to_dict() or {}
            lease = old.get('lease', {})
            if lease.get('holder') != self.journal.holder or lease.get('fence') != self.fence:
                raise RuntimeError('ACCOUNT_ORDER_GATE_LOST')
            tx.set(self.gate, {'lease': {**lease, 'expires': 0, 'updated_at': time.time()}}, merge=True)

        try:
            self.journal.transact(update)
        finally:
            self.fence = None

    def before_post(self):
        @self.journal.fs.transactional
        def verify(tx):
            root = self.root.get(transaction=tx).to_dict() or {}
            if (self.journal.config.subaccount == 0 or root.get('state')) and not current_btc_protocol(root):
                raise RuntimeError('BTC_SHARED_ACCOUNT_UPGRADE_REQUIRED')
            if root and root.get('enabled') is not True:
                raise RuntimeError('LIVE_KILL_SWITCH_DISABLED')
            gate = self.gate.get(transaction=tx).to_dict() or {}
            lease = gate.get('lease', {})
            if (self.fence is None or lease.get('holder') != self.journal.holder or
                    lease.get('fence') != self.fence or lease.get('expires', 0) <= time.time()):
                raise RuntimeError('ACCOUNT_ORDER_GATE_LOST')

        self.journal.transact(verify)

    def verify_positions(self, orders, positions):
        if orders:
            raise RuntimeError('ACCOUNT_RESTING_ORDER_UNVERIFIED')
        actual = {}
        for position in positions:
            count = money(position['position_fp'])
            if count:
                ticker = position['ticker']
                if ticker in actual:
                    raise RuntimeError('DUPLICATE_ACCOUNT_POSITION')
                actual[ticker] = count

        entries = []
        collection = self.journal.db.collection('kalshi_execution_sessions')
        for series in SERIES:
            ref = collection.document(strategy_session(self.broker.key_id,
                self.journal.config.subaccount, True, series))
            row = ref.get().to_dict() or {}
            entry = row.get('state', {}).get('active')
            if not entry:
                continue
            ticker = entry.get('ticker', '')
            if not ticker.startswith(series + '-') or entry.get('intent', {}).get('subaccount') != self.journal.config.subaccount:
                raise RuntimeError('ACCOUNT_INTENT_IDENTITY_MISMATCH')
            acknowledgement = entry.get('acknowledgement') or {}
            order = self.broker.find_order(entry['intent'], acknowledgement.get('order_id'))
            if order is None:
                raise RuntimeError('ACCOUNT_INTENT_UNRESOLVED')
            fill = reconciled_order(order, entry['intent'], entry['side'])
            entries.append((ticker, {**entry, **fill}))

        if expected_positions(entries, actual, time.time()) != actual:
            raise RuntimeError('ACCOUNT_POSITION_UNVERIFIED')
        return actual
