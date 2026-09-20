"""Private, fenced, bounded execution journal; never public Git or artifacts."""
from dataclasses import asdict
import hashlib
import json
import time

from .store import Store, claim_transition
from .kalshi_execution import recovery, money


SIZING_FIELDS = frozenset({'starting_contracts', 'recovery_multiplier', 'max_contracts'})


def approved_reconfiguration(existing, requested, state):
    """Allow reviewed sizing changes only between complete recovery cycles."""
    normalized = {**existing, 'starting_contracts': existing.get('starting_contracts', 1),
        'recovery_multiplier': existing.get('recovery_multiplier', '2.5')}
    if normalized == requested:
        return dict(state)
    changed = {key for key in set(normalized) | set(requested)
               if normalized.get(key) != requested.get(key)}
    if (not changed or not changed <= SIZING_FIELDS or state.get('active')
            or money(state.get('cycle', '0')) != 0):
        raise RuntimeError('LIVE_CONFIG_CHANGE_REQUIRES_RECOVERY_ZERO')
    return {**state, 'size': requested['starting_contracts']}


class LiveJournal(Store):
    def __init__(self, config, key_id, holder, live):
        # Same credential/subaccount is serialized across ALL forecast timings.
        identifier = hashlib.sha256(f'{key_id}:{config.subaccount}:{live}'.encode()).hexdigest()
        super().__init__(identifier, holder)
        self.ref = self.db.collection('kalshi_execution_sessions').document(identifier)
        self.config, self.live = config, live

    def claim(self, configuration=None):
        @self.fs.transactional
        def update(tx):
            old = self.ref.get(transaction=tx).to_dict() or {}
            requested = asdict(self.config)
            existing = old.get('configuration')
            state = old.get('state', {'size': self.config.starting_contracts,
                'cycle': '0', 'net': '0', 'active': None})
            if existing:
                state = approved_reconfiguration(existing, requested, state)
            lease = claim_transition(old.get('lease', {}), self.holder, time.time())
            tx.set(self.ref, {'lease': lease, 'configuration': requested,
                'paper_only': not self.live, 'state': state,
                'enabled': old.get('enabled', True)}, merge=True)
            return lease['fence']
        self.fence = self.transact(update)

    def state(self):
        return self.ref.get().to_dict()['state']

    def direction_snapshots(self):
        return self.ref.get().to_dict().get('direction_snapshots', [])

    def remember_direction(self, row):
        # Fenced, bounded handoff context, separate from active intent/P&L state.
        @self.fs.transactional
        def update(tx):
            self.check(tx)
            root = self.ref.get(transaction=tx).to_dict()
            rows = root.get('direction_snapshots', [])
            old = next((r for r in rows if r['market_id'] == row['market_id']), None)
            if old and old != row:
                raise RuntimeError('PROVISIONAL_SNAPSHOT_IMMUTABLE')
            rows = [r for r in rows if r['market_id'] != row['market_id']] + [row]
            tx.update(self.ref, {'direction_snapshots': sorted(rows, key=lambda r:r['close_at'])[-8:]})
        self.transact(update)

    def health(self, value):
        @self.fs.transactional
        def update(tx):
            self.check(tx)
            tx.update(self.ref, {'worker_health': {**value, 'at': time.time(), 'holder': self.holder}})
        self.transact(update)

    def used(self, ticker):
        return self.ref.collection('markets').document(ticker).get().exists

    def change(self, event, ticker, update):
        @self.fs.transactional
        def mutate(tx):
            self.check(tx)
            root = self.ref.get(transaction=tx).to_dict()
            market_ref = self.ref.collection('markets').document(ticker)
            previous = market_ref.get(transaction=tx)
            state, row = update(root['state'], previous.to_dict() if previous.exists else None)
            if event == 'intent' and root.get('enabled') is not True:
                raise RuntimeError('LIVE_KILL_SWITCH_DISABLED')
            if len(json.dumps(state)) > 60000 or len(json.dumps(row)) > 60000:
                raise ValueError('BOUNDED_EXECUTION_STATE_REQUIRED')
            tx.update(self.ref, {'state': state, 'updated_at': time.time()})
            tx.set(market_ref, row)
            tx.set(market_ref.collection('events').document(), {'event': event, 'at': time.time(), 'holder': self.holder})
        self.transact(mutate)

    def begin(self, entry):
        def update(state, previous):
            if state.get('active') or previous:
                raise RuntimeError('ENTRY_ALREADY_RESERVED')
            return {**state, 'active': entry}, entry
        self.change('intent', entry['ticker'], update)

    def finish(self, entry, net=None):
        def update(state, previous):
            if not state.get('active') or state['active']['intent'] != entry['intent']:
                raise RuntimeError('ACTIVE_INTENT_MISMATCH')
            if net is not None:
                state = recovery(state, net, self.config)
                day = time.strftime('%Y-%m-%d', time.gmtime())
                daily = money(state.get('daily_net', '0')) if state.get('day') == day else money(0)
                state.update(day=day, daily_net=str(daily + money(net)))
            return {**state, 'active': None}, entry
        self.change('closed', entry['ticker'], update)

    def before_post(self):
        @self.fs.transactional
        def verify(tx):
            self.check(tx)
            root = self.ref.get(transaction=tx).to_dict()
            if root.get('enabled') is not True:
                raise RuntimeError('LIVE_KILL_SWITCH_DISABLED')
        self.transact(verify)


class Trader:
    def __init__(self, config, broker, journal):
        self.config, self.broker, self.journal = config, broker, journal

    def enter(self, signal, quote, now):
        from .engine import stamp
        from .kalshi_execution import order_payload
        if (quote['timestamp'] != signal['signal_at'] + 60 or not quote.get('timely')
                or not quote['timestamp'] <= quote['received_at'] <= now <= quote['timestamp'] + 30
                or now >= signal['market_end'] or self.journal.used(signal['market_id'])):
            raise RuntimeError('MISSED_OR_DUPLICATE_LIVE_ENTRY')
        state = self.journal.state()
        if state.get('active'):
            raise RuntimeError('PRIOR_TRADE_UNRECONCILED')
        day = time.strftime('%Y-%m-%d', time.gmtime(now))
        if state.get('day') == day and money(state.get('daily_net', '0')) <= -money(self.config.daily_loss_dollars):
            raise RuntimeError('DAILY_LOSS_LIMIT')
        orders, positions = self.broker.account()
        if orders or any(money(p['position_fp']) != 0 for p in positions):
            raise RuntimeError('SUBACCOUNT_NOT_FLAT')
        market = self.broker.market(signal['market_id'])
        if (market.get('status') != 'active' or market.get('market_type') != 'binary'
                or stamp(market['close_time']) - stamp(market['open_time']) != 900
                or stamp(market['close_time']) != signal['market_end']):
            raise RuntimeError('MARKET_NOT_TRADEABLE')
        side = signal['contract_id'].rsplit(':', 1)[1]
        ask = quote[side + '_ask']
        quantity = min(int(state['size']), self.config.max_contracts)
        intent = order_payload(signal['market_id'], side, quantity, ask, market['exchange_index'], self.config)
        # Conservative fee reserve; actual fills/fees, not this reserve, drive P&L.
        if self.broker.balance(market['exchange_index']) < quantity * (money(ask) + money('.07')):
            raise RuntimeError('INSUFFICIENT_SHARD_FUNDS')
        entry = {'ticker': signal['market_id'], 'side': side, 'intent': intent,
                 'signal': signal, 'quote': quote, 'created_at': now,
                 'status': 'delivery_unknown' if self.broker.enabled else 'observed_no_order'}
        # Crash after this commit never permits resubmission of the intent.
        if time.time() > quote['timestamp'] + 30 or time.time() >= signal['market_end'] - 5:
            raise RuntimeError('PREFLIGHT_EXCEEDED_ENTRY_DEADLINE')
        self.journal.begin(entry)
        if not self.broker.enabled:
            self.journal.finish(entry)
            return entry
        self.journal.before_post()
        if time.time() > quote['timestamp'] + 30 or time.time() >= signal['market_end'] - 5:
            self.journal.finish({**entry, 'status': 'expired_before_submit'})
            raise RuntimeError('ENTRY_EXPIRED_BEFORE_POST')
        self.broker.submit(intent)  # No retry, even after a timeout or crash.
        return entry  # ACK is not authoritative accounting; reconcile via GET.

    def reconcile(self):
        from .kalshi_execution import reconciled_order
        state = self.journal.state()
        entry = state.get('active')
        if not entry:
            return 'flat'
        order = self.broker.find_order(entry['intent'])
        if order is None:
            return 'unknown_delivery_blocked'  # Never infer that absence proves no order.
        fill = reconciled_order(order, entry['intent'], entry['side'])
        if money(fill['filled']) == 0:
            self.journal.finish({**entry, **fill, 'status': 'unfilled'})
            return 'unfilled'
        market = self.broker.market(entry['ticker'])
        if market.get('status') not in ('settled', 'finalized') or market.get('result') not in ('yes', 'no'):
            orders, positions = self.broker.account()
            actual = {p['ticker']: money(p['position_fp']) for p in positions if money(p['position_fp']) != 0}
            expected = money(fill['filled']) * (1 if entry['side'] == 'yes' else -1)
            if orders or actual != {entry['ticker']: expected}:
                raise RuntimeError('POSITION_ACCOUNTING_MISMATCH')
            return 'held_to_settlement'
        settlements = self.broker.pages('/portfolio/settlements', 'settlements', ticker=entry['ticker'])
        rows = [s for s in settlements if s['ticker'] == entry['ticker'] and s.get('exchange_index') == entry['intent']['exchange_index']]
        if not rows:
            return 'settlement_accounting_pending'
        if len(rows) != 1:
            raise RuntimeError('AMBIGUOUS_ACCOUNT_SETTLEMENT')
        row = rows[0]
        side, other = entry['side'], 'no' if entry['side'] == 'yes' else 'yes'
        payout = money(fill['filled']) if market['result'] == side else money(0)
        if (row['market_result'] != market['result'] or money(row[side + '_count_fp']) != money(fill['filled'])
                or money(row[other + '_count_fp']) != 0
                or money(row[side + '_total_cost_dollars']) != money(fill['cost'])
                or money(row['revenue']) / 100 != payout or money(row['fee_cost']) != money(fill['fees'])):
            raise RuntimeError('SETTLEMENT_RECONCILIATION_MISMATCH')
        orders, positions = self.broker.account()
        if orders or any(money(p['position_fp']) != 0 for p in positions):
            raise RuntimeError('SETTLEMENT_NOT_FLAT')
        net = payout - money(fill['cost']) - money(fill['fees'])
        self.journal.finish({**entry, **fill, 'status': 'settled', 'net_pnl': str(net),
                             'settlement': row}, net)
        return 'settled'
