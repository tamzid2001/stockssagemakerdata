"""Private, fenced, bounded execution journal; never public Git or artifacts."""
from dataclasses import asdict
from decimal import InvalidOperation
import hashlib
import json
import time

from .store import Store, claim_transition
from .kalshi_execution import recovery, money


SIZING_FIELDS = frozenset({'starting_contracts', 'recovery_multiplier', 'max_recovery_increases'})
LEGACY_VERSION = 'btc-p90-sticky-hold-live-v2'
CAPPED_VERSION = 'btc-p90-sticky-hold-live-v3'


def empty_stats():
    return {'intents': 0, 'retries': 0, 'acknowledged': 0, 'rejected': 0, 'unfilled': 0,
            'settled': 0, 'wins': 0, 'losses': 0, 'breakeven': 0,
            'requested_contracts': '0', 'filled_contracts': '0'}


def with_stats(state):
    return {**state, 'stats': {**empty_stats(), **state.get('stats', {})}}


SESSION_COUNT_FIELDS = ('intents', 'retries', 'acknowledged', 'rejected', 'unfilled',
                        'settled', 'wins', 'losses', 'breakeven')
SESSION_DECIMAL_FIELDS = ('requested_contracts', 'filled_contracts')


def session_statistics(current, baseline):
    """Return process-local activity without resetting durable risk accounting."""
    result = {field: int(current.get(field, 0)) - int(baseline.get(field, 0))
              for field in SESSION_COUNT_FIELDS}
    result.update({field: str(money(current.get(field, '0')) - money(baseline.get(field, '0')))
                   for field in SESSION_DECIMAL_FIELDS})
    result['realized_net_pnl'] = str(money(current.get('realized_net_pnl', '0')) -
                                     money(baseline.get('realized_net_pnl', '0')))
    return result


def approved_reconfiguration(existing, requested, state):
    """Allow reviewed sizing changes only between complete recovery cycles."""
    normalized = {**existing, 'starting_contracts': existing.get('starting_contracts', 1),
        'recovery_multiplier': existing.get('recovery_multiplier', '2.5')}
    if normalized == requested:
        return {**state, 'recovery_increases': state.get('recovery_increases', 0)}
    # Firestore set(..., merge=True) merged the v4 configuration map into an
    # older v2/v3 map, retaining retired risk-limit keys. Those keys are not
    # active v4 settings. Strip only the exact previously approved legacy
    # values; never treat a changed live sizing field as a harmless cleanup.
    if normalized.get('version') == requested['version']:
        stale = set(normalized) - set(requested)
        if stale and stale <= {'max_contracts', 'max_order_dollars', 'daily_loss_dollars'}:
            if 'max_contracts' in stale and normalized['max_contracts'] != 100:
                raise RuntimeError('LEGACY_CAP_MIGRATION_NOT_APPROVED')
            if any(normalized[key] != '100' for key in stale & {'max_order_dollars', 'daily_loss_dollars'}):
                raise RuntimeError('LEGACY_DOLLAR_LIMIT_MIGRATION_NOT_APPROVED')
            for key in stale:
                normalized.pop(key)
            if normalized == requested:
                return {**state, 'recovery_increases': state.get('recovery_increases', 0)}
    legacy = normalized.get('version') in (LEGACY_VERSION, CAPPED_VERSION)
    if legacy:
        if (normalized.get('max_order_dollars') != '100'
                or normalized.get('daily_loss_dollars') != '100'):
            raise RuntimeError('LEGACY_DOLLAR_LIMIT_MIGRATION_NOT_APPROVED')
        normalized.pop('max_order_dollars')
        normalized.pop('daily_loss_dollars')
        if 'max_contracts' in normalized:
            if normalized['max_contracts'] != 100:
                raise RuntimeError('LEGACY_CAP_MIGRATION_NOT_APPROVED')
            normalized.pop('max_contracts')
        normalized['version'] = requested['version']
        normalized['max_recovery_increases'] = requested['max_recovery_increases']
    changed = {key for key in set(normalized) | set(requested)
               if normalized.get(key) != requested.get(key)}
    if ((not changed and not legacy) or not changed <= SIZING_FIELDS or state.get('active')
            or money(state.get('cycle', '0')) != 0 or
            (legacy and int(state.get('size', -1)) != int(existing.get('starting_contracts', 1)))):
        raise RuntimeError('LIVE_CONFIG_CHANGE_REQUIRES_RECOVERY_ZERO')
    return {**state, 'size': requested['starting_contracts'], 'recovery_increases': 0}


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
            snapshot = self.ref.get(transaction=tx)
            old = snapshot.to_dict() or {}
            requested = asdict(self.config)
            existing = old.get('configuration')
            state = with_stats(old.get('state', {'size': self.config.starting_contracts,
                'cycle': '0', 'net': '0', 'active': None, 'recovery_increases': 0}))
            if existing:
                state = approved_reconfiguration(existing, requested, state)
            lease = claim_transition(old.get('lease', {}), self.holder, time.time())
            values = {'lease': lease, 'configuration': requested,
                'paper_only': not self.live, 'state': state,
                'enabled': old.get('enabled', True)}
            if snapshot.exists:
                # update replaces the complete configuration map. A merge-set
                # retains deleted nested keys and re-triggers the guard at the
                # next 5-hour handoff while a recovery cycle is still open.
                tx.update(self.ref, values)
            else:
                tx.set(self.ref, values)
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
            if event in ('intent', 'retry_intent') and root.get('enabled') is not True:
                raise RuntimeError('LIVE_KILL_SWITCH_DISABLED')
            if len(json.dumps(state)) > 60000 or len(json.dumps(row)) > 60000:
                raise ValueError('BOUNDED_EXECUTION_STATE_REQUIRED')
            tx.update(self.ref, {'state': state, 'updated_at': time.time()})
            tx.set(market_ref, row)
            tx.set(market_ref.collection('events').document(), {'event': event, 'at': time.time(), 'holder': self.holder})
        self.transact(mutate)

    def begin(self, entry):
        def update(state, previous):
            state = with_stats(state)
            if state.get('active') or previous:
                raise RuntimeError('ENTRY_ALREADY_RESERVED')
            stats = state['stats']
            stats.update(intents=stats['intents'] + 1,
                requested_contracts=str(money(stats['requested_contracts']) +
                                        money(entry['intent']['count'])))
            return {**state, 'active': entry, 'stats': stats}, entry
        self.change('intent', entry['ticker'], update)

    def acknowledge(self, entry, acknowledgement):
        def update(state, previous):
            state = with_stats(state)
            active = state.get('active')
            if not active or active['intent'] != entry['intent']:
                raise RuntimeError('ACTIVE_INTENT_MISMATCH')
            if active.get('acknowledgement'):
                if active['acknowledgement'] != acknowledgement:
                    raise RuntimeError('ORDER_ACK_IMMUTABLE')
                return state, active
            active = {**active, 'status': 'acknowledged',
                      'acknowledgement': acknowledgement}
            stats = state['stats']
            stats['acknowledged'] += 1
            return {**state, 'active': active, 'stats': stats}, active
        self.change('acknowledged', entry['ticker'], update)

    def retry(self, completed, entry):
        """Atomically close a proven zero-fill IOC and reserve its next attempt."""
        def update(state, previous):
            state = with_stats(state)
            active = state.get('active')
            if (not active or active['intent'] != completed['intent']
                    or completed.get('status') != 'unfilled'
                    or money(completed.get('filled', '-1')) != 0
                    or entry.get('attempt') != active.get('attempt', 0) + 1):
                raise RuntimeError('RETRY_INTENT_MISMATCH')
            stats = state['stats']
            stats.update(unfilled=stats['unfilled'] + 1,
                retries=stats['retries'] + 1,
                intents=stats['intents'] + 1,
                requested_contracts=str(money(stats['requested_contracts']) +
                                        money(entry['intent']['count'])))
            history = list((previous or {}).get('attempt_history', []))[-31:]
            acknowledgement = active.get('acknowledgement', {})
            history.append({'attempt': active.get('attempt', 0),
                'client_order_id': active['intent']['client_order_id'],
                'order_id': acknowledgement.get('order_id'), 'status': 'unfilled',
                'filled': completed['filled'], 'at': time.time()})
            return {**state, 'active': entry, 'stats': stats}, {
                **entry, 'attempt_history': history}
        self.change('retry_intent', entry['ticker'], update)

    def finish(self, entry, net=None):
        def update(state, previous):
            state = with_stats(state)
            if not state.get('active') or state['active']['intent'] != entry['intent']:
                raise RuntimeError('ACTIVE_INTENT_MISMATCH')
            stats = state['stats']
            status = entry.get('status')
            if status == 'rejected':
                stats['rejected'] += 1
                stats['last_rejection_code'] = entry.get('rejection_code')
            elif status == 'unfilled':
                stats['unfilled'] += 1
            elif status == 'settled':
                stats['settled'] += 1
                stats['filled_contracts'] = str(money(stats['filled_contracts']) +
                                                money(entry['filled']))
                value = money(net)
                stats['wins' if value > 0 else 'losses' if value < 0 else 'breakeven'] += 1
                stats['last_settlement'] = {'ticker': entry['ticker'], 'side': entry['side'],
                    'contracts': entry['filled'], 'net_pnl': str(value), 'at': time.time()}
            if net is not None:
                state = recovery(state, net, self.config)
                day = time.strftime('%Y-%m-%d', time.gmtime())
                daily = money(state.get('daily_net', '0')) if state.get('day') == day else money(0)
                state.update(day=day, daily_net=str(daily + money(net)))
            return {**state, 'active': None, 'stats': stats}, entry
        self.change('closed', entry['ticker'], update)

    def public_summary(self):
        state = with_stats(self.state())
        active = state.get('active')
        safe_active = None
        if active:
            acknowledgement = active.get('acknowledgement', {})
            safe_active = {'ticker': active['ticker'], 'side': active['side'],
                'status': active.get('status'), 'requested_contracts': active['intent']['count'],
                'attempt': active.get('attempt', 0),
                'limit_ask': active.get('quote', {}).get(active['side'] + '_ask'),
                'acknowledged_fill': acknowledgement.get('acknowledged_fill'),
                'acknowledged_remaining': acknowledgement.get('acknowledged_remaining'),
                'created_at': active.get('created_at')}
        return {'active': safe_active, 'next_contracts': state.get('size'),
                'recovery_increases': state.get('recovery_increases', 0),
                'maximum_recovery_increases': self.config.max_recovery_increases,
                'recovery_cycle_pnl': state.get('cycle', '0'),
                'realized_net_pnl': state.get('net', '0'),
                'daily_net_pnl': state.get('daily_net', '0'), **state['stats']}

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
        quantity = int(state['size'])
        if quantity < 1:
            raise RuntimeError('INVALID_RECOVERY_STATE')
        intent = order_payload(signal['market_id'], side, quantity, ask, market['exchange_index'], self.config)
        # Conservative fee reserve; actual fills/fees, not this reserve, drive P&L.
        if self.broker.balance(market['exchange_index']) < quantity * (money(ask) + money('.07')):
            raise RuntimeError('INSUFFICIENT_SHARD_FUNDS')
        entry = {'ticker': signal['market_id'], 'side': side, 'intent': intent,
                 'signal': signal, 'quote': quote, 'created_at': now,
                 'attempt': 0,
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
        from .kalshi_execution import acknowledged_order, definitive_rejection
        try:
            response = self.broker.submit(intent)  # Never retry an ambiguous POST.
        except RuntimeError as exc:
            code = str(exc)
            if definitive_rejection(code):
                rejected = {**entry, 'status': 'rejected', 'rejection_code': code}
                self.journal.finish(rejected)
            raise
        acknowledgement = acknowledged_order(response, intent)
        self.journal.acknowledge(entry, acknowledgement)
        return {**entry, 'status': 'acknowledged', 'acknowledgement': acknowledgement}

    def retry_unfilled(self, entry, fill):
        """Retry only an authoritatively zero-filled IOC with a fresh live ask."""
        from .engine import stamp
        from .kalshi_execution import acknowledged_order, definitive_rejection, order_payload
        completed = {**entry, **fill, 'status': 'unfilled'}
        market = self.broker.market(entry['ticker'])
        now = time.time()
        try:
            close_at = stamp(market['close_time'])
        except (KeyError, TypeError, ValueError):
            raise RuntimeError('MARKET_CLOSE_TIME_UNAVAILABLE') from None
        signal = entry.get('signal', {})
        if (signal.get('agrees') is not True or market.get('status') != 'active'
                or market.get('market_type') != 'binary' or now >= close_at - 5):
            self.journal.finish(completed)
            return 'unfilled'
        try:
            opened_at = stamp(market['open_time'])
        except (KeyError, TypeError, ValueError):
            raise RuntimeError('MARKET_OPEN_TIME_UNAVAILABLE') from None
        if close_at != signal.get('market_end') or close_at - opened_at != 900:
            raise RuntimeError('MARKET_CLOSE_TIME_CHANGED')
        orders, positions = self.broker.account()
        if orders or any(money(p['position_fp']) != 0 for p in positions):
            raise RuntimeError('RETRY_ACCOUNT_NOT_FLAT')
        side = entry['side']
        try:
            ask = money(market[side + '_ask_dollars'])
        except (KeyError, TypeError, ValueError, InvalidOperation):
            raise RuntimeError('EXECUTABLE_QUOTE_UNAVAILABLE') from None
        quantity = int(money(entry['intent']['count']))
        attempt = int(entry.get('attempt', 0)) + 1
        try:
            intent = order_payload(entry['ticker'], side, quantity, ask,
                                   market['exchange_index'], self.config, attempt=attempt)
        except ValueError as exc:
            if str(exc) == 'ORDER_RISK_LIMIT':
                return 'retry_waiting_for_executable_quote'
            raise
        if self.broker.balance(market['exchange_index']) < quantity * (ask + money('.07')):
            raise RuntimeError('INSUFFICIENT_SHARD_FUNDS')
        quote = {'timestamp': int(now), 'received_at': now, 'timely': True,
                 side + '_ask': str(ask), 'source': 'authoritative_market_retry'}
        retry = {'ticker': entry['ticker'], 'side': side, 'intent': intent,
                 'signal': signal, 'quote': quote, 'created_at': now,
                 'attempt': attempt, 'status': 'delivery_unknown'}
        # This transaction both accounts for the prior no-fill and reserves the
        # next unique client order ID. A crash cannot cause a duplicate POST.
        self.journal.retry(completed, retry)
        self.journal.before_post()
        if time.time() >= close_at - 5:
            self.journal.finish({**retry, 'status': 'expired_before_submit'})
            return 'retry_expired_before_submit'
        try:
            response = self.broker.submit(intent)
        except RuntimeError as exc:
            code = str(exc)
            if definitive_rejection(code):
                self.journal.finish({**retry, 'status': 'rejected',
                                     'rejection_code': code})
            raise
        acknowledgement = acknowledged_order(response, intent)
        self.journal.acknowledge(retry, acknowledgement)
        return 'retry_acknowledged'

    def reconcile(self, *, allow_order_retry=True):
        from .engine import stamp
        from .kalshi_execution import reconciled_order
        state = self.journal.state()
        entry = state.get('active')
        if not entry:
            return 'flat'
        acknowledgement = entry.get('acknowledgement') or {}
        order = self.broker.find_order(entry['intent'], acknowledgement.get('order_id'))
        if order is None:
            # An acknowledgement proves acceptance, even if the authenticated
            # account read model has not caught up yet. A missing acknowledgement
            # remains ambiguous and must block resubmission.
            return ('acknowledged_waiting_for_read_model' if acknowledgement
                    else 'unknown_delivery_blocked')
        fill = reconciled_order(order, entry['intent'], entry['side'])
        if money(fill['filled']) == 0:
            if not allow_order_retry:
                return 'zero_fill_retry_disabled'
            return self.retry_unfilled(entry, fill)
        market = self.broker.market(entry['ticker'])
        if market.get('status') not in ('settled', 'finalized') or market.get('result') not in ('yes', 'no'):
            orders, positions = self.broker.account()
            actual = {p['ticker']: money(p['position_fp']) for p in positions if money(p['position_fp']) != 0}
            expected = money(fill['filled']) * (1 if entry['side'] == 'yes' else -1)
            if orders:
                raise RuntimeError('POSITION_ACCOUNTING_MISMATCH')
            if actual == {entry['ticker']: expected}:
                return 'held_to_settlement'
            # Kalshi's portfolio position can disappear after the official
            # market close before the market and settlement read models expose
            # the final result. The exact terminal order above proves the fill;
            # an empty account after close therefore remains blocked on the
            # durable active entry until authoritative settlement arrives. It
            # must never be interpreted as flat or eligible for another order.
            try:
                close_at = stamp(market['close_time'])
            except (KeyError, TypeError, ValueError):
                raise RuntimeError('MARKET_CLOSE_TIME_UNAVAILABLE') from None
            if time.time() >= close_at and not actual:
                return 'settlement_accounting_pending'
            raise RuntimeError('POSITION_ACCOUNTING_MISMATCH')
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
