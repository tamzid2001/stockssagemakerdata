"""Private, fenced, bounded execution journal; never public Git or artifacts."""
from dataclasses import asdict
from decimal import InvalidOperation
import json
import time
import uuid

from .store import Store, claim_transition
from .kalshi_execution import recovery, money


SIZING_FIELDS = frozenset({'starting_contracts', 'recovery_multiplier', 'max_recovery_increases'})
LEGACY_VERSION = 'btc-p90-sticky-hold-live-v2'
CAPPED_VERSION = 'btc-p90-sticky-hold-live-v3'


def empty_stats():
    return {'intents': 0, 'retries': 0, 'acknowledged': 0, 'rejected': 0, 'unfilled': 0,
            'settled': 0, 'stopped': 0, 'stop_triggers': 0, 'stop_attempts': 0,
            'wins': 0, 'losses': 0, 'breakeven': 0,
            'requested_contracts': '0', 'filled_contracts': '0'}


def with_stats(state):
    return {**state, 'stats': {**empty_stats(), **state.get('stats', {})}}


SESSION_COUNT_FIELDS = ('intents', 'retries', 'acknowledged', 'rejected', 'unfilled',
                        'settled', 'stopped', 'stop_triggers', 'stop_attempts',
                        'wins', 'losses', 'breakeven')
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
    def __init__(self, config, key_id, holder, live, *, shared_protocol=False):
        from .kalshi_shared_account import strategy_session
        # BTC retains its existing account document and durable recovery state.
        # Each additional series has an independent fenced trade journal; order
        # submission is coordinated separately at the account boundary.
        identifier = strategy_session(key_id, config.subaccount, live,
            getattr(config, 'series_ticker', 'KXBTC15M'))
        super().__init__(identifier, holder)
        self.ref = self.db.collection('kalshi_execution_sessions').document(identifier)
        self.config, self.live, self.shared_protocol = config, live, shared_protocol

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
            if (self.live and getattr(self, 'shared_protocol', False) and
                    getattr(self.config, 'series_ticker', 'KXBTC15M') == 'KXBTC15M'):
                values.update(shared_account_protocol=1, shared_account_fence=lease['fence'])
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
            if event in ('intent', 'retry_intent', 'stop_intent') and root.get('enabled') is not True:
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
        """Fold a terminal IOC fill into the target before reserving only its remainder."""
        def update(state, previous):
            state = with_stats(state)
            active = state.get('active')
            from .kalshi_execution import combined_entry_fill
            if (not active or active['intent'] != completed['intent']
                    or completed.get('status') != 'terminal'
                    or entry.get('attempt') != active.get('attempt', 0) + 1):
                raise RuntimeError('RETRY_INTENT_MISMATCH')
            total = combined_entry_fill(active, completed)
            target = money(active.get('target_contracts', active['intent']['count']))
            if (money(total['filled']) >= target or
                    money(entry['intent']['count']) != target - money(total['filled']) or
                    entry.get('accumulated') != {key: total[key] for key in ('filled', 'cost', 'fees')} or
                    money(entry.get('target_contracts', '-1')) != target):
                raise RuntimeError('RETRY_REMAINDER_MISMATCH')
            stats = state['stats']
            stats.update(unfilled=stats['unfilled'] + (money(completed['filled']) == 0),
                retries=stats['retries'] + 1,
                intents=stats['intents'] + 1,
                requested_contracts=str(money(stats['requested_contracts']) +
                                        money(entry['intent']['count'])))
            history = list((previous or {}).get('attempt_history', []))[-31:]
            acknowledgement = active.get('acknowledgement', {})
            history.append({'attempt': active.get('attempt', 0),
                'client_order_id': active['intent']['client_order_id'],
                'order_id': acknowledgement.get('order_id'), 'status': 'partial' if money(completed['filled']) else 'unfilled',
                'filled': completed['filled'], 'at': time.time()})
            reserved = {**entry, 'previous_entry': {key: value for key, value in active.items()
                if key != 'previous_entry'}}
            return {**state, 'active': reserved, 'stats': stats}, {
                **reserved, 'attempt_history': history}
        self.change('retry_intent', entry['ticker'], update)

    def reject_retry(self, entry, code):
        """A rejected top-up must never erase the already-filled position."""
        def update(state, previous):
            state = with_stats(state)
            active = state.get('active') or {}
            prior = active.get('previous_entry')
            if active.get('intent') != entry['intent'] or not prior:
                raise RuntimeError('RETRY_REJECTION_STATE_MISMATCH')
            restored = {**prior, 'attempt': active['attempt'],
                'retry_rejected_at': time.time(), 'retry_rejection_code': code}
            state['stats']['rejected'] += 1
            state['stats']['last_rejection_code'] = code
            return {**state, 'active': restored}, {
                **restored, 'attempt_history': (previous or {}).get('attempt_history', [])}
        self.change('retry_rejected', entry['ticker'], update)

    def trigger_stop(self, entry, fill, at):
        """Persist the five-cent trigger and authoritative entry fill before selling."""
        def update(state, previous):
            state = with_stats(state)
            active = state.get('active')
            if not active or active['intent'] != entry['intent'] or active.get('stop'):
                raise RuntimeError('STOP_TRIGGER_STATE_MISMATCH')
            if money(fill['filled']) <= 0:
                raise RuntimeError('STOP_REQUIRES_FILLED_ENTRY')
            active = {**active, **fill, 'stop': {'threshold': '0.05',
                'triggered_at': at, 'sold': '0', 'proceeds': '0', 'fees': '0',
                'attempt': 0, 'pending': None}}
            state['stats']['stop_triggers'] += 1
            return {**state, 'active': active}, active
        self.change('stop_triggered', entry['ticker'], update)

    def reserve_stop_order(self, entry, intent):
        def update(state, previous):
            state = with_stats(state)
            active = state.get('active')
            stop = (active or {}).get('stop') or {}
            if (not active or active['intent'] != entry['intent'] or not stop
                    or stop.get('pending') or money(stop['sold']) >= money(active['filled'])):
                raise RuntimeError('STOP_INTENT_STATE_MISMATCH')
            attempt = stop['attempt'] + 1
            if intent['client_order_id'] != str(uuid.uuid5(uuid.NAMESPACE_URL,
                    f"{entry['intent']['client_order_id']}:stop:{attempt}")):
                raise RuntimeError('STOP_INTENT_IDENTITY_MISMATCH')
            stop = {**stop, 'attempt': attempt, 'pending': {'intent': intent, 'at': time.time()}}
            active = {**active, 'stop': stop}
            state['stats']['stop_attempts'] += 1
            return {**state, 'active': active}, active
        self.change('stop_intent', entry['ticker'], update)

    def acknowledge_stop_order(self, entry, intent, acknowledgement):
        def update(state, previous):
            active = state.get('active')
            pending = ((active or {}).get('stop') or {}).get('pending')
            if not active or active['intent'] != entry['intent'] or not pending or pending['intent'] != intent:
                raise RuntimeError('STOP_ACK_STATE_MISMATCH')
            if pending.get('acknowledgement') and pending['acknowledgement'] != acknowledgement:
                raise RuntimeError('STOP_ACK_IMMUTABLE')
            pending = {**pending, 'acknowledgement': acknowledgement}
            active = {**active, 'stop': {**active['stop'], 'pending': pending}}
            return {**state, 'active': active}, active
        self.change('stop_acknowledged', entry['ticker'], update)

    def reconcile_stop_order(self, entry, intent, result):
        """Account one terminal IOC exactly once; keep only bounded totals."""
        def update(state, previous):
            active = state.get('active')
            stop = (active or {}).get('stop') or {}
            pending = stop.get('pending')
            if not active or active['intent'] != entry['intent'] or not pending or pending['intent'] != intent:
                raise RuntimeError('STOP_RECONCILIATION_STATE_MISMATCH')
            sold = money(stop['sold']) + money(result['filled'])
            if sold > money(active['filled']):
                raise RuntimeError('STOP_OVERSELL_UNVERIFIED')
            stop = {**stop, 'sold': str(sold),
                'proceeds': str(money(stop['proceeds']) + money(result['proceeds'])),
                'fees': str(money(stop['fees']) + money(result['fees'])),
                'pending': None, 'last_order_id': result.get('order_id')}
            active = {**active, 'stop': stop}
            return {**state, 'active': active}, active
        self.change('stop_reconciled', entry['ticker'], update)

    def reject_stop_order(self, entry, intent, code):
        def update(state, previous):
            active = state.get('active')
            stop = (active or {}).get('stop') or {}
            pending = stop.get('pending')
            if not active or active['intent'] != entry['intent'] or not pending or pending['intent'] != intent:
                raise RuntimeError('STOP_REJECTION_STATE_MISMATCH')
            active = {**active, 'stop': {**stop, 'pending': None,
                'last_rejection_code': code, 'last_rejection_at': time.time()}}
            return {**state, 'active': active}, active
        self.change('stop_rejected', entry['ticker'], update)

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
            elif status in ('settled', 'stopped'):
                stats[status] += 1
                stats['filled_contracts'] = str(money(stats['filled_contracts']) +
                                                money(entry['filled']))
                value = money(net)
                stats['wins' if value > 0 else 'losses' if value < 0 else 'breakeven'] += 1
                stats['last_settlement'] = {'ticker': entry['ticker'], 'side': entry['side'],
                    'contracts': entry['filled'], 'net_pnl': str(value), 'at': time.time(),
                    'exit_kind': status}
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
            stop = active.get('stop') or {}
            safe_active = {'ticker': active['ticker'], 'side': active['side'],
                'status': active.get('status'),
                'target_contracts': active.get('target_contracts', active['intent']['count']),
                'requested_contracts': active.get('target_contracts', active['intent']['count']),
                'latest_order_contracts': active['intent']['count'],
                'previously_reconciled_fills': (active.get('accumulated') or {}).get('filled', '0'),
                'attempt': active.get('attempt', 0),
                'limit_ask': active.get('quote', {}).get(active['side'] + '_ask'),
                'acknowledged_fill': acknowledgement.get('acknowledged_fill'),
                'acknowledged_remaining': acknowledgement.get('acknowledged_remaining'),
                'created_at': active.get('created_at'),
                'stop_triggered': bool(stop), 'stop_sold_contracts': stop.get('sold'),
                'stop_attempts': stop.get('attempt'),
                'stop_order_pending': bool(stop.get('pending'))}
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
    def __init__(self, config, broker, journal, coordinator=None):
        self.config, self.broker, self.journal = config, broker, journal
        self.coordinator = coordinator

    def enter(self, signal, quote, now):
        if self.coordinator:
            self.coordinator.claim()
        try:
            return self._enter(signal, quote, now)
        finally:
            if self.coordinator:
                self.coordinator.release()

    def _enter(self, signal, quote, now):
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
        if self.coordinator:
            self.coordinator.verify_positions(orders, positions)
        elif orders or any(money(p['position_fp']) != 0 for p in positions):
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
                 'target_contracts': intent['count'],
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
        if self.coordinator:
            try:
                self.coordinator.before_post()
            except RuntimeError as exc:
                self.journal.finish({**entry, 'status': 'rejected', 'rejection_code': str(exc)})
                raise
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

    def retry_unfilled(self, entry, fill, total, market):
        """Retry a terminal IOC for only the still-missing contracts."""
        if self.coordinator:
            self.coordinator.claim()
        try:
            return self._retry_unfilled(entry, fill, total, market)
        finally:
            if self.coordinator:
                self.coordinator.release()

    def _stop_position(self, entry):
        orders, positions = self.broker.account()
        own = [p for p in positions if p['ticker'] == entry['ticker'] and money(p['position_fp']) != 0]
        if len(own) > 1 or any(o.get('ticker') == entry['ticker'] for o in orders):
            raise RuntimeError('STOP_ACCOUNT_POSITION_AMBIGUOUS')
        if not self.coordinator and (orders or any(p['ticker'] != entry['ticker'] and
                money(p['position_fp']) != 0 for p in positions)):
            raise RuntimeError('STOP_FOREIGN_ACCOUNT_ACTIVITY')
        return money(own[0]['position_fp']) if own else money(0)

    def _submit_stop(self, entry, market):
        from .engine import stamp
        from .kalshi_execution import acknowledged_order, stop_exit_payload
        stop = entry['stop']
        # Entries stop five seconds early, but an already-triggered protective
        # exit should remain eligible until the exchange actually closes.
        if market.get('status') != 'active' or time.time() >= stamp(market['close_time']):
            return 'stop_waiting_for_settlement'
        raw_bid = market.get(entry['side'] + '_bid_dollars')
        if raw_bid is None or money(raw_bid) <= 0:
            return 'stop_waiting_for_executable_bid'
        remaining = money(entry['filled']) - money(stop['sold'])
        expected = remaining * (1 if entry['side'] == 'yes' else -1)
        if self.coordinator:
            self.coordinator.claim()
        try:
            orders, positions = self.broker.account()
            if self.coordinator:
                actual = self.coordinator.verify_positions(orders, positions)
                if actual.get(entry['ticker'], money(0)) != expected:
                    return 'stop_position_read_lag'
            elif self._stop_position(entry) != expected:
                return 'stop_position_read_lag'
            intent = stop_exit_payload(entry, remaining, raw_bid,
                market['exchange_index'], self.config, attempt=stop['attempt'] + 1)
            self.journal.reserve_stop_order(entry, intent)
            self.journal.before_post()
            if self.coordinator:
                self.coordinator.before_post()
            # Never resubmit this intent after an ambiguous delivery. A later
            # authenticated terminal order read is required for another try.
            try:
                response = self.broker.submit(intent)
            except RuntimeError as exc:
                from .kalshi_execution import definitive_rejection
                if definitive_rejection(str(exc)):
                    self.journal.reject_stop_order(entry, intent, str(exc))
                    return 'stop_definitive_rejection_retry_wait'
                raise
            self.journal.acknowledge_stop_order(entry, intent, acknowledged_order(response, intent))
            return 'stop_exit_acknowledged'
        finally:
            if self.coordinator:
                self.coordinator.release()

    def _reconcile_stop(self, entry, market, *, allow_order_retry=True):
        from .kalshi_execution import reconciled_stop_order
        stop = entry['stop']
        pending = stop.get('pending')
        if pending:
            intent = pending['intent']
            acknowledgement = pending.get('acknowledgement') or {}
            order = self.broker.find_order(intent, acknowledgement.get('order_id'))
            if order is None:
                return 'stop_unknown_delivery_blocked'
            try:
                terminal = reconciled_stop_order(order, intent, entry['side'])
            except RuntimeError as exc:
                if str(exc) == 'STOP_ORDER_NOT_TERMINAL':
                    return 'stop_order_read_lag'
                raise
            fill = self.broker.exit_fill_summary(intent, entry['side'], terminal)
            if fill is None:
                return 'stop_fills_read_lag'
            # A terminal fill is not enough by itself: the portfolio position
            # must have decreased by exactly that quantity before recording a
            # realized exit. Kalshi's position read model may lag order/fills.
            from .engine import stamp
            if market.get('status') == 'active' and time.time() < stamp(market['close_time']):
                expected = (money(entry['filled']) - money(stop['sold']) -
                    money(fill['filled'])) * (1 if entry['side'] == 'yes' else -1)
                if self.coordinator:
                    orders, positions = self.broker.account()
                    try:
                        actual = self.coordinator.verify_positions(orders, positions)
                    except RuntimeError as exc:
                        if str(exc) == 'ACCOUNT_POSITION_UNVERIFIED':
                            return 'stop_position_read_lag'
                        raise
                    if actual.get(entry['ticker'], money(0)) != expected:
                        return 'stop_position_read_lag'
                elif self._stop_position(entry) != expected:
                    return 'stop_position_read_lag'
            self.journal.reconcile_stop_order(entry, intent, {**fill, 'order_id': terminal['order_id']})
            entry = self.journal.state()['active']
            stop = entry['stop']
        remaining = money(entry['filled']) - money(stop['sold'])
        if remaining < 0:
            raise RuntimeError('STOP_OVERSELL_UNVERIFIED')
        if remaining == 0:
            if self._stop_position(entry) != 0:
                return 'stop_flat_read_lag'
            # V2 ASK/BID can leave offsetting YES and NO contracts even when
            # the net portfolio position is zero. They settle to a fixed
            # payout, but cash is not released yet. Do not mark the trade
            # realized or clear its journal before authenticated settlement.
            if market.get('status') in ('settled', 'finalized') and market.get('result') in ('yes', 'no'):
                return self._settle_entry(entry, market)
            return 'stop_hedged_waiting_for_settlement'
        if market.get('status') in ('settled', 'finalized') and market.get('result') in ('yes', 'no'):
            return self._settle_entry(entry, market)
        if not allow_order_retry:
            return 'stop_retry_disabled'
        if stop.get('last_rejection_at') and time.time() - stop['last_rejection_at'] < 1:
            return 'stop_rejection_backoff'
        return self._submit_stop(entry, market)

    def _retry_unfilled(self, entry, fill, total, market):
        from .engine import stamp
        from .kalshi_execution import acknowledged_order, definitive_rejection, order_payload
        completed = {**fill, 'status': 'terminal', 'intent': entry['intent']}
        now = time.time()
        if entry.get('retry_rejection_code') in ('KALSHI_HTTP_401', 'KALSHI_HTTP_403'):
            return 'entry_authorization_rejected'
        if now - entry.get('retry_rejected_at', 0) < 5:
            return 'entry_rejection_backoff'
        try:
            close_at = stamp(market['close_time'])
        except (KeyError, TypeError, ValueError):
            raise RuntimeError('MARKET_CLOSE_TIME_UNAVAILABLE') from None
        signal = entry.get('signal', {})
        if (signal.get('agrees') is not True or market.get('status') != 'active'
                or market.get('market_type') != 'binary' or now >= close_at - 5):
            if money(total['filled']) == 0:
                self.journal.finish({**entry, **total, 'status': 'unfilled'})
                return 'unfilled'
            return 'partial_waiting_for_settlement'
        try:
            opened_at = stamp(market['open_time'])
        except (KeyError, TypeError, ValueError):
            raise RuntimeError('MARKET_OPEN_TIME_UNAVAILABLE') from None
        if close_at != signal.get('market_end') or close_at - opened_at != 900:
            raise RuntimeError('MARKET_CLOSE_TIME_CHANGED')
        orders, positions = self.broker.account()
        if self.coordinator:
            actual = self.coordinator.verify_positions(orders, positions)
            expected = money(total['filled']) * (1 if entry['side'] == 'yes' else -1)
            if actual.get(entry['ticker'], money(0)) != expected:
                return 'entry_position_read_lag'
        else:
            actual = {p['ticker']: money(p['position_fp']) for p in positions
                      if money(p['position_fp']) != 0}
            expected = money(total['filled']) * (1 if entry['side'] == 'yes' else -1)
            if orders or actual != ({entry['ticker']: expected} if expected else {}):
                return 'entry_position_read_lag'
        side = entry['side']
        try:
            ask = money(market[side + '_ask_dollars'])
        except (KeyError, TypeError, ValueError, InvalidOperation):
            return 'entry_waiting_for_executable_quote'
        target = money(entry.get('target_contracts', entry['intent']['count']))
        remainder = target - money(total['filled'])
        if remainder <= 0 or remainder != remainder.quantize(money('.01')):
            raise RuntimeError('ENTRY_REMAINDER_INVALID')
        quantity = int(remainder) if remainder == int(remainder) else remainder
        attempt = int(entry.get('attempt', 0)) + 1
        try:
            intent = order_payload(entry['ticker'], side, quantity, ask,
                                   market['exchange_index'], self.config, attempt=attempt,
                                   fractional_remainder=type(quantity) is not int)
        except ValueError as exc:
            if str(exc) == 'ORDER_RISK_LIMIT':
                return 'retry_waiting_for_executable_quote'
            raise
        if self.broker.balance(market['exchange_index']) < quantity * (ask + money('.07')):
            return 'entry_waiting_for_shard_funds'
        quote = {'timestamp': int(now), 'received_at': now, 'timely': True,
                 side + '_ask': str(ask), 'source': 'authoritative_market_retry'}
        retry = {'ticker': entry['ticker'], 'side': side, 'intent': intent,
                 'signal': signal, 'quote': quote, 'created_at': now,
                 'target_contracts': str(target),
                 'accumulated': {key: total[key] for key in ('filled', 'cost', 'fees')},
                 'attempt': attempt, 'status': 'delivery_unknown'}
        # This transaction both accounts for the prior no-fill and reserves the
        # next unique client order ID. A crash cannot cause a duplicate POST.
        self.journal.retry(completed, retry)
        try:
            self.journal.before_post()
            if self.coordinator:
                self.coordinator.before_post()
        except RuntimeError as exc:
            self.journal.reject_retry(retry, str(exc))
            raise
        if time.time() >= close_at - 5:
            self.journal.reject_retry(retry, 'ENTRY_EXPIRED_BEFORE_POST')
            return 'retry_expired_before_submit'
        try:
            response = self.broker.submit(intent)
        except RuntimeError as exc:
            code = str(exc)
            if definitive_rejection(code):
                self.journal.reject_retry(retry, code)
            raise
        acknowledgement = acknowledged_order(response, intent)
        self.journal.acknowledge(retry, acknowledgement)
        return 'retry_acknowledged'

    def reconcile(self, *, allow_order_retry=True):
        from .engine import stamp
        from .kalshi_execution import STOP_BID, combined_entry_fill, reconciled_order
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
        terminal = reconciled_order(order, entry['intent'], entry['side'])
        fill = combined_entry_fill(entry, terminal)
        market = self.broker.market(entry['ticker'])
        if entry.get('stop'):
            return self._reconcile_stop(entry, market, allow_order_retry=allow_order_retry)
        target = money(entry.get('target_contracts', entry['intent']['count']))
        if money(fill['filled']) < target:
            if money(fill['filled']) and market.get('status') == 'active':
                raw_bid = market.get(entry['side'] + '_bid_dollars')
                if raw_bid is not None and money(raw_bid) <= STOP_BID:
                    self.journal.trigger_stop(entry, fill, time.time())
                    return self._reconcile_stop(self.journal.state()['active'], market)
            if market.get('status') in ('settled', 'finalized') and market.get('result') in ('yes', 'no'):
                if money(fill['filled']) == 0:
                    self.journal.finish({**entry, **fill, 'status': 'unfilled'})
                    return 'unfilled'
                return self._settle_entry({**entry, **fill}, market)
            if not allow_order_retry:
                return 'entry_topup_retry_disabled'
            return self.retry_unfilled(entry, terminal, fill, market)
        if market.get('status') not in ('settled', 'finalized') or market.get('result') not in ('yes', 'no'):
            orders, positions = self.broker.account()
            actual = {p['ticker']: money(p['position_fp']) for p in positions if money(p['position_fp']) != 0}
            expected = money(fill['filled']) * (1 if entry['side'] == 'yes' else -1)
            if orders and (not self.coordinator or any(o.get('ticker') == entry['ticker'] for o in orders)):
                raise RuntimeError('POSITION_ACCOUNTING_MISMATCH')
            if (actual.get(entry['ticker']) == expected if self.coordinator
                    else actual == {entry['ticker']: expected}):
                if allow_order_retry and self.broker.enabled and market.get('status') == 'active':
                    raw_bid = market.get(entry['side'] + '_bid_dollars')
                    if raw_bid is not None and money(raw_bid) <= STOP_BID:
                        self.journal.trigger_stop(entry, fill, time.time())
                        return self._reconcile_stop(self.journal.state()['active'], market)
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
            if time.time() >= close_at and (entry['ticker'] not in actual if self.coordinator else not actual):
                return 'settlement_accounting_pending'
            raise RuntimeError('POSITION_ACCOUNTING_MISMATCH')
        return self._settle_entry({**entry, **fill}, market)

    def _settle_entry(self, entry, market):
        """Settle unsold remainder after fully reconciling every stop IOC."""
        stop = entry.get('stop') or {}
        remaining = money(entry['filled']) - money(stop.get('sold', '0'))
        if remaining < 0:
            raise RuntimeError('STOP_OVERSELL_UNVERIFIED')
        settlements = self.broker.pages('/portfolio/settlements', 'settlements', ticker=entry['ticker'])
        rows = [s for s in settlements if s['ticker'] == entry['ticker'] and s.get('exchange_index') == entry['intent']['exchange_index']]
        if not rows:
            return 'settlement_accounting_pending'
        if len(rows) != 1:
            raise RuntimeError('AMBIGUOUS_ACCOUNT_SETTLEMENT')
        row = rows[0]
        side, other = entry['side'], 'no' if entry['side'] == 'yes' else 'yes'
        payout = remaining if market['result'] == side else money(0)
        # A V2 opposite-book reduce-only match can remain as a YES/NO pair in
        # the settlement record. Its net exposure is the unsold remainder;
        # the paired contracts contribute the same payout in either outcome.
        actual_payout = (money(entry['filled']) if market['result'] == side
            else money(stop.get('sold', '0')))
        if (row['market_result'] != market['result']
                or money(row[side + '_count_fp']) != money(entry['filled'])
                or money(row[other + '_count_fp']) != money(stop.get('sold', '0'))
                or money(row['revenue']) / 100 != actual_payout
                or (not stop and (money(row[side + '_total_cost_dollars']) != money(entry['cost'])
                    or money(row['fee_cost']) != money(entry['fees'])))):
            raise RuntimeError('SETTLEMENT_RECONCILIATION_MISMATCH')
        orders, positions = self.broker.account()
        if ((orders and (not self.coordinator or any(o.get('ticker') == entry['ticker'] for o in orders)))
                or (any(money(p['position_fp']) != 0 for p in positions) if not self.coordinator
                    else any(p['ticker'] == entry['ticker'] and money(p['position_fp']) != 0 for p in positions))):
            raise RuntimeError('SETTLEMENT_NOT_FLAT')
        net = payout + money(stop.get('proceeds', '0')) - money(entry['cost']) - money(entry['fees']) - money(stop.get('fees', '0'))
        exit_kind = 'stopped' if stop and remaining == 0 else 'settled'
        self.journal.finish({**entry, 'status': exit_kind, 'net_pnl': str(net),
                             'settlement': row}, net)
        return exit_kind
