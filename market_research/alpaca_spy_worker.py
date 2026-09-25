"""SPY 0DTE paper/live hourly worker. Never runs inside an HTTP request.

The broker is the authority for order fills and positions; Firestore only holds
the forecast snapshot, an expiring worker lease, and order intent identifiers.
Live mode requires a separately approved SHA, real-time SIP/OPRA entitlements,
and one-contract long-option approval. No entry can debit more than $200.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import os
import threading
import time
from zoneinfo import ZoneInfo

from .alpaca_spy_strategy import (
    MAX_PREMIUM_DOLLARS, NEW_YORK, MinuteBar, active_window, entry_signal,
    exit_reason, nearest_atm_contract, tradable_windows,
)


TERMINAL = frozenset({'filled', 'canceled', 'expired', 'rejected', 'done_for_day'})
ACTIVE = frozenset({'new', 'accepted', 'partially_filled', 'pending_new', 'pending_cancel', 'held'})


def event(name: str, **details):
    print(json.dumps({'event': name, **details}, sort_keys=True, default=str), flush=True)


class AlpacaAPI:
    def __init__(self, mode: str):
        import httpx

        self.mode = mode
        self.key = os.environ.get('ALPACA_LIVE_API_KEY' if mode == 'live' else 'ALPACA_API_KEY', '')
        self.secret = os.environ.get('ALPACA_LIVE_SECRET_KEY' if mode == 'live' else 'ALPACA_SECRET_KEY', '')
        if not self.key or not self.secret:
            raise RuntimeError('ALPACA_CREDENTIALS_MISSING')
        self.trading = 'https://api.alpaca.markets' if mode == 'live' else 'https://paper-api.alpaca.markets'
        self.data = 'https://data.alpaca.markets'
        self.stock_feed = 'sip' if mode == 'live' else 'iex'
        self.option_feed = 'opra' if mode == 'live' else 'indicative'
        self.client = httpx.Client(timeout=15, headers={
            'APCA-API-KEY-ID': self.key, 'APCA-API-SECRET-KEY': self.secret,
            'Accept': 'application/json',
        })

    def request(self, method: str, path: str, *, data=False, params=None, payload=None):
        base = self.data if data else self.trading
        response = self.client.request(method, base + path, params=params, json=payload)
        if response.status_code == 404 and method == 'GET' and path == '/v2/orders:by_client_order_id':
            return None
        if response.status_code >= 400:
            # Never emit upstream bodies: they may contain account or order data.
            raise RuntimeError(f'ALPACA_HTTP_{response.status_code}')
        return response.json() if response.content else {}

    def account(self):
        return self.request('GET', '/v2/account')

    def positions(self):
        return self.request('GET', '/v2/positions') or []

    def order_by_client_id(self, client_id: str):
        return self.request('GET', '/v2/orders:by_client_order_id', params={'client_order_id': client_id})

    def order_by_id(self, order_id: str):
        return self.request('GET', '/v2/orders/' + order_id)

    def submit(self, payload: dict):
        return self.request('POST', '/v2/orders', payload=payload)

    def cancel(self, order_id: str):
        return self.request('DELETE', '/v2/orders/' + order_id)

    def stock_bars(self, *, end: datetime, start: datetime | None = None, limit=1000):
        params = {'timeframe': '1Min', 'start': (start or end - timedelta(days=7)).isoformat(),
                  'end': end.isoformat(), 'limit': min(limit, 10000), 'sort': 'desc',
                  'adjustment': 'raw', 'feed': self.stock_feed}
        response = self.request('GET', '/v2/stocks/SPY/bars', data=True, params=params) or {}
        output = []
        for raw in response.get('bars') or []:
            try:
                stamp = datetime.fromisoformat(raw['t'].replace('Z', '+00:00'))
                close = stamp + timedelta(minutes=1)
                if close <= end and float(raw['c']) > 0:
                    output.append(MinuteBar(close, float(raw['c']), float(raw['h']), float(raw['l'])))
            except (KeyError, TypeError, ValueError):
                continue
        return sorted({bar.end: bar for bar in output}.values(), key=lambda bar: bar.end)[-limit:]

    def contracts(self, expiration: str):
        params = {'underlying_symbols': 'SPY', 'expiration_date': expiration,
                  'status': 'active', 'limit': '10000'}
        response = self.request('GET', '/v2/options/contracts', params=params) or {}
        if response.get('next_page_token'):
            raise RuntimeError('OPTION_CHAIN_TRUNCATED')
        return response.get('option_contracts') or []

    def option_quote(self, symbol: str):
        response = self.request('GET', '/v1beta1/options/quotes/latest', data=True,
                                params={'symbols': symbol, 'feed': self.option_feed}) or {}
        return (response.get('quotes') or {}).get(symbol) or {}


class Journal:
    def __init__(self, mode: str, account_id: str, day: str):
        import firebase_admin
        from firebase_admin import credentials, firestore

        service = os.environ.get('FIREBASE_SERVICE_ACCOUNT_JSON', '')
        if not service:
            raise RuntimeError('DURABLE_JOURNAL_REQUIRED')
        if not firebase_admin._apps:
            firebase_admin.initialize_app(credentials.Certificate(json.loads(service)))
        account_fingerprint = hashlib.sha256(account_id.encode()).hexdigest()[:16]
        self.db = firestore.client()
        self.ref = self.db.collection('alpaca_spy_0dte_workers').document(
            f'{mode}-{account_fingerprint}-{day}')
        self.owner = os.environ.get('GITHUB_RUN_ID') or f'local-{os.getpid()}'

    def claim(self):
        from firebase_admin import firestore

        transaction = self.db.transaction()

        @firestore.transactional
        def take(tx):
            snapshot = self.ref.get(transaction=tx)
            state = snapshot.to_dict() or {}
            if state.get('lease_expires', 0) > time.time() and state.get('lease_owner') != self.owner:
                raise RuntimeError('WORKER_LEASE_HELD')
            tx.set(self.ref, {'lease_owner': self.owner, 'lease_expires': time.time() + 90}, merge=True)

        take(transaction)

    def heartbeat(self):
        self.update(lease_expires=time.time() + 90)

    def load(self) -> dict:
        return self.ref.get().to_dict() or {}

    def update(self, **changes):
        from firebase_admin import firestore

        transaction = self.db.transaction()

        @firestore.transactional
        def apply(tx):
            state = self.ref.get(transaction=tx).to_dict() or {}
            if state.get('lease_owner') != self.owner or state.get('lease_expires', 0) <= time.time():
                raise RuntimeError('WORKER_LEASE_LOST')
            tx.set(self.ref, changes, merge=True)

        apply(transaction)

    def release(self):
        from firebase_admin import firestore

        transaction = self.db.transaction()

        @firestore.transactional
        def expire(tx):
            state = self.ref.get(transaction=tx).to_dict() or {}
            if state.get('lease_owner') == self.owner:
                tx.set(self.ref, {'lease_expires': 0}, merge=True)

        expire(transaction)


def forecast_at(api: AlpacaAPI, window, journal: Journal):
    key = window.start.isoformat()
    cached = journal.load().get('forecast') or {}
    if cached.get('window') == key:
        return cached['result']
    # The first completed bar arrives after the scheduled window opens. Never
    # relabel yesterday's close (or a sparse premarket quote) as 09:30 data.
    now = datetime.now(timezone.utc)
    history = api.stock_bars(end=now, limit=1000)[-500:]
    if not history or history[-1].end < window.start + timedelta(minutes=1):
        raise RuntimeError('WINDOW_FIRST_MINUTE_NOT_COMPLETE')
    if len(history) < 40:
        raise RuntimeError('INSUFFICIENT_REAL_SPY_MINUTES')
    from ensemble_forecasting.worker import execute_job
    from ensemble_forecasting.capabilities import timesfm_availability
    if not timesfm_availability('production')[0]:
        raise RuntimeError('FIVE_MODEL_ENSEMBLE_UNAVAILABLE')
    models = ('prophet', 'toto', 'granite', 'chronos', 'timesfm')
    payload = {'request': {
        'prediction_length': 60, 'horizon_mode': 'frequency_periods', 'frequency': '1min',
        'calendar': 'NONE', 'transform': 'auto', 'context_length': 500,
        'failure_policy': 'fail', 'quantiles': [0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99],
        'models': {name: {'enabled': True, 'weight': 1} for name in models},
    }, 'runtime_mode': 'production',
        'input': {'rows': [{'timestamp': bar.end.isoformat(), 'target': bar.close} for bar in history], 'frequency': '1min'},
    }
    result = execute_job(payload)
    if len(result.get('model_runs') or []) != 5 or any(m['status'] != 'completed' for m in result['model_runs']):
        raise RuntimeError('FIVE_MODEL_ENSEMBLE_INCOMPLETE')
    first_predicted = datetime.fromisoformat(result['predictions'][0]['timestamp'].replace('Z', '+00:00'))
    if first_predicted != history[-1].end + timedelta(minutes=1):
        raise RuntimeError('FORECAST_TIMESTAMP_MISMATCH')
    # Small immutable quantile matrix and provenance only; no component arrays.
    snapshot = {'window': key, 'result': {'predictions': result['predictions'],
        'dataset_hash': result['dataset_hash'], 'model_runs': result['model_runs'],
        'input_count': len(history), 'forecast_at': datetime.now(timezone.utc).isoformat()}}
    journal.update(forecast=snapshot)
    event('hourly_forecast', window=key, observations=len(history), models=5)
    return snapshot['result']


def client_id(mode: str, window, kind: str, bar_end: datetime) -> str:
    return f'qspy{mode[0]}{window.start.strftime("%y%m%d%H%M")}{kind[0]}{bar_end.strftime("%H%M")}'


def reconcile_order(api: AlpacaAPI, identifier: str, payload: dict, *, timeout_seconds=12):
    existing = api.order_by_client_id(identifier)
    if existing is None:
        try:
            existing = api.submit({**payload, 'client_order_id': identifier})
        except Exception:
            # Unknown delivery is not a rejection: never blindly send a second order.
            existing = api.order_by_client_id(identifier)
            if existing is None:
                raise RuntimeError('ORDER_DELIVERY_UNKNOWN')
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        current = api.order_by_client_id(identifier) or existing
        if current.get('status') in TERMINAL:
            return current
        time.sleep(1)
    if existing.get('id'):
        try:
            api.cancel(existing['id'])
        except RuntimeError:
            # The fill/cancel may have won the race; reconcile by client ID.
            pass
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        current = api.order_by_client_id(identifier) or existing
        if current.get('status') in TERMINAL:
            return current
        time.sleep(1)
    raise RuntimeError('ORDER_CANCEL_UNCONFIRMED')


def owned_position(api: AlpacaAPI, journal: Journal):
    state = journal.load()
    active = state.get('active') or None
    intent = state.get('intent') or None
    if intent and not active:
        order = api.order_by_client_id(intent['id'])
        if order and Decimal(str(order.get('filled_qty') or 0)) == 1:
            active = {**intent, 'entry_id': intent['id'], 'entered_at': order.get('filled_at')}
            journal.update(active=active, intent=None)
        elif order and order.get('status') in TERMINAL:
            journal.update(intent=None)
        else:
            raise RuntimeError('ENTRY_ORDER_UNRESOLVED')
    exit_intent = state.get('exit_intent') or None
    if exit_intent:
        order = api.order_by_client_id(exit_intent['id'])
        if order and Decimal(str(order.get('filled_qty') or 0)) == 1:
            still_held = any(p.get('symbol') == exit_intent['symbol'] for p in api.positions())
            if still_held:
                raise RuntimeError('EXIT_ORDER_UNRESOLVED')
            journal.update(active=None, exit_intent=None)
            active = None
        elif order and order.get('status') in TERMINAL:
            journal.update(exit_intent=None)
        else:
            raise RuntimeError('EXIT_ORDER_UNRESOLVED')
    positions = [p for p in api.positions() if str(p.get('symbol', '')).startswith('SPY')
                 and p.get('asset_class') == 'us_option']
    if not active:
        if positions:
            raise RuntimeError('UNOWNED_SPY_OPTION_POSITION')
        return None
    matching = [p for p in positions if p.get('symbol') == active['symbol']]
    if len(positions) != 1 or len(matching) != 1 or Decimal(str(matching[0].get('qty'))) != 1:
        raise RuntimeError('POSITION_ACCOUNTING_MISMATCH')
    return active


def enter(api: AlpacaAPI, journal: Journal, window, kind: str, bar: MinuteBar):
    if owned_position(api, journal):
        return
    account = api.account()
    if int(account.get('options_trading_level') or 0) < 2 or account.get('trading_blocked'):
        raise RuntimeError('LONG_OPTIONS_APPROVAL_REQUIRED')
    contracts = api.contracts(window.start.date().isoformat())
    # Select nearest listed strike before checking the premium; no cheaper OTM substitution.
    ranked = [c for c in contracts if c.get('type') == kind and c.get('expiration_date') == window.start.date().isoformat()
              and c.get('tradable') is True and c.get('underlying_symbol') == 'SPY']
    if not ranked:
        event('entry_skipped', reason='NO_SAME_DAY_CONTRACT', kind=kind)
        return
    ranked.sort(key=lambda c: (abs(float(c['strike_price'])-bar.close),
               not (float(c['strike_price']) <= bar.close if kind == 'call' else float(c['strike_price']) >= bar.close), c['symbol']))
    symbol = ranked[0]['symbol']
    quote = api.option_quote(symbol)
    choice = nearest_atm_contract(contracts, kind, bar.close, window.start.date().isoformat(),
                                  {symbol: quote}, now=datetime.now(timezone.utc))
    if choice is None:
        event('entry_skipped', reason='STALE_QUOTE_OR_PREMIUM_ABOVE_200', kind=kind, contract=symbol)
        return
    _, ask = choice
    if Decimal(str(account.get('options_buying_power') or '0')) < ask * 100:
        event('entry_skipped', reason='INSUFFICIENT_OPTIONS_BUYING_POWER', kind=kind)
        return
    identifier = client_id(api.mode, window, kind, bar.end)
    journal.update(intent={'id': identifier, 'symbol': symbol, 'kind': kind, 'window': window.start.isoformat()})
    order = reconcile_order(api, identifier, {'symbol': symbol, 'qty': '1', 'side': 'buy',
        'type': 'limit', 'limit_price': str(ask), 'time_in_force': 'day', 'extended_hours': False})
    if Decimal(str(order.get('filled_qty') or 0)) == 1:
        journal.update(active={'symbol': symbol, 'kind': kind, 'window': window.start.isoformat(),
            'entry_id': identifier, 'entered_at': datetime.now(timezone.utc).isoformat()}, intent=None)
        event('entry_filled', kind=kind, contract=symbol, premium=order.get('filled_avg_price'))
    else:
        journal.update(intent=None)
        event('entry_unfilled', kind=kind, contract=symbol, status=order.get('status'))


def exit_position(api: AlpacaAPI, journal: Journal, active: dict, reason: str, bar: MinuteBar):
    identifier = 'qspy' + api.mode[0] + hashlib.sha256(
        (active['entry_id'] + reason + bar.end.isoformat()).encode()).hexdigest()[:20]
    journal.update(exit_intent={'id': identifier, 'symbol': active['symbol'], 'reason': reason})
    order = reconcile_order(api, identifier, {'symbol': active['symbol'], 'qty': '1',
        'side': 'sell', 'type': 'market', 'time_in_force': 'day', 'extended_hours': False})
    if Decimal(str(order.get('filled_qty') or 0)) == 1:
        for _ in range(10):
            if not any(p.get('symbol') == active['symbol'] for p in api.positions()):
                journal.update(active=None, exit_intent=None)
                event('exit_filled', reason=reason, contract=active['symbol'], premium=order.get('filled_avg_price'))
                return
            time.sleep(1)
        raise RuntimeError('POSITION_CLOSE_UNCONFIRMED')
    else:
        event('exit_pending', reason=reason, contract=active['symbol'], status=order.get('status'))


def run(mode: str, duration_minutes: int):
    import pandas_market_calendars as mcal

    if mode == 'live':
        approved = os.environ.get('QUANTURA_ALPACA_APPROVED_SHA', '')
        if os.environ.get('QUANTURA_ALPACA_LIVE_ENABLED') != 'true' or not approved \
                or os.environ.get('QUANTURA_CODE_SHA') != approved:
            raise RuntimeError('LIVE_APPROVAL_REQUIRED')
    api = AlpacaAPI(mode)
    account = api.account()
    if int(account.get('options_trading_level') or 0) < 2:
        raise RuntimeError('LONG_OPTIONS_APPROVAL_REQUIRED')
    if mode == 'live':
        # A delayed/indicative option feed must never drive live orders.
        api.option_feed = 'opra'
        api.stock_feed = 'sip'
    now = datetime.now(timezone.utc)
    day = now.astimezone(NEW_YORK).date()
    schedule = mcal.get_calendar('NYSE').schedule(start_date=day, end_date=day)
    if schedule.empty:
        event('market_closed', day=str(day), mode=mode)
        return
    opening = schedule.iloc[0]['market_open'].to_pydatetime()
    closing = schedule.iloc[0]['market_close'].to_pydatetime()
    windows = tradable_windows(opening, closing)
    if not windows:
        event('no_tradable_window', day=str(day), mode=mode)
        return
    journal = Journal(mode, str(account['id']), str(day))
    journal.claim()
    heartbeat_stop = threading.Event()
    heartbeat_failure: list[Exception] = []

    def keep_lease():
        while not heartbeat_stop.wait(20):
            try:
                journal.heartbeat()
            except Exception as exc:
                heartbeat_failure.append(exc)
                heartbeat_stop.set()

    heartbeat_thread = threading.Thread(target=keep_lease, name='alpaca-spy-lease', daemon=True)
    heartbeat_thread.start()
    deadline = time.monotonic() + duration_minutes * 60
    last_bar_end = None
    forecast_failure_until = 0.0
    previous_window = None
    try:
        while time.monotonic() < deadline or journal.load().get('active'):
            now = datetime.now(timezone.utc)
            if heartbeat_failure:
                raise RuntimeError('WORKER_LEASE_REFRESH_FAILED')
            window = active_window(now, windows)
            if window and window.start != previous_window:
                forecast_failure_until = 0.0
                previous_window = window.start
            try:
                active = owned_position(api, journal)
            except RuntimeError as exc:
                if str(exc) in {'ENTRY_ORDER_UNRESOLVED', 'EXIT_ORDER_UNRESOLVED'}:
                    event('order_reconciliation_pending', code=str(exc))
                    time.sleep(5)
                    continue
                raise
            if window is None:
                if active and now.astimezone(NEW_YORK) >= windows[-1].end:
                    exit_position(api, journal, active, 'end_of_day', MinuteBar(now, 0, 0, 0))
                if now.astimezone(NEW_YORK) >= windows[-1].end and not journal.load().get('active'):
                    break
                time.sleep(5)
                continue
            if active and active['window'] != window.start.isoformat():
                # The prior-hour position is closed before model loading, which
                # can take minutes on a cold runner.
                exit_position(api, journal, active, 'window_end', MinuteBar(now, 0, 0, 0))
                time.sleep(1)
                continue
            if active and (journal.load().get('forecast') or {}).get('window') != window.start.isoformat():
                # A restart without the immutable snapshot cannot manage the
                # held option's P50/P90 exits. Flatten before inference.
                exit_position(api, journal, active, 'forecast_snapshot_missing', MinuteBar(now, 0, 0, 0))
                time.sleep(1)
                continue
            forecast = None
            if time.monotonic() >= forecast_failure_until:
                try:
                    forecast = forecast_at(api, window, journal)
                except Exception as exc:
                    event('forecast_unavailable', code=type(exc).__name__)
                    # The first bar is expected to be absent at exactly 09:30.
                    # Retry that condition promptly, but back off model errors.
                    forecast_failure_until = time.monotonic() + (5 if str(exc) == 'WINDOW_FIRST_MINUTE_NOT_COMPLETE' else 300)
            recent = api.stock_bars(end=now, start=window.start.astimezone(timezone.utc)-timedelta(minutes=1), limit=100)
            if heartbeat_failure:
                raise RuntimeError('WORKER_LEASE_REFRESH_FAILED')
            if not recent:
                time.sleep(5)
                continue
            latest = recent[-1]
            if latest.end == last_bar_end:
                time.sleep(5)
                continue
            last_bar_end = latest.end
            if active and active['window'] != window.start.isoformat():
                exit_position(api, journal, active, 'window_end', latest)
            elif active and not forecast:
                # An open option must not be held with no usable stop/target
                # forecast after a failed or unavailable model run.
                exit_position(api, journal, active, 'forecast_unavailable', latest)
            elif active and forecast:
                reason = exit_reason(active['kind'], latest, forecast['predictions'], window)
                if reason:
                    exit_position(api, journal, active, reason, latest)
            elif not active and forecast and len(recent) >= 2 and latest.end >= datetime.fromisoformat(forecast['forecast_at']) \
                    and now - latest.end < timedelta(minutes=2) and now.astimezone(NEW_YORK) < window.end-timedelta(minutes=5):
                kind = entry_signal(recent[-2], latest, forecast['predictions'])
                if kind:
                    enter(api, journal, window, kind, latest)
            event('minute_heartbeat', mode=mode, window=window.start.isoformat(),
                  bar_end=latest.end.isoformat(), position=bool(journal.load().get('active')))
            time.sleep(5)
    finally:
        heartbeat_stop.set()
        heartbeat_thread.join(timeout=5)
        journal.release()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['verify', 'paper', 'live'], default='verify')
    parser.add_argument('--duration-minutes', type=int, default=165)
    args = parser.parse_args()
    if not 1 <= args.duration_minutes <= 165:
        parser.error('duration-minutes must be 1–165')
    if args.mode == 'verify':
        event('configuration', symbol='SPY', expiration='same_day', max_contracts=1,
              max_premium_dollars=str(MAX_PREMIUM_DOLLARS), hourly_windows='09:30–15:30 America/New_York',
              paper_endpoint='paper-api.alpaca.markets', live_gate_required=True, orders_sent=0)
        return
    try:
        run(args.mode, args.duration_minutes)
    except Exception as exc:
        event('worker_failed', code=str(exc) if str(exc).isupper() else type(exc).__name__)
        raise SystemExit(1) from None


if __name__ == '__main__':
    main()
