"""Approval-gated Kalshi V2 execution, separate from all read-only research.

Ported protocol/safety patterns from the user's polymarketfuturesbot KalshiREST
(reviewed main ca41716): YES-book direction, shard routing, durable intent before
POST and reconciliation after ambiguous delivery. No legacy ladder strategy.
"""
import base64
from dataclasses import asdict, dataclass
from decimal import Decimal, ROUND_FLOOR
import hashlib
import json
import os
import re
import time
import uuid

import httpx

BASE = 'https://external-api.kalshi.com/trade-api/v2'
VERSION = 'btc-p90-sticky-hold-live-v4'
COIN_SERIES = frozenset({'KXBNB15M', 'KXDOGE15M', 'KXETH15M', 'KXNEAR15M', 'KXZEC15M'})


def money(value):
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError('NONFINITE_ACCOUNT_VALUE')
    return result


@dataclass(frozen=True)
class Config:
    history_minutes: int = 1
    subaccount: int = 0
    direction_policy: str = 'provisional_near_close'
    starting_contracts: int = 1
    recovery_multiplier: str = '2.5'
    max_recovery_increases: int = 3
    max_ask: str = '0.99'
    version: str = VERSION

    def __post_init__(self):
        if (type(self.history_minutes) is not int or not 1 <= self.history_minutes <= 12
                or type(self.subaccount) is not int or not 0 <= self.subaccount <= 63
                or self.direction_policy not in ('confirmed', 'provisional_near_close')
                or type(self.starting_contracts) is not int or self.starting_contracts < 1
                or not 1 < money(self.recovery_multiplier) <= 5
                or type(self.max_recovery_increases) is not int or not 0 <= self.max_recovery_increases <= 6
                or not 0 < money(self.max_ask) < 1 or self.version != VERSION):
            raise ValueError('INVALID_LIVE_CONFIGURATION')

    @property
    def fingerprint(self):
        return hashlib.sha256(json.dumps(asdict(self), sort_keys=True).encode()).hexdigest()

    @property
    def series_ticker(self):
        return 'KXBTC15M'


@dataclass(frozen=True)
class CoinConfig:
    """Separate approval identity; BTC's persisted v4 fingerprint is unchanged."""
    series_ticker: str
    history_minutes: int = 1
    subaccount: int = 0
    direction_policy: str = 'provisional_near_close'
    starting_contracts: int = 1
    recovery_multiplier: str = '2.5'
    max_recovery_increases: int = 3
    max_ask: str = '0.99'
    version: str = 'coin-p90-sticky-hold-live-v1'

    def __post_init__(self):
        if self.series_ticker not in COIN_SERIES or type(self.subaccount) is not int or not 0 <= self.subaccount <= 63:
            raise ValueError('INVALID_COIN_CONFIGURATION')
        # Reuse all numerical/risk validation without changing the BTC schema.
        Config(self.history_minutes, self.subaccount, self.direction_policy,
            self.starting_contracts, self.recovery_multiplier,
            self.max_recovery_increases, self.max_ask)
        if self.version != 'coin-p90-sticky-hold-live-v1':
            raise ValueError('INVALID_LIVE_CONFIGURATION')

    @property
    def fingerprint(self):
        return hashlib.sha256(json.dumps(asdict(self), sort_keys=True).encode()).hexdigest()


def live_allowed(config, requested, env=None):
    env = os.environ if env is None else env
    prefix = ('QUANTURA_KALSHI_' + config.series_ticker[2:-3] + '_'
              if isinstance(config, CoinConfig) else 'QUANTURA_KALSHI_')
    return bool(requested
        and env.get(prefix + 'LIVE_ENABLED') == 'true'
        and env.get(prefix + 'APPROVED_CONFIG') == config.fingerprint
        and re.fullmatch('[a-f0-9]{40}', env.get('QUANTURA_CODE_SHA', ''))
        and env.get(prefix + 'APPROVED_SHA') == env.get('QUANTURA_CODE_SHA'))


def order_payload(ticker, side, quantity, ask, shard, config, *, attempt=0):
    if (not re.fullmatch(re.escape(config.series_ticker) + r'-[A-Z0-9-]+', ticker) or side not in ('yes', 'no')
            or type(quantity) is not int or quantity < 1
            or type(shard) is not int or shard < 0
            or type(attempt) is not int or not 0 <= attempt <= 1000):
        raise ValueError('INVALID_ORDER_INTENT')
    p = money(ask)
    if not 0 < p <= money(config.max_ask):
        raise ValueError('ORDER_RISK_LIMIT')
    # V2 is a YES book: buy NO == sell YES at the complementary limit.
    price = p if side == 'yes' else 1 - p
    # Create Order V2 accepts 2-4 decimal places. Responses may contain six,
    # but sending six in a request is outside the documented request contract.
    if price != price.quantize(Decimal('.0001')):
        raise ValueError('INVALID_PRICE_PRECISION')
    identity = f'{config.version}:{config.subaccount}:{ticker}'
    if attempt:
        identity += f':retry:{attempt}'
    return dict(ticker=ticker, side='bid' if side == 'yes' else 'ask',
        count=f'{quantity:.2f}', price=f'{price:.4f}',
        client_order_id=str(uuid.uuid5(uuid.NAMESPACE_URL, identity)),
        exchange_index=shard, subaccount=config.subaccount,
        time_in_force='immediate_or_cancel', post_only=False, reduce_only=False,
        self_trade_prevention_type='taker_at_cross', cancel_order_on_pause=True)


def recovery(state, net, config=None):
    """Same whole-contract floor rounding as the reported research; actual net fees."""
    config = config or Config()
    cycle = money(state.get('cycle', '0')) + money(net)
    size = int(state.get('size', config.starting_contracts))
    increases = int(state.get('recovery_increases', 0))
    if not 0 <= increases <= config.max_recovery_increases or size < 1:
        raise RuntimeError('INVALID_RECOVERY_STATE')
    if cycle >= 0:
        cycle, size, increases = Decimal(0), config.starting_contracts, 0
    elif money(net) < 0 and increases < config.max_recovery_increases:
        size = int((Decimal(size) * money(config.recovery_multiplier))
            .to_integral_value(rounding=ROUND_FLOOR))
        # The read-only v2 settlement bridge must finish its existing journal
        # under the old sizing rule. Only newly approved v4 sessions are uncapped.
        if config.version in ('btc-p90-sticky-hold-live-v2', 'btc-p90-sticky-hold-live-v3'):
            size = min(100, size)
        increases += 1
    return {**state, 'cycle': str(cycle), 'size': size, 'recovery_increases': increases,
            'net': str(money(state.get('net', '0')) + money(net))}


class KalshiExecution:
    """No automatic POST retries, no redirects, no arbitrary hosts or paths."""
    def __init__(self, config, *, requested_live=False, client=None, env=None):
        self.config = config
        self.env = os.environ if env is None else env
        self.enabled = live_allowed(config, requested_live, self.env)
        if requested_live and not self.enabled:
            raise RuntimeError('LIVE_APPROVAL_REQUIRED')
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
        self.key_id = self.env['KALSHI_API_KEY_ID']
        self.key = serialization.load_pem_private_key(self.env['KALSHI_PRIVATE_KEY'].encode(), None)
        if not isinstance(self.key, RSAPrivateKey):
            raise ValueError('RSA_KEY_REQUIRED')
        self.client = client or httpx.Client(timeout=10, follow_redirects=False)

    def request(self, method, path, *, params=None, body=None):
        if method not in ('GET', 'POST') or not re.fullmatch(r'/[a-zA-Z0-9_/-]+', path):
            raise ValueError('INVALID_EXECUTION_ROUTE')
        if method == 'POST' and (not self.enabled or path != '/portfolio/events/orders'):
            raise RuntimeError('WRITE_NOT_AUTHORIZED')
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding
        timestamp = str(int(time.time() * 1000))
        signature = self.key.sign((timestamp + method + '/trade-api/v2' + path).encode(),
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256())
        try:
            response = self.client.request(method, BASE + path, params=params, json=body,
                headers={'KALSHI-ACCESS-KEY': self.key_id, 'KALSHI-ACCESS-TIMESTAMP': timestamp,
                         'KALSHI-ACCESS-SIGNATURE': base64.b64encode(signature).decode()})
            if not 200 <= response.status_code < 300:
                raise RuntimeError('KALSHI_HTTP_' + str(response.status_code))
            value = response.json()
            if not isinstance(value, dict):
                raise ValueError('INVALID_EXCHANGE_RESPONSE')
            return value
        except (httpx.HTTPError, ValueError):
            raise RuntimeError('KALSHI_DELIVERY_OR_RESPONSE_UNKNOWN') from None

    def pages(self, path, key, **params):
        rows, seen = [], set()
        for _ in range(100):
            value = self.request('GET', path, params={**params, 'subaccount': self.config.subaccount, 'limit': 1000})
            if not isinstance(value.get(key), list):
                raise RuntimeError('ACCOUNT_LIST_UNAVAILABLE')
            rows.extend(value[key])
            cursor = value.get('cursor')
            if not cursor:
                return rows
            if cursor in seen:
                raise RuntimeError('ACCOUNT_CURSOR_LOOP')
            seen.add(cursor)
            params['cursor'] = cursor
        raise RuntimeError('ACCOUNT_PAGINATION_INCOMPLETE')

    def market(self, ticker):
        if not re.fullmatch(re.escape(self.config.series_ticker) + r'-[A-Z0-9-]+', ticker):
            raise ValueError('CONFIGURED_SERIES_MARKET_REQUIRED')
        value = self.request('GET', '/markets/' + ticker)['market']
        if value.get('ticker') != ticker or type(value.get('exchange_index')) is not int or value['exchange_index'] < 0:
            raise RuntimeError('AUTHORITATIVE_MARKET_SHARD_REQUIRED')
        return value

    def account(self):
        return (self.pages('/portfolio/orders', 'orders', status='resting'),
                self.pages('/portfolio/positions', 'market_positions', count_filter='position'))

    def balance(self, shard):
        row = self.request('GET', '/portfolio/balance', params={'subaccount': self.config.subaccount, 'exchange_index': shard})
        value = money(row['balance_dollars']) if 'balance_dollars' in row else money(row['balance']) / 100
        if value < 0:
            raise RuntimeError('INVALID_BALANCE')
        return value

    def get_order(self, order_id):
        if not re.fullmatch(r'[A-Za-z0-9-]{8,128}', order_id or ''):
            raise ValueError('INVALID_ORDER_ID')
        value = self.request('GET', '/portfolio/orders/' + order_id)
        order = value.get('order')
        if not isinstance(order, dict) or order.get('order_id') != order_id:
            raise RuntimeError('ORDER_LOOKUP_INVALID')
        return order

    def find_order(self, intent, order_id=None):
        if order_id:
            try:
                return self.get_order(order_id)
            except RuntimeError as exc:
                if str(exc) != 'KALSHI_HTTP_404':
                    raise
                # Kalshi documents a short delay between a write response and
                # authenticated read models. Fall back to the deterministic
                # client ID list lookup, but never infer that absence is safe
                # to resubmit.
        matches = [o for o in self.pages('/portfolio/orders', 'orders', ticker=intent['ticker'],
                   exchange_index=intent['exchange_index']) if o.get('client_order_id') == intent['client_order_id']]
        if len(matches) > 1:
            raise RuntimeError('DUPLICATE_CLIENT_ORDER_ID')
        return matches[0] if matches else None

    def submit(self, payload):
        # Caller MUST persist intent and validate current fenced ownership first.
        return self.request('POST', '/portfolio/events/orders', body=payload)


def acknowledged_order(response, intent):
    """Validate and minimize the authoritative Create Order V2 acknowledgement."""
    if not isinstance(response, dict):
        raise RuntimeError('ORDER_ACK_INVALID')
    order_id = response.get('order_id')
    if not isinstance(order_id, str) or not re.fullmatch(r'[A-Za-z0-9-]{8,128}', order_id):
        raise RuntimeError('ORDER_ACK_INVALID')
    client_id = response.get('client_order_id')
    if client_id is not None and client_id != intent['client_order_id']:
        raise RuntimeError('ORDER_ACK_IDENTITY_MISMATCH')
    try:
        filled = money(response['fill_count'])
        remaining = money(response['remaining_count'])
        processed_at = response['ts_ms']
    except (KeyError, ValueError, TypeError):
        raise RuntimeError('ORDER_ACK_INVALID') from None
    requested = money(intent['count'])
    if (min(filled, remaining) < 0 or filled + remaining > requested
            or type(processed_at) is not int or processed_at <= 0):
        raise RuntimeError('ORDER_ACK_INVALID')
    return {'order_id': order_id, 'client_order_id': intent['client_order_id'],
            'acknowledged_fill': str(filled), 'acknowledged_remaining': str(remaining),
            'matching_engine_ts_ms': processed_at}


def definitive_rejection(code):
    """Only errors proving the exchange rejected the request may release intent."""
    return code in {'KALSHI_HTTP_400', 'KALSHI_HTTP_401', 'KALSHI_HTTP_403',
                    'KALSHI_HTTP_422'}


def reconciled_order(order, intent, economic_side):
    """Use final authoritative cost/fee totals, never ACK average-price guesses."""
    if (order.get('client_order_id') != intent['client_order_id'] or order.get('ticker') != intent['ticker']
            or order.get('exchange_index') != intent['exchange_index']
            or order.get('subaccount_number') != intent['subaccount']):
        raise RuntimeError('ORDER_IDENTITY_MISMATCH')
    # A NO purchase is an ASK on the YES book; outcome_side can therefore be YES.
    if order.get('book_side') != intent['side']:
        raise RuntimeError('ORDER_DIRECTION_MISMATCH')
    count, remaining = money(order['fill_count_fp']), money(order['remaining_count_fp'])
    if not 0 <= count <= money(intent['count']) or remaining != 0 or order.get('status') not in ('executed', 'canceled'):
        raise RuntimeError('ORDER_NOT_TERMINALLY_RECONCILED')
    cost = money(order['taker_fill_cost_dollars']) + money(order['maker_fill_cost_dollars'])
    fees = money(order['taker_fees_dollars']) + money(order['maker_fees_dollars'])
    # V1 GET order accounting is outcome-side economic cost. Verify explicitly,
    # rather than guessing a complement for an undocumented response shape.
    if count and order.get('outcome_side') != economic_side:
        raise RuntimeError('ECONOMIC_FILL_DIRECTION_UNVERIFIED')
    if min(cost, fees) < 0 or cost > count or (count == 0 and cost != 0):
        raise RuntimeError('INVALID_FILL_ACCOUNTING')
    return {'filled': str(count), 'cost': str(cost), 'fees': str(fees), 'order_id': order['order_id']}
