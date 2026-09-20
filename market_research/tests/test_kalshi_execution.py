"""Offline live-execution regression tests. No exchange orders or credentials."""
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
import time

import httpx
import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding

from market_research.kalshi_execution import (Config, KalshiExecution, live_allowed,
    money, order_payload, reconciled_order, recovery)
from market_research.kalshi_live_state import Trader

TICKER = 'KXBTC15M-26SEP201215-15'


@pytest.fixture
def config():
    return Config()


@pytest.fixture
def credentials(config):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    env = {'KALSHI_API_KEY_ID': 'test-only', 'KALSHI_PRIVATE_KEY': key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()).decode(),
        'QUANTURA_KALSHI_LIVE_ENABLED': 'true', 'QUANTURA_KALSHI_APPROVED_CONFIG': config.fingerprint,
        'QUANTURA_KALSHI_APPROVED_SHA': 'a'*40, 'QUANTURA_CODE_SHA': 'a'*40}
    return env, key


@pytest.mark.parametrize('field,value', [('history_minutes', 0), ('history_minutes', 13),
    ('history_minutes', True), ('subaccount', -1), ('subaccount', 64), ('direction_policy', 'settled_99c'), ('max_contracts', 101),
    ('max_order_dollars', 'NaN'), ('daily_loss_dollars', '0'), ('max_ask', '1')])
def test_strict_config(field, value):
    with pytest.raises(ValueError):
        Config(**{field: value})


def test_all_approval_gates(config, credentials):
    env, _ = credentials
    assert live_allowed(config, True, env)
    assert not live_allowed(config, False, env)
    for key in ('QUANTURA_KALSHI_LIVE_ENABLED', 'QUANTURA_KALSHI_APPROVED_CONFIG', 'QUANTURA_KALSHI_APPROVED_SHA'):
        assert not live_allowed(config, True, {**env, key: ''})
    assert not live_allowed(replace(config, history_minutes=2), True, env)


def test_requested_one_minute_preset_is_not_live_approval():
    config = Config()
    assert config.history_minutes == 1
    assert config.subaccount == 0
    assert config.direction_policy == 'provisional_near_close'
    assert config.max_contracts == 100
    assert not live_allowed(config, True, {})


def test_v2_yes_no_and_stable_single_market_identity(config):
    yes = order_payload(TICKER, 'yes', 2, '.61', 7, config)
    no = order_payload(TICKER, 'no', 2, '.61', 7, config)
    assert (yes['side'], yes['price']) == ('bid', '0.610000')
    assert (no['side'], no['price']) == ('ask', '0.390000')
    assert yes['client_order_id'] == no['client_order_id']
    assert yes['exchange_index'] == 7 and yes['subaccount'] == 0
    assert yes['count'] == '2.00' and yes['time_in_force'] == 'immediate_or_cancel'
    assert yes['post_only'] is False


@pytest.mark.parametrize('quantity,ask,shard', [(101, '.5', 0), (1.5, '.5', 0), (True, '.5', 0),
    (1, 'NaN', 0), (1, 0, 0), (1, '1', 0), (1, '.5555555', 0), (1, '.5', -1)])
def test_order_risk_validation(config, quantity, ask, shard):
    with pytest.raises(ValueError):
        order_payload(TICKER, 'yes', quantity, ask, shard, config)


def test_signed_get_query_excluded_and_post_not_retried(config, credentials):
    import base64
    env, key = credentials
    calls = []
    def handler(request):
        calls.append(request)
        message = request.headers['KALSHI-ACCESS-TIMESTAMP'] + request.method + request.url.path
        key.public_key().verify(base64.b64decode(request.headers['KALSHI-ACCESS-SIGNATURE']),
            message.encode(), padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256())
        return httpx.Response(500 if request.method == 'POST' else 200, json={})
    api = KalshiExecution(config, requested_live=True, env=env, client=httpx.Client(transport=httpx.MockTransport(handler)))
    api.request('GET', '/portfolio/balance', params={'exchange_index': 7})
    with pytest.raises(RuntimeError, match='KALSHI_HTTP_500'):
        api.submit(order_payload(TICKER, 'yes', 1, '.5', 7, config))
    assert len(calls) == 2


def test_read_only_cannot_submit(config, credentials):
    env, _ = credentials
    api = KalshiExecution(config, env=env)
    with pytest.raises(RuntimeError, match='WRITE_NOT_AUTHORIZED'):
        api.submit({})


def test_paginate_and_do_not_assume_empty_on_error(config, credentials):
    env, _ = credentials
    calls = []
    def handler(request):
        calls.append(request)
        assert request.url.params['subaccount'] == '0'
        return httpx.Response(200, json={'orders': [len(calls)], 'cursor': 'next' if len(calls) == 1 else ''})
    api = KalshiExecution(config, env=env, client=httpx.Client(transport=httpx.MockTransport(handler)))
    assert api.pages('/portfolio/orders', 'orders') == [1, 2]
    api.request = lambda *a, **k: {}
    with pytest.raises(RuntimeError, match='ACCOUNT_LIST_UNAVAILABLE'):
        api.pages('/portfolio/orders', 'orders')


def final_order(intent, side='yes', count='1.00'):
    return {**intent, 'order_id': 'test-order', 'book_side': intent['side'],
        'outcome_side': side, 'subaccount_number': intent['subaccount'],
        'fill_count_fp': count, 'remaining_count_fp': '0.00', 'status': 'canceled',
        'taker_fill_cost_dollars': '0.5000' if count != '0.00' else '0',
        'maker_fill_cost_dollars': '0', 'taker_fees_dollars': '0.0175' if count != '0.00' else '0',
        'maker_fees_dollars': '0'}


def test_partial_and_zero_fill_are_not_full_fills(config):
    intent = order_payload(TICKER, 'yes', 5, '.5', 7, config)
    assert reconciled_order(final_order(intent), intent, 'yes')['filled'] == '1.00'
    assert reconciled_order(final_order(intent, count='0.00'), intent, 'yes')['filled'] == '0.00'
    for patch in ({'remaining_count_fp': '1'}, {'subaccount_number': 1},
                  {'book_side': 'ask'}, {'outcome_side': 'no'}, {'exchange_index': 0}):
        with pytest.raises(RuntimeError):
            reconciled_order({**final_order(intent), **patch}, intent, 'yes')


def test_recovery_until_net_recovers_whole_contracts_and_fixed_cap():
    state = {'size': 1, 'cycle': '0', 'net': '0'}
    sizes = []
    for _ in range(7):
        state = recovery(state, '-1'); sizes.append(state['size'])
    assert sizes == [2, 5, 12, 30, 75, 100, 100]
    state = recovery(state, '1')
    assert state['size'] == 100 and money(state['cycle']) == -6
    state = recovery(state, '6')
    assert state['size'] == 1 and money(state['cycle']) == 0


class MemoryJournal:
    def __init__(self):
        self.value = {'size': 1, 'cycle': '0', 'net': '0', 'active': None}
        self.records = {}
        self.enabled = True
    def used(self, ticker): return ticker in self.records
    def state(self): return deepcopy(self.value)
    def begin(self, entry):
        assert self.value['active'] is None and entry['ticker'] not in self.records
        self.value['active'] = deepcopy(entry); self.records[entry['ticker']] = deepcopy(entry)
    def before_post(self):
        if not self.enabled: raise RuntimeError('LEASE_LOST')
    def finish(self, entry, net=None):
        if net is not None: self.value = recovery(self.value, net)
        self.value['active'] = None; self.records[entry['ticker']] = deepcopy(entry)


class Broker:
    enabled = True
    def __init__(self, journal, opened):
        self.journal, self.opened, self.sent = journal, opened, []
        self.order = None
        self.settled = False
    def account(self):
        return [], ([{'ticker': TICKER, 'position_fp': '1'}] if self.order and not self.settled else [])
    def market(self, ticker):
        from market_research.engine import iso
        return {'ticker': ticker, 'exchange_index': 7, 'market_type': 'binary',
            'status': 'settled' if self.settled else 'active', 'result': 'yes' if self.settled else '',
            'open_time': iso(self.opened), 'close_time': iso(self.opened+900)}
    def balance(self, shard): return Decimal(1000)
    def submit(self, intent):
        assert self.journal.value['active']['intent'] == intent
        self.sent.append(intent)
        raise RuntimeError('KALSHI_DELIVERY_OR_RESPONSE_UNKNOWN')
    def find_order(self, intent): return self.order
    def pages(self, *a, **k):
        return [{'ticker': TICKER, 'exchange_index': 7, 'market_result': 'yes',
                 'yes_count_fp': '1', 'no_count_fp': '0', 'yes_total_cost_dollars': '.5',
                 'revenue': 100, 'fee_cost': '.0175'}]


@pytest.fixture
def rig(config, monkeypatch):
    now = 1800000120
    monkeypatch.setattr(time, 'time', lambda: now)
    journal = MemoryJournal(); broker = Broker(journal, now-300)
    trader = Trader(config, broker, journal)
    signal = {'signal_at': now-60, 'market_end': now+600, 'market_id': TICKER,
              'contract_id': TICKER+':yes'}
    quote = {'timestamp': now, 'received_at': now, 'timely': True, 'yes_ask': .5}
    return trader, broker, journal, signal, quote, now


def test_crash_after_post_reconciles_without_resubmitting_and_settles_once(rig):
    trader, broker, journal, signal, quote, now = rig
    with pytest.raises(RuntimeError): trader.enter(signal, quote, now)
    assert len(broker.sent) == 1
    restarted = Trader(trader.config, broker, journal)
    assert restarted.reconcile() == 'unknown_delivery_blocked'
    with pytest.raises(RuntimeError): restarted.enter(signal, quote, now)
    broker.order = final_order(broker.sent[0])
    assert restarted.reconcile() == 'held_to_settlement'
    broker.settled = True
    assert restarted.reconcile() == 'settled'
    assert money(journal.state()['net']) == Decimal('.4825')
    assert restarted.reconcile() == 'flat'
    assert len(broker.sent) == 1


def test_expired_quote_and_lost_lease_never_post(rig):
    trader, broker, journal, signal, quote, now = rig
    with pytest.raises(RuntimeError): trader.enter(signal, quote, now+31)
    assert not broker.sent
    journal.enabled = False
    with pytest.raises(RuntimeError, match='LEASE_LOST'): trader.enter(signal, quote, now)
    assert not broker.sent and journal.state()['active']


def test_observe_mode_records_intent_but_never_fakes_a_fill(rig):
    trader, broker, journal, signal, quote, now = rig
    broker.enabled = False
    trader.enter(signal, quote, now)
    assert not broker.sent and journal.state()['active'] is None
    assert journal.records[TICKER]['status'] == 'observed_no_order'
    assert journal.state()['net'] == '0'


def test_daily_loss_and_foreign_positions_block_entries(rig):
    trader, broker, journal, signal, quote, now = rig
    journal.value.update(day=time.strftime('%Y-%m-%d', time.gmtime(now)), daily_net='-100')
    with pytest.raises(RuntimeError, match='DAILY_LOSS_LIMIT'): trader.enter(signal, quote, now)
    journal.value['daily_net'] = '0'
    broker.account = lambda: ([], [{'ticker': 'FOREIGN', 'position_fp': '1'}])
    with pytest.raises(RuntimeError, match='SUBACCOUNT_NOT_FLAT'): trader.enter(signal, quote, now)
    assert not broker.sent


def test_99c_provisional_quote_cannot_close_position_or_increase_recovery(rig):
    trader, broker, journal, signal, quote, now = rig
    with pytest.raises(RuntimeError): trader.enter(signal, quote, now)
    broker.order = final_order(broker.sent[0])
    original = broker.market
    broker.market = lambda ticker: {**original(ticker), 'yes_bid_dollars':'.99',
                                    'provisional_winner':'yes', 'result':'yes'}
    assert trader.reconcile() == 'held_to_settlement'
    assert journal.state()['net'] == '0' and journal.state()['size'] == 1
    assert journal.state()['active'] is not None


@pytest.mark.parametrize('minutes', range(1, 13))
def test_forecast_pair_uses_exact_opening_rows_strict_models_and_shared_grid(minutes, monkeypatch):
    import queue
    import sys
    from types import SimpleNamespace
    from market_research.kalshi_live_worker import forecast_pair
    from market_research.engine import iso
    from market_research.p1_oco import QUANTILES
    opened = 1800000000
    rows = [{'timestamp': opened+60*i, 'yes_ask': .6, 'yes_bid': .59,
             'no_ask': .41, 'no_bid': .4} for i in range(1, minutes+1)]
    calls = []
    def numerical(window, horizon, models, quantiles, **kwargs):
        calls.append((window, horizon, models, quantiles, kwargs))
        return {'forecast_id': 'test', 'origin': window[-1].timestamp,
                'rows': [{'timestamp': window[-1].timestamp+60*i,
                          'quantiles': {str(q): q for q in quantiles}} for i in range(1, horizon+1)]}
    monkeypatch.setitem(sys.modules, 'market_research.forecast', SimpleNamespace(forecast_window=numerical))
    output = queue.Queue()
    forecast_pair({'ticker': TICKER, 'event_ticker': 'BTC', 'open_time': iso(opened)}, minutes, rows, output)
    result = output.get_nowait()
    assert len(result['forecasts']) == 2
    assert result['forecasts'][0]['available_at'] == result['forecasts'][1]['available_at']
    assert len(calls) == 2
    for window, horizon, models, quantiles, kwargs in calls:
        assert len(window) == minutes and horizon == 15-minutes
        assert len(models) == (3 if minutes == 1 else 4) and 'toto' not in models
        assert quantiles == QUANTILES and kwargs['failure_policy'] == 'fail'
        assert kwargs['single_point_research'] == (minutes == 1)
        assert result['forecasts'][0]['rows'][-1]['timestamp'] == opened+900
