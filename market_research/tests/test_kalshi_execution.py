"""Offline live-execution regression tests. No exchange orders or credentials."""
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
import time

import httpx
import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding

from market_research.kalshi_execution import (Config, CoinConfig, KalshiExecution, live_allowed,
    acknowledged_order, definitive_rejection, money, order_payload,
    reconciled_order, recovery)
from market_research.kalshi_live_state import LiveJournal, Trader, session_statistics
from market_research.kalshi_live_state import approved_reconfiguration

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
    ('history_minutes', True), ('subaccount', -1), ('subaccount', 64), ('direction_policy', 'settled_99c'),
    ('max_recovery_increases', -1), ('max_recovery_increases', 7), ('max_recovery_increases', True),
    ('starting_contracts', 0), ('starting_contracts', True), ('recovery_multiplier', '1'),
    ('recovery_multiplier', '5.1'),
    ('max_ask', '1')])
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
    assert config.starting_contracts == 1
    assert config.recovery_multiplier == '2.5'
    assert config.max_recovery_increases == 3
    assert not live_allowed(config, True, {})


@pytest.mark.parametrize('series', ['KXBNB15M', 'KXDOGE15M', 'KXETH15M', 'KXNEAR15M', 'KXZEC15M'])
def test_coin_series_isolated_and_not_implicitly_live(series, credentials):
    config = CoinConfig(series_ticker=series, subaccount=1)
    ticker = series + '-26SEP201215-15'
    assert order_payload(ticker, 'yes', 1, '.50', 2, config)['subaccount'] == 1
    with pytest.raises(ValueError, match='INVALID_ORDER_INTENT'):
        order_payload(TICKER, 'yes', 1, '.50', 2, config)
    env, _ = credentials
    assert not live_allowed(config, True, env)  # BTC approval never enables another coin.
    coin = series[2:-3]
    gated = {**env, f'QUANTURA_KALSHI_{coin}_LIVE_ENABLED': 'true',
             f'QUANTURA_KALSHI_{coin}_APPROVED_CONFIG': config.fingerprint,
             f'QUANTURA_KALSHI_{coin}_APPROVED_SHA': 'a'*40}
    assert live_allowed(config, True, gated)
    assert not live_allowed(replace(config, subaccount=2), True, gated)


def test_coin_main_account_requires_its_own_approval():
    config = CoinConfig(series_ticker='KXETH15M', subaccount=0)
    assert not live_allowed(config, True, {})
    env = {'QUANTURA_KALSHI_ETH_LIVE_ENABLED': 'true',
           'QUANTURA_KALSHI_ETH_APPROVED_CONFIG': config.fingerprint,
           'QUANTURA_KALSHI_ETH_APPROVED_SHA': 'a' * 40,
           'QUANTURA_CODE_SHA': 'a' * 40}
    assert live_allowed(config, True, env)
    assert not live_allowed(replace(config, subaccount=1), True, env)
    with pytest.raises(ValueError, match='INVALID_COIN_CONFIGURATION'):
        CoinConfig(series_ticker='KXBTC15M', subaccount=1)


def test_v2_yes_no_and_stable_single_market_identity(config):
    yes = order_payload(TICKER, 'yes', 2, '.61', 7, config)
    no = order_payload(TICKER, 'no', 2, '.61', 7, config)
    assert (yes['side'], yes['price']) == ('bid', '0.6100')
    assert (no['side'], no['price']) == ('ask', '0.3900')
    assert yes['client_order_id'] == no['client_order_id']
    assert yes['exchange_index'] == 7 and yes['subaccount'] == 0
    assert yes['count'] == '2.00' and yes['time_in_force'] == 'immediate_or_cancel'
    assert yes['post_only'] is False
    retry = order_payload(TICKER, 'yes', 2, '.61', 7, config, attempt=1)
    assert retry['client_order_id'] != yes['client_order_id']
    assert retry == order_payload(TICKER, 'yes', 2, '.61', 7, config, attempt=1)


@pytest.mark.parametrize('quantity,ask,shard', [(0, '.5', 0), (-1, '.5', 0), (1.5, '.5', 0), (True, '.5', 0),
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


def test_create_v2_acknowledgement_and_definitive_rejection(config):
    intent = order_payload(TICKER, 'yes', 2, '.61', 7, config)
    ack = acknowledged_order({'order_id': '3b23c1c7-f4ef-4f0d-8b9a-9e53c61f1a0d',
        'client_order_id': intent['client_order_id'], 'fill_count': '1.00',
        'remaining_count': '1.00', 'ts_ms': 1715793600123}, intent)
    assert ack['acknowledged_fill'] == '1.00'
    assert ack['acknowledged_remaining'] == '1.00'
    for status in (400, 401, 403, 422):
        assert definitive_rejection(f'KALSHI_HTTP_{status}')
    for status in (409, 429, 500, 503):
        assert not definitive_rejection(f'KALSHI_HTTP_{status}')
    with pytest.raises(RuntimeError, match='ORDER_ACK_IDENTITY_MISMATCH'):
        acknowledged_order({**ack, 'client_order_id': 'wrong'}, intent)


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


def test_exact_order_lookup_precedes_client_id_fallback(config, credentials):
    env, _ = credentials
    order_id = '3b23c1c7-f4ef-4f0d-8b9a-9e53c61f1a0d'
    calls = []
    def handler(request):
        calls.append(request.url.path)
        return httpx.Response(200, json={'order': {'order_id': order_id}})
    api = KalshiExecution(config, env=env,
        client=httpx.Client(transport=httpx.MockTransport(handler)))
    intent = order_payload(TICKER, 'yes', 1, '.5', 7, config)
    assert api.find_order(intent, order_id)['order_id'] == order_id
    assert calls == ['/trade-api/v2/portfolio/orders/' + order_id]


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


def test_recovery_until_net_recovers_with_three_increases():
    state = {'size': 1, 'cycle': '0', 'net': '0'}
    sizes = []
    for _ in range(7):
        state = recovery(state, '-1'); sizes.append(state['size'])
    assert sizes == [2, 5, 12, 12, 12, 12, 12]
    assert state['recovery_increases'] == 3
    state = recovery(state, '1')
    assert state['size'] == 12 and money(state['cycle']) == -6
    state = recovery(state, '6')
    assert state['size'] == 1 and money(state['cycle']) == 0
    assert state['recovery_increases'] == 0


def test_recovery_uses_approved_start_multiplier_and_increase_limit():
    config = Config(starting_contracts=2, recovery_multiplier='3', max_recovery_increases=2)
    state = {'size': 2, 'cycle': '0', 'net': '0'}
    state = recovery(state, '-1', config)
    assert state['size'] == 6
    state = recovery(state, '-1', config)
    assert state['size'] == 18
    state = recovery(state, '-1', config)
    assert state['size'] == 18
    assert state['recovery_increases'] == 2
    state = recovery(state, '3', config)
    assert state['size'] == 2 and money(state['cycle']) == 0


def test_four_increases_are_not_capped_by_legacy_contract_ceiling():
    config = Config(starting_contracts=10, max_recovery_increases=4)
    state = {'size': 10, 'cycle': '0', 'net': '0'}
    sizes = []
    for _ in range(5):
        state = recovery(state, '-1', config)
        sizes.append(state['size'])
    assert sizes == [25, 62, 155, 387, 387]
    assert state['recovery_increases'] == 4
    assert order_payload(TICKER, 'yes', 387, '.5', 7, config)['count'] == '387.00'


def test_read_only_legacy_settlement_retains_its_original_100_contract_cap():
    from market_research.kalshi_live_settlement import LegacyConfig
    old = LegacyConfig()
    state = {'size': 100, 'cycle': '-1', 'net': '-1', 'recovery_increases': 2}
    assert recovery(state, '-1', old)['size'] == 100


def test_uncapped_start_still_requires_actual_shard_funds(rig):
    trader, broker, journal, signal, quote, now = rig
    trader.config = Config(starting_contracts=2000)
    journal.value['size'] = 2000
    with pytest.raises(RuntimeError, match='INSUFFICIENT_SHARD_FUNDS'):
        trader.enter(signal, quote, now)
    assert not broker.sent and journal.state()['active'] is None


def test_sizing_reconfiguration_only_when_recovery_zero_and_flat():
    old = Config()
    new = Config(starting_contracts=2, recovery_multiplier='3', max_recovery_increases=4)
    flat = {'size': 1, 'cycle': '0', 'net': '17.5', 'active': None}
    assert approved_reconfiguration(old.__dict__, new.__dict__, flat)['size'] == 2
    assert approved_reconfiguration(old.__dict__, new.__dict__, flat)['recovery_increases'] == 0
    with pytest.raises(RuntimeError, match='RECOVERY_ZERO'):
        approved_reconfiguration(old.__dict__, new.__dict__, {**flat, 'cycle': '-1'})
    with pytest.raises(RuntimeError, match='RECOVERY_ZERO'):
        approved_reconfiguration(old.__dict__, new.__dict__, {**flat, 'active': {'intent': 'open'}})
    with pytest.raises(RuntimeError, match='RECOVERY_ZERO'):
        approved_reconfiguration(old.__dict__, replace(new, history_minutes=2).__dict__, flat)


def test_v2_session_migrates_only_when_flat_and_cycle_zero():
    new = Config(max_recovery_increases=3)
    legacy = {**new.__dict__, 'version': 'btc-p90-sticky-hold-live-v2',
              'max_contracts': 100, 'max_order_dollars': '100', 'daily_loss_dollars': '100'}
    legacy.pop('max_recovery_increases')
    flat = {'size': 1, 'cycle': '0', 'net': '2', 'active': None}
    assert approved_reconfiguration(legacy, new.__dict__, flat)['recovery_increases'] == 0
    with pytest.raises(RuntimeError, match='RECOVERY_ZERO'):
        approved_reconfiguration(legacy, new.__dict__, {**flat, 'active': {'intent': 'open'}})
    with pytest.raises(RuntimeError, match='RECOVERY_ZERO'):
        approved_reconfiguration(legacy, new.__dict__, {**flat, 'cycle': '-0.5'})
    with pytest.raises(RuntimeError, match='LEGACY_CAP_MIGRATION_NOT_APPROVED'):
        approved_reconfiguration({**legacy, 'max_contracts': 50}, new.__dict__, flat)


def test_v3_session_migrates_only_when_flat_and_cycle_zero():
    new = Config(starting_contracts=10)
    old = {**new.__dict__, 'version': 'btc-p90-sticky-hold-live-v3',
           'max_contracts': 100, 'max_order_dollars': '100', 'daily_loss_dollars': '100'}
    flat = {'size': 10, 'cycle': '0', 'net': '2', 'active': None}
    assert approved_reconfiguration(old, new.__dict__, flat)['size'] == 10
    for blocked in ({**flat, 'active': {'intent': 'open'}},
                    {**flat, 'cycle': '-0.5'}, {**flat, 'size': 25}):
        with pytest.raises(RuntimeError, match='RECOVERY_ZERO'):
            approved_reconfiguration(old, new.__dict__, blocked)
    with pytest.raises(RuntimeError, match='LEGACY_DOLLAR_LIMIT_MIGRATION_NOT_APPROVED'):
        approved_reconfiguration({**old, 'daily_loss_dollars': '50'}, new.__dict__, flat)
    with pytest.raises(RuntimeError, match='LEGACY_CAP_MIGRATION_NOT_APPROVED'):
        approved_reconfiguration({**old, 'max_contracts': 50}, new.__dict__, flat)


def test_v4_stale_firestore_merge_keys_do_not_interrupt_an_unchanged_recovery():
    """A retired map key is not a risk change, even with a held position."""
    config = Config()
    merged = {**config.__dict__, 'max_contracts': 100,
              'max_order_dollars': '100', 'daily_loss_dollars': '100'}
    recovering = {'size': 2, 'cycle': '-0.416800', 'net': '23.369600',
                  'active': {'ticker': TICKER}, 'recovery_increases': 1}
    assert approved_reconfiguration(merged, config.__dict__, recovering) == recovering
    for changed in ({**merged, 'starting_contracts': 10},
                    {**merged, 'history_minutes': 2}):
        with pytest.raises(RuntimeError, match='LIVE_CONFIG_CHANGE_REQUIRES_RECOVERY_ZERO'):
            approved_reconfiguration(changed, config.__dict__, recovering)
    with pytest.raises(RuntimeError, match='LEGACY_CAP_MIGRATION_NOT_APPROVED'):
        approved_reconfiguration({**merged, 'max_contracts': 200}, config.__dict__, recovering)


def test_live_claim_replaces_configuration_map_without_resetting_recovery():
    from market_research.kalshi_live_state import LiveJournal

    config = Config()
    recovering = {'size': 2, 'cycle': '-0.416800', 'net': '23.369600',
                  'active': {'ticker': TICKER}, 'recovery_increases': 1}
    old = {'configuration': {**config.__dict__, 'max_contracts': 100,
                             'max_order_dollars': '100', 'daily_loss_dollars': '100'},
           'state': recovering, 'lease': {'expires': 0}, 'enabled': True}
    class Snapshot:
        exists = True
        def to_dict(self): return old
    class Ref:
        def get(self, transaction=None): return Snapshot()
    class Tx:
        def __init__(self): self.updated = None
        def update(self, ref, value): self.updated = value
        def set(self, *args, **kwargs): raise AssertionError('merge-set would keep retired map keys')
    tx = Tx()
    journal = LiveJournal.__new__(LiveJournal)
    journal.config = config
    journal.live = True
    journal.shared_protocol = True
    journal.holder = 'next-worker'
    journal.ref = Ref()
    journal.fs = type('Firestore', (), {'transactional': staticmethod(lambda fn: fn)})()
    journal.transact = lambda operation: operation(tx)
    journal.claim()
    assert tx.updated['shared_account_protocol'] == 1
    assert tx.updated['shared_account_fence'] == tx.updated['lease']['fence']
    assert tx.updated['configuration'] == config.__dict__
    for key, value in recovering.items():
        assert tx.updated['state'][key] == value
    assert journal.fence == 1


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
    def acknowledge(self, entry, acknowledgement):
        assert self.value['active']['intent'] == entry['intent']
        self.value['active'] = {**deepcopy(entry), 'status': 'acknowledged',
            'acknowledgement': deepcopy(acknowledgement)}
        self.records[entry['ticker']] = deepcopy(self.value['active'])
    def retry(self, completed, entry):
        assert self.value['active']['intent'] == completed['intent']
        assert completed['status'] == 'unfilled' and money(completed['filled']) == 0
        assert entry['attempt'] == self.value['active'].get('attempt', 0) + 1
        self.value['active'] = deepcopy(entry)
        self.records[entry['ticker']] = deepcopy(entry)
    def finish(self, entry, net=None):
        if net is not None: self.value = recovery(self.value, net)
        self.value['active'] = None; self.records[entry['ticker']] = deepcopy(entry)


class JournalHarness(LiveJournal):
    def __init__(self, config):
        self.config = config
        self.value = {'size': config.starting_contracts, 'cycle': '0', 'net': '0', 'active': None}
        self.rows = {}
    def state(self): return deepcopy(self.value)
    def change(self, event, ticker, update):
        self.value, self.rows[ticker] = update(deepcopy(self.value), self.rows.get(ticker))


class Broker:
    enabled = True
    def __init__(self, journal, opened):
        self.journal, self.opened, self.sent = journal, opened, []
        self.order = None
        self.settled = False
    def account(self):
        filled = money(self.order.get('fill_count_fp', '0')) if self.order else money(0)
        return [], ([{'ticker': TICKER, 'position_fp': str(filled)}]
                    if filled and not self.settled else [])
    def market(self, ticker):
        from market_research.engine import iso
        return {'ticker': ticker, 'exchange_index': 7, 'market_type': 'binary',
            'status': 'settled' if self.settled else 'active', 'result': 'yes' if self.settled else '',
            'open_time': iso(self.opened), 'close_time': iso(self.opened+900),
            'yes_ask_dollars': '.5000', 'no_ask_dollars': '.5000'}
    def balance(self, shard): return Decimal(1000)
    def submit(self, intent):
        assert self.journal.value['active']['intent'] == intent
        self.sent.append(intent)
        raise RuntimeError('KALSHI_DELIVERY_OR_RESPONSE_UNKNOWN')
    def find_order(self, intent, order_id=None):
        self.lookup_order_id = order_id
        return self.order
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


def test_closed_market_position_read_lag_waits_for_authoritative_settlement(rig):
    trader, broker, journal, signal, quote, now = rig
    with pytest.raises(RuntimeError):
        trader.enter(signal, quote, now)
    broker.order = final_order(broker.sent[0])
    original_market = broker.market
    from market_research.engine import iso
    official = {'settled': False}
    broker.market = lambda ticker: ({**original_market(ticker),
        'close_time': iso(now - 1), 'status': 'settled', 'result': 'yes'}
        if official['settled'] else {**original_market(ticker), 'close_time': iso(now - 1)})
    broker.account = lambda: ([], [])

    assert trader.reconcile() == 'settlement_accounting_pending'
    assert journal.state()['active'] is not None
    assert journal.state()['net'] == '0'
    official['settled'] = True
    assert trader.reconcile() == 'settled'
    assert journal.state()['active'] is None
    assert money(journal.state()['net']) == Decimal('.4825')


def test_missing_or_foreign_position_still_fails_before_or_after_close(rig):
    trader, broker, journal, signal, quote, now = rig
    with pytest.raises(RuntimeError):
        trader.enter(signal, quote, now)
    broker.order = final_order(broker.sent[0])
    broker.account = lambda: ([], [])
    with pytest.raises(RuntimeError, match='POSITION_ACCOUNTING_MISMATCH'):
        trader.reconcile()

    original_market = broker.market
    from market_research.engine import iso
    broker.market = lambda ticker: {**original_market(ticker), 'close_time': iso(now - 1)}
    broker.account = lambda: ([], [{'ticker': 'FOREIGN', 'position_fp': '1'}])
    with pytest.raises(RuntimeError, match='POSITION_ACCOUNTING_MISMATCH'):
        trader.reconcile()


def test_success_ack_is_persisted_and_exact_order_id_drives_lookup(rig):
    trader, broker, journal, signal, quote, now = rig
    order_id = '3b23c1c7-f4ef-4f0d-8b9a-9e53c61f1a0d'
    broker.submit = lambda intent: {'order_id': order_id,
        'client_order_id': intent['client_order_id'], 'fill_count': '0.00',
        'remaining_count': intent['count'], 'ts_ms': now * 1000}
    entry = trader.enter(signal, quote, now)
    assert entry['status'] == 'acknowledged'
    assert journal.state()['active']['acknowledgement']['order_id'] == order_id
    assert trader.reconcile() == 'acknowledged_waiting_for_read_model'
    assert broker.lookup_order_id == order_id


def test_authoritative_zero_fill_retries_until_a_fill_with_fresh_quotes(rig):
    trader, broker, journal, signal, quote, now = rig
    signal['agrees'] = True
    with pytest.raises(RuntimeError, match='DELIVERY_OR_RESPONSE_UNKNOWN'):
        trader.enter(signal, quote, now)
    first = broker.sent[0]
    broker.order = final_order(first, count='0.00')
    sequence = 0
    def acknowledge(intent):
        nonlocal sequence
        sequence += 1
        assert journal.value['active']['intent'] == intent
        broker.sent.append(intent)
        return {'order_id': f'3b23c1c7-f4ef-4f0d-8b9a-9e53c61f{sequence:04d}',
                'client_order_id': intent['client_order_id'], 'fill_count': '0.00',
                'remaining_count': '0.00', 'ts_ms': now * 1000 + sequence}
    broker.submit = acknowledge

    assert trader.reconcile() == 'retry_acknowledged'
    second = broker.sent[-1]
    assert second['client_order_id'] != first['client_order_id']
    assert journal.state()['active']['attempt'] == 1
    broker.order = final_order(second, count='0.00')
    assert trader.reconcile() == 'retry_acknowledged'
    third = broker.sent[-1]
    assert len({row['client_order_id'] for row in broker.sent}) == 3
    assert journal.state()['active']['attempt'] == 2

    broker.order = final_order(third, count='1.00')
    assert trader.reconcile() == 'held_to_settlement'
    assert len(broker.sent) == 3


def test_zero_fill_waits_for_safe_executable_quote_then_retries(rig):
    trader, broker, journal, signal, quote, now = rig
    signal['agrees'] = True
    with pytest.raises(RuntimeError):
        trader.enter(signal, quote, now)
    broker.order = final_order(broker.sent[0], count='0.00')
    original_market = broker.market
    broker.market = lambda ticker: {**original_market(ticker), 'yes_ask_dollars': '1.0000'}
    assert trader.reconcile() == 'retry_waiting_for_executable_quote'
    assert journal.state()['active'] is not None and len(broker.sent) == 1


def test_zero_fill_does_not_retry_after_market_closes(rig):
    trader, broker, journal, signal, quote, now = rig
    signal['agrees'] = True
    with pytest.raises(RuntimeError):
        trader.enter(signal, quote, now)
    broker.order = final_order(broker.sent[0], count='0.00')
    broker.settled = True
    assert trader.reconcile() == 'unfilled'
    assert journal.state()['active'] is None and len(broker.sent) == 1


def test_settlement_only_reconciliation_cannot_retry_zero_fill(rig):
    trader, broker, journal, signal, quote, now = rig
    signal['agrees'] = True
    with pytest.raises(RuntimeError):
        trader.enter(signal, quote, now)
    broker.order = final_order(broker.sent[0], count='0.00')
    assert trader.reconcile(allow_order_retry=False) == 'zero_fill_retry_disabled'
    assert len(broker.sent) == 1
    assert journal.state()['active'] is not None


def test_legacy_settlement_config_matches_v2_persisted_fields():
    from dataclasses import asdict
    from market_research.kalshi_live_settlement import LegacyConfig
    legacy = asdict(LegacyConfig())
    assert legacy['version'] == 'btc-p90-sticky-hold-live-v2'
    assert legacy['max_contracts'] == 100
    assert 'max_recovery_increases' not in legacy


def test_journal_summary_counts_acknowledgements_rejections_and_settlements(config):
    journal = JournalHarness(config)
    base = {'ticker': TICKER, 'side': 'yes', 'intent': order_payload(TICKER, 'yes', 1, '.5', 7, config),
            'created_at': 1800000000, 'status': 'delivery_unknown'}
    ack = {'order_id': '3b23c1c7-f4ef-4f0d-8b9a-9e53c61f1a0d',
           'client_order_id': base['intent']['client_order_id'], 'acknowledged_fill': '1',
           'acknowledged_remaining': '0', 'matching_engine_ts_ms': 1800000000000}
    journal.begin(base)
    journal.acknowledge(base, ack)
    journal.finish({**base, 'filled': '1', 'status': 'settled'}, '.4825')
    summary = journal.public_summary()
    assert summary['intents'] == summary['acknowledged'] == summary['settled'] == 1
    assert summary['wins'] == 1 and summary['losses'] == 0
    assert summary['filled_contracts'] == '1'
    assert summary['realized_net_pnl'] == '0.4825'
    assert summary['active'] is None


def test_journal_retry_atomically_counts_no_fill_and_reserves_unique_attempt(config):
    journal = JournalHarness(config)
    first = {'ticker': TICKER, 'side': 'yes',
             'intent': order_payload(TICKER, 'yes', 1, '.5', 7, config),
             'signal': {'agrees': True}, 'quote': {'yes_ask': '.5'},
             'attempt': 0, 'created_at': 1800000000, 'status': 'delivery_unknown'}
    ack = {'order_id': '3b23c1c7-f4ef-4f0d-8b9a-9e53c61f0000',
           'client_order_id': first['intent']['client_order_id'],
           'acknowledged_fill': '0', 'acknowledged_remaining': '0',
           'matching_engine_ts_ms': 1800000000000}
    journal.begin(first)
    journal.acknowledge(first, ack)
    retry = {**first, 'attempt': 1, 'intent': order_payload(
        TICKER, 'yes', 1, '.5', 7, config, attempt=1),
        'status': 'delivery_unknown'}
    journal.retry({**first, 'filled': '0', 'status': 'unfilled'}, retry)
    summary = journal.public_summary()
    assert summary['intents'] == 2 and summary['retries'] == 1 and summary['unfilled'] == 1
    assert summary['requested_contracts'] == '2.00'
    assert summary['active']['attempt'] == 1
    assert journal.rows[TICKER]['attempt_history'][0]['status'] == 'unfilled'


def test_session_statistics_start_fresh_without_resetting_lifetime_state():
    baseline = {'intents': 3, 'acknowledged': 3, 'rejected': 1, 'unfilled': 1,
                'retries': 0,
                'settled': 1, 'wins': 1, 'losses': 0, 'breakeven': 0,
                'requested_contracts': '3.00', 'filled_contracts': '1.00',
                'realized_net_pnl': '.2368'}
    current = {**baseline, 'intents': 4, 'acknowledged': 4, 'settled': 2,
               'wins': 2, 'requested_contracts': '4.00',
               'filled_contracts': '2.00', 'realized_net_pnl': '.4736'}
    assert session_statistics(baseline, baseline) == {
        'intents': 0, 'retries': 0, 'acknowledged': 0, 'rejected': 0, 'unfilled': 0,
        'settled': 0, 'wins': 0, 'losses': 0, 'breakeven': 0,
        'requested_contracts': '0.00', 'filled_contracts': '0.00',
        'realized_net_pnl': '0.0000'}
    assert session_statistics(current, baseline) == {
        'intents': 1, 'retries': 0, 'acknowledged': 1, 'rejected': 0, 'unfilled': 0,
        'settled': 1, 'wins': 1, 'losses': 0, 'breakeven': 0,
        'requested_contracts': '1.00', 'filled_contracts': '1.00',
        'realized_net_pnl': '0.2368'}


def test_stale_recovery_is_read_only_and_requires_complete_exchange_proof(monkeypatch, capsys):
    import market_research.kalshi_live_recovery as module
    from market_research.engine import iso
    now = 1800001200
    intent = order_payload(TICKER, 'yes', 1, '.5', 7, Config())
    active = {'ticker': TICKER, 'side': 'yes', 'intent': intent,
              'created_at': now - 900, 'status': 'delivery_unknown'}
    events = []
    class Client:
        def close(self): events.append('client_closed')
    class ReadOnlyBroker:
        def __init__(self, config, requested_live=False):
            assert requested_live is False
            self.enabled = False; self.key_id = 'test'; self.client = Client()
        def market(self, ticker):
            return {'ticker': ticker, 'close_time': iso(now - 300)}
        def request(self, method, path):
            assert (method, path) == ('GET', '/exchange/user_data_timestamp')
            return {'as_of_time': iso(now)}
        def find_order(self, intent): return None
        def pages(self, *args, **kwargs): return []
        def account(self): return [], []
    class Journal:
        def __init__(self, config, key_id, holder, live):
            assert live is True
            self.value = {'size': 1, 'cycle': '0', 'net': '0', 'active': active}
        def claim(self): events.append('claimed')
        def state(self): return deepcopy(self.value)
        def finish(self, entry):
            assert entry['rejection_code'] == module.RECOVERY_CODE
            self.value['active'] = None; events.append('finished')
        def public_summary(self): return {'active': None, 'rejected': 1}
        def release(self): events.append('released')
    monkeypatch.setattr(module, 'KalshiExecution', ReadOnlyBroker)
    monkeypatch.setattr(module, 'LiveJournal', Journal)
    monkeypatch.setattr(module.time, 'time', lambda: now)
    module.recover(0, TICKER, module.CONFIRMATION)
    assert events == ['claimed', 'finished', 'released', 'client_closed']
    assert 'stale_intent_resolved' in capsys.readouterr().out


@pytest.mark.parametrize('code', ['KALSHI_HTTP_400', 'KALSHI_HTTP_401',
                                  'KALSHI_HTTP_403', 'KALSHI_HTTP_422'])
def test_definitive_post_rejection_releases_intent_without_retry(rig, code):
    trader, broker, journal, signal, quote, now = rig
    attempts = []
    def reject(intent):
        attempts.append(intent)
        raise RuntimeError(code)
    broker.submit = reject
    with pytest.raises(RuntimeError, match=code):
        trader.enter(signal, quote, now)
    assert journal.state()['active'] is None
    assert journal.records[TICKER]['status'] == 'rejected'
    assert journal.records[TICKER]['rejection_code'] == code
    assert len(attempts) == 1


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


def test_historical_daily_loss_does_not_block_but_foreign_positions_do(rig):
    trader, broker, journal, signal, quote, now = rig
    journal.value.update(day=time.strftime('%Y-%m-%d', time.gmtime(now)), daily_net='-100')
    with pytest.raises(RuntimeError, match='KALSHI_DELIVERY_OR_RESPONSE_UNKNOWN'):
        trader.enter(signal, quote, now)
    assert len(broker.sent) == 1
    journal = MemoryJournal()
    broker = Broker(journal, now - 300)
    trader = Trader(trader.config, broker, journal)
    broker.account = lambda: ([], [{'ticker': 'FOREIGN', 'position_fp': '1'}])
    with pytest.raises(RuntimeError, match='SUBACCOUNT_NOT_FLAT'): trader.enter(signal, quote, now)
    assert not broker.sent


def test_shared_account_allows_attributed_foreign_position_and_own_settlement(rig):
    trader, broker, journal, signal, quote, now = rig
    calls = []
    class Coordinator:
        def claim(self): calls.append('claim')
        def release(self): calls.append('release')
        def before_post(self): calls.append('before_post')
        def verify_positions(self, orders, positions):
            calls.append('verify')
            assert not orders
            assert positions == [{'ticker': 'KXETH15M-OTHER', 'position_fp': '-1'}]
    trader.coordinator = Coordinator()
    original_account = broker.account
    broker.account = lambda: ([], [{'ticker': 'KXETH15M-OTHER', 'position_fp': '-1'},
        *original_account()[1]])
    with pytest.raises(RuntimeError, match='KALSHI_DELIVERY_OR_RESPONSE_UNKNOWN'):
        trader.enter(signal, quote, now)
    assert calls == ['claim', 'verify', 'before_post', 'release'] and len(broker.sent) == 1
    broker.order = final_order(broker.sent[0])
    assert trader.reconcile() == 'held_to_settlement'
    broker.settled = True
    assert trader.reconcile() == 'settled'
    assert money(journal.state()['net']) == Decimal('.4825')


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
