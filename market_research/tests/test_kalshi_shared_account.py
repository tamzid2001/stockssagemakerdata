"""Offline main-account attribution tests; no credentials or live orders."""

from decimal import Decimal
from types import SimpleNamespace

import pytest

from market_research.kalshi_shared_account import (
    SERIES,
    SharedAccountCoordinator,
    account_session,
    current_btc_protocol,
    expected_positions,
    strategy_session,
)


def test_btc_journal_keeps_its_identity_and_coins_are_independent():
    account = account_session('test-key', 0, True)
    assert strategy_session('test-key', 0, True, 'KXBTC15M') == account
    coin_ids = [strategy_session('test-key', 0, True, series) for series in SERIES[1:]]
    assert len(set(coin_ids + [account])) == 6
    assert all(value != account for value in coin_ids)
    assert strategy_session('test-key', 1, True, 'KXETH15M') not in coin_ids


def test_old_btc_worker_fence_invalidates_shared_account_protocol():
    assert not current_btc_protocol({'lease': {'fence': 4}})
    assert current_btc_protocol({'lease': {'fence': 4},
        'shared_account_protocol': 1, 'shared_account_fence': 4})
    assert not current_btc_protocol({'lease': {'fence': 5},
        'shared_account_protocol': 1, 'shared_account_fence': 4})


def test_multiple_owned_positions_are_exact_and_unknown_positions_block():
    entries = [
        ('KXBTC15M-A', {'filled': '1.00', 'side': 'yes', 'signal': {'market_end': 200}}),
        ('KXETH15M-B', {'filled': '2.00', 'side': 'no', 'signal': {'market_end': 200}}),
    ]
    actual = {'KXBTC15M-A': Decimal(1), 'KXETH15M-B': Decimal(-2)}
    assert expected_positions(entries, actual, 100) == actual
    assert expected_positions(entries, {}, 201) == {}
    assert expected_positions(entries, actual, 201) == actual
    assert expected_positions(entries, {'MANUAL': Decimal(1)}, 100) != {'MANUAL': Decimal(1)}
    with pytest.raises(RuntimeError, match='DUPLICATE_ACCOUNT_POSITION_OWNER'):
        expected_positions(entries + [entries[0]], actual, 100)


class FakeDocument:
    def __init__(self, value):
        self.value = value
    def get(self, transaction=None):
        return SimpleNamespace(to_dict=lambda: self.value)

    def collection(self, name):
        assert name == 'coordination'
        return FakeCollection({'order_admission': FakeDocument({})})


class FakeCollection:
    def __init__(self, docs):
        self.docs = docs
    def document(self, key):
        return FakeDocument(self.docs.get(key))


class FakeDatabase:
    def __init__(self, docs):
        self.docs = docs
    def collection(self, name):
        assert name == 'kalshi_execution_sessions'
        return FakeCollection(self.docs)


def filled_order(ticker, side, count):
    return {'client_order_id': 'client-' + ticker, 'ticker': ticker,
        'exchange_index': 2, 'subaccount_number': 0,
        'book_side': 'bid' if side == 'yes' else 'ask', 'outcome_side': side,
        'fill_count_fp': count, 'remaining_count_fp': '0.00', 'status': 'executed',
        'taker_fill_cost_dollars': '.50', 'maker_fill_cost_dollars': '0',
        'taker_fees_dollars': '.02', 'maker_fees_dollars': '0', 'order_id': ticker}


def test_account_verification_accepts_two_exact_bot_fills_but_not_manual_position(monkeypatch):
    monkeypatch.setattr('market_research.kalshi_shared_account.time.time', lambda: 100)
    key = 'key-for-test'
    docs = {}
    orders = {}
    for series, side, position in [('KXBTC15M', 'yes', '1.00'), ('KXETH15M', 'no', '-1.00')]:
        ticker = series + '-TEST'
        intent = {'ticker': ticker, 'exchange_index': 2, 'subaccount': 0,
            'client_order_id': 'client-' + ticker, 'side': 'bid' if side == 'yes' else 'ask',
            'count': '1.00'}
        entry = {'ticker': ticker, 'side': side, 'intent': intent,
            'signal': {'market_end': 200}, 'acknowledgement': {'order_id': ticker}}
        docs[strategy_session(key, 0, True, series)] = {'state': {'active': entry}}
        orders[ticker] = filled_order(ticker, side, '1.00')
    broker = SimpleNamespace(key_id=key,
        find_order=lambda intent, order_id: orders.get(intent['ticker']))
    journal = SimpleNamespace(db=FakeDatabase(docs), config=SimpleNamespace(subaccount=0))
    coordinator = SharedAccountCoordinator.__new__(SharedAccountCoordinator)
    coordinator.journal, coordinator.broker = journal, broker
    positions = [{'ticker': 'KXBTC15M-TEST', 'position_fp': '1.00'},
        {'ticker': 'KXETH15M-TEST', 'position_fp': '-1.00'}]
    assert coordinator.verify_positions([], positions) == {
        'KXBTC15M-TEST': Decimal(1), 'KXETH15M-TEST': Decimal(-1)}
    with pytest.raises(RuntimeError, match='ACCOUNT_POSITION_UNVERIFIED'):
        coordinator.verify_positions([], positions + [{'ticker': 'MANUAL', 'position_fp': '1'}])
    with pytest.raises(RuntimeError, match='ACCOUNT_RESTING_ORDER_UNVERIFIED'):
        coordinator.verify_positions([{'ticker': 'MANUAL'}], positions)
    del orders['KXETH15M-TEST']
    with pytest.raises(RuntimeError, match='ACCOUNT_INTENT_UNRESOLVED'):
        coordinator.verify_positions([], positions)


def test_account_order_gate_is_exclusive_and_old_btc_claim_revokes_it(monkeypatch):
    monkeypatch.setattr('market_research.kalshi_shared_account.time.time', lambda: 100)
    root = FakeDocument({'lease': {'fence': 2},
        'shared_account_protocol': 1, 'shared_account_fence': 2, 'enabled': True})
    gate = FakeDocument({})
    class Transaction:
        def set(self, doc, value, merge=False):
            doc.value = {**(doc.value or {}), **value} if merge else value
    def coordinator(holder):
        instance = SharedAccountCoordinator.__new__(SharedAccountCoordinator)
        instance.root, instance.gate, instance.fence = root, gate, None
        instance.journal = SimpleNamespace(holder=holder,
            config=SimpleNamespace(subaccount=0),
            fs=SimpleNamespace(transactional=lambda fn: fn),
            transact=lambda fn: fn(Transaction()))
        return instance
    first, second = coordinator('run-1'), coordinator('run-2')
    first.claim()
    first.before_post()
    with pytest.raises(RuntimeError, match='LEASE_HELD'):
        second.claim()
    gate.value['lease']['expires'] = 99
    with pytest.raises(RuntimeError, match='ACCOUNT_ORDER_GATE_LOST'):
        first.before_post()
    first.release()
    second.claim()
    second.release()
    root.value['lease']['fence'] = 3  # Older BTC code claimed without marker.
    with pytest.raises(RuntimeError, match='BTC_SHARED_ACCOUNT_UPGRADE_REQUIRED'):
        first.claim()
    root.value['lease']['fence'] = 2
    root.value['enabled'] = False
    with pytest.raises(RuntimeError, match='LIVE_KILL_SWITCH_DISABLED'):
        first.claim()
