"""Five-cent live stop: durable IOC reconciliation, not quote-based paper fills."""

from copy import deepcopy
from decimal import Decimal
import time

import pytest

from market_research.kalshi_execution import (
    Config, CoinConfig, KalshiExecution, money, order_payload, stop_exit_payload,
)
from market_research.kalshi_live_state import LiveJournal, Trader
from market_research.kalshi_shared_account import expected_positions


class Journal(LiveJournal):
    def __init__(self, config, entry):
        self.config = config
        self.value = {'size': config.starting_contracts, 'cycle': '0', 'net': '0',
                      'active': deepcopy(entry)}
        self.rows = {entry['ticker']: deepcopy(entry)}

    def state(self):
        return deepcopy(self.value)

    def change(self, event, ticker, update):
        self.value, self.rows[ticker] = update(deepcopy(self.value), self.rows.get(ticker))

    def before_post(self):
        pass


class Broker:
    enabled = True

    def __init__(self, entry, *, fills=(Decimal('1'),), bid='.05'):
        self.entry = entry
        self.fills = list(fills)
        self.bid = bid
        self.orders = {}
        self.submitted = []
        self.ambiguous = False
        self.settled = False
        self.result = entry['side']
        self.sold = Decimal(0)
        self.entry_order = self._order(entry['intent'], money(entry['intent']['count']),
            action='buy', price='.50', fees='.02')

    def _order(self, intent, count, *, action, price, fees='0'):
        # V2's legacy action/outcome fields follow the YES book, not the
        # economic side of the strategy's position.
        if intent.get('reduce_only'):
            action = 'sell' if intent['side'] == 'ask' else 'buy'
            outcome_side = 'no' if intent['side'] == 'ask' else 'yes'
        else:
            outcome_side = self.entry['side']
        return {'client_order_id': intent['client_order_id'], 'ticker': intent['ticker'],
            'exchange_index': intent['exchange_index'], 'subaccount_number': intent['subaccount'],
            'book_side': intent['side'], 'outcome_side': outcome_side, 'action': action,
            'fill_count_fp': str(count), 'remaining_count_fp': '0',
            'status': 'executed' if count else 'canceled',
            'taker_fill_cost_dollars': str(count * money(price)),
            'maker_fill_cost_dollars': '0', 'taker_fees_dollars': fees if count else '0',
            'maker_fees_dollars': '0', 'order_id': intent['client_order_id']}

    def market(self, ticker):
        from market_research.engine import iso
        now = time.time()
        return {'ticker': ticker, 'exchange_index': 2, 'market_type': 'binary',
            'status': 'settled' if self.settled else 'active',
            'result': self.result if self.settled else '',
            'open_time': iso(now - 300), 'close_time': iso(now + 600),
            self.entry['side'] + '_bid_dollars': self.bid}

    def account(self):
        remaining = money(self.entry['intent']['count']) - self.sold
        sign = 1 if self.entry['side'] == 'yes' else -1
        return [], ([{'ticker': self.entry['ticker'], 'position_fp': str(sign * remaining)}]
                    if remaining and not self.settled else [])

    def find_order(self, intent, order_id=None):
        if intent == self.entry['intent']:
            return self.entry_order
        return self.orders.get(intent['client_order_id'])

    def submit(self, intent):
        self.submitted.append(intent)
        if self.ambiguous:
            raise RuntimeError('KALSHI_DELIVERY_OR_RESPONSE_UNKNOWN')
        count = self.fills.pop(0)
        self.sold += count
        self.orders[intent['client_order_id']] = self._order(intent, count,
            action='sell', price=self.bid, fees='.001' if count else '0')
        return {'order_id': intent['client_order_id'],
            'client_order_id': intent['client_order_id'],
            'fill_count': str(count), 'remaining_count': '0',
            'ts_ms': int(time.time() * 1000)}

    def exit_fill_summary(self, intent, side, terminal):
        count = money(terminal['filled'])
        return {'filled': str(count), 'proceeds': str(count * money(self.bid)),
                'fees': terminal['fees']}

    def pages(self, path, key, **params):
        if path != '/portfolio/settlements':
            raise AssertionError(path)
        remaining = money(self.entry['intent']['count']) - self.sold
        side, other = self.entry['side'], 'no' if self.entry['side'] == 'yes' else 'yes'
        return [{'ticker': self.entry['ticker'], 'exchange_index': 2,
            'market_result': self.result, side + '_count_fp': str(remaining),
            other + '_count_fp': '0', 'revenue': int(remaining * 100) if self.result == side else 0,
            side + '_total_cost_dollars': '.50', 'fee_cost': '.02'}]


def setup(side='yes', series='KXBTC15M', *, fills=(Decimal('1'),), bid='.05'):
    config = Config() if series == 'KXBTC15M' else CoinConfig(series)
    ticker = series + '-26SEP241500-00'
    intent = order_payload(ticker, side, 1, '.50', 2, config)
    entry = {'ticker': ticker, 'side': side, 'intent': intent, 'status': 'acknowledged',
        'signal': {'market_end': time.time() + 600}}
    journal = Journal(config, entry)
    broker = Broker(entry, fills=fills, bid=bid)
    return Trader(config, broker, journal), broker, journal


@pytest.mark.parametrize('side', ['yes', 'no'])
@pytest.mark.parametrize('series', ['KXBTC15M', 'KXBNB15M', 'KXDOGE15M',
    'KXETH15M', 'KXNEAR15M', 'KXZEC15M'])
def test_stop_closes_each_series_and_side_with_recovery(side, series, monkeypatch):
    monkeypatch.setattr(time, 'time', lambda: 1800000000)
    trader, broker, journal = setup(side, series)
    assert trader.reconcile() == 'stop_exit_acknowledged'
    intent = broker.submitted[0]
    assert intent['reduce_only'] is True and intent['time_in_force'] == 'immediate_or_cancel'
    assert intent['side'] == ('ask' if side == 'yes' else 'bid')
    assert intent['price'] == ('0.0500' if side == 'yes' else '0.9500')
    assert trader.reconcile() == 'stopped'
    assert journal.state()['active'] is None
    assert money(journal.state()['net']) == Decimal('-.471')
    assert journal.state()['size'] == 2


def test_partial_stop_retries_only_remaining_quantity(monkeypatch):
    monkeypatch.setattr(time, 'time', lambda: 1800000000)
    trader, broker, journal = setup(fills=(Decimal('.50'), Decimal('.50')))
    assert trader.reconcile() == 'stop_exit_acknowledged'
    assert trader.reconcile() == 'stop_exit_acknowledged'
    assert broker.submitted[1]['count'] == '0.50'
    assert broker.submitted[0]['client_order_id'] != broker.submitted[1]['client_order_id']
    assert journal.state()['active']['stop']['sold'] == '0.50'
    assert trader.reconcile() == 'stopped'
    assert len(broker.submitted) == 2


@pytest.mark.parametrize('side,expected_price', [('yes', '0.0800'), ('no', '0.9200')])
def test_latched_stop_reprices_partial_remainder_at_latest_bid(side, expected_price, monkeypatch):
    monkeypatch.setattr(time, 'time', lambda: 1800000000)
    trader, broker, journal = setup(side, fills=(Decimal('.50'), Decimal('.50')))
    assert trader.reconcile() == 'stop_exit_acknowledged'
    assert broker.submitted[0]['price'] == ('0.0500' if side == 'yes' else '0.9500')
    broker.bid = '.08'
    assert trader.reconcile() == 'stop_exit_acknowledged'
    assert broker.submitted[1]['count'] == '0.50'
    assert broker.submitted[1]['price'] == expected_price
    assert trader.reconcile() == 'stopped'
    assert journal.state()['active'] is None


def test_zero_fill_retries_with_new_durable_client_id(monkeypatch):
    monkeypatch.setattr(time, 'time', lambda: 1800000000)
    trader, broker, journal = setup(fills=(Decimal('0'), Decimal('1')))
    assert trader.reconcile() == 'stop_exit_acknowledged'
    assert trader.reconcile() == 'stop_exit_acknowledged'
    assert journal.state()['active']['stop']['attempt'] == 2
    assert trader.reconcile() == 'stopped'


def test_definitive_stop_rejection_retries_after_one_second(monkeypatch):
    now = [1800000000.0]
    monkeypatch.setattr(time, 'time', lambda: now[0])
    trader, broker, journal = setup()
    original_submit = broker.submit
    attempts = []
    def reject_once(intent):
        attempts.append(intent['client_order_id'])
        if len(attempts) == 1:
            raise RuntimeError('KALSHI_HTTP_422')
        return original_submit(intent)
    broker.submit = reject_once
    assert trader.reconcile() == 'stop_definitive_rejection_retry_wait'
    now[0] += .5
    assert trader.reconcile() == 'stop_rejection_backoff'
    assert len(attempts) == 1
    now[0] += .5
    assert trader.reconcile() == 'stop_exit_acknowledged'
    assert len(attempts) == 2 and attempts[0] != attempts[1]
    assert journal.state()['active']['stop']['attempt'] == 2


def test_ambiguous_stop_delivery_blocks_resubmission_across_restart(monkeypatch):
    monkeypatch.setattr(time, 'time', lambda: 1800000000)
    trader, broker, journal = setup()
    broker.ambiguous = True
    with pytest.raises(RuntimeError, match='DELIVERY_OR_RESPONSE_UNKNOWN'):
        trader.reconcile()
    assert len(broker.submitted) == 1
    restarted = Trader(trader.config, broker, journal)
    assert restarted.reconcile() == 'stop_unknown_delivery_blocked'
    assert len(broker.submitted) == 1


def test_stop_trigger_is_latched_when_no_executable_bid(monkeypatch):
    monkeypatch.setattr(time, 'time', lambda: 1800000000)
    trader, broker, journal = setup(bid='0')
    assert trader.reconcile() == 'stop_waiting_for_executable_bid'
    assert journal.state()['active']['stop']['threshold'] == '0.05'
    assert broker.submitted == []


def test_stop_does_not_trigger_above_five_cents(monkeypatch):
    monkeypatch.setattr(time, 'time', lambda: 1800000000)
    trader, broker, journal = setup(bid='.051')
    assert trader.reconcile() == 'held_to_settlement'
    assert 'stop' not in journal.state()['active']


def test_no_side_exit_limit_tracks_current_bid_below_threshold():
    trader, _, journal = setup('no', bid='.03')
    intent = stop_exit_payload(journal.state()['active'], Decimal('0.50'), '.03', 2,
        trader.config, attempt=1)
    assert intent['price'] == '0.9700' and intent['count'] == '0.50'


def test_protective_stop_may_submit_in_final_five_seconds(monkeypatch):
    from market_research.engine import iso
    monkeypatch.setattr(time, 'time', lambda: 1800000000)
    trader, broker, _ = setup(bid='.05')
    original_market = broker.market
    broker.market = lambda ticker: {**original_market(ticker),
        'close_time': iso(1800000003)}
    assert trader.reconcile() == 'stop_exit_acknowledged'
    assert broker.submitted[0]['reduce_only'] is True


def test_stop_waits_without_an_executable_bid(monkeypatch):
    monkeypatch.setattr(time, 'time', lambda: 1800000000)
    trader, broker, journal = setup(bid='0')
    assert trader.reconcile() == 'stop_waiting_for_executable_bid'
    assert journal.state()['active']['stop']['pending'] is None
    assert broker.submitted == []


def test_stop_fill_waits_for_account_position_to_decrease(monkeypatch):
    monkeypatch.setattr(time, 'time', lambda: 1800000000)
    trader, broker, journal = setup()
    assert trader.reconcile() == 'stop_exit_acknowledged'
    authoritative_account = broker.account
    broker.account = lambda: ([], [{'ticker': journal.state()['active']['ticker'],
        'position_fp': '1.00'}])
    assert trader.reconcile() == 'stop_position_read_lag'
    assert journal.state()['active']['stop']['sold'] == '0'
    assert journal.state()['active']['stop']['pending'] is not None
    broker.account = authoritative_account
    assert trader.reconcile() == 'stopped'


def test_stop_partial_fill_can_settle_residual(monkeypatch):
    monkeypatch.setattr(time, 'time', lambda: 1800000000)
    trader, broker, journal = setup(fills=(Decimal('.50'),))
    assert trader.reconcile() == 'stop_exit_acknowledged'
    broker.settled = True
    assert trader.reconcile() == 'settled'
    assert journal.state()['active'] is None
    assert money(journal.state()['net']) == Decimal('.004')


def test_account_attribution_nets_reconciled_and_pending_stop_fills():
    ticker = 'KXBTC15M-26SEP241500-00'
    entry = {'side': 'yes', 'filled': '1.00', 'stop': {'sold': '.25'},
        'pending_stop_filled': '.25', 'signal': {'market_end': time.time() + 60}}
    assert expected_positions([(ticker, entry)], {ticker: Decimal('.50')}, time.time()) == {ticker: Decimal('.50')}


def test_exit_proceeds_use_authenticated_fills_not_trigger_quote():
    trader, _, journal = setup('no', bid='.05')
    intent = stop_exit_payload(journal.state()['active'], Decimal('1'), '.05', 2,
        trader.config, attempt=1)
    terminal = {'order_id': 'stop-order-id', 'filled': '1', 'fees': '.003'}
    fill = {'fill_id': 'fill-1', 'order_id': 'stop-order-id', 'ticker': intent['ticker'],
        'exchange_index': 2, 'subaccount_number': 0, 'book_side': 'bid',
        'outcome_side': 'yes', 'action': 'buy', 'count_fp': '1',
        'no_price_dollars': '.0475', 'fee_cost': '.003'}
    broker = KalshiExecution.__new__(KalshiExecution)
    broker.config = trader.config
    broker.pages = lambda path, key, **params: [fill]
    result = broker.exit_fill_summary(intent, 'no', terminal)
    assert {key: money(value) for key, value in result.items()} == {
        'filled': Decimal('1'), 'proceeds': Decimal('.0475'), 'fees': Decimal('.003')}
    broker.pages = lambda path, key, **params: []
    assert broker.exit_fill_summary(intent, 'no', terminal) is None
    broker.pages = lambda path, key, **params: [fill, fill]
    with pytest.raises(RuntimeError, match='STOP_EXIT_FILL_IDENTITY_MISMATCH'):
        broker.exit_fill_summary(intent, 'no', terminal)


def test_real_v2_yes_stop_legacy_labels_are_not_economic_direction():
    from market_research.kalshi_execution import reconciled_stop_order
    trader, _, journal = setup('yes')
    intent = stop_exit_payload(journal.state()['active'], Decimal('1'), '.05', 2,
        trader.config, attempt=1)
    order = {'client_order_id': intent['client_order_id'], 'ticker': intent['ticker'],
        'exchange_index': 2, 'subaccount_number': 0, 'book_side': 'ask',
        'outcome_side': 'no', 'action': 'sell', 'fill_count_fp': '.20',
        'remaining_count_fp': '0', 'status': 'canceled',
        'taker_fees_dollars': '.001', 'maker_fees_dollars': '0', 'order_id': 'stop-order-id'}
    terminal = reconciled_stop_order(order, intent, 'yes')
    assert money(terminal['filled']) == Decimal('.20')
    fill = {'fill_id': 'fill-1', 'order_id': 'stop-order-id', 'ticker': intent['ticker'],
        'exchange_index': 2, 'subaccount_number': 0, 'book_side': 'ask',
        'outcome_side': 'no', 'action': 'sell', 'count_fp': '.20',
        'yes_price_dollars': '.05', 'fee_cost': '.001'}
    broker = KalshiExecution.__new__(KalshiExecution)
    broker.config = trader.config
    broker.pages = lambda path, key, **params: [fill]
    result = broker.exit_fill_summary(intent, 'yes', terminal)
    assert money(result['proceeds']) == Decimal('.01')
    order['action'] = 'buy'
    with pytest.raises(RuntimeError, match='STOP_EXIT_DIRECTION_UNVERIFIED'):
        reconciled_stop_order(order, intent, 'yes')
