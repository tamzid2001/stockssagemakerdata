from datetime import datetime, timezone
from decimal import Decimal

import pytest

from market_research import alpaca_spy_worker as worker
from market_research.alpaca_spy_strategy import MinuteBar, tradable_windows


def at(hour, minute=0):
    return datetime(2026, 9, 25, hour, minute, tzinfo=timezone.utc)


class Journal:
    def __init__(self, **state):
        self.state = state

    def load(self):
        return self.state.copy()

    def update(self, **changes):
        self.state.update(changes)


class API:
    mode = 'paper'

    def __init__(self, ask='1.99'):
        self.ask = ask
        self.submissions = []
        self.position_rows = []

    def positions(self):
        return self.position_rows

    def account(self):
        return {'options_trading_level': 2, 'options_buying_power': '200'}

    def contracts(self, _expiration):
        return [{'symbol': 'SPY260925C00500000', 'type': 'call', 'expiration_date': '2026-09-25',
                 'tradable': True, 'underlying_symbol': 'SPY', 'strike_price': '500'}]

    def option_quote(self, _symbol):
        return {'t': datetime.now(timezone.utc).isoformat(), 'ap': self.ask}

    def order_by_client_id(self, _identifier):
        return None

    def submit(self, payload):
        self.submissions.append(payload)
        return {'id': 'order-1', 'status': 'filled', 'filled_qty': '1', 'filled_avg_price': self.ask}


def test_over_200_nearest_contract_never_submits(monkeypatch):
    api, journal = API('2.01'), Journal()
    window = tradable_windows(at(13, 30), at(20))[1]
    worker.enter(api, journal, window, 'call', MinuteBar(at(14, 32), 500, 501, 499))
    assert api.submissions == []
    assert journal.load().get('intent') is None


def test_one_contract_order_has_200_cap_and_durable_intent(monkeypatch):
    api, journal = API('1.99'), Journal()
    window = tradable_windows(at(13, 30), at(20))[1]
    monkeypatch.setattr(worker, 'reconcile_order', lambda _api, identifier, payload: (
        api.submissions.append(payload) or {'id': 'order-1', 'status': 'filled', 'filled_qty': '1', 'filled_avg_price': '1.99'}))
    worker.enter(api, journal, window, 'call', MinuteBar(at(14, 32), 500, 501, 499))
    assert len(api.submissions) == 1
    assert api.submissions[0]['qty'] == '1'
    assert api.submissions[0]['limit_price'] == '1.99'
    assert api.submissions[0]['side'] == 'buy'
    assert journal.load()['active']['kind'] == 'call'
    assert journal.load()['intent'] is None


def test_unknown_order_delivery_does_not_send_an_unidentified_retry():
    api = API()
    attempts = []
    def submit(_payload):
        attempts.append(1)
        raise TimeoutError('network')
    api.submit = submit
    with pytest.raises(RuntimeError, match='ORDER_DELIVERY_UNKNOWN'):
        worker.reconcile_order(api, 'qspy-test', {'symbol': 'SPY260925C00500000'})
    assert len(attempts) == 1


def test_restart_recovers_filled_intent_only_when_broker_shows_one_contract():
    api = API()
    api.position_rows = [{'symbol': 'SPY260925C00500000', 'asset_class': 'us_option', 'qty': '1'}]
    api.order_by_client_id = lambda _identifier: {'status': 'filled', 'filled_qty': '1', 'filled_at': at(14, 32).isoformat()}
    journal = Journal(intent={'id': 'entry-one', 'symbol': 'SPY260925C00500000', 'kind': 'call', 'window': at(14, 30).isoformat()})
    active = worker.owned_position(api, journal)
    assert active['entry_id'] == 'entry-one'
    assert journal.load()['intent'] is None
    api.position_rows[0]['qty'] = '2'
    with pytest.raises(RuntimeError, match='POSITION_ACCOUNTING_MISMATCH'):
        worker.owned_position(api, journal)


def test_forecast_is_anchored_to_the_latest_real_completed_minute(monkeypatch):
    from datetime import timedelta
    from ensemble_forecasting import capabilities
    from ensemble_forecasting import worker as engine

    window = tradable_windows(at(13, 30), at(20))[0]
    anchor = window.start + timedelta(minutes=1)
    history = [MinuteBar(anchor - timedelta(minutes=39-index), 500, 501, 499)
               for index in range(40)]
    class BarsAPI:
        def stock_bars(self, **_kwargs):
            return history
    class ForecastJournal(Journal):
        pass
    monkeypatch.setattr(capabilities, 'timesfm_availability', lambda _mode: (True, None))
    def fake_execute(payload):
        assert payload['input']['rows'][-1]['timestamp'] == anchor.isoformat()
        return {'predictions': [{'timestamp': (anchor + timedelta(minutes=1)).isoformat(),
                                 'quantiles': {'0.5': 500, '0.9': 505}}],
                'dataset_hash': 'snapshot', 'model_runs': [
                    {'model': name, 'status': 'completed'} for name in
                    ('prophet', 'toto', 'granite', 'chronos', 'timesfm')]}
    monkeypatch.setattr(engine, 'execute_job', fake_execute)
    result = worker.forecast_at(BarsAPI(), window, ForecastJournal())
    assert result['input_count'] == 40
    assert result['predictions'][0]['timestamp'] == (anchor + timedelta(minutes=1)).isoformat()


def test_prior_day_close_cannot_anchor_a_new_hourly_forecast():
    from datetime import timedelta
    window = tradable_windows(at(13, 30), at(20))[0]
    class PriorCloseAPI:
        def stock_bars(self, **_kwargs):
            return [MinuteBar(window.start - timedelta(hours=17), 500, 501, 499)] * 40
    with pytest.raises(RuntimeError, match='WINDOW_FIRST_MINUTE_NOT_COMPLETE'):
        worker.forecast_at(PriorCloseAPI(), window, Journal())


def test_exact_200_premium_is_allowed_but_above_is_not(monkeypatch):
    api, journal = API('2.00'), Journal()
    window = tradable_windows(at(13, 30), at(20))[1]
    monkeypatch.setattr(worker, 'reconcile_order', lambda *_args, **_kwargs: {
        'status': 'filled', 'filled_qty': '1', 'filled_avg_price': '2.00'})
    worker.enter(api, journal, window, 'call', MinuteBar(at(14, 32), 500, 501, 499))
    assert journal.load()['active']['symbol'] == 'SPY260925C00500000'
