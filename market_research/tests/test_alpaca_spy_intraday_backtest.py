from datetime import datetime, timedelta, timezone

from market_research.alpaca_spy_intraday_backtest import (
    first_observed_price, replay_window, summarize,
)
from market_research.alpaca_spy_strategy import MinuteBar, Window


def test_option_price_requires_observed_bar_in_bounded_window():
    at = datetime(2026, 9, 25, 13, 32, tzinfo=timezone.utc)
    assert first_observed_price({at - timedelta(minutes=1): 1.5}, at) is None
    assert first_observed_price({at + timedelta(minutes=3): 1.5}, at) is None
    assert first_observed_price({at + timedelta(minutes=1): 1.5}, at) == (at + timedelta(minutes=1), 1.5)


def test_replay_uses_only_500_pre_origin_rows_and_next_option_bars(monkeypatch):
    origin = datetime(2026, 9, 25, 13, 30, tzinfo=timezone.utc)
    history = [MinuteBar(origin - timedelta(minutes=499 - i), 100, 100, 100)
               for i in range(500)]
    future = [MinuteBar(origin + timedelta(minutes=i),
        101 if i == 1 else 99 if i == 2 else 94 if i == 4 else 98,
        101 if i == 1 else 99, 94 if i == 4 else 98)
        for i in range(1, 16)]
    def forecast(rows, horizon):
        assert rows == history and horizon == 15
        return {'predictions': [{'timestamp': (origin + timedelta(minutes=i)).isoformat(),
                'quantiles': {'0.5': 95, '0.9': 100}} for i in range(1, 16)],
            'runtime_seconds': 0, 'result_hash': 'forecast-hash',
            'model_runs': [{'model': name, 'status': 'completed'}
                for name in ('prophet', 'toto', 'granite', 'chronos', 'timesfm')]}
    symbol = 'SPY260925P00100000'
    monkeypatch.setattr('market_research.alpaca_spy_intraday_backtest.option_minutes',
        lambda api, key, day: {origin + timedelta(minutes=2): 1.5,
            origin + timedelta(minutes=4): 2.0})
    contracts = [{'symbol': symbol, 'type': 'put', 'expiration_date': '2026-09-25',
        'underlying_symbol': 'SPY', 'tradable': True, 'strike_price': '100'}]
    result = replay_window(object(), history + future, contracts,
        Window(origin, origin + timedelta(minutes=15)), feed='iex', forecast_fn=forecast)
    assert result['status'] == 'replayed'
    assert result['history_count'] == 500
    assert len(result['trades']) == 1
    trade = result['trades'][0]
    assert trade['kind'] == 'put' and trade['symbol'] == symbol
    assert trade['exit_reason'] == 'median_target'
    assert trade['pnl_proxy_usd'] == 50
    assert result['option_price_type'] == 'historical_trade_bar_open_proxy'
    assert summarize([result])['net_pnl_trade_bar_proxy_usd_before_costs'] == 50


def test_replay_rejects_missing_origin_minute_without_model_call():
    origin = datetime(2026, 9, 25, 13, 30, tzinfo=timezone.utc)
    bars = [MinuteBar(origin - timedelta(minutes=500 - i), 100, 100, 100)
            for i in range(500)]
    result = replay_window(object(), bars, [], Window(origin, origin + timedelta(minutes=60)),
        feed='iex', forecast_fn=lambda *_: (_ for _ in ()).throw(AssertionError('future leak')))
    assert result['status'] == 'origin_minute_missing'
    assert result['trades'] == []


def test_forbidden_option_history_records_signal_without_inventing_pnl(monkeypatch):
    origin = datetime(2026, 9, 25, 13, 30, tzinfo=timezone.utc)
    history = [MinuteBar(origin - timedelta(minutes=499 - i), 100, 100, 100)
               for i in range(500)]
    future = [MinuteBar(origin + timedelta(minutes=i), close, close, close)
              for i, close in enumerate((101, 99, 101, 99), start=1)]
    def forecast(rows, horizon):
        return {'predictions': [{'timestamp': (origin + timedelta(minutes=i)).isoformat(),
                'quantiles': {'0.5': 95, '0.9': 100}} for i in range(1, horizon + 1)],
            'runtime_seconds': 0, 'result_hash': 'hash', 'model_runs': []}
    def forbidden(*_):
        raise RuntimeError('ALPACA_HTTP_403')
    monkeypatch.setattr('market_research.alpaca_spy_intraday_backtest.option_minutes', forbidden)
    contracts = [{'symbol': 'SPY260925P00100000', 'type': 'put',
        'expiration_date': '2026-09-25', 'underlying_symbol': 'SPY',
        'tradable': True, 'strike_price': '100'}]
    result = replay_window(object(), history + future, contracts,
        Window(origin, origin + timedelta(minutes=15)), feed='iex', forecast_fn=forecast)
    summary = summarize([result])
    assert result['status'] == 'replayed'
    assert len(result['signals']) == 3
    assert result['signals_skipped']['option_history_forbidden'] == 2
    assert summary['trade_count'] == 0
    assert summary['underlying_cross_signals'] == 3
    assert summary['net_pnl_trade_bar_proxy_usd_before_costs'] is None
    assert not summary['complete']
