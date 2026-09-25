import json
from datetime import date, datetime, timedelta, timezone

from market_research import alpaca_spy_confirmation_compare as comparison
from market_research.alpaca_spy_intraday_backtest import replay_underlying_window
from market_research.alpaca_spy_strategy import MinuteBar, Window


def test_pairing_reuses_immutable_forecasts_and_rejects_changed_history(monkeypatch, tmp_path):
    first = datetime(2026, 9, 25, 13, 30, tzinfo=timezone.utc)
    bars = []
    for offset in range(-499, 6 * 60 + 16):
        close = {1: 101, 2: 99, 3: 98, 4: 94}.get(offset, 94 if offset > 4 else 100)
        opening = {3: 98, 4: 97, 5: 94}.get(offset, close)
        bars.append(MinuteBar(first + timedelta(minutes=offset), close,
                              close, close, opening))
    class API:
        client = type('Client', (), {'close': lambda self: None})()
    monkeypatch.setattr(comparison, 'AlpacaAPI', lambda mode: API())
    monkeypatch.setattr(comparison, 'stock_minutes', lambda api, day, feed: bars)
    source = tmp_path / 'source'
    source.mkdir()
    for index in range(6):
        origin = first + timedelta(hours=index)
        window = Window(origin, origin + timedelta(minutes=15))
        def forecast(history, horizon, origin=origin):
            return {'predictions': [{'timestamp': (origin + timedelta(minutes=i)).isoformat(),
                    'quantiles': {'0.5': 95, '0.9': 100}} for i in range(1, 16)],
                'runtime_seconds': 0, 'result_hash': 'hash', 'model_runs': []}
        saved = replay_underlying_window(bars, window, feed='sip', forecast_fn=forecast)
        assert saved['status'] == 'replayed'
        (source / f'window-{index:02d}.json').write_text(json.dumps(saved))
    report = comparison.compare(date(2026, 9, 25), 15, source, tmp_path / 'result')
    assert report['baseline']['trade_count'] == 1
    assert report['confirmed']['trade_count'] == 1
    assert report['net_delta_usd_per_share'] == -1
    assert (tmp_path / 'result' / 'comparison.json').exists()
