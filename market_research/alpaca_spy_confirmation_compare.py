"""Paired SPY P90-entry comparison using immutable saved forecast matrices.

This is read-only: no model inference, option data request, or broker order.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime
import hashlib
import json
from pathlib import Path

from .alpaca_spy_intraday_backtest import (
    replay_underlying_window, stock_minutes, summarize_underlying,
)
from .alpaca_spy_strategy import Window
from .alpaca_spy_worker import AlpacaAPI


def compare(day: date, horizon: int, input_dir: Path, output_dir: Path,
            *, feed: str = 'sip') -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    api = AlpacaAPI('paper')
    try:
        bars = stock_minutes(api, day, feed)
    finally:
        api.client.close()
    baseline_windows = []
    confirmed_windows = []
    for index in range(6):
        saved = json.loads((input_dir / f'window-{index:02d}.json').read_text())
        if saved.get('status') != 'replayed' or saved.get('stock_feed') != feed:
            raise ValueError('BASELINE_WINDOW_NOT_COMPARABLE')
        start = datetime.fromisoformat(saved['origin'])
        end = datetime.fromisoformat(saved['end'])
        if int((end - start).total_seconds() // 60) != horizon:
            raise ValueError('BASELINE_HORIZON_MISMATCH')
        history = [bar for bar in bars if bar.end <= start][-500:]
        history_hash = hashlib.sha256(json.dumps(
            [(bar.end.isoformat(), bar.close) for bar in history],
            separators=(',', ':')).encode()).hexdigest()
        if history_hash != saved['history_sha256']:
            raise ValueError('BASELINE_STOCK_HISTORY_CHANGED')
        forecast = {'predictions': saved['predictions'],
            'runtime_seconds': saved['inference_seconds'],
            'result_hash': saved['forecast_hash'], 'model_runs': saved['model_runs']}
        forecast_fn = lambda _history, _horizon: forecast
        window = Window(start, end)
        baseline = replay_underlying_window(bars, window, feed=feed,
            forecast_fn=forecast_fn, confirmation_minutes=0)
        if (baseline['status'] != 'replayed' or
                baseline['trades'] != saved['trades'] or
                baseline['signals'] != saved['signals']):
            raise ValueError('BASELINE_REPLAY_MISMATCH')
        confirmed = replay_underlying_window(bars, window, feed=feed,
            forecast_fn=forecast_fn, confirmation_minutes=1)
        baseline_windows.append(baseline)
        confirmed_windows.append(confirmed)
        (output_dir / f'confirmed-window-{index:02d}.json').write_text(
            json.dumps(confirmed, indent=2))
    baseline_summary = summarize_underlying(baseline_windows)
    confirmed_summary = summarize_underlying(confirmed_windows)
    baseline_net = baseline_summary['net_directional_spy_change_usd_per_share']
    confirmed_net = confirmed_summary['net_directional_spy_change_usd_per_share']
    report = {'day': day.isoformat(), 'horizon_minutes': horizon,
        'feed': feed, 'comparison': 'one_completed_minute_confirmation',
        'baseline': baseline_summary, 'confirmed': confirmed_summary,
        'net_delta_usd_per_share': (
            round(confirmed_net - baseline_net, 6)
            if baseline_net is not None and confirmed_net is not None else None),
        'limitations': [
            'This measures signed SPY price movement per share, not option fills or P&L.',
            'Both scenarios use identical saved five-model quantiles and model inference latency.',
            'Both scenarios require the next observed one-minute open after a completed-bar signal.',
        ]}
    (output_dir / 'comparison.json').write_text(json.dumps(report, indent=2))
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--day', type=date.fromisoformat, required=True)
    parser.add_argument('--horizon', type=int, choices=(15, 30, 45, 60), required=True)
    parser.add_argument('--input-dir', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--feed', choices=('iex', 'sip'), default='sip')
    args = parser.parse_args()
    print(json.dumps(compare(args.day, args.horizon, args.input_dir,
        args.output_dir, feed=args.feed)), flush=True)


if __name__ == '__main__':
    main()
