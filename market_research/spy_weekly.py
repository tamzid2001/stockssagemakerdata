"""Bounded, read-only SPY Friday-close/hourly five-model research.

Uses Quantura's existing equity data service and numerical adapters. No orders,
web-request inference, forward-filled bars, or synthesized model outputs.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import date, timedelta
import hashlib
import json
import math
import os
from pathlib import Path
import time

import httpx
import numpy as np
import pandas as pd
import pandas_market_calendars as mcal

VERSION = 'spy_friday_hourly_v1'
QUANTILES = (0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99)


def iso(value):
    return pd.Timestamp(value).isoformat().replace('+00:00', 'Z')


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False, separators=(',', ':')).encode()).hexdigest()


def save(path, value):
    """Immutable records, including input/provider snapshot and every forecast."""
    data = json.dumps(value, sort_keys=True, allow_nan=False, indent=2)
    if path.exists():
        if path.read_text() != data:
            raise ValueError('IMMUTABLE_RESEARCH_RECORD_CONFLICT')
        return
    with path.open('x') as out:
        os.chmod(path, 0o600)
        out.write(data)


def session_grid(start, end):
    """NYSE 09:30-anchored hours, with a labeled final partial session bar."""
    rows = []
    for _, session in mcal.get_calendar('NYSE').schedule(start_date=start, end_date=end).iterrows():
        current = session.market_open
        while current < session.market_close:
            stop = min(current + pd.Timedelta(hours=1), session.market_close)
            rows.append({'start': iso(current), 'timestamp': iso(stop),
                         'minutes': int((stop-current).total_seconds()/60)})
            current = stop
    return rows


def aggregate_minutes(rows, grid):
    """Missing interior minutes stay missing; no carry-forward fake prices.

    A bin without its opening or closing minute cannot be used for execution or
    close-based forecasting. Internal IEX no-trade minutes are counted explicitly.
    """
    by_time = {}
    for row in rows:
        t = iso(pd.to_datetime(row['timestamp'], utc=True))
        values = [row[k] for k in ('open', 'high', 'low', 'close')]
        if any(not isinstance(v, (float, int)) or not math.isfinite(v) or v <= 0 for v in values):
            raise ValueError('INVALID_OHLC')
        if row['low'] > min(row['open'], row['close']) or row['high'] < max(row['open'], row['close']):
            raise ValueError('INCONSISTENT_OHLC')
        if t in by_time and by_time[t] != row:
            raise ValueError('CONFLICTING_MINUTE_DUPLICATE')
        by_time[t] = row
    output, rejected = [], []
    for bin_ in grid:
        timestamps = [iso(t) for t in pd.date_range(bin_['start'], periods=bin_['minutes'], freq='1min')]
        if timestamps[0] not in by_time or timestamps[-1] not in by_time:
            rejected.append({**bin_, 'reason': 'MISSING_BOUNDARY_MINUTE'})
            continue
        observed = [by_time[t] for t in timestamps if t in by_time]
        output.append({**bin_, 'open': observed[0]['open'], 'close': observed[-1]['close'],
                       'high': max(r['high'] for r in observed), 'low': min(r['low'] for r in observed),
                       'volume': sum(r.get('volume', 0) for r in observed),
                       'observed_minutes': len(observed), 'missing_minutes': bin_['minutes']-len(observed)})
    return output, rejected


def weekly_windows(end_friday, weeks=4):
    end = date.fromisoformat(end_friday)
    if end.weekday() != 4 or not 1 <= weeks <= 8:
        raise ValueError('ONE_TO_EIGHT_COMPLETED_FRIDAY_WEEKS_REQUIRED')
    closes = mcal.get_calendar('NYSE').schedule(start_date=end-timedelta(weeks=weeks), end_date=end)
    result = []
    for i in range(weeks, 0, -1):
        start_day, end_day = end-timedelta(weeks=i), end-timedelta(weeks=i-1)
        if pd.Timestamp(start_day) not in closes.index or pd.Timestamp(end_day) not in closes.index:
            raise ValueError('FRIDAY_HOLIDAY_REQUIRES_EXPLICIT_ALTERNATIVE_POLICY')
        result.append((iso(closes.loc[str(start_day), 'market_close']), iso(closes.loc[str(end_day), 'market_close'])))
    return result


def forecast_week(history, future):
    # Same production interfaces, with exchange-aware intraday timestamps instead
    # of asking a continuous frequency calendar to forecast nights/weekends.
    from ensemble_forecasting.worker import adapter_factory
    from ensemble_forecasting.adapters.base import cleanup_memory
    from ensemble_forecasting.capabilities import validate_request_capabilities, MODEL_REGISTRY
    from ensemble_forecasting.schemas import ForecastRequest, APPROVED_MODELS
    from ensemble_forecasting.preprocessing import prepare_series
    from ensemble_forecasting.ensemble import build_ensemble
    if len(history) != 500 or not future or history[-1]['timestamp'] >= future[0]['timestamp']:
        raise ValueError('500_PAST_HOURLY_BARS_AND_STRICT_FUTURE_REQUIRED')
    series = prepare_series([{'timestamp': r['timestamp'], 'target': r['close']} for r in history],
                            transform='log', frequency='1h', timezone='America/New_York', minimum_rows=500)
    request = ForecastRequest.from_dict({
        'prediction_length': len(future), 'horizon_mode': 'frequency_periods', 'frequency': '1h',
        'calendar': 'NYSE', 'quantiles': QUANTILES, 'context_length': 500, 'transform': 'log',
        'failure_policy': 'fail', 'models': {m: {'enabled': True, 'weight': 1} for m in APPROVED_MODELS},
        'model_checkpoints': {m: MODEL_REGISTRY['models'][m].get('checkpoint') for m in APPROVED_MODELS},
    })
    validate_request_capabilities(request)
    seed = int(series.dataset_hash[:8], 16)
    np.random.seed(seed)
    import torch
    torch.manual_seed(seed)
    torch.set_num_threads(min(2, os.cpu_count() or 2))
    timestamps = tuple(r['timestamp'] for r in future)
    models, runs = {}, []
    started = time.monotonic()
    for model in APPROVED_MODELS:
        print(json.dumps({'event': 'spy_model_started', 'origin': history[-1]['timestamp'], 'model': model}), flush=True)
        try:
            output = adapter_factory(model).forecast(series, timestamps, request)
            output.validate()
            models[model] = output
            runs.append({k: v for k, v in asdict(output).items() if k != 'quantile_matrix'})
        finally:
            cleanup_memory()
    result = build_ensemble(request, models, timestamps, chosen_transform=series.transform).to_dict()
    return {**result, 'origin': history[-1]['timestamp'], 'history_start': history[0]['timestamp'],
            'history_count': len(history), 'training_sha256': digest(history), 'request': asdict(request),
            'model_runs': runs, 'seed': seed, 'runtime_seconds': time.monotonic()-started,
            'created_at': iso(pd.Timestamp.now(tz='UTC'))}


def excursions(signal, bars, predictions, week_end):
    following = [b for b in bars if signal['timestamp'] < b['timestamp'] <= week_end]
    q = predictions[signal['timestamp']]['quantiles']
    upper = signal['side'] == 'long'
    tail = '0.99' if upper else '0.01'
    frozen = q[tail]
    close = signal['close']
    gap = max(0., (frozen-close) if upper else (close-frozen))
    if not following:
        return {'future_bars': 0, 'right_censored': True}
    high = max(b['high'] for b in following)
    low = min(b['low'] for b in following)
    best = high if upper else low
    residual = max(0., (frozen-best) if upper else (best-frozen))
    dynamic = [(b, predictions[b['timestamp']]['quantiles'][tail]) for b in following if b['timestamp'] in predictions]
    return {'future_bars': len(following), 'right_censored': False, 'high_after_signal': high,
            'low_after_signal': low, 'favorable_excursion_pct': ((best/close-1) if upper else (1-best/close))*100,
            'tail': tail, 'tail_at_signal': frozen, 'initial_tail_gap_usd': gap,
            'closest_frozen_tail_gap_usd': residual,
            'fraction_initial_gap_closed': (gap-residual)/gap if gap else None,
            'frozen_tail_reached': residual == 0,
            'moving_tail_reached': any(b['high'] >= target if upper else b['low'] <= target for b, target in dynamic),
            'closest_moving_tail_gap_usd': min(max(0., target-b['high'] if upper else b['low']-target) for b, target in dynamic),
            'week_end_close': following[-1]['close'],
            'direction_profitable_at_week_end': following[-1]['close'] > close if upper else following[-1]['close'] < close}


def simulate(bars, forecasts, *, cost_bps=0., contrarian=False, trailing_fraction=None, minute_bars=None):
    """Default long P90 / short P10; opt-in reversed signals and close trails.

    No same-candle high/low order fill assumptions. Crossing means entry into the
    time-aligned >=P90 or <=P10 region; the first eligible bar may already qualify.
    Terminal liquidation is explicitly a research end-of-sample assumption.
    Minute execution uses the SAME hourly signals, with minute-close trailing
    triggers filled at the next scheduled minute open. It is not a tick stop.
    """
    if not math.isfinite(cost_bps) or not 0 <= cost_bps <= 100:
        raise ValueError('INVALID_COST')
    if type(contrarian) is not bool:
        raise ValueError('INVALID_SIGNAL_DIRECTION')
    if trailing_fraction is not None and (type(trailing_fraction) not in (int,float) or
            not math.isfinite(trailing_fraction) or not 0 < trailing_fraction < 1):
        raise ValueError('INVALID_TRAILING_FRACTION')
    predictions, week_ends, grid = {}, {}, {}
    for forecast in forecasts:
        for row in forecast['predictions']:
            if row['timestamp'] <= forecast['origin'] or row['timestamp'] in predictions:
                raise ValueError('LOOKAHEAD_OR_OVERLAPPING_FORECAST')
            predictions[row['timestamp']] = row
            week_ends[row['timestamp']] = forecast['week_end']
        for bin_ in forecast['future_grid']:
            grid[bin_['timestamp']] = bin_
    hourly_bars = sorted([b for b in bars if b['timestamp'] in predictions], key=lambda b: b['timestamp'])
    if minute_bars is not None:
        minute_grid = {}
        for bin_ in grid.values():
            for start in pd.date_range(bin_['start'], pd.Timestamp(bin_['timestamp'])-pd.Timedelta(minutes=1), freq='1min'):
                end = iso(start+pd.Timedelta(minutes=1))
                minute_grid[end] = {'start': iso(start), 'timestamp': end}
        grid = minute_grid
        bars = []
        seen = set()
        for row in minute_bars:
            end = iso(pd.Timestamp(row['timestamp'])+pd.Timedelta(minutes=1))
            if end not in grid:
                continue
            if end in seen or any(not math.isfinite(row[k]) or row[k] <= 0 for k in ('open','high','low','close')):
                raise ValueError('INVALID_MINUTE_TAPE')
            seen.add(end)
            bars.append({**row, 'start': iso(pd.Timestamp(row['timestamp'])), 'timestamp': end})
        bars.sort(key=lambda b:b['timestamp'])
        # Signals must still be based on the same genuine hourly-close dataset.
        closing = {b['timestamp']: b['close'] for b in bars}
        if any(closing.get(b['timestamp']) != b['close'] for b in hourly_bars):
            raise ValueError('MINUTE_HOURLY_CLOSE_MISMATCH')
    else:
        bars = hourly_bars
    expected = sorted(grid)
    next_bar = dict(zip(expected, expected[1:]))
    if not bars:
        raise ValueError('NO_EVALUATION_BARS')
    trades, signals, equity, position, pending = [], [], [], None, None
    realized, peak, drawdown, prior_zone = 0., 0., 0., None
    skipped = 0
    wait_fresh = False
    trailing_decisions = []

    def finish(price, timestamp, reason):
        nonlocal position, realized
        direction = 1 if position['side'] == 'long' else -1
        gross = direction*(price-position['entry_price'])
        fees = (price+position['entry_price'])*cost_bps/10000
        trade = {**position, 'exit_time': timestamp, 'exit_price': price, 'exit_reason': reason,
                 'gross_pnl': gross, 'assumed_cost': fees, 'net_pnl': gross-fees}
        trades.append(trade)
        realized += gross-fees
        position = None

    for b in bars:
        if pending:
            if b['timestamp'] == pending['execute_bar']:
                if position and pending.get('reason') == 'trailing_stop':
                    position['trailing_exit_signal'] = {k:v for k,v in pending.items() if k != 'execute_bar'}
                    finish(b['open'], b['start'], 'trailing_stop')
                elif position and position['side'] != pending['side']:
                    finish(b['open'], b['start'], 'opposite_quantile_signal')
                if not position and pending['side'] is not None:
                    position = {'side': pending['side'], 'entry_time': b['start'], 'entry_price': b['open'],
                                'signal_time': pending['timestamp'], 'quantity': 1}
                    if trailing_fraction is not None:
                        position['favorable_close_watermark'] = b['open']
            else:
                skipped += 1
            pending = None
        q = predictions.get(b['timestamp'], {}).get('quantiles')
        zone = None
        if q:
            if not q['0.1'] <= q['0.5'] <= q['0.9']:
                raise ValueError('CROSSING_QUANTILES')
            if q['0.1'] != q['0.9']:
                zone = 'long' if b['close'] >= q['0.9'] else 'short' if b['close'] <= q['0.1'] else None
            if contrarian and zone:
                zone = 'short' if zone == 'long' else 'long'
        fresh = zone and zone != prior_zone
        if fresh:
            signal = {'timestamp': b['timestamp'], 'side': zone, 'close': b['close'],
                      'threshold': q['0.9'] if (zone == 'long') != contrarian else q['0.1'],
                      'week_end': week_ends[b['timestamp']]}
            signal['excursion'] = excursions(signal, bars, predictions, signal['week_end'])
            signals.append(signal)
        if zone and (not position or position['side'] != zone) and (not wait_fresh or fresh):
            if b['timestamp'] in next_bar:
                pending = {'side': zone, 'timestamp': b['timestamp'], 'execute_bar': next_bar[b['timestamp']]}
                wait_fresh = False
        if q:
            prior_zone = zone
        # Opposite hourly signal has priority over a simultaneous trailing exit.
        # Watermarks use completed closes only, never an unknowable OHLC path.
        if position and trailing_fraction is not None:
            long = position['side'] == 'long'
            old = position['favorable_close_watermark']
            watermark = max(old,b['close']) if long else min(old,b['close'])
            position['favorable_close_watermark'] = watermark
            stop = watermark*(1-trailing_fraction if long else 1+trailing_fraction)
            hit = b['close'] <= stop if long else b['close'] >= stop
            if hit and pending is None and b['timestamp'] in next_bar:
                pending = {'side': None, 'timestamp': b['timestamp'], 'execute_bar': next_bar[b['timestamp']],
                           'reason': 'trailing_stop', 'stop_level': stop, 'watermark': watermark,
                           'signal_close': b['close'], 'fraction': trailing_fraction}
                trailing_decisions.append(dict(pending))
                wait_fresh = True
        value = realized
        if position:
            value += (1 if position['side']=='long' else -1)*(b['close']-position['entry_price'])
            value -= position['entry_price']*cost_bps/10000
        peak = max(peak, value)
        drawdown = max(drawdown, peak-value)
        equity.append({'timestamp': b['timestamp'], 'pnl': value})
    if position:
        finish(bars[-1]['close'], bars[-1]['timestamp'], 'study_end_liquidation_assumption')
    drawdown = max(drawdown, peak-realized)
    wins = sum(t['net_pnl'] > 0 for t in trades)
    losses = sum(t['net_pnl'] < 0 for t in trades)
    capital = bars[0]['open']
    summary = {'trades': len(trades), 'wins': wins, 'losses': losses, 'breakeven': len(trades)-wins-losses,
               'win_rate': wins/len(trades) if trades else None, 'net_pnl_usd_one_share': realized,
               'return_on_initial_one_share_capital_pct': realized/capital*100,
               'initial_capital_usd': capital, 'hourly_marked_max_drawdown_usd': drawdown,
               'assumed_cost_bps_per_fill': cost_bps, 'skipped_missing_execution_bar': skipped,
               'buy_hold_price_return_pct': (bars[-1]['close']/bars[0]['open']-1)*100}
    for side in ('long', 'short'):
        closed = [t for t in trades if t['side'] == side]
        summary[side] = {'trades': len(closed), 'wins': sum(t['net_pnl']>0 for t in closed),
                         'losses': sum(t['net_pnl']<0 for t in closed), 'pnl_usd': sum(t['net_pnl'] for t in closed)}
    if contrarian or trailing_fraction is not None or minute_bars is not None:
        summary['observation_marked_max_drawdown_usd'] = drawdown
        if minute_bars is not None:
            summary.pop('hourly_marked_max_drawdown_usd')
        summary['monitoring_frequency'] = 'completed_minute_close' if minute_bars is not None else 'completed_hourly_close'
        summary['trailing_exits'] = sum(t['exit_reason']=='trailing_stop' for t in trades)
        summary['contrarian'] = contrarian
        summary['trailing_fraction'] = trailing_fraction
    result = {'summary': summary, 'trades': trades, 'signals': signals, 'equity': equity}
    if trailing_fraction is not None:
        result['trailing_decisions'] = trailing_decisions
    return result


def metrics(bars, forecasts):
    by_time = {b['timestamp']: b for b in bars}
    output = []
    for forecast in forecasts:
        aligned = [(by_time[r['timestamp']], r['quantiles']) for r in forecast['predictions'] if r['timestamp'] in by_time]
        errors = [q['0.5']-b['close'] for b,q in aligned]
        output.append({'origin': forecast['origin'], 'week_end': forecast['week_end'],
                       'label': 'retrospective_out_of_sample_input_window', 'evaluated_bars': len(aligned),
                       'mae': float(np.mean(np.abs(errors))), 'rmse': float(np.sqrt(np.mean(np.square(errors)))),
                       'bias': float(np.mean(errors)),
                       'p10_p90_interval_coverage': sum(q['0.1'] <= b['close'] <= q['0.9'] for b,q in aligned)/len(aligned),
                       'pinball_loss': {str(tau): float(np.mean([max(tau*(b['close']-q[str(tau)]),
                                        (tau-1)*(b['close']-q[str(tau)])) for b,q in aligned])) for tau in QUANTILES}})
    return output


def run(output, end_friday, weeks=4):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    windows = weekly_windows(end_friday, weeks)
    if pd.Timestamp(windows[-1][1]) >= pd.Timestamp.now(tz='UTC'):
        raise ValueError('COMPLETED_WEEKS_ONLY')
    start_day = (pd.Timestamp(windows[0][0])-pd.Timedelta(days=150)).date().isoformat()
    source_path = output/'report-spy-source.json'
    if source_path.exists():
        source = json.loads(source_path.read_text())
    else:
        # The shared backend owns provider auth, pagination, normalization and
        # entitlement. Never embed vendor credentials or arbitrary URL inputs.
        url = 'https://quantura.studio/api/ticker/history'
        params = {'symbol': 'SPY', 'source': 'alpaca', 'timeframe': '1Min', 'session': 'regular',
                  'adjustment': 'raw', 'limit': 50000, 'start': start_day+'T00:00:00Z', 'end': windows[-1][1]}
        with httpx.Client(timeout=180) as client:
            response = client.get(url, params=params)
            response.raise_for_status()
            source = response.json()
        if not source.get('ok') or source.get('symbol')!='SPY' or source.get('timeframe')!='1Min':
            raise ValueError('UNEXPECTED_SOURCE_SCHEMA')
        source['retrieved_at'] = iso(pd.Timestamp.now(tz='UTC'))
        source['request'] = params
        source['redistribution_status'] = 'review_required'
        save(source_path, source)
    grid = session_grid(start_day, end_friday)
    bars, rejected = aggregate_minutes(source['rows'], grid)
    configuration = {'version': VERSION, 'end_friday': end_friday, 'weeks': weeks,
                     'source_sha256': digest(source), 'code_sha': os.getenv('GITHUB_SHA'),
                     'source_provider': source['provider'], 'feed': source.get('feed'), 'history_rows': 500,
                     'signal': 'hourly_close_at_or_above_p90_long_at_or_below_p10_short',
                     'execution': 'next_scheduled_bar_open', 'quantity': 1,
                     'horizon': 'next_friday_regular_session_close', 'quantiles': QUANTILES}
    save(output/'report-spy-configuration.json', configuration)
    forecasts = []
    for origin, end in windows:
        history = [b for b in bars if b['timestamp'] <= origin][-500:]
        future = [b for b in grid if origin < b['timestamp'] <= end]
        if not history or history[-1]['timestamp'] != origin:
            raise ValueError('FRIDAY_CLOSE_OBSERVATION_REQUIRED')
        path = output/('report-spy-forecast-'+origin[:10]+'.json')
        if path.exists():
            forecast = json.loads(path.read_text())
            if forecast['training_sha256'] != digest(history) or forecast['future_grid'] != future:
                raise ValueError('CHECKPOINT_DATA_CONFLICT')
        else:
            forecast = {**forecast_week(history, future), 'week_end': end, 'future_grid': future}
            save(path, forecast)
        forecasts.append(forecast)
        print(json.dumps({'event': 'spy_week_checkpointed', 'origin': origin, 'steps': len(future),
                          'session_hours': sum(b['minutes'] for b in future)/60}), flush=True)
    scenarios = {str(cost): simulate(bars, forecasts, cost_bps=cost) for cost in (0., 1., 5.)}
    report = {'configuration': configuration, 'hourly_bars': len(bars), 'rejected_boundary_bars': rejected,
              'missing_interior_minutes': sum(b['missing_minutes'] for b in bars),
              'validation_metrics': metrics(bars, forecasts),
              'forecasts': [{'origin': f['origin'], 'week_end': f['week_end'], 'history_count': f['history_count'],
                             'steps': len(f['predictions']), 'models': [m['model_id'] for m in f['model_runs']]} for f in forecasts],
              'scenarios': scenarios, 'limitations': [
                  'Retrospective research, not prospective archived forecasts; current checkpoint training cutoffs may not be auditable.',
                  'IEX-only traded bars, not consolidated SIP or executable NBBO quotes. Missing minutes are not filled.',
                  'Hourly close signals execute at next bar open without queue/spread/borrow verification.',
                  'Dollar P&L uses one share; percentage return denominator is starting SPY price, not broker short margin.',
                  'Costs are sensitivity assumptions; excludes borrow fees, dividends, taxes and financing.',
                  'Final bar is 30 minutes (shorter on exchange early-close days); all other bins are one hour.',
                  'Four weeks is a small exploratory sample, not evidence of durable predictive skill.',
                  'Tail excursions start strictly after signal bar; intrabar ordering and target fills are not asserted.',
              ]}
    save(output/'report-spy-summary.json', report)
    print(json.dumps({'event': 'spy_completed', 'summaries': {k:v['summary'] for k,v in scenarios.items()}}), flush=True)
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    parser.add_argument('--end-friday', required=True)
    parser.add_argument('--weeks', type=int, default=4)
    args = parser.parse_args()
    run(args.output, args.end_friday, args.weeks)
