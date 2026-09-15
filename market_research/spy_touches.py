"""Observed price-path diagnostics, never model probabilities or option fills.

Weekly averages are frozen arithmetic path means, not quantiles of a weekly
return distribution. Excursions deliberately use subsequent observations and
are descriptive outcomes only; they must never feed a trading decision.
"""
import math
from statistics import mean

import pandas as pd

from .spy_weekly import iso


def tape(forecast, rows):
    expected = {}
    predictions = {r['timestamp']: r['quantiles'] for r in forecast['predictions']}
    for bin_ in forecast['future_grid']:
        if bin_['timestamp'] not in predictions or bin_['start'] < forecast['origin']:
            raise ValueError('UNALIGNED_TOUCH_FORECAST')
        for t in pd.date_range(bin_['start'], periods=bin_['minutes'], freq='1min'):
            timestamp = iso(t)
            if timestamp in expected:
                raise ValueError('OVERLAPPING_TOUCH_GRID')
            expected[timestamp] = predictions[bin_['timestamp']]
    observed = {}
    for row in rows:
        t = iso(pd.to_datetime(row['timestamp'], utc=True))
        if t not in expected:
            continue
        prices = [row[k] for k in ('open', 'high', 'low', 'close')]
        if any(type(v) not in (int, float) or not math.isfinite(v) or v <= 0 for v in prices):
            raise ValueError('INVALID_TOUCH_OHLC')
        if row['low'] > min(row['open'], row['close']) or row['high'] < max(row['open'], row['close']):
            raise ValueError('INCONSISTENT_TOUCH_OHLC')
        value = {**row, 'timestamp': t}
        if t in observed and observed[t] != value:
            raise ValueError('CONFLICTING_TOUCH_DUPLICATE')
        observed[t] = value
    return expected, [observed[t] for t in sorted(observed)]


def averages(forecast):
    paths = forecast['predictions']
    if not paths:
        raise ValueError('EMPTY_TOUCH_FORECAST')
    keys = sorted(paths[0]['quantiles'], key=float)
    if not keys or any(not 0 < float(k) < 1 for k in keys):
        raise ValueError('INVALID_TOUCH_QUANTILE')
    for p in paths:
        q = p['quantiles']
        if set(q) != set(keys) or any(not math.isfinite(q[k]) or q[k] <= 0 for k in keys):
            raise ValueError('INVALID_TOUCH_QUANTILE')
        if any(q[a] > q[b] for a, b in zip(keys, keys[1:])):
            raise ValueError('CROSSING_TOUCH_QUANTILES')
    return {k: mean(p['quantiles'][k] for p in paths) for k in keys}


def average_touches(forecast, rows):
    expected, observed = tape(forecast, rows)
    levels = averages(forecast)
    result = {}
    for q, level in levels.items():
        hits, episodes, up, down, gaps = [], 0, 0, 0, []
        previous, previous_hit = None, False
        for row in observed:
            contiguous = previous is not None and pd.Timestamp(row['timestamp']) - pd.Timestamp(previous['timestamp']) == pd.Timedelta(minutes=1)
            hit = row['low'] <= level <= row['high']
            if hit:
                hits.append(row['timestamp'])
                if not contiguous or not previous_hit:
                    episodes += 1
            if previous is not None:
                if contiguous:
                    up += previous['close'] < level <= row['close']
                    down += previous['close'] > level >= row['close']
                gap_up = previous['close'] < level < row['open']
                gap_down = previous['close'] > level > row['open']
                if gap_up or gap_down:
                    gaps.append({'timestamp': row['timestamp'], 'direction': 'up' if gap_up else 'down',
                                 'consecutive_minutes': contiguous, 'same_bar_range_touch': hit})
            previous, previous_hit = row, hit
        result[q] = {'average_value': level, 'ever_range_touched': bool(hits) if observed else None,
                     'range_touch_minutes': len(hits), 'touch_episodes': episodes,
                     'upward_close_crossings': up, 'downward_close_crossings': down,
                     'first_touch': hits[0] if hits else None, 'last_touch': hits[-1] if hits else None,
                     'gap_through_count': len(gaps), 'gap_through_events': gaps,
                     'first_observed_open': observed[0]['open'] if observed else None}
    return {'origin': forecast['origin'], 'week_end': forecast['week_end'],
            'expected_minutes': len(expected), 'observed_minutes': len(observed),
            'missing_minutes': len(expected) - len(observed),
            'coverage_fraction': len(observed) / len(expected) if expected else None,
            'levels': result,
            'method': 'low <= frozen arithmetic weekly average <= high, observed regular-session minute bars; episodes reset across gaps',
            'limitation': 'Bar range intersection is a touch proxy, not a verified trade or option fill at that exact price. Gaps are reported separately; missing minutes remain unknown.'}


def target_distance(rows, target, reference, upper):
    if not rows:
        return {'target': target, 'observations': 0, 'reached_or_exceeded': None}
    field = 'high' if upper else 'low'
    best = (max if upper else min)(rows, key=lambda r: r[field])
    sign = 1 if upper else -1
    initial_gap = sign * (target - reference)
    signed_residual = sign * (target - best[field])
    reached = [r for r in rows if sign * (r[field] - target) >= 0]
    return {'target': target, 'observations': len(rows), 'initial_gap_usd': initial_gap,
            'closest_remaining_gap_usd': max(0., signed_residual),
            'overshoot_usd': max(0., -signed_residual),
            'fraction_initial_gap_closed': min(1., max(0., (initial_gap - signed_residual) / initial_gap)) if initial_gap > 0 else None,
            'reached_or_exceeded': bool(reached), 'first_reach_bar_start': reached[0]['timestamp'] if reached else None,
            'actual_range_touch': any(r['low'] <= target <= r['high'] for r in rows),
            'extreme': best[field], 'extreme_bar_start': best['timestamp']}


def crossing_excursions(forecast, rows, hourly_bars):
    """Completed-hour region entries, measured strictly AFTER signal close.

Report both the moving hourly bands and frozen weekly-average thresholds.
P90 continuation targets P99; P10 continuation targets P1. Both outer targets
are also included per event to make reversal/recovery paths inspectable.
"""
    expected, observed = tape(forecast, rows)
    avg = averages(forecast)
    if not {'0.01', '0.1', '0.9', '0.99'} <= set(avg):
        raise ValueError('OUTER_QUANTILES_REQUIRED')
    actual = {b['timestamp']: b for b in hourly_bars}
    events = []
    for basis in ('hourly_path', 'weekly_average'):
        prior_zone = None
        prior_timestamp = None
        for index, p in enumerate(forecast['predictions']):
            b = actual.get(p['timestamp'])
            if b is None:
                prior_zone = prior_timestamp = None
                continue
            q = p['quantiles'] if basis == 'hourly_path' else avg
            zone = '0.9' if b['close'] >= q['0.9'] else '0.1' if b['close'] <= q['0.1'] else None
            if zone and zone != prior_zone:
                following = [r for r in observed if r['timestamp'] >= b['timestamp']]
                expected_remaining = sum(t >= b['timestamp'] for t in expected)
                tail = '0.99' if zone == '0.9' else '0.01'
                upper = zone == '0.9'
                sign = 1 if upper else -1
                targets = {name: target_distance(following, value, b['close'], is_upper)
                           for name, value, is_upper in (
                               ('signal_p99', p['quantiles']['0.99'], True),
                               ('signal_p1', p['quantiles']['0.01'], False),
                               ('average_p99', avg['0.99'], True),
                               ('average_p1', avg['0.01'], False))}
                moving_gaps = [max(0., sign * (expected[r['timestamp']][tail] - r['high' if upper else 'low'])) for r in following]
                high = max((r['high'] for r in following), default=None)
                low = min((r['low'] for r in following), default=None)
                events.append({'basis': basis, 'quantile': zone, 'timestamp': b['timestamp'],
                    'time_et': pd.Timestamp(b['timestamp']).tz_convert('America/New_York').strftime('%a %Y-%m-%d %I:%M %p %Z'),
                    'signal_kind': 'first_observed_bar_already_beyond' if prior_timestamp is None else 'hourly_region_entry',
                    'observed_hour_index': index, 'close': b['close'], 'threshold': q[zone],
                    'threshold_minus_average_usd': q[zone] - avg[zone],
                    'close_minus_average_usd': b['close'] - avg[zone],
                    'continuation_tail': tail, 'targets': targets,
                    'subsequent_minutes': len(following), 'right_censored': not following,
                    'expected_subsequent_minutes': expected_remaining,
                    'missing_subsequent_minutes': expected_remaining - len(following),
                    'high_after_signal': high, 'low_after_signal': low,
                    'continuation_move_pct': sign * ((high if upper else low) / b['close'] - 1) * 100 if following else None,
                    'largest_adverse_move_pct': max(0., -sign * ((low if upper else high) / b['close'] - 1)) * 100 if following else None,
                    'moving_tail_reached_or_exceeded': any(g == 0 for g in moving_gaps) if following else None,
                    'closest_moving_tail_gap_usd': min(moving_gaps) if following else None})
            prior_zone, prior_timestamp = zone, b['timestamp']
    return {'events': events,
            'method': 'new hourly-close signal region; first qualifying bar is labeled, not claimed a confirmed crossing; future minute OHLC starts at/after completed signal close',
            'limitation': 'Descriptive overlapping outcomes to week end, not independent trades, executed fills, option delta, or an ex-ante probability.'}


def summarize_touches(weeks):
    keys = sorted({q for w in weeks for q in w.get('average_touches', {}).get('levels', {})}, key=float)
    summary = {}
    for q in keys:
        reports = [w['average_touches'] for w in weeks if q in w.get('average_touches', {}).get('levels', {})]
        observed = [r for r in reports if r['observed_minutes']]
        complete = [r for r in observed if r['missing_minutes'] == 0]
        touched = sum(r['levels'][q]['ever_range_touched'] for r in observed)
        summary[q] = {'weeks_with_observations': len(observed), 'weeks_touched': touched,
                      'observed_week_touch_rate': touched / len(observed) if observed else None,
                      'full_coverage_weeks': len(complete),
                      'full_coverage_touch_rate': sum(r['levels'][q]['ever_range_touched'] for r in complete) / len(complete) if complete else None,
                      'weeks_with_missing_minutes': sum(r['missing_minutes'] > 0 for r in reports),
                      'range_touch_minutes': sum(r['levels'][q]['range_touch_minutes'] for r in observed),
                      'touch_episodes': sum(r['levels'][q]['touch_episodes'] for r in observed)}
    return summary
