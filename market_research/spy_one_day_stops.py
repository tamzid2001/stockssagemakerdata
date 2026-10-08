"""Frozen-forecast SPY one-session buys and fixed-stop sweep; cannot place orders."""
from __future__ import annotations

import argparse
from bisect import bisect_left
import csv
from datetime import date
import gzip
import json
import math
from pathlib import Path
from statistics import median

from .ftmo_dukas_data import digest, stamp
from .ftmo_p99_study import read_json, write_json
from .spy_p99_share_study import COSTS, NY, assumed_quotes

STOPS = (.25, .5, 1., 2., 3., 5., 7.5, 10., 15., 20., 30., 50.)
FILTERS = ('above_p99', 'above_p99_rising_median')


def prepare_sessions(rows, records, costs=COSTS[0]):
    """Entry is next executable open after inference; exit is forecast session close."""
    times = [stamp(r['start']) for r in rows]
    if times != sorted(set(times)):
        raise ValueError('UNIQUE_SORTED_MINUTES_REQUIRED')
    result = []
    for record in records:
        if record['status'] != 'completed':
            continue
        first = stamp(record['earliest_actionable_at'])
        close = stamp(record['predictions'][0]['timestamp'])
        a, b = bisect_left(times, first), bisect_left(times, close)
        observed = rows[a:b]
        if not observed or stamp(observed[-1]['end']) != close:
            raise ValueError('NEXT_SESSION_CLOSE_MISSING')
        if any((stamp(r['end']) - stamp(r['start'])).total_seconds() != 60 for r in observed):
            raise ValueError('GENUINE_MINUTE_REPLAY_REQUIRED')
        quotes = []
        for row in observed:
            bid, ask = assumed_quotes(row, 'SPY', costs)
            quotes.append({'at': stamp(row['start']), 'end': stamp(row['end']), 'bid': bid, 'ask': ask})
        result.append({'day': record['day'], 'quotes': quotes,
                       'cutoff_close': record['cutoff_close'],
                       'p99': record['predictions'][0]['p99'],
                       'qualifies': record['cutoff_close'] > record['predictions'][0]['p99'],
                       'rising_median': record['predictions'][-1]['p50'] >= record['predictions'][0]['p50']})
    return result


def replay(sessions, stop_distance, *, ordering='low_first', filter_name='above_p99',
           initial_balance=100000., risk_fraction=.01, profit_target=None):
    if (stop_distance <= 0 or not math.isfinite(stop_distance)
            or ordering not in ('low_first', 'high_first') or filter_name not in FILTERS
            or (profit_target is not None and (profit_target <= 0 or not math.isfinite(profit_target)))):
        raise ValueError('INVALID_ONE_DAY_CONFIGURATION')
    cash = peak = initial_balance
    drawdown = daily_loss = 0.
    trades = []
    skipped = 0
    path = ('o', 'l', 'h', 'c') if ordering == 'low_first' else ('o', 'h', 'l', 'c')
    for session in sessions:
        if not session['qualifies'] or (filter_name.endswith('rising_median') and not session['rising_median']):
            continue
        bars = session['quotes']
        entry = bars[0]['ask']['o']
        stop = math.floor((entry-stop_distance)*100 + 1e-8)/100
        target = math.ceil((entry+profit_target)*100 - 1e-8)/100 if profit_target is not None else None
        distance = entry - stop
        budget = risk_fraction * cash
        shares = math.floor(min(budget/distance, cash/entry) + 1e-10)
        if shares < 1 or stop <= 0:
            skipped += 1
            continue
        opening_cash = cash
        assert shares*entry <= cash + 1e-7
        assert shares*distance <= budget + 1e-7
        closed = False
        def mark(price):
            nonlocal peak, drawdown, daily_loss
            value = opening_cash + shares*(price-entry)
            drawdown = max(drawdown, peak-value)
            peak = max(peak, value)
            daily_loss = max(daily_loss, opening_cash-value)
        for bar in bars:
            for field in path:
                px = bar['bid'][field]
                if px <= stop + 1e-8:
                    exit_price = px if field == 'o' else stop
                    exit_at = bar['at'] if field == 'o' else bar['end']
                    reason = 'gap_stop' if field == 'o' else 'fixed_stop'
                    mark(exit_price)
                    closed = True
                    break
                if target is not None and px >= target - 1e-8:
                    exit_price = px if field == 'o' else target
                    exit_at = bar['at'] if field == 'o' else bar['end']
                    reason = 'take_profit'
                    mark(exit_price)
                    closed = True
                    break
                mark(px)
            if closed:
                break
        if not closed:
            exit_price, exit_at, reason = bars[-1]['bid']['c'], bars[-1]['end'], 'next_session_close'
        pnl = shares*(exit_price-entry)
        cash += pnl
        trades.append({'day': session['day'], 'entry_at': bars[0]['at'].isoformat(),
                       'exit_at': exit_at.isoformat(), 'entry': entry, 'exit': exit_price,
                       'stop': stop, 'shares': shares, 'risk_budget_usd': budget,
                       'entry_equity': opening_cash, 'planned_stop_risk_usd': shares*distance,
                       'net_pnl': pnl, 'pnl_per_share': exit_price-entry, 'reason': reason,
                       'duration_hours': (exit_at-bars[0]['at']).total_seconds()/3600})
    wins, losses = sum(t['net_pnl'] > 0 for t in trades), sum(t['net_pnl'] < 0 for t in trades)
    durations = [t['duration_hours'] for t in trades]
    ws = ls = mw = ml = 0
    for trade in trades:
        ws = ws+1 if trade['net_pnl'] > 0 else 0
        ls = ls+1 if trade['net_pnl'] < 0 else 0
        mw, ml = max(mw, ws), max(ml, ls)
    assert math.isclose(cash-initial_balance, sum(t['net_pnl'] for t in trades), abs_tol=1e-7)
    return {'stop_distance': stop_distance, 'profit_target': profit_target, 'filter': filter_name, 'ordering': ordering,
            'initial_balance': initial_balance, 'ending_equity': cash, 'net_pnl': cash-initial_balance,
            'return_pct': 100*(cash/initial_balance-1), 'closed_trades': len(trades),
            'wins': wins, 'losses': losses, 'breakevens': len(trades)-wins-losses,
            'win_rate': wins/len(trades) if trades else None,
            'max_equity_drawdown': drawdown, 'max_daily_loss': daily_loss,
            'median_duration_hours': median(durations) if durations else None,
            'mean_duration_hours': sum(durations)/len(durations) if durations else None,
            'max_win_streak': mw, 'max_loss_streak': ml, 'minimum_size_skips': skipped,
            'stop_exits': sum(t['reason'] in ('fixed_stop', 'gap_stop') for t in trades),
            'target_exits': sum(t['reason'] == 'take_profit' for t in trades),
            'shares_peak': max((t['shares'] for t in trades), default=0),
            'net_pnl_one_share_same_exits': sum(t['pnl_per_share'] for t in trades),
            'trades': trades, 'orders_sent': 0}


def study(source, forecasts, output):
    plan, identity = read_json(source/'study-plan.json'), read_json(source/'source.json')
    with gzip.open(source/'minutes.jsonl.gz', 'rt') as f:
        rows = [json.loads(line) for line in f]
    if digest(rows) != identity['minutes_sha256']:
        raise ValueError('MINUTE_SOURCE_HASH_MISMATCH')
    records = {}
    for path in forecasts.rglob('????-??-??.json.gz'):
        record = read_json(path)
        if (record.get('status') != 'completed' or record.get('symbol') != 'SPY'
                or record['day'] in records or record.get('history_rows') != 500
                or record.get('source_rows_sha256') != identity['rows_sha256']):
            raise ValueError('FORECAST_INTEGRITY_MISMATCH')
        records[record['day']] = record
    if set(records) != set(plan['days']):
        raise ValueError('ALL_FROZEN_FORECASTS_REQUIRED')
    ordered = [records[d] for d in plan['days']]
    sessions = prepare_sessions(rows, ordered)
    split = plan['development_end_exclusive']
    development = [s for s in sessions if s['day'] < split]
    holdout = [s for s in sessions if s['day'] >= split]
    candidates, full, validation = [], [], []
    for filter_name in FILTERS:
        for stop in STOPS:
            for path in ('low_first', 'high_first'):
                candidates.append(replay(development, stop, ordering=path, filter_name=filter_name))
                full.append(replay(sessions, stop, ordering=path, filter_name=filter_name))
        pairs = [[r for r in candidates if r['filter'] == filter_name and r['stop_distance'] == stop] for stop in STOPS]
        eligible = [p for p in pairs if all(r['closed_trades'] >= 5 and r['max_daily_loss'] <= 5000
                                           and r['max_equity_drawdown'] <= 10000 for r in p)]
        selected = max(eligible, key=lambda p: min(r['net_pnl'] for r in p))[0]['stop_distance'] if eligible else None
        if selected is not None:
            for cost in COSTS:
                stressed = prepare_sessions(rows, ordered, cost)
                later = [s for s in stressed if s['day'] >= split]
                for path in ('low_first', 'high_first'):
                    validation.append({'filter': filter_name, 'selected_stop': selected, 'costs': cost.name,
                                       'full_year': replay(stressed, selected, ordering=path, filter_name=filter_name),
                                       'fresh_flat_holdout': replay(later, selected, ordering=path, filter_name=filter_name)})
    selected_baseline = next((v['selected_stop'] for v in validation if v['filter'] == 'above_p99'), None)
    target_comparison, target_selection = [], None
    if selected_baseline is not None:
        for target in (1., 2., 3., 5., 7.5, 10.):
            dev = [replay(development, selected_baseline, ordering=path, profit_target=target)
                   for path in ('low_first', 'high_first')]
            target_comparison.append({'target': target, 'development': dev,
                'full_year': [replay(sessions, selected_baseline, ordering=path, profit_target=target)
                              for path in ('low_first', 'high_first')],
                'fresh_flat_holdout': [replay(holdout, selected_baseline, ordering=path, profit_target=target)
                                       for path in ('low_first', 'high_first')]})
        chosen = max(target_comparison, key=lambda v: min(r['net_pnl'] for r in v['development']))
        target_selection = chosen['target']
    controls = {'all_forecasts': sessions, 'above_p99_signals': [s for s in sessions if s['qualifies']]}
    calibration = {}
    for name, group in controls.items():
        moves = [s['quotes'][-1]['bid']['c'] - s['quotes'][0]['ask']['o'] for s in group]
        calibration[name] = {'observations': len(group), 'next_session_close_above_entry': sum(m > 0 for m in moves),
                             'next_session_close_above_cutoff': sum(s['quotes'][-1]['ask']['c'] > s['cutoff_close'] for s in group),
                             'next_session_close_above_forecast_p99': sum(s['quotes'][-1]['ask']['c'] > s['p99'] for s in group),
                             'one_share_no_stop_eod_pnl': sum(moves)}
    output.mkdir(parents=True, exist_ok=True)
    write_json(output/'results.json.gz', {'complete': True, 'source': identity, 'study': plan,
               'strategy': 'above first P99; single buy; fixed dollar stop; next NYSE session close; no averaging or trailing',
               'stop_candidates': STOPS, 'selection_period_end_exclusive': split,
               'calibration': calibration, 'development': candidates, 'full_year_candidates': full,
               'selected_validation': validation, 'take_profit_comparison': target_comparison,
               'selected_take_profit_on_development': target_selection,
               'orders_sent': 0, 'firestore_writes': 0})
    fields = ('filter', 'stop_distance', 'ordering', 'net_pnl', 'return_pct', 'closed_trades', 'wins', 'losses',
              'win_rate', 'max_equity_drawdown', 'max_daily_loss', 'median_duration_hours', 'mean_duration_hours',
              'stop_exits', 'max_win_streak', 'max_loss_streak', 'net_pnl_one_share_same_exits')
    with (output/'all-stops.csv').open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore'); writer.writeheader(); writer.writerows(full)
    lines = ['# SPY one-day fixed-stop comparison', '',
             'Original 251 saved forecasts; October 7, 2025–October 6, 2026 origins, execution through October 7, 2026. $100,000 starting equity; whole shares; no borrowing. Same strict above-first-P99 signal.', '',
             'Buy at the first regular-session ask after the measured forecast completion time; sell at the next predicted NYSE session close or fixed stop. One purchase per qualifying origin; no averaging or trailing. Quantity = floor(min(1% equity / fixed-stop distance, equity / entry ask)). Gaps can exceed the planned stop risk.', '',
             f'Stops were selected on origins before {split}, maximizing worse-path development price P&L among 12 fixed dollar distances; final-quarter validation starts flat. Median-trend filtering is a separately labeled exploratory comparison, not a proven improvement.', '',
             '| Filter | Selected stop | Full-year net | Worst equity DD | W/L | Win rate | Median hours | Holdout net |',
             '|---|---:|---:|---:|---|---:|---:|---:|']
    for filter_name in FILTERS:
        pair = [v for v in validation if v['filter'] == filter_name and v['costs'] == COSTS[0].name]
        if not pair:
            lines.append(f'| {filter_name} | No eligible selection | — | — | — | — | — | — |')
            continue
        ref = [v['full_year'] for v in pair]
        hold = [v['fresh_flat_holdout'] for v in pair]
        worst = min(ref, key=lambda r: r['net_pnl'])
        lines.append(f"| {filter_name} | ${pair[0]['selected_stop']:g} | ${min(r['net_pnl'] for r in ref):,.2f}–${max(r['net_pnl'] for r in ref):,.2f} | ${max(r['max_equity_drawdown'] for r in ref):,.2f} | {worst['wins']}/{worst['losses']} | {100*worst['win_rate']:.2f}% | {worst['median_duration_hours']:.2f} | ${min(r['net_pnl'] for r in hold):,.2f}–${max(r['net_pnl'] for r in hold):,.2f} |")
    lines += ['', '## Shorter-duration take-profit comparison', '',
              f'Keep the development-selected ${selected_baseline:g} stop; compare six fixed take-profit distances. The development-selected target is ${target_selection:g}. All are exploratory alternatives to the next-close exit.', '',
              '| Take profit | Full-year net range | Worst DD | Win-rate range | Median duration range, hours | Holdout net range |',
              '|---:|---:|---:|---:|---:|---:|']
    for v in target_comparison:
        pair, hold = v['full_year'], v['fresh_flat_holdout']
        lines.append(f"| ${v['target']:g} | ${min(r['net_pnl'] for r in pair):,.2f}–${max(r['net_pnl'] for r in pair):,.2f} | ${max(r['max_equity_drawdown'] for r in pair):,.2f} | {100*min(r['win_rate'] for r in pair):.2f}%–{100*max(r['win_rate'] for r in pair):.2f}% | {min(r['median_duration_hours'] for r in pair):.2f}–{max(r['median_duration_hours'] for r in pair):.2f} | ${min(r['net_pnl'] for r in hold):,.2f}–${max(r['net_pnl'] for r in hold):,.2f} |")
    lines += ['', '## What the 47.81% observation means', '',
              'The earlier 120/251 (47.81%) count uses provider daily close bars. Executable replay closes use the final regular-session minute. One classification differs: July 24, 2026 daily close 738.93 versus minute close 738.85, relative to predicted P99 738.9172657. Neither count estimates the probability that a purchase earns money.', '']
    for name, c in calibration.items():
        n = c['observations']
        lines.append(f"- {name}: {n} origins; next-session close above entry in {c['next_session_close_above_entry']}/{n}, above cutoff in {c['next_session_close_above_cutoff']}/{n}, above forecast P99 in {c['next_session_close_above_forecast_p99']}/{n}. One share, no-stop next-close price P&L: ${c['one_share_no_stop_eod_pnl']:.2f}.")
    lines += ['', 'Full-year stop sweep:', '',
              '| Filter | Stop | Net range | Worst equity DD | W/L | Median duration hours |',
              '|---|---:|---:|---:|---|---:|']
    for filter_name in FILTERS:
        for stop in STOPS:
            pair = [r for r in full if r['filter'] == filter_name and r['stop_distance'] == stop]
            worst = min(pair, key=lambda r: r['net_pnl'])
            lines.append(f"| {filter_name} | ${stop:g} | ${min(r['net_pnl'] for r in pair):,.2f}–${max(r['net_pnl'] for r in pair):,.2f} | ${max(r['max_equity_drawdown'] for r in pair):,.2f} | {worst['wins']}/{worst['losses']} | {worst['median_duration_hours']:.2f} |")
    lines += ['', 'Trade bars use the original modeled 1-cent spread; selected stops also have a 2-cent sensitivity in results.json.gz. Historical NBBO, order queues, dividends, regulatory fees and interest are unverified or excluded. Returns are research price P&L; FTMO US500 contract execution is a separate test. The one-share no-stop control has no risk sizing and must not be compared directly with account-sized returns.']
    (output/'report.md').write_text('\n'.join(lines)+'\n')
    print(json.dumps({'event': 'spy_one_day_stops_complete', 'full_year_replays': len(full),
                      'calibration': calibration, 'output': str(output)}), flush=True)


def main():
    parser = argparse.ArgumentParser()
    for field in ('source', 'forecasts', 'output'):
        parser.add_argument('--'+field, type=Path, required=True)
    args = parser.parse_args()
    study(args.source, args.forecasts, args.output)


if __name__ == '__main__':
    main()
