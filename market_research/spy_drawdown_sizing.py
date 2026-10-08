"""Offline fixed-grid sizing experiment; saved forecasts, no provider/order calls."""
from __future__ import annotations

import argparse
from datetime import date, datetime, time, timedelta
import gzip
import json
from pathlib import Path

from .ftmo_dukas_data import digest, stamp
from .ftmo_dukas_engine import PRAGUE
from .ftmo_p99_engine import Candidate
from .ftmo_p99_study import read_json, write_json
from .spy_p99_share_study import NY, COSTS, run_share_replay


def study(source, forecasts, baseline, output):
    old = read_json(baseline)
    identity = read_json(source/'source.json')
    plan = read_json(source/'study-plan.json')
    if identity != old['source'] or plan != old['study']:
        raise ValueError('BASELINE_SOURCE_MISMATCH')
    records = {}
    for path in forecasts.rglob('????-??-??.json.gz'):
        row = read_json(path)
        if (row.get('status') != 'completed' or row.get('symbol') != 'SPY'
                or row.get('source_rows_sha256') != identity['rows_sha256'] or row['day'] in records):
            raise ValueError('FORECAST_INTEGRITY_MISMATCH')
        records[row['day']] = row
    if set(records) != set(plan['days']):
        raise ValueError('ALL_FROZEN_FORECASTS_REQUIRED')
    records = [records[d] for d in plan['days']]
    with gzip.open(source/'minutes.jsonl.gz', 'rt') as stream:
        rows = [json.loads(line) for line in stream]
    if digest(rows) != identity['minutes_sha256']:
        raise ValueError('MINUTE_SOURCE_HASH_MISMATCH')
    for row in rows:
        row['_at'], row['_end'] = stamp(row['start']), stamp(row['end'])
    first = datetime.combine(date.fromisoformat(plan['start']), time.min, NY)
    last = datetime.combine(date.fromisoformat(plan['end'])+timedelta(days=2), time.min, NY)
    original = [r for r in old['full_year_grid_comparison'] if r['candidate'] == 'grid-2-equal']
    worst_dd = max(r['max_equity_drawdown'] for r in original)
    multiplier = 10000./worst_dd
    linear = {'size_multiplier': multiplier,
              'net_low_usd': min(r['net_pnl'] for r in original)*multiplier,
              'net_high_usd': max(r['net_pnl'] for r in original)*multiplier,
              'peak_notional_usd': max(r['max_invested_notional_usd'] for r in original)*multiplier,
              'max_daily_loss_usd': max(r['max_daily_loss'] for r in original)*multiplier,
              'note': 'Arithmetic scaling only, unchanged trade schedule and fractional sizing; exceeds cash buying power. Not a new strategy replay or future drawdown guarantee.'}
    multipliers = (1., 2., 5., 10., 20., 50., 100., multiplier, 500., 1000.)
    results = []
    for scale in multipliers:
        for ordering in ('low_first', 'high_first'):
            row = run_share_replay(rows, records, first, last, Candidate(2., 'equal'),
                                   ordering=ordering, detail=True,
                                   research_budget_multiplier=scale, account_timezone=PRAGUE)
            results.append(row)
            if scale == 1.:
                expected = next(r for r in original if r['ordering'] == ordering)
                for key in expected:
                    if row[key] != expected[key]:
                        raise ValueError('ORIGINAL_GRID_REPLAY_CHANGED_'+key.upper())
        print(json.dumps({'event': 'spy_grid_sizing', 'budget_multiplier': scale,
                          'effective_planned_risk_fraction': .01*scale,
                          'net_range': [r['net_pnl'] for r in results[-2:]],
                          'drawdown_range': [r['max_equity_drawdown'] for r in results[-2:]],
                          'ladder_range': [r['max_ladder'] for r in results[-2:]],
                          'peak_notional': max(r['max_invested_notional_usd'] for r in results[-2:])}), flush=True)
    eligible = []
    for scale in multipliers:
        pair = [r for r in results if r.get('research_budget_multiplier', 1.) == scale]
        if all(r['max_equity_drawdown'] <= 10000 and r['max_daily_loss'] <= 5000
               and not r['first_total_breach'] and not r['first_margin_breach'] for r in pair):
            eligible.append((min(r['net_pnl'] for r in pair), scale))
    selected = max(eligible)[1] if eligible else None
    stress = []
    if selected is not None:
        for ordering in ('low_first', 'high_first'):
            stress.append(run_share_replay(rows, records, first, last, Candidate(2., 'equal'),
                                          COSTS[1], ordering, True,
                                          research_budget_multiplier=selected, account_timezone=PRAGUE))
    value = {'complete': True, 'source': identity, 'source_run': 37826329337,
             'initial_balance': 100000., 'grid': 2., 'sizing': 'equal',
             'peak_drawdown_selection_limit': 10000., 'daily_loss_selection_limit': 5000.,
             'static_equity_floor': 90000., 'daily_reset_timezone': 'Europe/Prague',
             'notional_cap': '1x actual equity; no cash borrowing or inflated buying power',
             'linear_estimate': linear, 'experimental_budget_multipliers': multipliers,
             'full_year_replays': results, 'posthoc_highest_profit_eligible_multiplier': selected,
             'selected_cost_stress': stress,
             'selection_note': 'Sizing chosen after reviewing the entire year; retrospective sensitivity, not a fresh holdout or recommended risk limit.',
             'orders_sent': 0, 'firestore_writes': 0}
    write_json(output/'results.json.gz', value)
    lines = ['# SPY $2 equal grid: drawdown and sizing sensitivity', '',
             'The original 82 closed baskets each contained exactly one entry order. Multiple shares in one order are one ladder leg. Larger sizing reruns actual execution and can change share rounding, eligibility, fills and exits.', '',
             f'Arithmetic scaling to $10,000 historical equity drawdown is {multiplier:.4f}×; estimated net ${linear["net_low_usd"]:,.2f} to ${linear["net_high_usd"]:,.2f}, requiring peak exposure ${linear["peak_notional_usd"]:,.2f}. This exceeds the $100,000 cash-only account and is not an executable return estimate.', '',
             'The following replays raise the reserved ladder risk budget while leaving actual cash buying power at 1× equity. The original 1% rule applies only to multiplier 1. Larger multipliers change that rule. Account remains $100,000; $2 grid, above-first-P99 signal, fixed final P01 stop, higher/lower path sensitivity and original profitable trailing are unchanged. Daily loss uses Prague midnight. Limits reject outcomes afterward; they do not force liquidation when hit.', '',
             '| Budget multiplier | Planned risk budget as % equity | Net price P&L range | Worst equity DD | Worst daily loss | Max filled ladder | Peak shares | Peak gross notional |',
             '|---:|---:|---:|---:|---:|---:|---:|---:|']
    for scale in multipliers:
        pair = [r for r in results if r.get('research_budget_multiplier', 1.) == scale]
        lines.append(f'| {scale:.4g} | {.01*scale*100:g}% | ${min(r["net_pnl"] for r in pair):,.2f} to ${max(r["net_pnl"] for r in pair):,.2f} | ${max(r["max_equity_drawdown"] for r in pair):,.2f} | ${max(r["max_daily_loss"] for r in pair):,.2f} | {max(r["max_ladder"] for r in pair)} | {max(r["max_shares"] for r in pair):g} | ${max(r["max_invested_notional_usd"] for r in pair):,.2f} |')
    lines += ['', f'Highest retrospective worst-path profit within $10,000 peak drawdown, $5,000 daily loss, $90,000 static floor and buying-power limits: budget multiplier {selected}. This selection uses hindsight.', '',
              'Extremely large planned budgets are saturation experiments showing the effect of the cash cap, not proposed risk management. A permitted drawdown limit does not oblige the strategy to use that amount. The limit is not a guarantee of future maximum loss.', '',
              'Results exclude dividends, regulatory fees and interest. Trade OHLC is a modeled 1-cent spread proxy, with a 2-cent sensitivity for the selected retrospective case. Calibration limitations in the source forecasts remain. This is SPY share research, not an FTMO US500 CFD execution replay. Overnight CFD prices, lot multipliers, spread and swap costs would require separate data.', '',
              'Original source/forecast run: https://github.com/tamzid2001/stockssagemakerdata/actions/runs/37826329337. No models rerun or brokerage orders submitted.']
    (output/'report.md').write_text('\n'.join(lines)+'\n')


def main():
    parser = argparse.ArgumentParser()
    for field in ('source', 'forecasts', 'baseline', 'output'):
        parser.add_argument('--'+field, type=Path, required=True)
    args = parser.parse_args()
    study(args.source, args.forecasts, args.baseline, args.output)


if __name__ == '__main__':
    main()
