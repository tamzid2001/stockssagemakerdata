"""Validate and summarize the corrected ten-asset artifact, including medians."""
from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
from statistics import median

from .ftmo_dukas_engine import COSTS
from .ftmo_p99_study import read_json


def cash_range(values, key):
    if not values:
        return '—'
    low, high = min(r[key] for r in values), max(r[key] for r in values)
    return f'${low:,.2f}' if abs(high-low) < .005 else f'${low:,.2f} to ${high:,.2f}'


def validate(data):
    assert data['complete'] and not data['missing_assets']
    assert data['study']['trailing_rule'] == 'extreme_entry_distance'
    assert len(data['reports']) == len({r['symbol'] for r in data['reports']}) == 10
    for report in data['reports']:
        assert report['entry_rule'] == 'daily_buy' and report['averaging_gate'] == 'grid'
        assert report['trailing_rule'] == 'extreme_entry_distance'
        assert len(report['full_year_grid_comparison']) == len(report['development']) == 36
        for r in report['full_year_grid_comparison'] + report['development']:
            assert math.isclose(r['net_pnl'], r['ending_equity']-r['initial_balance'], abs_tol=1e-7)
            assert r['closed_baskets'] == r['basket_wins'] + r['basket_losses'] + r['basket_breakeven']
            assert r['trailing_rule'] == 'extreme_entry_distance'
        for r in report['selected_full_year_details'] + report['fresh_flat_holdout']:
            baskets = r['baskets']
            assert math.isclose(r['net_pnl'], sum(b['net_pnl'] for b in baskets)+r['open_net_pnl'], abs_tol=1e-6)
            assert r['closed_entries'] == sum(b['legs'] for b in baskets)
            assert math.isclose(sum(e['net_pnl'] for e in r['entries']), sum(b['net_pnl'] for b in baskets), abs_tol=1e-6)
            if baskets:
                assert median(b['duration_hours'] for b in baskets) == r['median_basket_hours']
                assert math.isclose(sum(b['legs'] for b in baskets)/len(baskets), r['average_ladder'])
                assert math.isclose(sum(b['duration_hours'] for b in baskets)/len(baskets), r['average_basket_hours'])
            for event in r['trailing_activations']:
                assert event['executable_quote'] + 1e-8 >= event['activation_threshold']
                assert event['initial_trailing_stop'] > event['extreme_entry']
            assert r['orders_sent'] == 0


def summarize(input_file, original_file, output, run_id):
    data, old = read_json(input_file), read_json(original_file)
    validate(data)
    old_by_symbol = {r['symbol']: r for r in old['reports']}
    rows = []
    lines = ['# FTMO daily-buy yearly backtest — corrected trailing', '',
             f"Updated GitHub replay: https://github.com/tamzid2001/stockssagemakerdata/actions/runs/{run_id}", '',
             'Origins October 7, 2025–October 6, 2026, execution through October 7, 2026 at 17:00 UTC. Ten separate $100,000 accounts. Daily buys when flat; downward averaging without a P90 gate; 500 historical daily session bars and seven-session forecasts from the original five models and six quantiles.', '',
             'Trailing now requires positive basket P&L and bid at least lowest entry plus trailing distance. Distance is one grid for a single entry, or 75% of the filled ladder span. The trail stays at least one price tick above the lowest fill, ratchets upward and never loosens. The original forecast final P01 stop and final P99−P01 sizing range stay fixed for each basket; 1% risk is shared across the complete ladder.', '',
             'Configurations were reselected using only the original first nine months under the corrected rule. The final quarter starts flat. Ranges represent low-first/high-first minute-price paths, not confidence intervals. Full-year net includes floating positions and modeled costs; W/L and holding-time statistics cover closed baskets.', '',
             '| Asset | Selected grid/profile | Net P&L | Max equity DD | Basket W/L | Win rate | Median hours | Average/max legs | Open legs / P&L |',
             '|---|---|---:|---:|---|---:|---:|---|---:|']
    for report in data['reports']:
        symbol = report['symbol']
        ref = [r for r in report['selected_full_year_details'] if r['costs'] == COSTS[0].name]
        hold = [r for r in report['fresh_flat_holdout'] if r['costs'] == COSTS[0].name]
        stress = [r for r in report['selected_full_year_details'] if r['costs'] == COSTS[2].name]
        if not ref:
            all_results = report['full_year_grid_comparison']
            no_trades = all(r['closed_entries'] == r['open_entries'] == 0 for r in all_results)
            lines.append(f'| {symbol} | No eligible selection | '+('$0 — no trades' if no_trades else 'No selected result')+' | $0 | 0/0 | N/A | N/A | 0/0 | 0 / $0 |')
            rows.append({'symbol': symbol, 'selected': None, 'no_trades': no_trades})
            continue
        worst = min(ref, key=lambda r: r['net_pnl'])
        win_low, win_high = min(r['basket_win_rate'] for r in ref), max(r['basket_win_rate'] for r in ref)
        rate = f'{100*win_low:.2f}%' if win_low == win_high else f'{100*win_low:.2f}%–{100*win_high:.2f}%'
        row = {'symbol': symbol, 'selected': report['selected_on_development'],
               'net_low': min(r['net_pnl'] for r in ref), 'net_high': max(r['net_pnl'] for r in ref),
               'drawdown': max(r['max_equity_drawdown'] for r in ref),
               'daily_loss': max(r['max_daily_loss'] for r in ref),
               'closed_baskets': worst['closed_baskets'], 'wins': worst['basket_wins'], 'losses': worst['basket_losses'],
               'win_rate_low': win_low, 'win_rate_high': win_high,
               'median_hours': worst['median_basket_hours'], 'mean_hours': worst['average_basket_hours'],
               'average_ladder': worst['average_ladder'], 'max_ladder': max(r['max_ladder'] for r in ref),
               'open_entries': worst['open_entries'], 'open_pnl': worst['open_net_pnl'],
               'open_age_hours': worst['open_age_hours'], 'max_win_streak': worst['max_win_streak'],
               'max_loss_streak': worst['max_loss_streak'], 'swap_pnl': worst['swap_pnl'], 'fees': worst['fees'],
               'holdout_net_low': min(r['net_pnl'] for r in hold), 'holdout_net_high': max(r['net_pnl'] for r in hold),
               'stress_net_low': min(r['net_pnl'] for r in stress), 'stress_net_high': max(r['net_pnl'] for r in stress),
               'first_daily_breach': worst['first_daily_breach'], 'first_total_breach': worst['first_total_breach'],
               'first_margin_breach': worst['first_margin_breach']}
        rows.append(row)
        lines.append(f"| {symbol} | {row['selected']} | {cash_range(ref, 'net_pnl')} | ${row['drawdown']:,.2f} | {row['wins']}/{row['losses']} | {rate} | {row['median_hours']:.2f} | {row['average_ladder']:.2f}/{row['max_ladder']} | {row['open_entries']} / ${row['open_pnl']:,.2f} |")
    lines += ['', '## Holdout, cost stress and duration', '',
              '| Asset | Mean / median closed duration, days | Holdout net | Cost-stress net | Reference swap P&L | Max W/L streak | Daily loss |',
              '|---|---:|---:|---:|---:|---|---:|']
    for r in rows:
        if not r['selected']:
            continue
        lines.append(f"| {r['symbol']} | {r['mean_hours']/24:.3f} / {r['median_hours']/24:.3f} | ${r['holdout_net_low']:,.2f} to ${r['holdout_net_high']:,.2f} | ${r['stress_net_low']:,.2f} to ${r['stress_net_high']:,.2f} | ${r['swap_pnl']:,.2f} | {r['max_win_streak']}/{r['max_loss_streak']} | ${r['daily_loss']:,.2f} |")
    lines += ['', '## Isolating the correction at the previous selected settings', '',
              'This comparison holds the old grid/profile fixed, so it separates the rule correction from the newly selected configurations above.', '',
              '| Asset | Previous configuration | Original net | Corrected net at same configuration | Corrected max ladder |',
              '|---|---|---:|---:|---:|']
    for report in data['reports']:
        previous = old_by_symbol[report['symbol']]
        selected = previous['selected_on_development']
        if not selected:
            continue
        old_ref = [r for r in previous['selected_full_year_details'] if r['costs'] == COSTS[0].name]
        new_ref = [r for r in report['full_year_grid_comparison'] if r['candidate'] == selected]
        lines.append(f"| {report['symbol']} | {selected} | {cash_range(old_ref, 'net_pnl')} | {cash_range(new_ref, 'net_pnl')} | {max(r['max_ladder'] for r in new_ref)} |")
    lines += ['', '## Validation and limits', '',
              '- Complete ten-asset source report; all 360 full-year candidate outcomes and 360 development outcomes are preserved.',
              '- Selected full-year/holdout net reconciles to closed-basket P&L plus marked open P&L. Basket legs, durations, medians and activation thresholds were independently checked.',
              '- Median holding time excludes baskets still open at year end. Their open age and marked P&L remain in selected-assets.json.',
              '- The daily $5,000 loss and $90,000 static equity floor are diagnostics, not automatic liquidation rules. All breaches remain in the source result.',
              '- Costs use the same current FTMO snapshot projected backward. Historical spreads, swap rates and 0.01 minimum lot/step are not verified; gold/BTC may be blocked by minimum sizing.',
              '- These are separate accounts. Their profits and drawdowns cannot be summed into a shared $100,000 portfolio.',
              '- This is historical research with modeled execution, not a guarantee of reaching a challenge target. No orders, provider downloads, new model runs or Firestore writes.']
    output.mkdir(parents=True, exist_ok=True)
    (output/'report.md').write_text('\n'.join(lines)+'\n')
    (output/'selected-assets.json').write_text(json.dumps(rows, indent=2)+'\n')
    fields = list(dict.fromkeys(k for r in rows for k in r))
    with (output/'selected-assets.csv').open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fields); writer.writeheader(); writer.writerows(rows)
    (output/'validation.json').write_text(json.dumps({'status': 'share with documented cost/execution caveats',
           'assets': 10, 'completed_forecasts': sum(r['completed_forecasts'] for r in data['reports']),
           'full_year_candidate_outcomes': 360, 'median_and_pnl_reconciliation': 'passed',
           'source_run': run_id, 'orders_sent': 0}, indent=2)+'\n')


def main():
    parser = argparse.ArgumentParser()
    for field in ('input', 'original', 'output'):
        parser.add_argument('--'+field, type=Path, required=True)
    parser.add_argument('--run-id', type=int, required=True)
    args = parser.parse_args()
    summarize(args.input, args.original, args.output, args.run_id)


if __name__ == '__main__':
    main()
