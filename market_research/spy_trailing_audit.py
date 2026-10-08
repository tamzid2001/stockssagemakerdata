"""Corrected SPY trailing replay from frozen source/forecasts; no network calls."""
from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import date, datetime, time, timedelta
import gzip
import json
import math
from pathlib import Path

from .ftmo_dukas_data import digest, stamp
from .ftmo_p99_engine import Candidate
from .ftmo_p99_study import read_json, write_json
from .spy_p99_share_study import COSTS, NY, run_share_replay


def validate(result):
    legs = result['entries'] + result['open_basket']
    assert len(legs) == sum(b['legs'] for b in result['baskets']) + result['open_entries']
    assert all(e['initial_shares'] >= 1 and e['initial_shares'].is_integer() for e in legs)
    realized = sum(b['net_pnl'] for b in result['baskets'])
    assert math.isclose(result['net_pnl'], realized + result['open_net_pnl'], abs_tol=1e-7)
    for leg in result['entries']:
        pnl = sum((x['price'] - leg['entry']) * x['shares'] for x in leg['exits'])
        assert math.isclose(pnl, leg['net_pnl'], abs_tol=1e-7)
    for event in result['trailing_activations']:
        assert event['executable_quote'] + 1e-8 >= event['activation_threshold']
        assert event['initial_trailing_stop'] >= event['extreme_entry'] + .01 - 1e-8
        assert event['initial_trailing_stop'] <= event['executable_quote'] - .01 + 1e-8
    groups = {}
    for leg in legs:
        groups.setdefault(leg['exits'][0]['at'] if leg['exits'] else 'open', []).append(leg)
    for basket in result['baskets']:
        basket_legs = groups[basket['at']]
        assert len(basket_legs) == basket['legs']
        assert all(b['entry'] < a['entry'] for a, b in zip(basket_legs, basket_legs[1:]))
    assert result['orders_sent'] == result['brokerage_orders_sent'] == 0


def study(source, forecasts, baseline, output):
    plan, identity, old = read_json(source/'study-plan.json'), read_json(source/'source.json'), read_json(baseline)
    if plan != old['study'] or identity != old['source']:
        raise ValueError('BASELINE_SOURCE_MISMATCH')
    records = {}
    for path in forecasts.rglob('????-??-??.json.gz'):
        r = read_json(path)
        if (r.get('status') != 'completed' or r.get('symbol') != 'SPY'
                or r.get('source_rows_sha256') != identity['rows_sha256'] or r['day'] in records
                or r.get('history_rows') != 500 or len(r['predictions']) != 7):
            raise ValueError('FORECAST_INTEGRITY_MISMATCH')
        records[r['day']] = r
    if set(records) != set(plan['days']):
        raise ValueError('ALL_FROZEN_FORECASTS_REQUIRED')
    ordered = [records[d] for d in plan['days']]
    with gzip.open(source/'minutes.jsonl.gz', 'rt') as stream:
        rows = [json.loads(line) for line in stream]
    if digest(rows) != identity['minutes_sha256']:
        raise ValueError('MINUTE_SOURCE_HASH_MISMATCH')
    for row in rows:
        row['_at'], row['_end'] = stamp(row['start']), stamp(row['end'])
    first = datetime.combine(date.fromisoformat(plan['start']), time.min, NY)
    last = datetime.combine(date.fromisoformat(plan['end']) + timedelta(days=2), time.min, NY)
    originals, results = [], []
    # First reproduce both audited $2 results field-for-field with the explicitly
    # named legacy rule. This keeps the rule correction separate from data drift.
    for ordering in ('low_first', 'high_first'):
        original = run_share_replay(rows, ordered, first, last, Candidate(2.), ordering=ordering,
                                    trailing_rule='legacy_basket_profit')
        expected = next(r for r in old['full_year_grid_comparison']
                        if r['candidate'] == 'grid-2-equal' and r['ordering'] == ordering)
        assert original == expected, 'LEGACY_REPLAY_CHANGED'
        originals.append(original)
    for grid in (1., 2.):
        for cost in COSTS:
            for ordering in ('low_first', 'high_first'):
                r = run_share_replay(rows, ordered, first, last, Candidate(grid), cost, ordering, True)
                validate(r)
                results.append(r)
                print(json.dumps({'event': 'spy_corrected_trailing_replay', 'grid': grid,
                                  'ordering': ordering, 'costs': cost.name,
                                  'net_pnl': r['net_pnl'], 'drawdown': r['max_equity_drawdown'],
                                  'closed_baskets': r['closed_baskets'], 'max_ladder': r['max_ladder']}), flush=True)
    output.mkdir(parents=True, exist_ok=True)
    write_json(output/'results.json.gz', {'complete': True, 'study': plan, 'source': identity,
               'trailing_rule': 'extreme_entry_distance', 'legacy_baseline_identical': True,
               'forecast_hashes': {d: digest(records[d]) for d in plan['days']},
               'completed_forecasts': len(ordered), 'legacy_grid2_results': originals,
               'corrected_replays': results, 'orders_sent': 0, 'firestore_writes': 0})
    fields = ('grid', 'ordering', 'costs', 'net_pnl', 'return_pct', 'closed_baskets', 'basket_wins',
              'basket_losses', 'basket_win_rate', 'max_equity_drawdown', 'max_daily_loss',
              'average_ladder', 'max_ladder', 'closed_entries', 'average_basket_hours',
              'median_basket_hours',
              'max_win_streak', 'max_loss_streak', 'max_shares', 'open_entries', 'open_net_pnl')
    with (output/'summary.csv').open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        writer.writeheader(); writer.writerows(results)
    basket_fields = ('grid', 'ordering', 'costs', 'started_at', 'at', 'reason', 'legs', 'net_pnl', 'duration_hours')
    with (output/'baskets.csv').open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=basket_fields, extrasaction='ignore'); writer.writeheader()
        for r in results:
            writer.writerows({**b, 'grid': r['grid'], 'ordering': r['ordering'], 'costs': r['costs']} for b in r['baskets'])
    lines = ['# SPY corrected trailing audit', '',
             'Execution: October 7, 2025–October 7, 2026; 251 frozen daily forecasts through October 6, 2026. Starting equity $100,000; whole shares; 1% equity risk shared across each complete ladder.', '',
             'The old audit correctly described the old code, but that code activated trailing too early for the intended rule. Both original $2 reference results were reproduced exactly before applying the correction.', '',
             'Corrected activation: basket P&L must be positive AND executable bid must reach the lowest filled entry plus trailing distance. One entry uses the grid; multiple entries use 75% of highest-minus-lowest fill. The trail is favorable bid peak minus the distance, floored one cent above the lowest fill, and never loosens. The fixed triggering final P01 protects the basket before activation. No change to the above-first-P99 entry signal, risk formula, lower-only additions or next-minute limit eligibility.', '',
             '| Grid | Spread proxy | Minute path | Net price P&L | W/L | Win rate | Equity DD | Daily loss | Avg/max legs | Mean/median hours | Max W/L streak | Open legs |',
             '|---:|---:|---|---:|---|---:|---:|---:|---|---|---|---:|']
    for r in results:
        lines.append(f"| ${r['grid']:g} | {int(round(COSTS[0].spread_factor if r['costs']==COSTS[0].name else COSTS[1].spread_factor))}¢ | {r['ordering']} | ${r['net_pnl']:,.2f} | {r['basket_wins']}/{r['basket_losses']} | {100*r['basket_win_rate']:.2f}% | ${r['max_equity_drawdown']:,.2f} | ${r['max_daily_loss']:,.2f} | {r['average_ladder']:.2f}/{r['max_ladder']} | {r['average_basket_hours']:.2f}/{r['median_basket_hours']:.2f} | {r['max_win_streak']}/{r['max_loss_streak']} | {r['open_entries']} |")
    lines += ['', '## Averaging verification', '']
    for r in results[:]:
        if r['costs'] != COSTS[0].name:
            continue
        averaged = sum(b['legs'] > 1 for b in r['baskets'])
        adds = sum(max(0, b['legs'] - 1) for b in r['baskets']) + max(0, r['open_entries'] - 1)
        lines.append(f"- ${r['grid']:g}, {r['ordering']}: {averaged} closed baskets averaged; {adds} additional fills; exit reasons {dict(Counter(b['reason'] for b in r['baskets']))}.")
    lines += ['', 'All eight corrected path/cost replays passed independent price-P&L, closed/open reconciliation, whole-share, downward-only entry and activation-threshold/stop-floor checks. The original two baselines matched exactly. Models and data were not fetched or rerun; no orders or Firestore writes.', '',
              'These are separate alternative accounts and minute-path assumptions; do not add their returns. Trade OHLC is a modeled spread proxy, not historical NBBO or verified limit queue execution. Dividends, regulatory fees and interest are excluded. This is SPY share research. All positions remaining at year end are marked rather than forced closed.', '',
              'The earlier $51.57–$52.22 return and $45.14 drawdown describe the superseded early-activation rule. Its sizing sensitivities must also be recalculated before using them with this corrected strategy.']
    (output/'report.md').write_text('\n'.join(lines) + '\n')
    (output/'validation.json').write_text(json.dumps({'validated_corrected_replays': len(results),
           'legacy_baselines_exact': len(originals), 'frozen_forecasts': len(ordered),
           'source_hash_verified': True, 'orders_sent': 0}, indent=2) + '\n')


def main():
    parser = argparse.ArgumentParser()
    for field in ('source', 'forecasts', 'baseline', 'output'):
        parser.add_argument('--'+field, type=Path, required=True)
    args = parser.parse_args()
    study(args.source, args.forecasts, args.baseline, args.output)


if __name__ == '__main__':
    main()
