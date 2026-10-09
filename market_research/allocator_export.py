"""Export audited replays as private allocator research, never broker instructions.

python -m market_research.allocator_export build --spy FILE --ftmo FILE --output DIR
python -m market_research.allocator_export weights --input positions.json --output weights.csv
python -m market_research.allocator_export stamp --file manifest.json --collection CID

The optional stamp/verify commands use vbase-api. Exporting uses only stdlib.
"""
from __future__ import annotations

import argparse
import csv
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal
import gzip
import hashlib
import io
import json
import math
from pathlib import Path
import re
import secrets
import shutil

IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,79}$")
CID = re.compile(r"^0x[0-9a-fA-F]{64}$")
METRICS = (
    'symbol', 'candidate', 'grid', 'sizing', 'ordering', 'costs', 'initial_balance',
    'ending_equity', 'net_pnl', 'return_pct', 'closed_baskets', 'basket_wins',
    'basket_losses', 'basket_breakeven', 'basket_win_rate', 'max_equity_drawdown',
    'max_daily_loss', 'average_ladder', 'max_ladder', 'average_basket_hours',
    'median_basket_hours', 'max_win_streak', 'max_loss_streak', 'fees', 'swap_pnl',
    'open_entries', 'open_net_pnl', 'open_age_hours', 'risk_fraction',
    'first_daily_breach', 'first_total_breach', 'first_margin_breach',
)


def append_summary(root, metrics):
    lines = ['\n## Archived performance\n',
             'Dollar P&L and drawdown are per standalone account. Rows are alternatives, never additive.\n',
             '| Asset | Grid | Path | Cost model | Net P&L | Return | Max equity DD | W/L | Median hours | Open legs |',
             '|---|---:|---|---|---:|---:|---:|---|---:|---:|']
    for r in metrics:
        duration = f"{r['median_basket_hours']:.2f}" if r['median_basket_hours'] is not None else 'N/A'
        lines.append(f"| {r['symbol']} | {r['grid']:g} | {r['ordering']} | {r['costs']} | "
                     f"${r['net_pnl']:,.2f} | {r['return_pct']:.4f}% | ${r['max_equity_drawdown']:,.2f} | "
                     f"{r['basket_wins']}/{r['basket_losses']} | {duration} | {r['open_entries']} |")
    with (root / 'README.md').open('a') as stream:
        stream.write('\n'.join(lines) + '\n')


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                      allow_nan=False).encode('utf-8')


def content_cid(data):
    return '0x' + hashlib.sha3_256(data).hexdigest()


def finite(value):
    if isinstance(value, bool):
        raise ValueError('FINITE_NUMBER_REQUIRED')
    number = Decimal(str(value))
    if not number.is_finite():
        raise ValueError('FINITE_NUMBER_REQUIRED')
    return number


def identifier(value):
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value) or value.upper() in ('CASH', 'USD_CASH'):
        raise ValueError('SECURITY_IDENTIFIER_REQUIRED')
    return value


def portfolio_csv(positions, equity_usd):
    """Standard vBase Ticker,Weight CSV. Units × quote price × USD conversion / NAV.

    Signed quantities express shorts. Cash is implicit; no weights normalization.
    CFD callers must supply contract units, not lots, and actual USD conversion.
    """
    equity = finite(equity_usd)
    if equity <= 0 or not isinstance(positions, list) or not positions:
        raise ValueError('POSITIVE_EQUITY_AND_SECURITY_UNIVERSE_REQUIRED')
    weights = {}
    for p in positions:
        symbol = identifier(p['identifier'])
        quantity, price, conversion = finite(p['quantity']), finite(p['price']), finite(p['usd_per_quote'])
        if price <= 0 or conversion <= 0:
            raise ValueError('POSITIVE_MARK_AND_CONVERSION_REQUIRED')
        weights[symbol] = weights.get(symbol, Decimal(0)) + quantity * price * conversion / equity
    buffer = io.StringIO(newline='')
    writer = csv.writer(buffer, lineterminator='\n')
    writer.writerow(['Ticker', 'Weight'])
    for symbol, weight in sorted(weights.items()):
        writer.writerow([symbol, format(weight, '.12f')])
    return buffer.getvalue().encode('utf-8')


def write_json(path, value):
    path.write_bytes(canonical(value))


def write_csv(path, rows, fields):
    with path.open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator='\n')
        writer.writeheader()
        writer.writerows({k: row.get(k) for k in fields} for row in rows)


def read_result(path):
    data = path.read_bytes()
    return json.loads(gzip.decompress(data) if path.suffix == '.gz' else data)


def validate_replay(r, unit):
    """Reject incomplete ledgers instead of synthesizing trades from KPIs."""
    if r.get('orders_sent') != 0 or r.get('trailing_rule') != 'extreme_entry_distance':
        raise ValueError('CORRECTED_OFFLINE_REPLAY_REQUIRED')
    for key in ('entries', 'baskets', 'open_basket', 'hourly_equity'):
        if not isinstance(r.get(key), list):
            raise ValueError('COMPLETE_TRADE_AND_EQUITY_LEDGER_REQUIRED')
    if not math.isclose(r['ending_equity'] - r['initial_balance'], r['net_pnl'], abs_tol=1e-6):
        raise ValueError('EQUITY_RECONCILIATION_FAILED')
    if not math.isclose(sum(b['net_pnl'] for b in r['baskets']) + r['open_net_pnl'], r['net_pnl'], abs_tol=1e-6):
        raise ValueError('BASKET_RECONCILIATION_FAILED')
    if len(r['entries']) != r['closed_entries'] or len(r['open_basket']) != r['open_entries']:
        raise ValueError('ENTRY_COUNT_MISMATCH')
    if r['closed_baskets'] != r['basket_wins'] + r['basket_losses'] + r['basket_breakeven']:
        raise ValueError('BASKET_COUNT_MISMATCH')
    for leg in r['entries'] + r['open_basket']:
        quantity = leg['initial_' + unit]
        if quantity <= 0 or not math.isfinite(quantity) or leg['entry'] <= 0:
            raise ValueError('INVALID_FILL')
        remaining = quantity - sum(x[unit] for x in leg['exits'])
        if not math.isclose(remaining, leg[unit], abs_tol=1e-7):
            raise ValueError('POSITION_RECONCILIATION_FAILED')
        if any(x[unit] <= 0 or x['at'] < leg['at'] for x in leg['exits']):
            raise ValueError('INVALID_EXIT')


def fill_ledger(r, unit):
    rows = []
    for i, leg in enumerate(r['entries'] + r['open_basket']):
        key = f'leg-{i + 1:05d}'
        rows.append({'at': leg['at'], 'leg_id': key, 'symbol': r['symbol'], 'side': 'buy',
                     'quantity': leg['initial_' + unit], 'unit': unit, 'price': leg['entry'],
                     'reason': 'initial_or_lower_grid_fill'})
        for x in leg['exits']:
            rows.append({'at': x['at'], 'leg_id': key, 'symbol': r['symbol'], 'side': 'sell',
                         'quantity': x[unit], 'unit': unit, 'price': x['price'], 'reason': x['reason']})
    return sorted(rows, key=lambda x: (x['at'], x['side'] != 'sell', x['leg_id']))


def write_replay(root, r, unit):
    validate_replay(r, unit)
    root.mkdir(parents=True)
    write_json(root / 'replay.json', r)
    fills = fill_ledger(r, unit)
    write_csv(root / 'fills.csv', fills, ['at', 'leg_id', 'symbol', 'side', 'quantity', 'unit', 'price', 'reason'])
    write_csv(root / 'baskets.csv', r['baskets'], ['started_at', 'at', 'reason', 'legs', 'net_pnl',
              'fixed_final_p01', 'fixed_final_quantile_span', 'equity_at_entry', 'risk_budget_usd', 'duration_hours'])
    write_csv(root / 'equity.csv', [{**p, 'realized_balance': p['cash']} for p in r['hourly_equity']],
              ['at', 'equity', 'realized_balance', unit])
    positions, quantity = [], Decimal(0)
    for at in sorted({x['at'] for x in fills}):
        group = [x for x in fills if x['at'] == at]
        quantity += sum((finite(x['quantity']) * (1 if x['side'] == 'buy' else -1) for x in group), Decimal(0))
        if quantity < -Decimal('0.0000001'):
            raise ValueError('NEGATIVE_LONG_POSITION')
        positions.append({'at': at, 'symbol': r['symbol'], 'quantity': float(quantity), 'unit': unit,
                          'evidence_kind': 'historical_simulated_fill'})
    if not math.isclose(float(quantity), sum(x[unit] for x in r['open_basket']), abs_tol=1e-7):
        raise ValueError('FINAL_POSITION_MISMATCH')
    write_csv(root / 'positions.csv', positions, ['at', 'symbol', 'quantity', 'unit', 'evidence_kind'])
    if unit == 'shares':
        # Bid-market-value / NAV from archived hourly marks. These are historical
        # snapshots, not prospective target weights or broker instructions.
        weights = root / 'historical-weights'
        weights.mkdir()
        index, skipped = [], []
        held, event_index = {}, 0
        by_leg = {x['leg_id']: x['price'] for x in fills if x['side'] == 'buy'}
        fill_times = {x['at'] for x in fills}
        for i, point in enumerate(r['hourly_equity']):
            # A bar-end mark and next bar's opening fill can share the same
            # timestamp. The archive has no sequence ID; do not guess its state.
            if point['at'] in fill_times:
                skipped.append({'at': point['at'], 'reason': 'mark_and_fill_timestamp_ambiguous'})
                continue
            while event_index < len(fills) and fills[event_index]['at'] < point['at']:
                event = fills[event_index]
                sign = 1 if event['side'] == 'buy' else -1
                held[event['leg_id']] = held.get(event['leg_id'], 0.) + sign * event['quantity']
                event_index += 1
            quantity = point['shares']
            if not math.isclose(sum(held.values()), quantity, abs_tol=1e-7):
                raise ValueError('MARK_POSITION_RECONCILIATION_FAILED')
            # The engine's "cash" is realized account balance, not settlement
            # cash after buying shares. Add remaining cost basis to unrealized
            # P&L to recover bid market value; subtract it to recover real cash.
            basis = sum(q * by_leg[key] for key, q in held.items())
            invested = point['equity'] - point['cash'] + basis
            if point['equity'] <= 0 or (quantity == 0 and abs(invested) > 1e-7):
                raise ValueError('INVALID_SHARE_MARK')
            price = invested / quantity if quantity else 1
            data = portfolio_csv([{'identifier': r['symbol'], 'quantity': quantity,
                                  'price': price, 'usd_per_quote': 1}], point['equity'])
            filename = f'{i:05d}.csv'
            (weights / filename).write_bytes(data)
            index.append({'observed_at': point['at'], 'file': 'historical-weights/' + filename,
                          'content_cid': content_cid(data), 'equity_usd': point['equity'],
                          'shares': quantity, 'bid_notional_usd': invested,
                          'settlement_cash_usd': point['cash'] - basis, 'stamp_timestamp': None,
                          'evidence_kind': 'historical_simulation'})
        write_json(root / 'weights-index.json', index)
        write_json(root / 'weights-skipped-marks.json', skipped)
    return {k: r.get(k) for k in METRICS}


def seal_bundle(root, family, source):
    files = []
    for path in sorted(root.rglob('*')):
        if path.is_file():
            data = path.read_bytes()
            files.append({'path': path.relative_to(root).as_posix(), 'bytes': len(data),
                          'sha256': hashlib.sha256(data).hexdigest()})
    manifest = {'schema_version': 'quantura_allocator_research_v1', 'family': family,
                'evidence_kind': 'historical_backtest', 'generated_at': datetime.now(timezone.utc).isoformat(),
                'nonce': secrets.token_hex(32), 'live_eligible': False, 'orders_sent': 0,
                'source_sha256': hashlib.sha256(source.read_bytes()).hexdigest(), 'files': files,
                'timestamp_meaning': 'A receipt proves bundle existence at stamp time, not during the replay year.'}
    write_json(root / 'manifest.json', manifest)
    (root / 'manifest.cid').write_text(content_cid(canonical(manifest)) + '\n')


def spy_bundle(source, root):
    d = read_result(source)
    if not d.get('complete') or len(d.get('corrected_replays', [])) != 8 or d.get('orders_sent') != 0:
        raise ValueError('COMPLETE_EIGHT_VARIANT_SPY_AUDIT_REQUIRED')
    root.mkdir(parents=True)
    shutil.copyfile(source, root / ('source-results' + ''.join(source.suffixes)))
    write_json(root / 'study.json', d['study'])
    write_json(root / 'source-and-forecast-hashes.json', {'source': d['source'], 'forecast_hashes': d['forecast_hashes']})
    metrics = []
    for r in d['corrected_replays']:
        name = f"grid-{r['grid']:g}-{r['ordering']}-{r['costs']}"
        metrics.append(write_replay(root / 'variants' / name, r, 'shares'))
    write_csv(root / 'summary.csv', metrics, METRICS)
    (root / 'README.md').write_text('''# SPY share research portfolio

Historical simulation; no live performance track record or orders.

One $100,000 account per variant. A daily five-model, seven-session forecast uses 500 completed daily bars and P01/P10/P25/P50/P75/P90/P99. Start a buy basket only when the cutoff close exceeds the first predicted P99. Average downward at the selected $1 or $2 grid, without a P90 condition. Share one 1% equity risk budget across the full ladder, using the fixed final P99−P01 range, stop distance and buying power; round down to whole shares. The triggering forecast's final P01 remains fixed.

A trail activates only when the basket is profitable and bid rises at least one grid above the lowest fill for one leg, or 75% of the filled ladder span for multiple legs. It ratchets upward, stays at least $0.01 above the lowest fill, and never loosens. Positions can carry overnight. Entry timing and exact sizing implementation are preserved in study.json and the source repository.

All eight corrected grid/spread/path variants are included. No retrospectively selected winner is represented as a preselected strategy. Hourly `historical-weights/*.csv` uses the vBase Ticker,Weight format: bid position value / account equity, with implicit cash. The index preserves each historical observation time. These files are NOT prospectively stamped. The source's intraday execution differs from vBase's next-close weight index calculation; their returns must not be presented as the same strategy.

`fills.csv` contains simulated fills, not unfilled signal instructions. `positions.csv` reconstructs quantities at fills; `equity.csv` retains hourly marks, while reported maximum drawdown comes from the original finer replay. `replay.json` retains all open positions, trailing activations and detailed source metrics. Backtest source and forecast hashes are included; raw provider bars and licensed model weights are not redistributed.

Execution uses Alpaca SIP minute OHLC and assumed 1¢/2¢ spreads, not verified historical NBBO or queue fills. Both possible OHLC paths are kept. Dividends, regulatory fees and interest are excluded. Model tails retain the original model support. Annual outcomes are simulated research, not verified investment returns.

The bundle manifest commits every file. A vBase receipt added now confirms this bundle existed now; it does not backdate forecasts or certify profitability. A prospective portfolio needs a frozen rule, contemporaneous weights at each update, actual execution/cost records and separate live/paper collections.
''')
    append_summary(root, metrics)
    seal_bundle(root, 'SPY-shares', source)


def ftmo_bundle(source, root):
    d = read_result(source)
    if not d.get('complete') or d.get('missing_assets') or len(d.get('reports', [])) != 10:
        raise ValueError('COMPLETE_TEN_ASSET_FTMO_AUDIT_REQUIRED')
    if d['study'].get('trailing_rule') != 'extreme_entry_distance' or d['study'].get('entry_rule') != 'daily_buy':
        raise ValueError('CORRECTED_DAILY_BUY_FTMO_AUDIT_REQUIRED')
    root.mkdir(parents=True)
    shutil.copyfile(source, root / ('source-results' + ''.join(source.suffixes)))
    write_json(root / 'study.json', d['study'])
    write_json(root / 'cost-snapshot.json', d['current_cost_snapshot'])
    metrics, unavailable = [], []
    for report in d['reports']:
        symbol = identifier(report['symbol'])
        if not report.get('selection_valid'):
            if report['selected_full_year_details']:
                raise ValueError('INVALID_SELECTION_HAS_TRADES')
            unavailable.append({'symbol': symbol, 'status': 'no_eligible_configuration', 'performance': 'no_trades'})
            continue
        for r in report['selected_full_year_details']:
            if r['candidate'] != report['selected_on_development']:
                raise ValueError('DEVELOPMENT_SELECTION_MISMATCH')
            name = r['ordering'] + '-' + r['costs']
            metrics.append(write_replay(root / 'sleeves' / symbol / 'full-year' / name, r, 'lots'))
        for r in report['fresh_flat_holdout']:
            if r['candidate'] != report['selected_on_development']:
                raise ValueError('HOLDOUT_SELECTION_MISMATCH')
            write_replay(root / 'sleeves' / symbol / 'holdout' / (r['ordering'] + '-' + r['costs']), r, 'lots')
        # Preserve all development candidates to expose the selection process.
        write_json(root / 'sleeves' / symbol / 'development-candidates.json', report['development'])
    write_csv(root / 'summary.csv', metrics, METRICS)
    write_json(root / 'inactive-assets.json', unavailable)
    (root / 'README.md').write_text('''# FTMO research portfolio sleeves

Historical simulated CFD/FX lot signals, with eight active standalone sleeves. Each asset was replayed in a SEPARATE $100,000 account; this is not a shared $100,000 portfolio. No consolidated return, drawdown, or FTMO challenge-passing claim is made. Gold and BTC have no eligible selected configuration and remain explicitly listed.

Daily buys when flat follow five-model, seven-session forecasts based on 500 completed bars. Origins use 18:00 UTC after the 17:00 UTC session cutoff. There is no P99 initial gate and no P90 averaging gate. Average only downward by the selected asset grid. Carry until the fixed triggering forecast final P01 or a profitable trailing stop. One 1% entry-equity risk budget covers the entire ladder, using final P99−P01 and modeled price/cost risk. Quantities round down to assumed 0.01 lots, with margin and volume checks.

Trailing activation requires positive basket P&L and price at least one grid above the lowest fill for one leg, or 75% of the filled ladder span for multiple legs. The stop stays at least one price tick above the lowest fill and never loosens. The exact study, lot profiles, source ledgers and cost specifications are preserved.

Candidate grid/profile selection uses the first nine months under the corrected rule. `development-candidates.json` keeps every candidate; the final-quarter `holdout/` replays start flat. Each full-year sleeve includes both minute OHLC paths and reference/stress costs. No sleeves are removed because their selected full-year return is negative.

`fills.csv` and `positions.csv` expose executable units (lots) and hypothetical fill times/prices, not newly issued trading instructions. `equity.csv` preserves account marks including floating positions. Each replay retains open baskets, costs, swaps, drawdown diagnostics and durations. Reported maximum drawdown uses the original finer replay, not just exported hourly marks.

No fabricated portfolio weights: a CFD lot is not a percentage weight. To derive future USD exposure weights, use signed lots × actual contract size × contemporaneous quote × contemporaneous USD-per-quote conversion / account equity. This archive does not contain all historical conversion marks required for a reliable vBase weight history. vBase identifier mapping and next-close index methodology also need agreement before a CFD tearsheet is activated.

Costs project the current FTMO snapshot backward; historical spreads/swaps and minimum lot constraints are not verified. Static $90,000 equity-floor and $5,000 daily-loss diagnostics remain in every result; they are not automatic liquidation rules. This is research, not broker-verified live performance. A bundle stamp now proves existence now only. Use separate prospectively stamped collections for future signals, portfolio weights and actual fills.
''')
    append_summary(root, metrics)
    seal_bundle(root, 'FTMO-independent-sleeves', source)


def validate_bundle(manifest_path):
    root = manifest_path.parent.resolve()
    manifest = json.loads(manifest_path.read_bytes())
    expected = set()
    for row in manifest['files']:
        relative = Path(row['path'])
        original = root / relative
        path = original.resolve()
        if (relative.is_absolute() or '..' in relative.parts or not path.is_relative_to(root)
                or path == root or any(p.is_symlink() for p in [original, *original.parents] if p != root)):
            raise ValueError('INVALID_BUNDLE_PATH')
        data = path.read_bytes()
        if len(data) != row['bytes'] or hashlib.sha256(data).hexdigest() != row['sha256']:
            raise ValueError('BUNDLE_FILE_HASH_MISMATCH')
        if row['path'] in expected:
            raise ValueError('DUPLICATE_BUNDLE_PATH')
        expected.add(row['path'])
    actual = {p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file()}
    if actual - {'manifest.json', 'manifest.cid', 'vbase-receipt.json', 'vbase-verification.json'} != expected:
        raise ValueError('BUNDLE_INVENTORY_MISMATCH')
    return manifest


def stamp_file(path, collection, verify=False):
    """Commit bytes only; never upload holdings or private key, never backdate."""
    import os
    from vbase_api import VBaseAPIClient
    if not CID.fullmatch(collection):
        raise ValueError('VALID_COLLECTION_CID_REQUIRED')
    if path.name == 'manifest.json':
        validate_bundle(path)
    cid = content_cid(path.read_bytes())
    client = VBaseAPIClient(api_key=os.environ['VBASE_API_KEY'])
    try:
        matches = [r for r in client.verify_stamps(cids=[cid], filter_by_user=True).stamp_list
                   if r.object_cid.lower() == cid and r.set_cid.lower() == collection.lower()]
        if verify and not matches:
            raise ValueError('MATCHING_STAMP_NOT_FOUND')
        receipt = matches[0] if matches else client.create_stamp(
            data_cid=cid, collection_cid=collection, store_stamped_file=False,
            idempotent=True, idempotency_window=3600).commitment_receipt
        normalized = asdict(receipt)
        if (normalized['object_cid'].lower() != cid or normalized['set_cid'].lower() != collection.lower()
                or not CID.fullmatch(normalized['transaction_hash']) or not normalized['timestamp']):
            raise ValueError('INVALID_STAMP_RECEIPT')
        address = normalized['user_address']
        if not re.fullmatch(r'0x[0-9a-fA-F]{40}', address):
            if not address or len(address) > 200:
                raise ValueError('INVALID_STAMP_IDENTITY')
            normalized['user_address'], normalized['user_label'] = None, address
        result = {'content_cid': cid, 'collection_cid': collection,
                  'receipt': normalized, 'stamp_found': True,
                  'timestamp_meaning': 'Actual stamp time, never the historical research observation time.'}
        suffix = 'vbase-verification.json' if verify else 'vbase-receipt.json'
        sidecar = path.parent / (suffix if path.name == 'manifest.json' else path.name + '.' + suffix)
        write_json(sidecar, result)
        return result
    finally:
        client.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    build = sub.add_parser('build')
    build.add_argument('--spy', required=True, type=Path)
    build.add_argument('--ftmo', required=True, type=Path)
    build.add_argument('--output', required=True, type=Path)
    weights = sub.add_parser('weights')
    weights.add_argument('--input', required=True, type=Path)
    weights.add_argument('--output', required=True, type=Path)
    for command in ('stamp', 'verify'):
        action = sub.add_parser(command)
        action.add_argument('--file', required=True, type=Path)
        action.add_argument('--collection', required=True)
    args = parser.parse_args()
    if args.command == 'build':
        if args.output.exists():
            raise ValueError('NEW_OUTPUT_DIRECTORY_REQUIRED')
        args.output.mkdir(parents=True, mode=0o700)
        spy_bundle(args.spy, args.output / 'SPY')
        ftmo_bundle(args.ftmo, args.output / 'FTMO')
        for path in args.output.rglob('manifest.json'):
            validate_bundle(path)
        print(json.dumps({'output': str(args.output), 'bundles': ['SPY', 'FTMO'], 'orders_sent': 0}))
    elif args.command == 'weights':
        data = json.loads(args.input.read_bytes())
        if args.output.exists():
            raise ValueError('NEW_OUTPUT_FILE_REQUIRED')
        args.output.write_bytes(portfolio_csv(data['positions'], data['equity_usd']))
        print(content_cid(args.output.read_bytes()))
    else:
        result = stamp_file(args.file, args.collection, verify=args.command == 'verify')
        print(json.dumps({'content_cid': result['content_cid'], 'stamp_found': result['stamp_found']}))


if __name__ == '__main__':
    main()
