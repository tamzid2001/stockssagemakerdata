"""Daily seven-session/500-bar FTMO P99 ladder study, entirely artifact-backed."""
from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import date, datetime, timedelta
import gzip
import json
import math
from pathlib import Path
import re
import time

from .ftmo_dukas_data import INSTRUMENTS, UTC, digest, load_quotes, stamp
from .dukascopy_s3_source import download as download_authenticated
from .ftmo_dukas_engine import COSTS
from .ftmo_dukas_study import MODELS, snapshot_specs
from .ftmo_p99_engine import Candidate, KnownConversion, candidates, replay

QUANTILES = (.01, .25, .50, .75, .90, .99)


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == '.gz':
        with gzip.open(path, 'wt') as f:
            json.dump(value, f, separators=(',', ':'), allow_nan=False)
    else:
        path.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')


def read_json(path):
    if path.suffix == '.gz':
        with gzip.open(path, 'rt') as f:
            return json.load(f)
    return json.loads(path.read_text())


def daily_sessions(rows, symbol):
    """Observed 18:00–17:00 UTC sessions; no fabricated missing days/candles."""
    groups = {}
    for row in rows:
        at = stamp(row['start'])
        if at.hour == 17:
            continue
        day = at.date() if at.hour < 17 else at.date() + timedelta(days=1)
        if symbol != 'BTCUSD.sim' and day.weekday() >= 5:
            continue
        cutoff = datetime.combine(day, datetime.min.time(), UTC) + timedelta(hours=17)
        if stamp(row['end']) > cutoff:
            raise ValueError('INCOMPLETE_DAILY_SOURCE_BAR')
        mid = (row['bid']['c'] + row['ask']['c']) / 2
        key = cutoff.isoformat()
        if key not in groups:
            groups[key] = {'timestamp': key, 'target': mid, 'observed_hours': 0,
                           'observation_start': row['start']}
        groups[key].update(target=mid, observation_end=row['end'])
        groups[key]['observed_hours'] += 1
    return [groups[k] for k in sorted(groups)]


def future_sessions(cutoff, symbol):
    result = []
    at = cutoff
    while len(result) < 7:
        at += timedelta(days=1)
        if symbol == 'BTCUSD.sim' or at.weekday() < 5:
            result.append(at)
    return result


def plan(start, end, symbols, chunk_size=21):
    last = datetime.combine(end + timedelta(days=1), datetime.min.time(), UTC) + timedelta(hours=17)
    if start > end or (end - start).days >= 365 or last >= datetime.now(UTC):
        raise ValueError('COMPLETED_MAXIMUM_365_DAY_RANGE_REQUIRED')
    if not symbols or len(set(symbols)) != len(symbols) or any(s not in INSTRUMENTS for s in symbols):
        raise ValueError('APPROVED_DISTINCT_SYMBOLS_REQUIRED')
    days = [(start + timedelta(days=i)).isoformat() for i in range((end-start).days+1)]
    return {'schema_version': 'ftmo-p99-daily-v1', 'start': start.isoformat(), 'end': end.isoformat(),
            'symbols': symbols, 'days': days, 'initial_balance': 100000, 'risk_fraction': .01,
            'history_rows': 500, 'horizon_sessions': 7, 'quantiles': QUANTILES, 'models': MODELS,
            'origin_hour_utc': 18, 'completed_session_cutoff_hour_utc': 17,
            'first_predicted_p99_entry_signal': True,
            'stop': 'Final P01 of the triggering forecast, fixed for the basket',
            'risk_span': 'Final P99 minus final P01 of the triggering forecast',
            'averaging_gate': 'Latest daily first P90 gates only the first averaging buy; subsequent additions use the grid without P90',
            'risk_budget_scope': 'One 100k standalone account per asset; 1% shared by all basket legs, not 1% per leg',
            'carry': 'Until fixed P01 or profitable trailing stop; open baskets remain marked at study end',
            'sources': [{'symbol': s} for s in dict.fromkeys([*symbols, 'USDJPY.sim', 'USDCAD.sim'])],
            'forecasts': [{'symbol': s, 'chunk': i//chunk_size, 'start': days[i], 'end': days[min(i+chunk_size, len(days))-1]}
                          for s in symbols for i in range(0, len(days), chunk_size)],
            'replays': [{'symbol': s} for s in symbols],
            'candidates': {s: [asdict(c) for c in candidates(s)] for s in symbols},
            'development_end_exclusive': min(end+timedelta(days=1), start+timedelta(days=274)).isoformat(),
            'selection': 'Highest worst-path development net profit with no FTMO limit/margin breaches; fresh-flat final-quarter validation',
            'broker_cost_note': 'Current public specifications projected backwards, not verified historical swaps/spreads/volumes',
            'calendar_note': 'Weekday CFD/FX session labels, seven-day BTC labels; actual source bars decide origin availability. Broker holidays are not asserted.',
            'orders_sent': 0, 'firestore_writes': 0}


def source(symbol, start, end, output):
    # At least 500 real daily sessions for even sparse weekday instruments.
    download_authenticated(symbol, start, end, output, warmup_days=1100)
    hourly = load_quotes(output)
    sessions = daily_sessions(hourly, symbol)
    cutoff = datetime.combine(start, datetime.min.time(), UTC) + timedelta(hours=17)
    if len([r for r in sessions if stamp(r['timestamp']) <= cutoff]) < 500:
        raise ValueError('INSUFFICIENT_500_DAILY_SESSION_WARMUP')
    minute_identity = read_json(output/'minutes-source.json')
    write_json(output/'daily-sessions.json.gz', sessions)
    print(json.dumps({'event': 'p99_source_complete', 'symbol': symbol, 'daily_sessions': len(sessions),
                      'paired_minutes': minute_identity['paired_minutes'], 'orders_sent': 0}), flush=True)


def load_minutes(folder):
    with gzip.open(folder/'minutes.jsonl.gz', 'rt') as f:
        rows = [json.loads(line) for line in f]
    identity = read_json(folder/'minutes-source.json')
    if digest(rows) != identity['rows_sha256'] or [r['start'] for r in rows] != sorted({r['start'] for r in rows}):
        raise ValueError('FROZEN_MINUTE_INTEGRITY_FAILED')
    for row in rows:
        row['_at'], row['_end'] = stamp(row['start']), stamp(row['end'])
    return rows


def forecast_day(sessions, symbol, day, execute=None):
    cutoff = datetime.combine(day, datetime.min.time(), UTC) + timedelta(hours=17)
    origin = cutoff + timedelta(hours=1)
    history = [r for r in sessions if stamp(r['timestamp']) <= cutoff][-500:]
    if not history or stamp(history[-1]['timestamp']) != cutoff:
        return {'status': 'market_closed_no_observed_session', 'symbol': symbol, 'day': day.isoformat(), 'orders_sent': 0}
    if len(history) != 500:
        raise ValueError('EXACT_500_COMPLETED_DAILY_CANDLES_REQUIRED')
    from scripts.weekly_screener import weekly_configuration
    configuration = weekly_configuration()
    request = {k: configuration[k] for k in ('models', 'transform', 'frequency', 'horizon_mode')}
    request.update(prediction_length=7, context_length=500, quantiles=QUANTILES,
                   failure_policy='fail', calendar='FTMO_UTC_DAILY' if symbol == 'BTCUSD.sim' else 'FTMO_UTC_WEEKDAYS')
    inputs = [{'timestamp': r['timestamp'], 'target': r['target']} for r in history]
    if execute is None:
        from ensemble_forecasting.worker import execute_job
        execute = execute_job
    started = time.monotonic()
    result = execute({'request': request, 'runtime_mode': 'production', 'maximum_history_rows': 500,
                      'model_checkpoints': configuration['model_checkpoints'], 'model_revisions': configuration['model_revisions'],
                      'input': {'rows': inputs, 'frequency': '1D', 'timezone': 'UTC'},
                      'source': {'type': 'ticker', 'provider': 'dukascopy', 'symbol': INSTRUMENTS[symbol][0]}}, minimum_history_rows=500)
    latency = time.monotonic() - started
    if result.get('runtime', {}).get('mock') or result.get('failures') or {r['model'] for r in result['model_runs'] if r['status']=='completed'} != set(MODELS):
        raise ValueError('FIVE_REAL_COMPLETED_MODELS_REQUIRED')
    predictions = result['predictions']
    if [stamp(r['timestamp']) for r in predictions] != future_sessions(cutoff, symbol):
        raise ValueError('EXACT_SEVEN_ASSET_SESSIONS_REQUIRED')
    for row in predictions:
        values = [float(row[f'p{round(q*100):02d}']) for q in QUANTILES]
        if values != sorted(values) or any(not math.isfinite(v) or v <= 0 for v in values):
            raise ValueError('INVALID_SIX_QUANTILES')
    return {'status': 'completed', 'symbol': symbol, 'day': day.isoformat(), 'origin': origin.isoformat(),
            'cutoff': cutoff.isoformat(), 'cutoff_close': history[-1]['target'], 'history_rows': 500,
            'history_input_sha256': digest(inputs), 'history_start': history[0]['timestamp'],
            'latest_real_observation_end': history[-1]['observation_end'], 'inference_seconds': latency,
            'earliest_actionable_at': (origin+timedelta(seconds=latency)).isoformat(),
            'predictions': predictions, 'model_runs': result['model_runs'],
            'effective_weights_by_quantile': result.get('effective_weights_by_quantile'),
            'model_checkpoints': configuration['model_checkpoints'], 'model_revisions': configuration['model_revisions'],
            'ensemble_warnings': result.get('warnings', []), 'orders_sent': 0}


def run_forecasts(symbol, start, end, folder, output):
    identity = read_json(folder/'source.json')
    if identity['symbol'] != symbol:
        raise ValueError('SOURCE_SYMBOL_MISMATCH')
    sessions = daily_sessions(load_quotes(folder), symbol)
    records = []
    for i in range((end-start).days+1):
        day = start + timedelta(days=i)
        path = output/(day.isoformat()+'.json.gz')
        existing = read_json(path) if path.exists() else None
        if existing and existing.get('status') in ('completed', 'market_closed_no_observed_session'):
            if existing.get('source_rows_sha256') != identity['rows_sha256'] or existing.get('symbol') != symbol or existing.get('day') != day.isoformat():
                raise ValueError('FORECAST_RESUME_SOURCE_MISMATCH')
            record = existing
        else:
            try:
                record = forecast_day(sessions, symbol, day)
            except Exception as exc:
                code = str(exc) if isinstance(exc, ValueError) and re.fullmatch('[A-Z0-9_]+', str(exc)) else type(exc).__name__
                record = {'status': 'failed', 'symbol': symbol, 'day': day.isoformat(), 'error_code': code, 'orders_sent': 0}
            record['source_rows_sha256'] = identity['rows_sha256']
            write_json(path, record)
        records.append(record)
        print(json.dumps({'event': 'p99_daily_forecast', 'symbol': symbol, 'day': record['day'],
                          'status': record['status'], 'error_code': record.get('error_code')}), flush=True)
    write_json(output/'summary.json', {'symbol': symbol, 'days': len(records),
                'completed': sum(r['status']=='completed' for r in records),
                'closed_sessions': sum(r['status']=='market_closed_no_observed_session' for r in records),
                'failed_days': [r['day'] for r in records if r['status']=='failed']})
    if any(r['status']=='failed' for r in records):
        raise ValueError('INCOMPLETE_FORECAST_CHUNK')


def read_forecasts(root, symbol, study, identity):
    records = {}
    for folder in root.rglob(symbol+'-c*'):
        for path in folder.glob('????-??-??.json.gz'):
            record = read_json(path)
            if record.get('symbol') != symbol or record.get('source_rows_sha256') != identity or record['day'] in records:
                raise ValueError('FORECAST_SOURCE_OR_DUPLICATE_MISMATCH')
            records[record['day']] = record
    missing = [d for d in study['days'] if records.get(d, {}).get('status') not in ('completed', 'market_closed_no_observed_session')]
    if missing:
        raise ValueError('MISSING_DAILY_FORECASTS_NO_OPTIMIZATION')
    return [records[d] for d in study['days']]


def selection(results):
    grouped = {}
    for result in results:
        grouped.setdefault(result['candidate'], []).append(result)
    eligible = [(min(r['net_pnl'] for r in pair), name) for name, pair in grouped.items()
                if len(pair)==2 and all(r['closed_baskets'] >= 5 and not r['first_daily_breach'] and not r['first_total_breach']
                                       and not r['first_margin_breach'] for r in pair)]
    return max(eligible)[1] if eligible else None


def run_replay(symbol, study, snapshot, sources, forecasts_root, output):
    identity = read_json(sources/symbol/'source.json')
    forecasts = read_forecasts(forecasts_root, symbol, study, identity['rows_sha256'])
    rows = load_minutes(sources/symbol)
    spec = snapshot['specifications'][symbol]
    currency = spec['profitCurrency']
    conversion = KnownConversion(currency, load_minutes(sources/('USDJPY.sim' if currency=='JPY' else 'USDCAD.sim')) if currency!='USD' else [])
    first = datetime.combine(date.fromisoformat(study['start']), datetime.min.time(), UTC)+timedelta(hours=18)
    last = datetime.combine(date.fromisoformat(study['end'])+timedelta(days=1), datetime.min.time(), UTC)+timedelta(hours=17)
    split = datetime.combine(date.fromisoformat(study['development_end_exclusive']), datetime.min.time(), UTC)+timedelta(hours=18)
    development = []
    full = []
    # Freeze the tested grids and sizing profiles in the plan before forecasts.
    for raw in study['candidates'][symbol]:
        candidate = Candidate(**raw)
        for path in ('low_first', 'high_first'):
            development.append(replay(symbol, rows, forecasts, spec, conversion, first, min(split, last), candidate, ordering=path))
            full.append(replay(symbol, rows, forecasts, spec, conversion, first, last, candidate, ordering=path))
        print(json.dumps({'event': 'p99_grid_replay', 'symbol': symbol, 'candidate': candidate.name,
                          'full_year_worst_path_net': min(r['net_pnl'] for r in full[-2:])}), flush=True)
    selected = selection(development)
    chosen = next((Candidate(**r) for r in study['candidates'][symbol] if Candidate(**r).name==selected), None)
    validations = []
    details = []
    if chosen:
        for costs in (COSTS[0], COSTS[2]):
            for path in ('low_first', 'high_first'):
                details.append(replay(symbol, rows, forecasts, spec, conversion, first, last, chosen, costs, path, detail=True))
                if split < last:
                    # Earlier forecasts must not initialize a fresh-flat holdout.
                    later = [f for f in forecasts if f.get('origin') and stamp(f['origin']) >= split]
                    validations.append(replay(symbol, rows, later, spec, conversion, split, last, chosen, costs, path, detail=True))
    result = {'complete': True, 'symbol': symbol, 'selected_on_development': selected,
              'selection_valid': selected is not None, 'development': development, 'full_year_grid_comparison': full,
              'selected_full_year_details': details, 'fresh_flat_holdout': validations,
              'completed_forecasts': sum(f['status']=='completed' for f in forecasts),
              'closed_session_origins': sum(f['status']!='completed' for f in forecasts),
              'costs_historically_verified': False, 'minimum_lot_historically_verified': False,
              'orders_sent': 0, 'firestore_writes': 0}
    write_json(output/'report.json.gz', result)


def aggregate(study, root, snapshot, output):
    reports = []
    missing = []
    for symbol in study['symbols']:
        paths = list(root.rglob(symbol+'/report.json.gz'))
        if len(paths) != 1:
            missing.append(symbol)
            continue
        report = read_json(paths[0])
        if not report.get('complete') or report.get('symbol') != symbol:
            missing.append(symbol)
        else:
            reports.append(report)
    write_json(output/'results.json.gz', {'complete': not missing, 'missing_assets': missing,
               'study': study, 'current_cost_snapshot': snapshot, 'reports': reports})
    lines = ['# FTMO daily P99 buy-ladder yearly backtest', '',
             f"Origins: {study['start']} through {study['end']}; $100,000 standalone account per asset. Complete: {not missing}.", '',
             '500 completed daily candles; forecast seven asset sessions once per observed day using all five models and P01/P25/P50/P75/P90/P99. ',
             'Entry signal: cutoff close above FIRST predicted P99. Stop: FINAL P01 of that triggering forecast, fixed until exit. Risk reference: FINAL P99 − FINAL P01. ',
             'One percent equity is shared across the complete ladder, with commissions, conservative USD conversion and seven-day adverse swap reserve. The first averaging buy requires latest daily P90; after that fill, additions use only lower grid levels. Breach-confirmed limits are eligible from the next minute. ',
             'Arm trailing only when net basket P&L is positive. Distance: 0.75 × highest-minus-lowest filled entry, or one grid with a single entry. No reentry until a later daily above-P99 forecast. ', '',
             '| Asset | Development-selected grid/profile | Full-year net range | Worst drawdown | Basket W/L | Fresh-flat holdout net range |',
             '|---|---|---:|---:|---:|---:|']
    for r in reports:
        ref = [x for x in r['selected_full_year_details'] if x['costs']==COSTS[0].name]
        later = [x for x in r['fresh_flat_holdout'] if x['costs']==COSTS[0].name]
        def pnl(values):
            return f"${min(x['net_pnl'] for x in values):,.2f} to ${max(x['net_pnl'] for x in values):,.2f}" if values else '—'
        worst = min(ref, key=lambda x:x['net_pnl']) if ref else None
        lines.append(f"| {r['symbol']} | {r['selected_on_development'] or 'No eligible selection'} | {pnl(ref)} | "
                     + (f"${max(x['max_equity_drawdown'] for x in ref):,.2f} | {worst['basket_wins']}/{worst['basket_losses']}" if worst else '— | —')
                     + f" | {pnl(later)} |")
    lines.extend(['', 'These standalone returns cannot be added into one shared $100k account. Grids are chosen on the first nine months; the final quarter starts flat without reoptimization. All candidates remain in results.json.gz.', '',
                  'Costs use a current FTMO specification snapshot and cost stress cases, not verified historical spreads/swaps. Public volume steps/minimums are unavailable; 0.01-lot steps are a research assumption. ',
                  'Both minute OHLC path orders are reported; they do not prove tick-exact execution. Bid/ask extrema can occur at different instants. Gaps, currency moves and future swaps can exceed a planned stop budget. ',
                  'All five models run, but bounded-native Toto/TimesFM do not support P01/P99: those tails renormalize the supported Prophet/Granite/Chronos contributions; no invented tail extrapolation. ',
                  'Weekday session labels are CFD/FX calendars, not NYSE holidays. Availability uses actual observations and measured inference latency. Missing forecasts prevent optimization reporting. No broker orders or Firestore writes.'])
    if missing:
        lines.extend(['', 'Missing assets: '+', '.join(missing)])
    output.mkdir(parents=True, exist_ok=True)
    (output/'report.md').write_text('\n'.join(lines)+'\n')
    if missing:
        raise ValueError('INCOMPLETE_TEN_ASSET_STUDY')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=('plan', 'source', 'forecast', 'replay', 'aggregate'))
    parser.add_argument('--start', type=date.fromisoformat)
    parser.add_argument('--end', type=date.fromisoformat)
    parser.add_argument('--symbol', choices=INSTRUMENTS)
    parser.add_argument('--symbols', default=','.join(INSTRUMENTS))
    for name in ('source', 'sources', 'forecasts', 'study', 'specs', 'artifacts', 'output'):
        parser.add_argument('--'+name, type=Path)
    args = parser.parse_args()
    if args.command == 'plan':
        symbols = args.symbols.split(',')
        study = plan(args.start, args.end, symbols)
        if (args.output/'study-plan.json').exists():
            if read_json(args.output/'study-plan.json') != json.loads(json.dumps(study)):
                raise ValueError('RESUME_PLAN_CHANGED')
            if not (args.output/'ftmo-cost-snapshot.json').exists():
                raise ValueError('RESUME_COST_SNAPSHOT_MISSING')
            return
        write_json(args.output/'study-plan.json', study)
        write_json(args.output/'ftmo-cost-snapshot.json', snapshot_specs(symbols))
    elif args.command == 'source':
        source(args.symbol, args.start, args.end, args.output)
    elif args.command == 'forecast':
        run_forecasts(args.symbol, args.start, args.end, args.source, args.output)
    elif args.command == 'replay':
        run_replay(args.symbol, read_json(args.study), read_json(args.specs), args.sources, args.forecasts, args.output)
    else:
        aggregate(read_json(args.study), args.artifacts, read_json(args.specs), args.output)


if __name__ == '__main__':
    main()
