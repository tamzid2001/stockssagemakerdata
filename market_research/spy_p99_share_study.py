"""Artifact-backed Alpaca SPY daily P99 share-ladder research; GET-only data access."""
from __future__ import annotations

import argparse
from datetime import date, datetime, time as daytime, timedelta
import gzip
import json
import math
import os
from pathlib import Path
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from .ftmo_dukas_data import UTC, digest, stamp
from .ftmo_dukas_engine import Costs
from .ftmo_p99_engine import Candidate, KnownConversion, replay
from .ftmo_p99_study import read_json, selection, write_json

NY = ZoneInfo('America/New_York')
MODELS = ('prophet', 'toto', 'granite', 'chronos', 'timesfm')
QUANTILES = (.01, .10, .25, .50, .75, .90, .99)
GRIDS = (.25, .50, 1., 2., 5., 10.)
COSTS = (Costs('assumed_1_cent_spread_price_pnl'), Costs('assumed_2_cent_spread_price_pnl', spread_factor=2.))


class DataAPI:
    """Only stock-bar and calendar GET requests; no account/order API surface."""
    def __init__(self):
        key, secret = os.environ.get('ALPACA_API_KEY'), os.environ.get('ALPACA_SECRET_KEY')
        if not key or not secret:
            raise ValueError('ALPACA_CREDENTIALS_MISSING')
        self.headers = {'APCA-API-KEY-ID': key, 'APCA-API-SECRET-KEY': secret, 'Accept': 'application/json'}

    def get(self, path, params):
        if path not in ('/v2/calendar', '/v2/stocks/SPY/bars'):
            raise ValueError('READ_ONLY_ENDPOINT_REQUIRED')
        base = 'https://paper-api.alpaca.markets' if path == '/v2/calendar' else 'https://data.alpaca.markets'
        for attempt in range(5):
            try:
                with urlopen(Request(base+path+'?'+urlencode(params), headers=self.headers), timeout=30) as response:
                    return json.load(response)
            except HTTPError as exc:
                if exc.code not in (429, 500, 502, 503, 504) or attempt == 4:
                    raise RuntimeError(f'ALPACA_HTTP_{exc.code}') from None
                time.sleep(min(10., 2.**attempt))
            except (URLError, TimeoutError):
                if attempt == 4:
                    raise RuntimeError('ALPACA_DATA_UNAVAILABLE') from None
                time.sleep(min(10., 2.**attempt))


def calendar_rows(raw):
    found = {}
    for row in raw:
        day = date.fromisoformat(row['date'])
        opening = datetime.combine(day, daytime.fromisoformat(row['open']), NY).astimezone(UTC)
        closing = datetime.combine(day, daytime.fromisoformat(row['close']), NY).astimezone(UTC)
        if not opening < closing or day.isoformat() in found:
            raise ValueError('INVALID_OR_DUPLICATE_CALENDAR_SESSION')
        found[day.isoformat()] = {'day': day.isoformat(), 'open': opening.isoformat(), 'close': closing.isoformat()}
    return [found[k] for k in sorted(found)]


def fetch_bars(api, start, end, timeframe, feed, *, adjustment='split'):
    found, token, seen = {}, '', set()
    while True:
        params = {'timeframe': timeframe, 'start': start.isoformat(), 'end': end.isoformat(),
                  'feed': feed, 'adjustment': adjustment, 'sort': 'asc', 'limit': 10000}
        if token:
            params['page_token'] = token
        response = api.get('/v2/stocks/SPY/bars', params)
        for raw in response.get('bars') or []:
            at = stamp(raw['t'])
            # Alpaca endpoint bounds may include a bar stamped exactly at end.
            # Do not admit its potentially incomplete daily observation.
            if not start <= at < end:
                continue
            values = {k: float(raw[k]) for k in ('o', 'h', 'l', 'c')}
            if (not all(math.isfinite(v) and v > 0 for v in values.values())
                    or values['l'] > min(values['o'], values['c']) or values['h'] < max(values['o'], values['c'])
                    or values['l'] > values['h']):
                raise ValueError('INVALID_ALPACA_BAR')
            row = {'timestamp': at.isoformat(), **values}
            if at in found and found[at] != row:
                raise ValueError('CONFLICTING_ALPACA_BAR')
            found[at] = row
        token = str(response.get('next_page_token') or '')
        if not token:
            break
        if token in seen:
            raise ValueError('ALPACA_PAGINATION_STALLED')
        seen.add(token)
    return [found[k] for k in sorted(found)]


def regular_minutes(bars, calendar):
    sessions = {r['day']: r for r in calendar}
    result = []
    for row in bars:
        at = stamp(row['timestamp']); end = at+timedelta(minutes=1)
        session = sessions.get(at.astimezone(NY).date().isoformat())
        if session and stamp(session['open']) <= at and end <= stamp(session['close']):
            prices = {k: row[k] for k in ('o', 'h', 'l', 'c')}
            result.append({'start': at.isoformat(), 'end': end.isoformat(), 'bid': prices, 'ask': dict(prices)})
    return result


def source(start, end, feed, output, api=None):
    if start > end or (end-start).days != 364 or feed not in ('sip', 'iex'):
        raise ValueError('ONE_YEAR_RANGE_AND_APPROVED_FEED_REQUIRED')
    api = api or DataAPI()
    last = end+timedelta(days=1)
    warmup = start-timedelta(days=1100)
    calendar = calendar_rows(api.get('/v2/calendar', {'start': warmup.isoformat(), 'end': (last+timedelta(days=30)).isoformat()}))
    completed = [r for r in calendar if r['day'] <= last.isoformat()]
    if not completed or stamp(completed[-1]['close'])+timedelta(minutes=16) >= datetime.now(UTC):
        raise ValueError('ONLY_COMPLETED_EXCHANGE_SESSIONS_ALLOWED')
    end_at = datetime.combine(last+timedelta(days=1), daytime.min, NY).astimezone(UTC)
    daily = fetch_bars(api, datetime.combine(warmup, daytime.min, NY).astimezone(UTC), end_at, '1Day', feed)
    by_day = {stamp(r['timestamp']).astimezone(NY).date().isoformat(): r for r in daily}
    if len(by_day) != len(daily):
        raise ValueError('DUPLICATE_DAILY_SESSION')
    sessions = []
    for session in completed:
        bar = by_day.get(session['day'])
        if bar is None:
            raise ValueError('MISSING_DAILY_SOURCE_SESSION')
        sessions.append({**session, 'timestamp': session['day']+'T00:00:00+00:00', 'target': bar['c']})
    origins = [r['day'] for r in sessions if start.isoformat() <= r['day'] <= end.isoformat()]
    if not origins or len([r for r in sessions if r['day'] <= origins[0]]) < 500:
        raise ValueError('INSUFFICIENT_500_SESSION_WARMUP')
    minute_start = datetime.combine(start, daytime.min, NY).astimezone(UTC)
    raw_daily = fetch_bars(api, minute_start, end_at, '1Day', feed, adjustment='raw')
    for bar in raw_daily:
        day = stamp(bar['timestamp']).astimezone(NY).date().isoformat()
        if day not in by_day or not math.isclose(bar['c'], by_day[day]['c'], rel_tol=1e-10, abs_tol=1e-8):
            raise ValueError('REPLAY_YEAR_SPLIT_REQUIRES_SHARE_REBASE')
    if len(raw_daily) != len([r for r in completed if r['day'] >= start.isoformat()]):
        raise ValueError('RAW_SPLIT_ADJUSTMENT_VERIFICATION_INCOMPLETE')
    minutes = regular_minutes(fetch_bars(api, minute_start, end_at, '1Min', feed), calendar)
    observed_days = {stamp(r['start']).astimezone(NY).date().isoformat() for r in minutes}
    if any(r['day'] not in observed_days for r in completed if r['day'] >= start.isoformat()):
        raise ValueError('MISSING_MINUTE_SOURCE_SESSION')
    if not minutes:
        raise ValueError('NO_REGULAR_SESSION_MINUTES')
    study = {'schema_version': 'spy-alpaca-p99-shares-v1', 'symbol': 'SPY', 'start': start.isoformat(), 'end': end.isoformat(),
             'days': origins, 'initial_balance': 100000., 'risk_fraction': .01, 'history_rows': 500,
             'horizon_sessions': 7, 'quantiles': QUANTILES, 'models': MODELS, 'feed': feed,
             'adjustment': 'split', 'entry_rule': 'cutoff close strictly above FIRST predicted P99',
             'averaging_gate': 'lower grid only; no P90 condition', 'quantity_unit': 'whole shares',
             'stop': 'fixed final triggering P01', 'risk_reference': 'final triggering P99 minus P01',
             'development_end_exclusive': (start+timedelta(days=274)).isoformat(),
             'candidates': [{'grid': g, 'sizing': s} for g in GRIDS for s in ('equal', 'larger_deeper', 'smaller_deeper')],
             'forecasts': [{'chunk': i//21, 'days': origins[i:i+21]} for i in range(0, len(origins), 21)],
             'execution_note': 'RTH trade OHLC rounded up to cents as ask proxy; bid proxy is ask minus assumed 1-cent/2-cent spread. Not historical NBBO. Price P&L excludes dividends, regulatory fees and interest.',
             'orders_sent': 0, 'firestore_writes': 0}
    identity = {'provider': 'alpaca', 'symbol': 'SPY', 'feed': feed, 'adjustment': 'split',
                'rows_sha256': digest(sessions), 'minutes_sha256': digest(minutes), 'calendar_sha256': digest(calendar),
                'completed_daily_sessions': len(sessions), 'regular_minutes': len(minutes), 'orders_sent': 0}
    identity['replay_year_no_split_adjustment_difference_verified'] = True
    for name, payload in (('study-plan.json', study), ('source.json', identity),
                          ('daily-sessions.json.gz', sessions), ('calendar.json.gz', calendar)):
        write_json(output/name, payload)
    with gzip.open(output/'minutes.jsonl.gz', 'wt') as f:
        for row in minutes:
            f.write(json.dumps(row, separators=(',', ':'))+'\n')
    print(json.dumps({'event': 'spy_share_source_complete', **identity, 'forecast_origins': len(origins)}), flush=True)


def forecast_day(sessions, calendar, day, execute=None):
    history = [r for r in sessions if r['day'] <= day][-500:]
    if len(history) != 500 or history[-1]['day'] != day:
        raise ValueError('EXACT_500_COMPLETED_DAILY_CANDLES_REQUIRED')
    from scripts.weekly_screener import weekly_configuration
    configuration = weekly_configuration()
    request = {k: configuration[k] for k in ('models', 'transform', 'frequency', 'horizon_mode')}
    request.update(prediction_length=7, context_length=500, quantiles=QUANTILES, failure_policy='fail', calendar='NYSE')
    inputs = [{'timestamp': r['timestamp'], 'target': r['target']} for r in history]
    if execute is None:
        from ensemble_forecasting.worker import execute_job
        execute = execute_job
    started = time.monotonic()
    result = execute({'request': request, 'runtime_mode': 'production', 'maximum_history_rows': 500,
                      'model_checkpoints': configuration['model_checkpoints'], 'model_revisions': configuration['model_revisions'],
                      'input': {'rows': inputs, 'frequency': '1D', 'timezone': 'America/New_York'},
                      'source': {'type': 'ticker', 'provider': 'alpaca', 'symbol': 'SPY'}}, minimum_history_rows=500)
    latency = time.monotonic()-started
    if (result.get('runtime', {}).get('mock') or result.get('failures')
            or {m['model'] for m in result['model_runs'] if m['status'] == 'completed'} != set(MODELS)):
        raise ValueError('FIVE_REAL_COMPLETED_MODELS_REQUIRED')
    future = [r for r in calendar if r['day'] > day][:7]
    raw = result['predictions']
    if len(future) != 7 or [stamp(p['timestamp']).date().isoformat() for p in raw] != [r['day'] for r in future]:
        raise ValueError('EXACT_SEVEN_NYSE_SESSIONS_REQUIRED')
    predictions = []
    for row, session in zip(raw, future):
        values = [float(row[f'p{round(q*100):02d}']) for q in QUANTILES]
        if values != sorted(values) or any(not math.isfinite(v) or v <= 0 for v in values):
            raise ValueError('INVALID_SEVEN_QUANTILES')
        predictions.append({**row, 'worker_timestamp': row['timestamp'], 'timestamp': session['close']})
    cutoff = stamp(history[-1]['close']); origin = cutoff+timedelta(minutes=15)
    return {'status': 'completed', 'symbol': 'SPY', 'day': day, 'origin': origin.isoformat(), 'cutoff': cutoff.isoformat(),
            'cutoff_close': history[-1]['target'], 'history_rows': 500, 'history_start': history[0]['timestamp'],
            'history_input_sha256': digest(inputs), 'inference_seconds': latency,
            'earliest_actionable_at': (origin+timedelta(seconds=latency)).isoformat(),
            'predictions': predictions, 'raw_predictions_sha256': digest(raw), 'model_runs': result['model_runs'],
            'effective_weights_by_quantile': result.get('effective_weights_by_quantile'),
            'model_checkpoints': configuration['model_checkpoints'], 'model_revisions': configuration['model_revisions'],
            'warnings': result.get('warnings', []), 'orders_sent': 0}


def forecasts(folder, output, chunk):
    study, identity = read_json(folder/'study-plan.json'), read_json(folder/'source.json')
    sessions, calendar = read_json(folder/'daily-sessions.json.gz'), read_json(folder/'calendar.json.gz')
    if digest(sessions) != identity['rows_sha256'] or digest(calendar) != identity['calendar_sha256']:
        raise ValueError('SOURCE_HASH_MISMATCH')
    days = study['forecasts'][chunk]['days']
    for day in days:
        path = output/(day+'.json.gz')
        record = read_json(path) if path.exists() else None
        if record and record.get('status') == 'completed':
            if record.get('day') != day or record.get('source_rows_sha256') != identity['rows_sha256']:
                raise ValueError('FORECAST_RESUME_IDENTITY_MISMATCH')
        else:
            record = forecast_day(sessions, calendar, day)
            record['source_rows_sha256'] = identity['rows_sha256']
            write_json(path, record)
        print(json.dumps({'event': 'spy_daily_share_forecast', 'day': day, 'status': record['status'],
                          'above_first_p99': record['cutoff_close'] > record['predictions'][0]['p99']}), flush=True)


def share_spec():
    return {'contractSize': 1., 'profitCurrency': 'USD', 'commission': 0., 'commissionType': 'flat_USD',
            'swapType': 'percentage', 'swapLong': 0., 'swapShort': 0., 'digits': 2, 'leverageStandard': 1.,
            'minimumVolume': 1., 'volumeStep': 1., 'maxTradeVolume': 100000000., 'maxTotalVolume': 100000000.,
            'weekly_minutes': [[0, 10080]]}


def assumed_quotes(row, symbol, costs):
    if symbol != 'SPY':
        raise ValueError('SPY_SHARE_QUOTES_REQUIRED')
    ask = {k: math.ceil(row['ask'][k]*100-1e-8)/100 for k in ('o', 'h', 'l', 'c')}
    return ({k: ask[k]-.01*costs.spread_factor for k in ask}, ask)


def run_share_replay(rows, records, first, last, candidate, costs=COSTS[0], ordering='low_first', detail=False, *,
                     side='long', research_budget_multiplier=1., account_timezone=NY):
    result = replay('SPY', rows, records, share_spec(), KnownConversion('USD', []), first, last, candidate, costs, ordering,
                    detail=detail, averaging_gate='grid', quote_adjuster=assumed_quotes,
                    account_timezone=account_timezone, side=side,
                    research_budget_multiplier=research_budget_multiplier)
    rename = {'max_lots': 'max_shares', 'open_lots': 'open_shares', 'max_margin': 'max_invested_notional_usd'}
    for old, new in rename.items():
        result[new] = result.pop(old)
    result.update(quantity_unit='whole shares', dividends_included=False, regulatory_fees_included=False,
                  historical_spread_verified=False, brokerage_orders_sent=0)
    if side == 'short':
        result.update(historical_borrow_availability_verified=False, stock_borrow_fees_included=False,
                      dividend_in_lieu_included=False,
                      collateral_assumption='100% notional reserved; short proceeds do not enlarge capacity')
    if detail:
        for leg in result['entries']+result['open_basket']:
            leg['shares'], leg['initial_shares'] = leg.pop('lots'), leg.pop('initial_lots')
            for exit in leg['exits']:
                exit['shares'] = exit.pop('lots')
        for point in result['hourly_equity']:
            point['shares'] = point.pop('lots')
    return result


def run_replays(folder, forecast_root, output, *, side='long'):
    study, identity = read_json(folder/'study-plan.json'), read_json(folder/'source.json')
    records = {}
    for path in forecast_root.rglob('????-??-??.json.gz'):
        record = read_json(path)
        if (record.get('symbol') != 'SPY' or record.get('status') != 'completed'
                or record.get('source_rows_sha256') != identity['rows_sha256'] or record['day'] in records):
            raise ValueError('FORECAST_SOURCE_OR_DUPLICATE_MISMATCH')
        records[record['day']] = record
    if set(records) != set(study['days']):
        raise ValueError('MISSING_DAILY_FORECASTS_NO_REPORT')
    with gzip.open(folder/'minutes.jsonl.gz', 'rt') as f:
        rows = [json.loads(line) for line in f]
    if digest(rows) != identity['minutes_sha256']:
        raise ValueError('MINUTE_SOURCE_HASH_MISMATCH')
    for row in rows:
        row['_at'], row['_end'] = stamp(row['start']), stamp(row['end'])
    first = datetime.combine(date.fromisoformat(study['start']), daytime.min, NY).astimezone(UTC)
    last = datetime.combine(date.fromisoformat(study['end'])+timedelta(days=2), daytime.min, NY).astimezone(UTC)
    split = datetime.combine(date.fromisoformat(study['development_end_exclusive']), daytime.min, NY).astimezone(UTC)
    ordered = [records[d] for d in study['days']]
    development, full = [], []
    for raw in study['candidates']:
        candidate = Candidate(**raw)
        for path in ('low_first', 'high_first'):
            development.append(run_share_replay(rows, ordered, first, split, candidate, ordering=path, side=side))
            full.append(run_share_replay(rows, ordered, first, last, candidate, ordering=path, side=side))
        print(json.dumps({'event': 'spy_share_grid_replay', 'candidate': candidate.name,
                          'net_range': [r['net_pnl'] for r in full[-2:]],
                          'closed_baskets': [r['closed_baskets'] for r in full[-2:]]}), flush=True)
    selected = selection(development)
    detail_results, holdout = [], []
    if selected:
        candidate = next(Candidate(**r) for r in study['candidates'] if Candidate(**r).name == selected)
        later = [r for r in ordered if stamp(r['origin']) >= split]
        for cost in COSTS:
            for path in ('low_first', 'high_first'):
                detail_results.append(run_share_replay(rows, ordered, first, last, candidate, cost, path, True, side=side))
                holdout.append(run_share_replay(rows, later, split,last, candidate, cost, path, True, side=side))
    signal_days = [r['day'] for r in ordered if r['cutoff_close'] > r['predictions'][0]['p99']]
    replay_study = study if side == 'long' else {**study, 'side': 'short',
                   'averaging_gate': 'higher grid only; no quantile gate', 'stop': 'fixed final triggering P99',
                   'collateral_assumption': '100% notional reserved; short proceeds do not enlarge capacity',
                   'historical_borrow_availability_verified': False, 'stock_borrow_fees_included': False,
                   'dividend_in_lieu_included': False}
    result = {'complete': True, 'study': replay_study, 'source': identity, 'completed_forecasts': len(ordered),
              'signal_days': signal_days, 'selected_on_development': selected, 'development': development,
              'full_year_grid_comparison': full, 'selected_full_year_details': detail_results,
              'fresh_flat_holdout': holdout, 'orders_sent': 0, 'firestore_writes': 0}
    write_json(output/'results.json.gz', result)
    lines = [f'# SPY Alpaca above-P99 yearly {side} share-ladder backtest', '',
             f"Forecast origins: {study['start']} through {study['end']}; $100,000 account; exactly 500 prior daily bars; seven-session forecasts.", '',
             f"Completed forecasts: {len(ordered)}. Cutoff close above first predicted P99: {len(signal_days)} day(s). Development-selected configuration: {selected or 'none eligible'}.", '',
             'P01/P10/P25/P50/P75/P90/P99; Prophet, Toto, Granite, Chronos and TimesFM. Supported models supply P01/P99 without tail extrapolation.', '',
             ('Whole shares, 1% risk shared across the ladder, no borrowing. Fixed final triggering P01 stop; final P99−P01 risk reference. Average only lower, without a P90 gate.' if side == 'long' else
              'Whole short shares, 1% risk shared across the ladder. Fixed final triggering P99 stop; final P99−P01 risk reference. Average only higher. Reserve 100% gross notional; short proceeds do not enlarge capacity. Skip entry if the fixed P99 stop is not above the short fill. Historical borrow availability, borrow fees and dividend-in-lieu costs are unverified and excluded.')
              + ' Carry positions; profitability-activated trail; wait for a later qualifying forecast after exit.', '',
             'Regular-session trade OHLC rounded up to cents as ask proxy; modeled bid = ask minus assumed spread (1 cent reference, 2 cents sensitivity). Dividends, regulatory fees and interest are excluded; results are price P&L, not verified broker returns.', '',
             '| Grid | Profile | Minute path | Net price P&L | Drawdown | Basket W/L | Win rate | Max ladder | Avg duration hours | Open shares |',
             '|---:|---|---|---:|---:|---|---:|---:|---:|---:|']
    for r in full:
        win = f"{r['basket_win_rate']*100:.2f}%" if r['basket_win_rate'] is not None else 'N/A'
        duration = f"{r['average_basket_hours']:.2f}" if r['average_basket_hours'] is not None else 'N/A'
        lines.append(f"| ${r['grid']:g} | {r['sizing']} | {r['ordering']} | ${r['net_pnl']:,.2f} | ${r['max_equity_drawdown']:,.2f} | {r['basket_wins']}/{r['basket_losses']} | {win} | {r['max_ladder']} | {duration} | {r['open_shares']:g} |")
    lines += ['', 'Candidates are separate alternative accounts; their returns cannot be added. Selection uses the first nine months only; last-quarter holdout starts flat. Minute OHLC does not prove tick order or limit queue fills. Open positions remain marked, without forced year-end liquidation. No live or paper broker orders were placed.']
    (output/'report.md').write_text('\n'.join(lines)+'\n')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=('source', 'forecast', 'replay'))
    parser.add_argument('--start', type=date.fromisoformat, default=date(2025, 10, 7))
    parser.add_argument('--end', type=date.fromisoformat, default=date(2026, 10, 6))
    parser.add_argument('--feed', choices=('sip', 'iex'), default='sip')
    parser.add_argument('--side', choices=('long', 'short'), default='long')
    parser.add_argument('--source', type=Path)
    parser.add_argument('--forecasts', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--chunk', type=int)
    args = parser.parse_args()
    if args.command == 'source':
        source(args.start, args.end, args.feed, args.output)
    elif args.command == 'forecast':
        forecasts(args.source, args.output, args.chunk)
    else:
        run_replays(args.source, args.forecasts, args.output, side=args.side)


if __name__ == '__main__':
    main()
