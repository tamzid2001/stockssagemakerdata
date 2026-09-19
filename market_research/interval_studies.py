"""Per-series, per-origin hold-to-settlement comparisons; no exchange execution.

Reuse the numerical engine, first-P90 selector, sticky settlement state, fee
sensitivities and immutable encrypted checkpoint transport. Historical forecasts
are ALWAYS labelled replay; quotes collected live do not make a later fit live.
"""
import argparse
from collections import Counter, defaultdict
from dataclasses import asdict
import json
import math
import os
import re
import time

from .btc_horizons import Checkpoints
from .btc_hold_tracking import first_signals
from .btc_sticky_tracking import direction_at, simulate
from .engine import digest, stamp, validate_forecast
from .forecast import forecast_window
from .interval_markets import KalshiIntervalProvider
from .p1_oco import QUANTILES
from ensemble_forecasting.capabilities import model_supports_quantile
from ensemble_forecasting.schemas import canonical_quantile_string

VERSION = 'interval_three_strategies_v1'
ORIGINS = tuple(range(1, 13))


def models_for(n):
    if type(n) is not int or n not in ORIGINS:
        raise ValueError('FIRST_1_TO_12_MINUTES_REQUIRED')
    return ('granite', 'chronos', 'timesfm') if n == 1 else ('prophet', 'granite', 'chronos', 'timesfm')


def configuration(series, as_of, maximum, code):
    KalshiIntervalProvider(series)  # Strict internal ticker validation.
    if series in ('KXCRYPTOLEAD15M', 'KXCRYPTOCOMP15M'):
        raise ValueError('DIRECTIONAL_PRICE_SERIES_REQUIRED')
    if type(as_of) is not int or not 0 < as_of <= time.time():
        raise ValueError('PAST_CUTOFF_REQUIRED')
    if type(maximum) is not int or not 1 <= maximum <= 100:
        raise ValueError('ONE_TO_100_MARKETS_REQUIRED')
    if not re.fullmatch('[a-f0-9]{40}', code):
        raise ValueError('IMMUTABLE_CODE_SHA_REQUIRED')
    return dict(version=VERSION, series=series, as_of=as_of, max_markets=maximum,
                code_sha=code, origins=list(ORIGINS), paper_only=True,
                failure_policy='fail', quantiles=list(QUANTILES),
                entry='next_minute_ask', exit='official_settlement_only',
                publication='historical_measured_sequential_runtime_plus_5s')


def window_at(rows, opened, n):
    models_for(n)
    index = {}
    for q in rows:
        if q.timestamp in index and index[q.timestamp] != q:
            raise ValueError('CONFLICTING_HISTORY_MINUTE')
        if q.observed:
            index[q.timestamp] = q
    times = [opened + 60*i for i in range(1, n+1)]
    if any(t not in index for t in times):
        raise ValueError('MISSING_FIRST_N_COMPLETED_MINUTES')
    return [index[t] for t in times]


def validate_pair_member(f, origin, horizon, models):
    validate_forecast(f, origin, horizon)
    actual = [r.get('id', r.get('model')) for r in f.get('models', []) if r.get('status') == 'completed']
    if sorted(actual) != sorted(models) or len(f.get('models', [])) != len(models) or f.get('failures'):
        raise ValueError('STRICT_INTERVAL_ENSEMBLE_REQUIRED')
    for q in QUANTILES:
        # TimesFM has no native tails. Equal raw weights are renormalized among
        # only capable models for EACH quantile by the authoritative engine.
        contributors = {m for m in models if model_supports_quantile(m, q)}
        weights = f.get('weights', {}).get(canonical_quantile_string(q), {})
        if not contributors or set(weights) != contributors or any(not math.isfinite(w) or abs(w-1/len(contributors)) > 1e-8 for w in weights.values()):
            raise ValueError('EQUAL_INTERVAL_MODEL_WEIGHTS_REQUIRED')
    if not math.isfinite(f.get('duration_seconds', float('nan'))) or f['duration_seconds'] < 0:
        raise ValueError('INVALID_INFERENCE_DURATION')


def discover(provider, config):
    response = provider.get('/markets', {'series_ticker':config['series'],
        'max_close_ts':config['as_of'], 'limit':1000})
    eligible = {m['ticker']:m for m in response.get('markets', []) if provider.valid_market(m)
                and stamp(m['close_time']) <= config['as_of']}
    ordered = sorted(eligible.values(), key=lambda m:(stamp(m['close_time']), m['ticker']), reverse=True)
    markets = list(reversed(ordered[:config['max_markets']]))
    return {'markets':markets, 'settlement_seed_markets':ordered[config['max_markets']:config['max_markets']+20],
        'retrieved_at':int(time.time()), 'series':config['series'],
        'coverage':{'selected':len(markets), 'requested':config['max_markets'],
                    'upstream_has_more':bool(response.get('cursor')), 'all_history_claimed':False}}


def settlement(market, retrieved):
    if market.get('status') not in ('settled', 'finalized') or market.get('result') not in ('yes', 'no'):
        return None
    end = stamp(market['close_time'])
    raw = market.get('settlement_ts')
    try:
        actual = int(raw) if type(raw) in (int, float) else stamp(raw) if isinstance(raw, str) else 0
    except (TypeError, ValueError):
        actual = 0
    return {'market_id':market['ticker'], 'close_at':end, 'result':market['result'],
        'resolution_status':'resolved', 'first_confirmed_at':max(end+60, actual),
        'confirmation_clock':'historical_official_or_assumed_60s_floor', 'retrieved_at':retrieved}


def source_for(market, archive, provider):
    ticker = market['ticker']
    saved = archive.get('source', ticker)
    if saved is not None:
        return saved
    end = stamp(market['close_time'])
    raw = provider.candles(market, end)
    resolved = provider.market(ticker)
    if resolved.get('ticker') != ticker:
        raise ValueError('MARKET_IDENTITY_MISMATCH')
    value = {'market':market, 'candles':raw, 'resolution_market':resolved,
             'retrieved_at':int(time.time()), 'redistribution_status':'review_required'}
    archive.put('source', ticker, value)
    return value


def forecast_origin(market, n, source, config, archive, provider, forecaster=forecast_window):
    ticker = market['ticker']; key = ticker+'-n'+str(n)
    prior = archive.get('market', key)
    if prior is not None:
        return prior
    opened, end = stamp(market['open_time']), stamp(market['close_time'])
    data = provider.quotes(source['candles'], market, end)
    windows = {s:window_at(data[s], opened, n) for s in ('yes', 'no')}
    models = models_for(n); origin = opened + n*60; pair = []
    for side in ('yes', 'no'):
        fkey = key+'-'+side
        f = archive.get('forecast', fkey)
        if f is None:
            f = forecaster(windows[side], 15-n, models, QUANTILES,
                           failure_policy='fail', **({'single_point_research':True} if n == 1 else {}))
            validate_pair_member(f, origin, 15-n, models)
            f.update(market_context={'market_id':ticker, 'event_id':market['event_ticker'],
                'contract_id':ticker+':'+side, 'side':side, 'provider':'kalshi', 'series':config['series']},
                history_count=n, input_snapshot=[asdict(q) for q in windows[side]],
                source_hash=digest(source), execution_code_sha=config['code_sha'])
            f['forecast_id'] = digest([f['forecast_id'], ticker, side, n, VERSION])
            archive.put('forecast', fkey, f)
        validate_pair_member(f, origin, 15-n, models)
        if f['source_hash'] != digest(source) or f['input_snapshot'] != [asdict(q) for q in windows[side]]:
            raise ValueError('FROZEN_SOURCE_CONFLICT')
        pair.append(f)
    # Archive replay preserves original quote receipts; a late input cannot be
    # treated as available at the candle timestamp. Legacy API replay retains
    # its explicitly disclosed five-second receipt assumption.
    receipts = source.get('minute_receipts', {})
    input_ready = max([origin + 5] + [receipts.get(str(q.timestamp), origin + 5)
                                    for window in windows.values() for q in window])
    publication = input_ready + max(1, math.ceil(sum(f['duration_seconds'] for f in pair)))
    pair = [{**f, 'available_at':publication, 'publication_clock':'historical_measured_runtime_proxy'} for f in pair]
    record = {'market':ticker, 'history_minutes':n, 'horizon_minutes':15-n, 'forecasts':pair,
              'status':'evaluated' if publication < end else 'missed_deadline',
              'paper_only':True, 'live_execution_verified':False}
    archive.put('market', key, record)
    return record


def first_p10_signals(forecasts, observations, as_of):
    """First minute after publication vs frozen FIRST forecast row; strict <."""
    groups = defaultdict(list); counts = Counter(); signals = []
    for f in forecasts:
        groups[f['market_context']['market_id']].append(f)
    tape = {(q['contract_id'],q['timestamp']):q for q in observations}
    for market, pair in groups.items():
        if len(pair) != 2 or len({f['market_context']['contract_id'] for f in pair}) != 2:
            counts['unpaired'] += 1; continue
        publication = max(f['available_at'] for f in pair)
        t = publication//60*60+60
        end = min(f['rows'][-1]['timestamp'] for f in pair)
        if t >= end or t > as_of:
            counts['missed_deadline'] += 1; continue
        candidates = []; missing = False
        for f in pair:
            side = f['market_context']['contract_id']; q = tape.get((side, t))
            if not q or not t <= q['received_at'] <= min(as_of, t+30) or q['received_at'] >= end:
                missing = True; break
            if 0 < q['ask'] < f['rows'][0]['quantiles']['0.1']:
                candidates.append({'market_id':market, 'game_id':f['market_context']['event_id'],
                    'contract_id':side, 'signal_at':t, 'signal_received_at':q['received_at'],
                    'market_end':end, 'forecast_id':f['forecast_id'],
                    'first_row_p10':f['rows'][0]['quantiles']['0.1'], 'signal_ask':q['ask']})
        if missing:
            counts['missing_first_minute'] += 1
        elif len(candidates) == 1:
            signals.extend(candidates); counts['signals'] += 1
        else:
            counts['ambiguous' if candidates else 'neutral'] += 1
    return signals, dict(counts)


def compare(forecasts, observations, settlements, fee, as_of):
    series = {f['market_context']['series'] for f in forecasts}
    origins = {f['history_count'] for f in forecasts}
    if len(series) > 1 or len(origins) > 1:
        raise ValueError('SEPARATE_SERIES_AND_ORIGIN_COHORTS_REQUIRED')
    current = next(iter(series), None)
    settlements = [s for s in settlements if current and s['market_id'].startswith(current+'-')]
    p90, ambiguous = first_signals(forecasts, observations, as_of)
    sticky = []
    for signal in p90:
        d = direction_at(settlements, signal['signal_received_at'], signal['market_id'])
        if d and signal['contract_id'].endswith(':'+d['side']):
            sticky.append({**signal, 'direction':d})
    p10, p10_counts = first_p10_signals(forecasts, observations, as_of)
    fee_ok = (fee.get('fee_type') in ('quadratic', 'quadratic_with_maker_fees')
        and type(fee.get('multiplier')) in (int,float)
        and math.isfinite(fee['multiplier']) and fee['multiplier'] >= 0)
    result = {'series':current, 'history_minutes':next(iter(origins), None),
        'ambiguous_p90_minutes':ambiguous, 'p10_coverage':p10_counts,
        'fee_status':'current_schedule_sensitivity' if fee_ok else 'unknown_no_net_return',
        'strategies':{}}
    for label, signals in (('first_p90', p90), ('p90_sticky', sticky), ('first_quote_below_p10', p10)):
        # Fee-free run supplies settlement outcomes even if fee policy is unknown.
        gross = simulate(signals, observations, settlements, as_of=as_of, policy='fixed_one', precision='0.0001', multiplier=0)
        scenarios = {}
        if fee_ok:
            for policy in ('fixed_one', 'recover_cycle'):
                scenarios[policy] = simulate(signals, observations, settlements, as_of=as_of,
                    policy=policy, precision='0.0001', multiplier=fee['multiplier'])
        closed = [t for t in gross['trades'] if t['status'] == 'closed']
        wins = sum(t['exit_price'] == 1 for t in closed)
        result['strategies'][label] = {'signals':len(signals), 'wins':wins, 'losses':len(closed)-wins,
            'settlement_win_rate':wins/len(closed) if closed else None,
            'pending':sum(t['status'] != 'closed' for t in gross['trades']),
            'missed_entries':len(gross['missed_entries']), 'scenarios':scenarios}
    return result


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--series', required=True); p.add_argument('--as-of', type=int, required=True)
    p.add_argument('--max-markets', type=int, default=2); p.add_argument('--budget-minutes', type=int, default=270)
    args = p.parse_args()
    if not 1 <= args.budget_minutes <= 270: p.error('INVALID_BUDGET')
    config = configuration(args.series, args.as_of, args.max_markets, os.environ['QUANTURA_CODE_SHA'])
    archive = Checkpoints(config); provider = KalshiIntervalProvider(args.series)
    catalog = archive.get('catalog', 'markets')
    if catalog is None:
        catalog = discover(provider, config)
        archive.put('catalog', 'markets', catalog)
    fee = archive.get('catalog', 'fee')
    if fee is None:
        raw = provider.get('/series/'+args.series)['series']
        fee = {'fee_type':raw.get('fee_type'), 'multiplier':raw.get('fee_multiplier'), 'observed_at':int(time.time())}
        archive.put('catalog', 'fee', fee)
    deadline = time.monotonic()+60*args.budget_minutes; records = []; tape = []; settlements = []
    for m in catalog['settlement_seed_markets']:
        s = settlement(m, catalog['retrieved_at'])
        if s: settlements.append(s)
    for market in catalog['markets']:
        if time.monotonic() >= deadline: break
        source = source_for(market, archive, provider); ticker = market['ticker']
        s = settlement(source['resolution_market'], source['retrieved_at'])
        if s: settlements.append(s)
        for side, rows in provider.quotes(source['candles'], market, stamp(market['close_time'])).items():
            tape.extend({**asdict(q), 'contract_id':ticker+':'+side, 'received_at':q.timestamp+5,
                         'collection_mode':'historical', 'game_id':market['event_ticker']} for q in rows)
        for n in ORIGINS:
            if time.monotonic() >= deadline: break
            key = ticker+'-n'+str(n); record = archive.get('market', key)
            if record is None:
                try:
                    record = forecast_origin(market, n, source, config, archive, provider)
                except (RuntimeError, ValueError, OSError) as error:
                    code = str(error)
                    record = {'market':ticker, 'history_minutes':n, 'status':'failed',
                              'error_code':code if re.fullmatch('[A-Z][A-Z0-9_]{3,80}', code) else 'MODEL_OR_DATA_FAILURE'}
                    archive.put('market', key, record)
            records.append(record)
            print(json.dumps({'event':'interval_origin', 'series':args.series, 'market':ticker,
                              'observed':n, 'forecast':15-n, 'status':record['status']}), flush=True)
    results = {str(n):compare([f for r in records if r['history_minutes'] == n for f in r.get('forecasts', [])],
                              tape, settlements, fee, int(time.time())) for n in ORIGINS}
    report = {'version':VERSION, 'configuration':config, 'coverage':catalog['coverage'],
        'complete':bool(catalog['markets']) and len(records) == len(catalog['markets'])*len(ORIGINS),
        'status_counts':dict(Counter(r['status'] for r in records)), 'origins':results,
        'paper_only':True, 'execution_verified':False,
        'limitations':['Historical runtime proxy, not live order evidence.',
            'Missing opening minutes are not replaced. Genuine flat windows are allowed.',
            'Fees use observed current series terms, not verified historical fills.',
            'Whole-contract 2.5x recovery until cycle P&L >= 0, cap100, independent per series/origin/strategy.',
            'No Toto. First minute uses three models; remaining origins use four.']}
    key = 'report-'+digest(report)[:32]; archive.put('report', key, report)
    print(json.dumps({'event':'interval_report', 'campaign_id':archive.id, 'report_key':key,
                      'series':args.series, 'complete':report['complete'], 'status_counts':report['status_counts']}), flush=True)
    if os.getenv('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'], 'a') as out:
            out.write('complete='+str(report['complete']).lower()+'\n')
    if records and not any(r.get('forecasts') for r in records):
        raise RuntimeError('NO_SUCCESSFUL_INTERVAL_FORECASTS_SEE_CHECKPOINT_REPORT')


if __name__ == '__main__':
    main()
