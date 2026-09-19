"""Resumable all-origin paper research from the existing paired-minute archive.

No provider order calls, no new quote backfills, and no model outputs in
Firestore. All large records use the existing encrypted, immutable transport.
"""
import argparse
from collections import Counter
from dataclasses import asdict
import heapq
import json
import math
import os
from pathlib import Path
import re
import tempfile
import time

from .btc_horizons import Checkpoints
from .btc_hold_tracking import first_signals
from .btc_sticky_tracking import direction_at, simulate, taker_fee
from .engine import Quote, digest, stamp
from .interval_markets import KalshiIntervalProvider
from .interval_studies import ORIGINS, configuration, forecast_origin, first_p10_signals
from .recovery_cloud import Campaign, decode_catalog
from .recovery_switch import statistics
from ensemble_forecasting.adapters.base import ModelExecutionError

VERSION = 'interval_archived_minutes_p90_sticky_v2'
COLLECTOR_VERSION = 'interval_paired_minute_collection_v1'
SERIES = ('KXBTC15M', 'KXBNB15M', 'KXCOPPER15M', 'KXDOGE15M', 'KXETH15M',
          'KXGOLD15M', 'KXHYPE15M', 'KXNATGAS15M', 'KXNEAR15M', 'KXSILVER15M',
          'KXSOL15M', 'KXWTI15M', 'KXXRP15M', 'KXZEC15M')


def collector_id(series):
    KalshiIntervalProvider(series)
    return 'p90-' + digest({'version':COLLECTOR_VERSION, 'series':series,
                           'code_sha':'collector-schema-v1'})[:24]


class ReadArchive:
    """Read-only: never constructs Checkpoints/creates a collector campaign."""
    def __init__(self, series):
        self.cloud = Campaign(collector_id(series), 'read-interval-archive')

    def manifest(self):
        return [{'key':d.id, **d.to_dict()} for d in
                self.cloud.lease.ref.collection('btc_horizon_source').stream()
                if not re.search(r'-[a-f0-9]{12}$', d.id)]

    def read(self, entry):
        with tempfile.TemporaryDirectory(prefix='interval-source-') as folder:
            path = Path(folder)/'source.enc'
            self.cloud.download(entry['archive'], path)
            value = decode_catalog(path)
        if digest(value) != entry['content_sha256']:
            raise ValueError('ARCHIVE_SOURCE_HASH_MISMATCH')
        if value['market']['ticker'] != entry['key']:
            raise ValueError('ARCHIVE_MARKET_IDENTITY_MISMATCH')
        return value


class PairedProvider(KalshiIntervalProvider):
    """The common forecast adapter consumes these genuine candle-close quotes."""
    def quotes(self, raw, market, end):
        out = {'yes':[], 'no':[]}
        seen = set()
        opened = stamp(market['open_time'])
        for row in sorted(raw, key=lambda r:r['timestamp']):
            t, received = row['timestamp'], row['received_at']
            if row.get('market_id') != market['ticker'] or type(t) is not int or t % 60:
                raise ValueError('INVALID_ARCHIVED_MINUTE_IDENTITY')
            if t in seen:
                raise ValueError('DUPLICATE_ARCHIVED_MINUTE')
            seen.add(t)
            # Real delayed first receipts are valid HISTORY, but not timely
            # trade observations. forecast_origin delays publication until all
            # inputs were actually received. Never substitute revised values.
            if not (opened < t <= end and row.get('collection_mode') == 'live'
                    and type(received) is int and t <= received):
                continue
            for side in out:
                out[side].append(Quote(t, row[side+'_ask'], row[side+'_bid']))
        return out


def inputs(record, provider):
    market = record['market']
    if not provider.valid_market(market):
        raise ValueError('INVALID_ARCHIVED_MARKET')
    minutes = record['records']['btc_minutes']
    data = provider.quotes(minutes, market, stamp(market['close_time']))
    receipts = {r['timestamp']:r['received_at'] for r in minutes}
    tape = [{**asdict(q), 'contract_id':market['ticker']+':'+side,
             'game_id':market['event_ticker'], 'received_at':receipts[q.timestamp],
             'collection_mode':'live'} for side, rows in data.items() for q in rows
            if receipts[q.timestamp] <= q.timestamp+30]
    source = {'market':market, 'candles':minutes, 'minute_receipts':
              {str(q.timestamp):receipts[q.timestamp] for q in data['yes']},
              'collector_record_hash':digest(record), 'format':'archived_paired_candle_closes',
              'redistribution_status':'review_required'}
    return source, tape


def select_sources(records, as_of, maximum):
    """Settlement seeds inform direction, never inflate forecast denominators."""
    closed = [r for r in records if r['lifecycle']['close_at'] <= as_of]
    eligible = [r for r in closed if r['records'].get('btc_minutes')]
    settlements = {(s['market_id'],s['first_confirmed_at']):s for r in closed
                   for s in r['records'].get('btc_settlements', [])}
    selected = sorted(eligible, key=lambda r:(r['lifecycle']['close_at'],r['market']['ticker']))[-maximum:]
    coverage = {'closed_source_records':len(closed), 'quote_bearing_markets':len(eligible),
                'settlement_only_records':len(closed)-len(eligible)}
    return selected, list(settlements.values()), coverage


def failure_record(ticker, n, error):
    """Keep structured causes, never serialize exception text/credentials."""
    if isinstance(error, ModelExecutionError):
        code, model, retryable = error.code, error.model, error.retryable
    else:
        code = str(error)
        code = code if re.fullmatch('[A-Z][A-Z0-9_]{3,80}',code) else 'MODEL_OR_DATA_FAILURE'
        model, retryable = None, False
    return {'market':ticker, 'history_minutes':n,
            'status':'skipped_missing_history' if code == 'MISSING_FIRST_N_COMPLETED_MINUTES' else 'failed',
            'error_code':code, 'model':model, 'retryable':retryable, 'paper_only':True}


def signals_for(pair, tape, settlements, as_of):
    p90, ambiguous = first_signals(pair, tape, as_of)
    sticky = []
    for signal in p90:
        d = direction_at(settlements, signal['signal_received_at'], signal['market_id'])
        if d and signal['contract_id'].endswith(':'+d['side']):
            sticky.append({**signal, 'direction':d})
    p10, _ = first_p10_signals(pair, tape, as_of)
    return {'first_p90':p90, 'p90_sticky':sticky, 'first_quote_below_p10':p10}, ambiguous


def disagreement(entry, records, tape, settlements, cutoff, mode):
    """Newly published origins 3..cutoff, first subsequent complete quote only.

    opposite_p90: explicit opposite P90, neutral does not exit.
    no_longer_agrees: neutral/ambiguous/opposite/no sticky confirmation exits.
    Neither pretends an opposite P90 must agree with an unchanged sticky state.
    """
    if mode not in ('opposite_p90', 'no_longer_agrees') or cutoff not in range(3,12):
        raise ValueError('INVALID_DISAGREEMENT_POLICY')
    index = {(q['contract_id'],q['timestamp']):q for q in tape}
    candidates = []
    for record in records:
        n = record['history_minutes']; pair = record.get('forecasts', [])
        if record.get('market') != entry['market_id'] or not 3 <= n <= cutoff or len(pair) != 2:
            continue
        published = max(f['available_at'] for f in pair)
        # A forecast already available at entry is not a later change of view.
        if published <= entry['entry_at']:
            continue
        t = published//60*60+60
        if t >= entry['market_end']:
            continue
        sides = []; receipts = []; missing = False
        for f in pair:
            side = f['market_context']['contract_id']
            q = index.get((side,t))
            curve = next((r['quantiles'] for r in f['rows'] if r['timestamp']==t), None)
            if not q or curve is None or not t <= q['received_at'] <= t+30:
                missing = True; break
            receipts.append(q['received_at'])
            if q['bid'] >= curve['0.9']:
                sides.append(side)
        if missing:
            continue
        d = direction_at(settlements, max(receipts), entry['market_id'])
        agrees = (sides == [entry['contract_id']] and d is not None
                  and entry['contract_id'].endswith(':'+d['side']))
        opposite = len(sides)==1 and sides[0] != entry['contract_id']
        if (mode=='opposite_p90' and not opposite) or (mode=='no_longer_agrees' and agrees):
            continue
        q = index.get((entry['contract_id'],t+60))
        if (not q or not max(receipts)<q['received_at']<entry['market_end']
                or not t+60 <= q['received_at'] <= t+90):
            continue
        candidates.append({'exit_at':q['received_at'], 'exit_price':q['bid'],
            'outcome_confirmed_at':q['received_at'], 'exit_reason':mode,
            'exit_signal_at':t, 'exit_origin_minutes':n, 'exit_signal_sides':sides,
            'neutral_or_ambiguous':len(sides)!=1})
    return min(candidates,key=lambda r:(r['exit_at'],r['exit_origin_minutes']),default=None)


def price_exits(entries, decisions, policy, fee):
    """Same paired entries, recompute recovery using each variant's realized P&L."""
    pending=[]; size=1; cycle=0.; trades=[]
    def recognize(at):
        nonlocal size,cycle
        while pending and pending[0][0] < at:
            _,_,net=heapq.heappop(pending); cycle+=net
            if cycle >= -1e-10: cycle,size=0.,1
            elif net < 0: size=min(100,math.floor(size*2.5))
    for original in sorted(entries,key=lambda r:(r['entry_at'],r['market_id'])):
        if original['status']!='closed': continue
        recognize(original['entry_at'])
        t={**original,**decisions.get(original['market_id'],{})}
        q=size if policy=='recover_cycle' else 1
        entry_fee=taker_fee(q,t['entry_price'],'0.0001',fee)
        exit_fee=(taker_fee(q,t['exit_price'],'0.0001',fee)
                  if t['exit_reason']!='authoritative_settlement' and 0<t['exit_price']<1 else 0)
        gross=q*(t['exit_price']-t['entry_price'])
        t.update(quantity=q,gross_pnl=gross,fees=entry_fee+exit_fee,
                 net_pnl=gross-entry_fee-exit_fee)
        # Don't carry stale unit-fill/recovery accounting into resized variants.
        for field in ('fills','known_pnl_before_entry','next_recovery_size',
                      'recovery_cycle_pnl','confirmed_cumulative_pnl'):
            t.pop(field,None)
        heapq.heappush(pending,(t['outcome_confirmed_at'],t['market_id'],t['net_pnl']))
        trades.append(t)
    return {'trades':trades,'summary':statistics(trades)}


def make_report(config, games, settlements, fee, selected, as_of):
    tape=[q for g in games for q in g['tape']]
    origins={}; unknown_fee=not (fee.get('fee_type') in ('quadratic','quadratic_with_maker_fees')
        and type(fee.get('multiplier')) in (int,float) and math.isfinite(fee['multiplier']) and fee['multiplier']>=0)
    for n in ORIGINS:
        by_strategy={k:[s for g in games for s in g['signals'].get(str(n),{}).get(k,[])]
                     for k in ('first_p90','p90_sticky','first_quote_below_p10')}
        origins[str(n)]={}
        for k,signals in by_strategy.items():
            scenarios={p:simulate(signals,tape,settlements,as_of=as_of,policy=p,
                precision='0.0001',multiplier=0 if unknown_fee else fee['multiplier'])
                for p in ('fixed_one','recover_cycle')}
            if unknown_fee:
                scenarios={'gross_only_no_fee_estimate':scenarios['fixed_one']}
            origins[str(n)][k]=scenarios
    exits={}
    baseline=origins['2']['p90_sticky'].get('fixed_one')
    if baseline:
        for mode in ('opposite_p90','no_longer_agrees'):
            exits[mode]={}
            for cutoff in range(3,12):
                decisions={market:d for g in games for market,d in
                           g['exits'].get(mode,{}).get(str(cutoff),{}).items()}
                exits[mode][str(cutoff)]={p:price_exits(baseline['trades'],decisions,p,fee['multiplier'])
                                         for p in ('fixed_one','recover_cycle')}
    return {'version':VERSION,'configuration':config,'as_of':as_of,'complete':len(games)==selected,
        'coverage':{'selected_markets':selected,'analyzed_markets':len(games),
            'paired_minute_rows_used':len(tape)//2,
            'origin_status_counts':dict(Counter(s for g in games for s in g['statuses'].values()))},
        'origins':origins,'early_exit_after_two_minutes':exits,'fee_policy':fee,
        'fee_status':'unknown_gross_only' if unknown_fee else 'current_schedule_sensitivity',
        'paper_only':True,'execution_verified':False,
        'limitations':['Models fit retrospectively; measured runtime plus stored receipt is a publication proxy, not live inference evidence.',
            'Genuine first-received history, including delayed inputs with publication delayed accordingly. Only timely minute bid/ask observations may trigger simulated trades; no gap filling.',
            'One minute uses three models; 2..12 use four; no Toto; equal per-quantile capability weights.',
            'First unambiguous bid >= time-aligned P90; not necessarily a fresh crossing from below.',
            'Next-minute ask entry, next-minute bid early exit; depth and actual fills are unverified.',
            'Late-origin results overlap the same markets; they are not independent samples.',
            'Fee schedule sensitivity, not verified historical partial-fill fees.']}


def save_report(archive, report):
    """Bound each encrypted blob; a long cohort never becomes one giant artifact."""
    compact={**report,'origins':{},'early_exit_after_two_minutes':{}}
    for n, result in report['origins'].items():
        key='origin-'+n+'-'+digest(result)[:32]
        archive.put('report',key,result)
        compact['origins'][n]={strategy:{policy:{'summary':v['summary'],'report_key':key}
            for policy,v in policies.items()} for strategy,policies in result.items()}
    for mode, cutoffs in report['early_exit_after_two_minutes'].items():
        compact['early_exit_after_two_minutes'][mode]={}
        for n,result in cutoffs.items():
            key='exit-'+mode+'-'+n+'-'+digest(result)[:32]
            archive.put('report',key,result)
            compact['early_exit_after_two_minutes'][mode][n]={p:{'summary':v['summary'],'report_key':key}
                                                          for p,v in result.items()}
    key='report-'+digest(compact)[:32];archive.put('report',key,compact)
    return key,compact


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--series',required=True);p.add_argument('--as-of',type=int,required=True)
    p.add_argument('--max-markets',type=int,default=1000);p.add_argument('--budget-minutes',type=int,default=270)
    a=p.parse_args()
    if not 1<=a.max_markets<=1000 or not 1<=a.budget_minutes<=270: p.error('INVALID_BOUNDS')
    config=configuration(a.series,a.as_of,min(a.max_markets,100),os.environ['QUANTURA_CODE_SHA'])
    config.update(version=VERSION,max_markets=a.max_markets,source='private_minute_archive',
                  collector_campaign=collector_id(a.series),exit='settlement_and_later_origin_disagreement')
    archive=Checkpoints(config); source_reader=ReadArchive(a.series); provider=PairedProvider(a.series)
    catalog=archive.get('catalog','collector-sources')
    if catalog is None:
        # Freeze exact immutable generations. Later additions cannot change a resumed cohort.
        catalog={'sources':sorted(source_reader.manifest(),key=lambda x:x['key']), 'as_of':a.as_of}
        archive.put('catalog','collector-sources',catalog)
    deadline=time.monotonic()+a.budget_minutes*60
    all_sources=[]
    for entry in catalog['sources']:
        all_sources.append(source_reader.read(entry))
    source_records,settlements,source_coverage=select_sources(all_sources,a.as_of,a.max_markets)
    games=[]; last_fee={}
    for raw in source_records:
        ticker=raw['market']['ticker']; last_fee=raw.get('fee_policy') or last_fee
        saved=archive.get('report','game-'+ticker)
        if saved is not None:
            games.append(saved); continue
        if time.monotonic()>=deadline: break
        records=[]; by_origin={};statuses={}; input_error=None
        try:
            source,tape=inputs(raw,provider)
        except (ValueError,RuntimeError,KeyError,TypeError) as error:
            source,tape={},[]; input_error=error
        for n in ORIGINS:
            if time.monotonic()>=deadline: break
            key=ticker+'-n'+str(n); record=archive.get('market',key)
            if record is None:
                try:
                    if input_error is not None: raise input_error
                    record=forecast_origin(raw['market'],n,source,config,archive,provider)
                except (ValueError,RuntimeError,OSError,KeyError,TypeError) as error:
                    record=failure_record(ticker,n,error)
                    archive.put('market',key,record)
            records.append(record); statuses[str(n)]=record['status']
            by_origin[str(n)],_=signals_for(record.get('forecasts',[]),tape,settlements,a.as_of)
            print(json.dumps({'event':'archived_interval_origin','series':a.series,'market':ticker,
                'observed':n,'status':record['status'],'error_code':record.get('error_code'),
                'model':record.get('model'),'sticky_signals':len(by_origin[str(n)]['p90_sticky'])}),flush=True)
        if len(records)!=len(ORIGINS): break
        baseline=simulate(by_origin['2']['p90_sticky'],tape,settlements,as_of=a.as_of,
                          policy='fixed_one',precision='0.0001',multiplier=0)
        exits={mode:{str(n):{t['market_id']:d for t in baseline['trades']
                  if (d:=disagreement(t,records,tape,settlements,n,mode)) is not None}
                  for n in range(3,12)} for mode in ('opposite_p90','no_longer_agrees')}
        game={'market_id':ticker,'tape':tape,'signals':by_origin,'exits':exits,'statuses':statuses,
              'source_hash':digest(raw)}
        archive.put('report','game-'+ticker,game); games.append(game)
    report=make_report(config,games,settlements,last_fee,len(source_records),a.as_of)
    report['coverage'].update(source_coverage)
    if not source_records: raise RuntimeError('NO_ARCHIVED_MARKETS_BEFORE_CUTOFF')
    key,report=save_report(archive,report)
    print(json.dumps({'event':'archive_strategy_report','campaign_id':archive.id,'report_key':key,
        'series':a.series,'complete':report['complete'],'coverage':report['coverage'],
        'sticky':{n:{p:v['summary'] for p,v in s['p90_sticky'].items()} for n,s in report['origins'].items()}}),flush=True)
    if os.getenv('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'],'a') as out: out.write('complete='+str(report['complete']).lower()+'\n')


if __name__=='__main__':main()
