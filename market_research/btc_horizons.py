"""Checkpointed four-model BTC first-P90 hold study, first 3–12m → next 12–3m.

Uses the existing Kalshi adapter, numerical ensemble, minute limit simulation,
recovery sizing and encrypted private research storage. Historical replay clocks
are explicit assumptions and never promoted into the prospective live trackers.
"""
import argparse
from collections import Counter
from dataclasses import asdict
import json
import math
import os
from pathlib import Path
import re
import tempfile
import time

from .kalshi_btc import KalshiBTCProvider, SERIES
from .forecast import forecast_window
from .engine import digest, stamp, validate_forecast
from .p1_oco import QUANTILES
from .btc_hold_tracking import first_signals, first_signal_limits
from .btc_hold_sizing import replay
from .recovery_cloud import Campaign, encode_catalog, decode_catalog

VERSION = 'btc_four_model_horizon_hold_v1'
MODELS = ('prophet', 'granite', 'chronos', 'timesfm')
HISTORY_MINUTES = tuple(range(3, 13))
QUOTE_DELAY = 5
SETTLEMENT_DELAY = 60


def configuration(history, as_of, maximum, code):
    if type(history) is not int or history not in HISTORY_MINUTES:
        raise ValueError('BTC_HISTORY_MUST_BE_3_TO_12_MINUTES')
    if type(as_of) is not int or as_of <= 0 or as_of > time.time():
        raise ValueError('PAST_MARKET_CUTOFF_REQUIRED')
    if type(maximum) is not int or not 1 <= maximum <= 100:
        raise ValueError('ONE_TO_100_MARKETS_REQUIRED')
    if not re.fullmatch(r'[a-f0-9]{40}', code):
        raise ValueError('IMMUTABLE_CODE_SHA_REQUIRED')
    return dict(version=VERSION, history_minutes=history, horizon_minutes=15-history,
        market_close_cutoff=as_of, max_markets=maximum, code_sha=code, models=list(MODELS),
        raw_weights={m:1 for m in MODELS}, failure_policy='fail', quantiles=list(QUANTILES),
        quote_delay_seconds=QUOTE_DELAY, fallback_settlement_delay_seconds=SETTLEMENT_DELAY,
        paper_only=True, strategy='first_p90_hold_settlement', ladder=False)


class Checkpoints:
    """Only hashes/pointers in Firestore; inputs, forecasts and ledgers encrypted."""
    def __init__(self, config):
        self.config=config
        self.id='p90-'+digest(config)[:24]
        self.campaign=Campaign(self.id,'btc-horizons-'+os.getenv('GITHUB_RUN_ID','local'))
        ref=self.campaign.lease.ref
        existing=ref.get()
        if existing.exists:
            if existing.to_dict().get('configuration')!=config:
                raise ValueError('BTC_HORIZON_CONFIGURATION_CONFLICT')
        else:
            from google.api_core.exceptions import AlreadyExists
            try:ref.create({'configuration':config,'research_kind':config.get('version', VERSION),'paper_only':True})
            except AlreadyExists:
                if ref.get().to_dict().get('configuration')!=config:
                    raise ValueError('BTC_HORIZON_CONFIGURATION_CONFLICT')

    def ref(self, kind, key):
        if kind not in ('catalog','source','forecast','market','report') or not re.fullmatch(r'[A-Za-z0-9_-]{1,120}',key):
            raise ValueError('INVALID_BTC_HORIZON_RECORD')
        return self.campaign.lease.ref.collection('btc_horizon_'+kind).document(key)

    def get(self, kind, key):
        record=self.ref(kind,key).get()
        if not record.exists:return None
        data=record.to_dict()
        with tempfile.TemporaryDirectory(prefix='btc-horizon-read-') as folder:
            path=Path(folder)/'record.enc'
            self.campaign.download(data['archive'],path)
            value=decode_catalog(path)
        if digest(value)!=data['content_sha256']:raise ValueError('BTC_RECORD_HASH_MISMATCH')
        return value

    def put(self, kind, key, value):
        ref=self.ref(kind,key);existing=ref.get();checksum=digest(value)
        if existing.exists:
            if existing.to_dict()['content_sha256']!=checksum:raise ValueError('IMMUTABLE_BTC_RECORD_CONFLICT')
            return
        with tempfile.TemporaryDirectory(prefix='btc-horizon-write-') as folder:
            path=Path(folder)/'record.enc';encode_catalog(value,path)
            archive=self.campaign.upload(path,'catalog' if kind in ('catalog','source') else 'game')
            blob=self.campaign.bucket.blob(archive['object'],generation=int(archive['generation']))
            blob.reload()
            if blob.size!=archive['bytes']:raise ValueError('BTC_ARCHIVE_SIZE_MISMATCH')
        metadata={'archive':archive,'content_sha256':checksum,'code_sha':self.config['code_sha']}
        assert len(json.dumps(metadata))<4096
        from google.api_core.exceptions import AlreadyExists
        try:ref.create(metadata)
        except AlreadyExists:
            if ref.get().to_dict()['content_sha256']!=checksum:raise ValueError('IMMUTABLE_BTC_RECORD_CONFLICT')


def discover(provider, as_of, maximum):
    """Recent closed-market cohort, status-neutral time filter per Kalshi docs."""
    result=provider.get('/markets',{'series_ticker':SERIES,'max_close_ts':as_of,'limit':1000})
    eligible={m['ticker']:m for m in result.get('markets',[]) if provider.valid_market(m)
              and stamp(m['close_time'])<=as_of}
    ordered=sorted(eligible.values(),key=lambda m:(stamp(m['close_time']),m['ticker']),reverse=True)
    selected=list(reversed(ordered[:maximum]))
    return {'markets':selected,'provider':'kalshi','series':SERIES,'retrieved_at':int(time.time()),
        'coverage':{'requested_markets':maximum,'returned_markets':len(result.get('markets',[])),
            'eligible_markets':len(eligible),'selected_markets':len(selected),'bounded_recent_page':True,
            'upstream_has_more':bool(result.get('cursor')),'all_history_claimed':False},
        'selection':'Latest closed exact-15-minute markets in bounded current-tier page, before looking at result or history quality',
        'redistribution_status':'review_required'}


def first_window(quotes, opened, count):
    if count not in HISTORY_MINUTES:raise ValueError('BTC_HISTORY_MUST_BE_3_TO_12_MINUTES')
    indexed={}
    for q in quotes:
        if q.timestamp in indexed and indexed[q.timestamp]!=q:raise ValueError('CONFLICTING_HISTORY_MINUTE')
        if q.observed:indexed[q.timestamp]=q
    required=[opened+i*60 for i in range(1,count+1)]
    if any(t not in indexed for t in required):raise ValueError('MISSING_FIRST_N_COMPLETED_MINUTES')
    return [indexed[t] for t in required]


def validate_four_model_forecast(forecast, origin, horizon):
    validate_forecast(forecast, origin, horizon)
    actual={m.get('id',m.get('model')) for m in forecast.get('models',[]) if m.get('status')=='completed'}
    if (actual!=set(MODELS) or len(forecast.get('models',[]))!=len(MODELS)
            or forecast.get('failures')):
        raise ValueError('FOUR_SUCCESSFUL_MODELS_REQUIRED')
    weights=forecast.get('weights',{}).get('0.9',forecast.get('weights',{}).get('p90',{}))
    if set(weights)!=set(MODELS) or any(not math.isfinite(w) or abs(w-.25)>1e-8 for w in weights.values()):
        raise ValueError('EQUAL_FOUR_MODEL_P90_REQUIRED')
    if not math.isfinite(forecast['duration_seconds']) or forecast['duration_seconds']<0:
        raise ValueError('INVALID_INFERENCE_DURATION')


def process_market(market, config, archive, provider, forecaster=forecast_window):
    ticker=market['ticker'];prior=archive.get('market',ticker)
    if prior is not None:return prior
    source=archive.get('source',ticker)
    end,opened=stamp(market['close_time']),stamp(market['open_time'])
    if source is None:
        raw=provider.candles(market,end)
        # Recheck official market result; never infer settlement from last price.
        actual=provider.market(ticker)
        if actual.get('ticker')!=ticker:raise ValueError('KALSHI_MARKET_IDENTITY_MISMATCH')
        source={'market':market,'candles':raw,'resolution_market':actual,'retrieved_at':int(time.time()),
                'provider':'kalshi','redistribution_status':'review_required'}
        archive.put('source',ticker,source)
    if source['market']!=market:raise ValueError('FROZEN_MARKET_IDENTITY_CONFLICT')
    quotes=provider.quotes(source['candles'],market,end)
    n=config['history_minutes'];horizon=config['horizon_minutes'];origin=opened+n*60
    windows={s:first_window(q,opened,n) for s,q in quotes.items()}
    pair=[]
    for side in ('yes','no'):
        key=ticker+'-'+side;f=archive.get('forecast',key)
        if f is None:
            f=forecaster(windows[side],horizon,MODELS,QUANTILES,failure_policy='fail')
            validate_four_model_forecast(f,origin,horizon)
            f.update(market_context={'event_id':market['event_ticker'],'market_id':ticker,
                'contract_id':ticker+':'+side,'side':side,'provider':'kalshi'},
                strategy=VERSION,history_count=n,input_snapshot=[asdict(q) for q in windows[side]],
                source_hash=digest(source),execution_code_sha=config['code_sha'])
            f['forecast_id']=digest([f['forecast_id'],ticker,side,n,VERSION])
            archive.put('forecast',key,f)
        if (f['origin']!=origin or f['history_count']!=n or f['source_hash']!=digest(source)
                or f['input_snapshot']!=[asdict(q) for q in windows[side]]
                or f.get('execution_code_sha')!=config['code_sha']):
            raise ValueError('FROZEN_FORECAST_CONFIGURATION_CONFLICT')
        validate_four_model_forecast(f,origin,horizon)
        pair.append(f)
    # Both sides must exist before either side can generate an entry signal.
    latency=QUOTE_DELAY+max(1,math.ceil(sum(f['duration_seconds'] for f in pair)))
    pair=[{**f,'available_at':origin+latency,'publication_clock':'historical_measured_runtime_proxy'} for f in pair]
    tape=[{'game_id':market['event_ticker'],'contract_id':ticker+':'+side,**asdict(q),
        'received_at':q.timestamp+QUOTE_DELAY,'collection_mode':'historical',
        'receipt_clock':'assumed_5_seconds_after_completed_minute'} for side,rows in quotes.items() for q in rows]
    result_market=source['resolution_market'];outcomes={}
    if result_market.get('status') in ('settled','finalized') and result_market.get('result') in ('yes','no'):
        raw_settlement=result_market.get('settlement_ts')
        try:
            confirmation=(int(raw_settlement) if type(raw_settlement) in (int,float)
                          else stamp(raw_settlement)) if raw_settlement is not None else None
        except (ValueError,TypeError):confirmation=None
        official_confirmation=confirmation is not None and confirmation>=end
        confirmation=max(end+SETTLEMENT_DELAY,confirmation or 0)
        for side in ('yes','no'):
            outcomes[ticker+':'+side]={'resolution_status':'resolved','settled_at':end,
                'selected_side_payout':int(side==result_market['result']),'first_confirmed_at':confirmation,
                'confirmation_clock':'official_settlement_ts_with_60s_floor' if official_confirmation else 'assumed_60s_after_close'}
    signals,ambiguous=first_signals(pair,tape,source['retrieved_at'])
    trades,orders=first_signal_limits(signals,tape,pair,outcomes,source['retrieved_at'])
    record={'market':ticker,'origin':origin,'history_minutes':n,'horizon_minutes':horizon,
        'status':'missed_deadline' if origin+latency>=end else 'evaluated',
        'forecasts':pair,'source_hash':digest(source),'signals':signals,'trades':trades,'orders':orders,
        'outcomes':outcomes,'publication_latency_seconds':latency,'ambiguous_signal_minutes':ambiguous,
        'history_observations':{s:len(q) for s,q in quotes.items()},'resolution_verified':bool(outcomes),
        'paper_only':True,'live_execution_verified':False}
    archive.put('market',ticker,record)
    return record


def summarize(results, catalog, config):
    trades=[t for r in results for t in r.get('trades',[])]
    confirmations={s:o['first_confirmed_at'] for r in results for s,o in r.get('outcomes',{}).items()}
    analysis_asof=int(time.time())
    complete=len(results)==len(catalog['markets'])
    usable=sum(r['status']=='evaluated' and r.get('resolution_verified',False) for r in results)
    return {'version':VERSION,'configuration':config,'coverage':catalog['coverage'],
        'attempted_markets':len(results),'planned_markets':len(catalog['markets']),'complete':complete,
        'status_counts':dict(Counter(r['status'] for r in results)),
        'usable_market_count':usable,'usable_fraction':usable/len(catalog['markets']) if catalog['markets'] else None,
        'ranking_valid_for_full_cohort':bool(catalog['markets']) and complete and usable==len(catalog['markets']),
        'forecast_count':sum(len(r.get('forecasts',[])) for r in results),
        'first_p90_signals':sum(len(r.get('signals',[])) for r in results),
        'scenarios':{p:replay(trades,confirmations,as_of=analysis_asof,policy=p,quantity_step=1.)
                     for p in ('fixed_one','recover_cycle')},
        'order_counts':dict(Counter(o['status'] for r in results for o in r.get('orders',[]))),
        'market_statuses':[{k:v for k,v in r.items() if k in ('market','origin','status','error_type','error_code','publication_latency_seconds','resolution_verified')}
                           for r in results],
        'limitations':['Historical replay, not live quote or order evidence; candle revisions may exist.',
            'Five-second quote availability and at-least-60-second settlement notification are explicit simulation assumptions.',
            'Measured sequential inference time delays both sides; missed deadlines remain visible.',
            'Post-only trade-through candidates do not verify queue, depth or fills; no forced orders.',
            'One first-P90 entry then settlement only; no ladder, P10 stop or opposite-side switch.',
            '1% entry notional fee assumption; no settlement sell fee; not verified maker fees.',
            'Whole-contract 2.5x recovery across markets can magnify losses; cap100 does not ensure recovery.',
            'Return is net closed P&L / cumulative entry notional, not starting-bankroll ROI.',
            'Different horizons can fail on different histories; compare common eligible markets before ranking.',
            '3–12 observations do not establish forecasting reliability.']}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--history-minutes',type=int,choices=HISTORY_MINUTES,required=True)
    parser.add_argument('--as-of',type=int,required=True)
    parser.add_argument('--max-markets',type=int,default=50)
    parser.add_argument('--budget-minutes',type=int,default=270)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if not 1<=args.budget_minutes<=270:parser.error('INVALID_WALL_CLOCK_BUDGET')
    config=configuration(args.history_minutes,args.as_of,args.max_markets,os.environ.get('QUANTURA_CODE_SHA',''))
    archive=Checkpoints(config);provider=KalshiBTCProvider()
    print(json.dumps({'event':'btc_horizon_started','campaign_id':archive.id,'configuration':config}),flush=True)
    catalog=archive.get('catalog','markets')
    if catalog is None:
        catalog=discover(provider,args.as_of,args.max_markets)
        if not catalog['markets']:raise RuntimeError('NO_RECENT_CLOSED_BTC_MARKETS')
        archive.put('catalog','markets',catalog)
    deadline=time.monotonic()+args.budget_minutes*60;results=[]
    args.output.mkdir(parents=True,exist_ok=True,mode=0o700)
    try:
        for market in catalog['markets']:
            record=archive.get('market',market['ticker'])
            if record is None and time.monotonic()>=deadline:break
            if record is None:
                try:record=process_market(market,config,archive,provider)
                except (ValueError,RuntimeError,OSError) as error:
                    # Configuration/provider/model failures are explicit, not silently reduced ensembles.
                    code=str(error)
                    # Only machine identifiers may leave an exception; no paths, payloads or tokens.
                    safe_code=code if re.fullmatch(r'[A-Z][A-Z0-9_]{3,80}',code) else 'RESEARCH_STEP_FAILED'
                    record={'market':market['ticker'],'status':'failed','error_type':type(error).__name__,'error_code':safe_code}
                    archive.put('market',market['ticker'],record)
            results.append(record)
            print(json.dumps({'event':'btc_horizon_market','history_minutes':args.history_minutes,
                'market':market['ticker'],'status':record['status'],'attempted':len(results),
                'planned':len(catalog['markets'])}),flush=True)
    finally:
        report=summarize(results,catalog,config);key='report-'+digest(report)[:32]
        archive.put('report',key,report)
        encode_catalog({'campaign_id':archive.id,'report_key':key,'report':report},args.output/(key+'.qrc.enc'))
        print(json.dumps({'event':'btc_horizon_report','campaign_id':archive.id,'report_key':key,
            **{k:report[k] for k in ('complete','attempted_markets','planned_markets','status_counts','forecast_count')},
            'scenarios':{k:v['summary'] for k,v in report['scenarios'].items()}}),flush=True)
        if os.environ.get('GITHUB_OUTPUT'):
            with open(os.environ['GITHUB_OUTPUT'],'a') as out:
                out.write(f"campaign_id={archive.id}\ncomplete={str(report['complete']).lower()}\n")
    if not report['forecast_count']:raise RuntimeError('NO_FOUR_MODEL_BTC_FORECASTS')


if __name__=='__main__':main()
