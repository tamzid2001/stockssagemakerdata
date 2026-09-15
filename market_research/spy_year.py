"""Resumable 52-week SPY option-target research using existing five-model services.

Encrypted, immutable source/month and week objects live in the private research
bucket. Firestore holds only small content-hash/pointer records, never prices or
forecast arrays. Identical configuration resumes rather than overwriting history.
"""
import argparse
from datetime import date, timedelta
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
import time

import httpx
import pandas as pd
import pandas_market_calendars as mcal

from .spy_weekly import aggregate_minutes, digest, forecast_week, iso, session_grid, simulate
from .spy_diagnostics import diagnose
from .spy_exit_sweep import load_archive
from .spy_option_targets import MODES, prices_by_minute, summarize, target_comparison
from .spy_options import contract
from .recovery_cloud import Campaign, encode_catalog, decode_catalog
from .spy_touches import average_touches, crossing_excursions, summarize_touches, averages

VERSION='spy_year_average_targets_v1'


def windows(end_friday, weeks=52):
    end=date.fromisoformat(end_friday)
    if end.weekday()!=4 or type(weeks) is not int or not 1<=weeks<=52:
        raise ValueError('ONE_TO_52_FRIDAY_ANCHORED_WEEKS_REQUIRED')
    first=end-timedelta(weeks=weeks)
    calendar=mcal.get_calendar('NYSE').schedule(start_date=first-timedelta(days=6),end_date=end)
    anchors=[]
    for i in range(weeks+1):
        nominal=first+timedelta(weeks=i)
        eligible=calendar.loc[str(nominal-timedelta(days=6)):str(nominal)]
        if eligible.empty:raise ValueError('NO_WEEKLY_EXCHANGE_SESSION')
        last=eligible.iloc[-1]
        anchors.append({'nominal_friday':nominal.isoformat(),'actual_close':iso(last.market_close),
                        'holiday_shifted':eligible.index[-1].date()!=nominal})
    return [{'index':i,'origin':a['actual_close'],'week_end':b['actual_close'],
             'nominal_origin_friday':a['nominal_friday'],'nominal_end_friday':b['nominal_friday'],
             'origin_holiday_shifted':a['holiday_shifted'],'expiry_holiday_shifted':b['holiday_shifted']}
            for i,(a,b) in enumerate(zip(anchors,anchors[1:]))]


def date_chunks(start,end):
    """Non-overlapping at-most-28-day requests cannot hit the 50,000-row cap."""
    current=date.fromisoformat(start);last=date.fromisoformat(end)
    while current<=last:
        stop=min(last,current+timedelta(days=27))
        yield current.isoformat(),stop.isoformat()
        current=stop+timedelta(days=1)


def merge_sources(chunks,seed_rows):
    by_time={}
    for chunk in chunks:
        for row in chunk['rows']:
            t=iso(pd.to_datetime(row['timestamp'],utc=True))
            if t in by_time and by_time[t]!=row:raise ValueError('CONFLICTING_YEAR_SOURCE_ROW')
            by_time[t]=row
    # Published prior-study snapshot is authoritative over newly fetched overlap.
    for row in seed_rows:
        by_time[iso(pd.to_datetime(row['timestamp'],utc=True))]=row
    return [by_time[t] for t in sorted(by_time)]


class Checkpoints:
    def __init__(self,configuration):
        self.configuration=configuration
        self.id='p90-'+digest(['spy-year',configuration])[:24]
        self.campaign=Campaign(self.id,'spy-year-'+os.getenv('GITHUB_RUN_ID','local'))
        ref=self.campaign.lease.ref
        existing=ref.get()
        if existing.exists:
            if existing.to_dict().get('configuration')!=configuration:raise ValueError('YEAR_CONFIGURATION_CONFLICT')
        else:
            from google.api_core.exceptions import AlreadyExists
            try:ref.create({'configuration':configuration,'research_kind':'spy-year','paper_only':True})
            except AlreadyExists:
                if ref.get().to_dict().get('configuration')!=configuration:raise ValueError('YEAR_CONFIGURATION_CONFLICT')

    @classmethod
    def resume(cls,identifier,code,end_friday,weeks):
        campaign=Campaign(identifier,'spy-year-resume-'+os.getenv('GITHUB_RUN_ID','local'))
        data=campaign.lease.ref.get().to_dict() or {}
        config=data.get('configuration',{})
        if data.get('research_kind')!='spy-year' or config.get('code_sha')!=code or config.get('end_friday')!=end_friday or config.get('weeks')!=weeks:
            raise ValueError('YEAR_RESUME_REQUIRES_ORIGINAL_CODE_AND_CONFIG')
        return cls(config)

    def reference(self,kind,key):
        if kind not in ('source','week','stage','report') or not re.fullmatch(r'[A-Za-z0-9_-]{1,100}',key):
            raise ValueError('INVALID_YEAR_CHECKPOINT_KEY')
        return self.campaign.lease.ref.collection('spy_'+kind).document(key)

    def get(self,kind,key):
        record=self.reference(kind,key).get()
        if not record.exists:return None
        pointer=record.to_dict()
        with tempfile.TemporaryDirectory(prefix='spy-year-restore-') as directory:
            path=Path(directory)/'record.enc'
            self.campaign.download(pointer['archive'],path)
            value=decode_catalog(path)
        if digest(value)!=pointer['content_sha256']:raise ValueError('YEAR_CONTENT_HASH_MISMATCH')
        return value

    def put(self,kind,key,value):
        ref=self.reference(kind,key);existing=ref.get()
        checksum=digest(value)
        if existing.exists:
            if existing.to_dict()['content_sha256']!=checksum:raise ValueError('IMMUTABLE_YEAR_RECORD_CONFLICT')
            return
        with tempfile.TemporaryDirectory(prefix='spy-year-save-') as directory:
            path=Path(directory)/'record.enc';encode_catalog(value,path)
            archive=self.campaign.upload(path,'catalog' if kind=='source' else 'game')
            # Upload completed before index publication; readers pin generation.
            blob=self.campaign.bucket.blob(archive['object'],generation=int(archive['generation']))
            blob.reload()
            if blob.size!=archive['bytes']:raise ValueError('YEAR_ARCHIVE_SIZE_MISMATCH')
        metadata={'archive':archive,'content_sha256':checksum,'code_sha':self.configuration['code_sha'],
                  'created_at':iso(pd.Timestamp.now(tz='UTC'))}
        assert len(json.dumps(metadata))<4096
        from google.api_core.exceptions import AlreadyExists
        try:ref.create(metadata)
        except AlreadyExists:
            if ref.get().to_dict()['content_sha256']!=checksum:raise ValueError('IMMUTABLE_YEAR_RECORD_CONFLICT')


def fetch(client,route,request,*,post=False,attempts=4):
    for attempt in range(attempts):
        try:
            response=client.post('https://quantura.studio/api/'+route,json=request) if post else client.get('https://quantura.studio/api/'+route,params=request)
        except httpx.TransportError:
            if attempt==attempts-1:raise RuntimeError('YEAR_DATA_TRANSPORT_FAILED') from None
            time.sleep(2**attempt);continue
        if response.status_code in (429,500,502,503,504):
            if attempt==attempts-1:raise RuntimeError('YEAR_PROVIDER_TEMPORARILY_UNAVAILABLE')
            time.sleep(2**attempt);continue
        if response.status_code==404:return None
        if response.status_code!=200:raise RuntimeError(f'YEAR_DATA_HTTP_{response.status_code}')
        data=response.json()
        if not data.get('ok') or data.get('provider')!='alpaca' or data.get('fallbackUsed'):
            raise ValueError('UNEXPECTED_YEAR_SOURCE')
        data.update({'request':request,'retrieved_at':iso(pd.Timestamp.now(tz='UTC')),'redistribution_status':'review_required'})
        return data


def yearly_summary(results,planned):
    ordered=sorted(results,key=lambda r:r['origin'])
    report={'attempted_weeks':len(ordered),'planned_weeks':planned,'complete':len(ordered)==planned,
            'all_weeks':summarize(ordered),
            'additional_weeks':summarize([r for r in ordered if not r.get('seed_forecast_reused')]),
            'previously_tested_weeks':summarize([r for r in ordered if r.get('seed_forecast_reused')]),
            'average_quantile_touch_rates':summarize_touches(ordered),'chronology':{}}
    for scenario in report['all_weeks']:
        equity=peak=drawdown=0.;winning=losing=longest_win=longest_loss=0;chronology=[]
        for r in ordered:
            s=r.get('scenarios',{}).get(scenario,{})
            pnl=s.get('net_pnl_proxy_before_costs')
            if s.get('status')!='priced_trade_bar_proxy':
                winning=losing=0
                chronology.append({'origin':r['origin'],'expiry':r['expiry'],'pnl':None,'status':s.get('status','unavailable')});continue
            equity+=pnl;peak=max(peak,equity);drawdown=max(drawdown,peak-equity)
            winning=winning+1 if pnl>0 else 0;losing=losing+1 if pnl<0 else 0
            longest_win=max(longest_win,winning);longest_loss=max(longest_loss,losing)
            chronology.append({'origin':r['origin'],'expiry':r['expiry'],'pnl':pnl,'cumulative_pnl':equity,
                'premiums':s['entry_premiums_usd'],'seed_forecast_reused':r.get('seed_forecast_reused',False)})
        report['chronology'][scenario]={'weeks':chronology,'longest_winning_streak':longest_win,
            'longest_losing_streak':longest_loss,'closed_week_marked_drawdown_usd':drawdown,
            'drawdown_method':'closed weekly pairs only; not intraday option mark-to-market'}
    return report


def process_week(w,bars,minute_rows,grid,seed,store,client):
    key=f"week-{w['index']:02d}";previous=store.get('week',key)
    if previous:return previous
    history=[b for b in bars if b['timestamp']<=w['origin']][-500:]
    future=[g for g in grid if w['origin']<g['timestamp']<=w['week_end']]
    if len(history)!=500 or history[-1]['timestamp']!=w['origin']:
        raise ValueError('YEAR_HISTORY_500_COMPLETE_HOURS_REQUIRED')
    saved=seed.get(w['origin'])
    f=store.get('stage',key+'-forecast')
    if f is None:
        if saved:
            if digest(history)!=saved['training_sha256'] or saved['future_grid']!=future:
                raise ValueError('PRIOR_STUDY_FORECAST_INPUT_CHANGED')
            f=saved
        else:
            f={**forecast_week(history,future),'week_end':w['week_end'],'future_grid':future}
        store.put('stage',key+'-forecast',f)
    if f['training_sha256']!=digest(history):raise ValueError('YEAR_TRAINING_HASH_CONFLICT')
    if {m['model_id'] for m in f['model_runs']}!={'prophet','toto','granite','chronos','timesfm'}:
        raise ValueError('FIVE_MODELS_REQUIRED')
    selected=[r for r in minute_rows if w['origin']<=r['timestamp']<w['week_end']]
    diagnostic=diagnose(bars,[f]);week=diagnostic['weeks'][0]
    legs={};contracts={};errors=[]
    for leg,kind,q in (('call','C','0.9'),('put','P','0.1')):
        symbol,strike=contract(w['week_end'],kind,week['average_quantiles'][q])
        contracts[leg]={'symbol':symbol,'strike':strike,'average_target':week['average_quantiles'][q]}
        data=store.get('stage',key+'-'+leg)
        if data is None:
            request={'source':'alpaca','contractSymbol':symbol,'timeframe':'1Min','limit':5000,
                     'start':w['origin'],'end':iso(pd.Timestamp(w['week_end'])-pd.Timedelta(minutes=1))}
            data=fetch(client,'market-data/options/history',request,post=True)
            if data is None:
                errors.append({'contract':symbol,'code':'HISTORICAL_CONTRACT_UNAVAILABLE'});continue
            if data.get('contractSymbol')!=symbol or len(data['rows'])>=5000:
                raise ValueError('INVALID_YEAR_OPTION_PATH')
            prices_by_minute(data);store.put('stage',key+'-'+leg,data)
        legs[leg]=data
    scenarios={}
    if not errors:
        origin=pd.Timestamp(w['origin'])
        next_open=pd.Timestamp(f['future_grid'][0]['start'])
        entries={'friday_after_close':(origin+pd.Timedelta(minutes=5),origin+pd.Timedelta(minutes=14)),
                 'next_session_morning':(next_open,next_open+pd.Timedelta(minutes=10))}
        scenarios={f'{timing}/{mode}':target_comparison(week,selected,legs,iso(a),iso(b),mode)
                   for timing,(a,b) in entries.items() for mode in MODES}
    result={'origin':w['origin'],'expiry':w['week_end'],'calendar':w,'contracts':contracts,'scenarios':scenarios,
            'errors':errors,'seed_forecast_reused':saved is not None,
            'forecast_sha256':digest(f),'diagnostics':diagnostic,
            'average_touches':average_touches(f,selected),
            'crossing_excursions':crossing_excursions(f,selected,bars),
            'model_ids':[m['model_id'] for m in f['model_runs']]}
    # Each stage is already durable; the complete week includes the small
    # evaluation tape for audit and references the shared immutable input chunks.
    store.put('week',key,{'result':result,'forecast':f,'training_hourly':history,'evaluation_minutes':selected,
                          'option_paths':legs})
    return {'result':result,'forecast':f,'training_hourly':history,'evaluation_minutes':selected,'option_paths':legs}


def strategy_comparison(bars,forecasts):
    """Same execution engine for forward/reverse, averaged/aligned bands.

Positions carry across weekly forecast refreshes, exit/reverse only on the
opposite signal, and liquidate at study end. One share, no recovery multiplier.
"""
    output={}
    frozen=[{**f,'predictions':[{**p,'quantiles':averages(f)} for p in f['predictions']]} for f in forecasts]
    for basis,selected in (('hourly_path',forecasts),('weekly_average',frozen)):
        for reverse in (False,True):
            for cost in (0.,1.,5.):
                key=f"{basis}/{'reverse' if reverse else 'momentum'}/{cost:g}bp"
                run=simulate(bars,selected,cost_bps=cost,contrarian=reverse)
                win=loss=longest_win=longest_loss=0
                for trade in run['trades']:
                    win=win+1 if trade['net_pnl']>0 else 0
                    loss=loss+1 if trade['net_pnl']<0 else 0
                    longest_win=max(longest_win,win);longest_loss=max(longest_loss,loss)
                # Excursions use our direction-independent crossing report; the
                # legacy simulator's trade-side excursion labels are not reused.
                output[key]={'summary':{**run['summary'],'longest_winning_streak':longest_win,
                            'longest_losing_streak':longest_loss},'trades':run['trades'],
                            'equity':run['equity']}
    return output


def run(seed_archive,end_friday,output,*,weeks=52,budget_minutes=270,max_new_weeks=52,resume_campaign=None):
    started=time.monotonic();schedule=windows(end_friday,weeks)
    if pd.Timestamp(schedule[-1]['week_end'])>=pd.Timestamp.now(tz='UTC'):raise ValueError('COMPLETED_YEAR_WEEKS_ONLY')
    if not 1<=budget_minutes<=270 or not 1<=max_new_weeks<=52:raise ValueError('INVALID_YEAR_EXECUTION_BUDGET')
    code=os.getenv('QUANTURA_CODE_SHA',os.getenv('GITHUB_SHA',''))
    if not re.fullmatch(r'[a-f0-9]{40}',code):raise ValueError('IMMUTABLE_CODE_SHA_REQUIRED')
    if resume_campaign:
        store=Checkpoints.resume(resume_campaign,code,end_friday,weeks)
        original=store.get('stage','seed')
        if original is None:raise ValueError('YEAR_SEED_CHECKPOINT_REQUIRED')
        records,manifest=original['records'],original['manifest'];configuration=store.configuration
    else:
        records,manifest=load_archive(seed_archive)
        configuration={'version':VERSION,'code_sha':code,'seed_manifest_sha256':digest(manifest),
            'end_friday':end_friday,'weeks':weeks,'schedule':schedule,'source':'alpaca_iex',
            'history_hours':500,'model_weights':'all_five_equal_central; supported_tail_renormalization',
            'holiday_policy':'last_exchange_session_on_or_before_nominal_Friday',
            'option_policy':'frozen_average_targets; both_exit_modes; both_entry_times; one_contract_each',
            'touch_policy':'observed_minute_ranges; all_generated_averages; separate_gaps_and_episodes',
            'share_policy':'momentum_and_reverse; aligned_and_average_bands; hourly_close_next_open; 0_1_5bp; one_share',
            'inference_lock_sha256':hashlib.sha256(Path('market_research/requirements.lock').read_bytes()).hexdigest()}
        store=Checkpoints(configuration)
        store.put('stage','seed',{'records':records,'manifest':manifest})
    seed_source=records['report-spy-source.json']
    seed={f['origin']:f for n,f in records.items() if n.startswith('report-spy-forecast-')}
    output=Path(output);output.mkdir(parents=True,exist_ok=True)
    print(json.dumps({'event':'spy_year_started','campaign_id':store.id,'weeks':weeks}),flush=True)
    completed=[]
    for w in schedule:
        prior=store.get('week',f"week-{w['index']:02d}")
        if prior:completed.append(prior['result'])
    start=(pd.Timestamp(schedule[0]['origin'])-pd.Timedelta(days=150)).date().isoformat()
    chunks=[]
    with httpx.Client(timeout=180) as client:
        for a,b in date_chunks(start,end_friday):
            key=a+'_'+b;chunk=store.get('source',key)
            if chunk is None:
                request={'symbol':'SPY','source':'alpaca','timeframe':'1Min','session':'regular','adjustment':'raw',
                         'limit':50000,'start':a+'T00:00:00Z','end':b+'T23:59:59Z'}
                chunk=fetch(client,'ticker/history',request)
                if chunk is None:raise ValueError('YEAR_SPY_HISTORY_UNAVAILABLE')
                if chunk.get('symbol')!='SPY' or chunk.get('feed')!='iex' or len(chunk['rows'])>=50000:
                    raise ValueError('YEAR_SPY_SOURCE_OR_TRUNCATION_ERROR')
                store.put('source',key,chunk)
            chunks.append(chunk)
        minute_rows=merge_sources(chunks,seed_source['rows'])
        grid=session_grid(start,end_friday);bars,rejected=aggregate_minutes(minute_rows,grid)
        processed=0
        for w in schedule:
            if any(r['origin']==w['origin'] for r in completed):continue
            if processed>=max_new_weeks or time.monotonic()-started>budget_minutes*60:break
            week=process_week(w,bars,minute_rows,grid,seed,store,client)
            completed.append(week['result']);processed+=1
            summary=yearly_summary(completed,weeks)
            print(json.dumps({'event':'spy_year_week_saved','campaign_id':store.id,'origin':w['origin'],
                'completed_weeks':len(completed),'planned_weeks':weeks,'seed_forecast_reused':week['result']['seed_forecast_reused'],
                'coverage':{k:v.get('status') for k,v in week['result']['scenarios'].items()}}),flush=True)
            store.put('report',f'progress-{len(completed):02d}',summary)
        if not processed and len(completed)<weeks:
            raise RuntimeError('YEAR_NO_PROGRESS_RESUME_CHECKPOINT_MANUALLY')
    strategies={}
    if len(completed)==weeks:
        forecasts=[store.get('stage',f"week-{w['index']:02d}-forecast") for w in schedule]
        strategies=store.get('stage','underlying-strategies')
        if strategies is None:
            strategies=strategy_comparison(bars,forecasts)
            store.put('stage','underlying-strategies',strategies)
    report={'configuration':configuration,'campaign_id':store.id,'summary':yearly_summary(completed,weeks),
        'weeks':sorted(completed,key=lambda r:r['origin']),'underlying_rows':len(minute_rows),'hourly_rows':len(bars),
        'rejected_hourly_bins':rejected,'runtime_seconds':time.monotonic()-started,
        'underlying_strategy_summaries':{k:v['summary'] for k,v in strategies.items()},
        'underlying_strategy_trades':{k:v['trades'] for k,v in strategies.items()},
        'limitations':['Option bar estimates, not executable bid/ask fills; feed entitlement is not certified OPRA.',
            'Regular-session targets only. Unhit legs close five minutes before actual expiry-session close, never intrinsic substitution.',
            'Four prior tuning weeks are separate from 48 additional retrospective weeks; current model pretraining cutoffs are unaudited.',
            'P&L on total premiums, no leverage/recovery multiplier, no annualization, fees are sensitivities.',
            'Holiday shifts use last actual session and matching option expiry; unavailable contracts/exit prices remain unpriced.',
            'Average quantile paths are reference levels, not probabilities for weekly average price. Minute ranges are touch proxies, not exact trades/fills.',
            'Excursions use future observations only as descriptive outcomes, never entry decisions; overlapping crossings are not independent samples.',
            'Share strategies use hourly-close signals, next scheduled hourly open, positions carried across weekly updates and final liquidation. Short borrow, dividends, margin and fills are unverified.']}
    from .spy_weekly import save
    final_key=f'final-{len(completed):02d}'
    previous_report=store.get('report',final_key)
    if previous_report is not None:
        report=previous_report
    else:
        store.put('report',final_key,report)
    save(output/f'report-spy-year-{len(completed):02d}.json',report)
    # Keep this small summary as a GitHub artifact; encrypted raw data remains in bucket.
    encode_catalog(report,output/f'spy-year-{len(completed):02d}.qrc.enc')
    if os.getenv('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'],'a') as out:
            out.write(f"complete={'true' if len(completed)==weeks else 'false'}\n")
            out.write(f'campaign_id={store.id}\ncompleted_weeks={len(completed)}\n')
    print(json.dumps({'event':'spy_year_checkpoint_complete','campaign_id':store.id,'summary':report['summary']['all_weeks'],
                      'completed_weeks':len(completed),'planned_weeks':weeks}),flush=True)
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser();seed_args=parser.add_mutually_exclusive_group(required=True)
    seed_args.add_argument('--seed-archive');seed_args.add_argument('--resume-campaign')
    parser.add_argument('--end-friday',required=True)
    parser.add_argument('--output',required=True);parser.add_argument('--weeks',type=int,default=52)
    parser.add_argument('--budget-minutes',type=int,default=270);parser.add_argument('--max-new-weeks',type=int,default=52)
    args=parser.parse_args();run(args.seed_archive,args.end_friday,args.output,weeks=args.weeks,
                               budget_minutes=args.budget_minutes,max_new_weeks=args.max_new_weeks,resume_campaign=args.resume_campaign)
