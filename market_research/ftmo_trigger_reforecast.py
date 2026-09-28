"""Minute-confirmed P01/P99 triggers, causal second forecasts, separate H1 ladders.

Reuse frozen source candles, primary forecasts and FTMO specifications from a
completed yearly study. New forecasts are actual production model inference.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
import gzip
import json
import math
from pathlib import Path
import re
import time

from .ftmo_dukas_data import BASE, INSTRUMENTS, UTC, decode, digest, get_json, hourly_from_minutes, load_quotes, stamp
from .ftmo_dukas_engine import COSTS, RULES, Conversion, adjusted_quotes, bar_tradeable, replay
from .ftmo_dukas_study import MODELS, QUANTILES

SYMBOLS=tuple(s for s in INSTRUMENTS if s not in ('XAUUSD.sim','BTCUSD.sim'))


def read_json(path: Path):
    if path.suffix=='.gz':
        with gzip.open(path,'rt')as stream:return json.load(stream)
    return json.loads(path.read_text())


def write_json(path: Path, value):
    path.parent.mkdir(parents=True,exist_ok=True)
    if path.suffix=='.gz':
        with gzip.open(path,'wt')as stream:json.dump(value,stream,separators=(',',':'))
    else:path.write_text(json.dumps(value,indent=2)+'\n')


def read_primary(root: Path, symbol: str, study: dict, source_hash: str):
    records=[]
    for path in root.rglob('????-??-??.json.gz'):
        r=read_json(path)
        if r.get('symbol')==symbol:records.append(r)
    by_day={r['day']:r for r in records}
    if len(records)!=len(by_day)or set(by_day)!=set(study['days']):
        raise ValueError('EXACT_PRIMARY_FORECAST_DAYS_REQUIRED')
    for r in records:
        if r.get('status')!='completed'or r.get('source_rows_sha256')!=source_hash:
            raise ValueError('PRIMARY_FORECAST_SOURCE_MISMATCH')
        if {v['model']for v in r['model_runs']if v['status']=='completed'}!=set(MODELS):
            raise ValueError('FIVE_PRIMARY_MODELS_REQUIRED')
    return [by_day[day]for day in study['days']]


def candidate_days(rows: list[dict], primary: list[dict], spec: dict):
    """H1 extrema select downloads only; they never timestamp a trading signal."""
    by_time={stamp(r['start']):r for r in rows};days=set()
    for f in primary:
        available=stamp(f['earliest_actionable_at'])
        for pred in f['predictions']:
            end=stamp(pred['timestamp']);at=end-timedelta(hours=1);row=by_time.get(at)
            if row and end>available and bar_tradeable(at,spec)and (
                row['ask']['l']<pred['p01']or row['bid']['h']>pred['p99']):days.add(at.date())
    return sorted(days)


def minute_quotes(symbol: str, days: list[date], hourly: list[dict], cache: Path):
    code=INSTRUMENTS[symbol][0];cache.mkdir(parents=True,exist_ok=True)
    metadata_path=cache/'instrument.json'
    if not metadata_path.exists():write_json(metadata_path,get_json(BASE+'/instruments/'+code))
    metadata=read_json(metadata_path)
    if metadata.get('code')!=code:raise ValueError('MINUTE_INSTRUMENT_MISMATCH')
    scale=metadata['priceScale'];tolerance=.51*10**-scale
    original={r['start']:r for r in hourly};result=[];audit=[]
    for day in days:
        sides={}
        for side in ('BID','ASK'):
            path=f'/candles/minute/{code}/{side}/{day.year}/{day.month}/{day.day}'
            local=cache/f'{day.isoformat()}-{side}.json.gz'
            if not local.exists():
                write_json(local,get_json(BASE+path));time.sleep(1.25)
            payload=read_json(local);decoded=decode(payload,scale,1)
            aggregated={r['start']:r for r in hourly_from_minutes(decoded)}
            for hour,prior in original.items():
                if not hour.startswith(day.isoformat()):continue
                current=aggregated.get(hour)
                if current is None or any(abs(current[k]-prior[side.lower()][k])>tolerance for k in ('o','h','l','c')):
                    raise ValueError('MINUTE_HOURLY_SOURCE_DISAGREEMENT')
            sides[side]={r['start']:r for r in decoded}
            audit.append({'path':path,'sha256':digest(payload),'observed_minutes':len(decoded)})
        for key in sorted(sides['BID'].keys()&sides['ASK'].keys()):
            bid=sides['BID'][key];ask=sides['ASK'][key];at=stamp(key)
            if ask['o']<bid['o']or ask['c']<bid['c']:raise ValueError('CROSSED_MINUTE_QUOTES')
            result.append({'start':key,'end':(at+timedelta(minutes=1)).isoformat(),
                'bid':{k:bid[k]for k in ('o','h','l','c')},'ask':{k:ask[k]for k in ('o','h','l','c')}})
        print(json.dumps({'event':'minutes_verified','symbol':symbol,'day':day.isoformat(),'paired_minutes':len(sides['BID'].keys()&sides['ASK'].keys())}),flush=True)
    return result,audit


def detect_triggers(symbol: str, primary: list[dict], minutes: list[dict], spec: dict):
    """First executable-price breach per side/session, observed causally.

    Outside minute opens are known at their start. A minute's high/low can only
    confirm a breach when that minute completes. Partial unavailable minutes
    are skipped. The first observed outside price after publication qualifies.
    """
    intervals={}
    for f in primary:
        for pred in f['predictions']:
            at=stamp(pred['timestamp'])-timedelta(hours=1)
            if at in intervals:raise ValueError('OVERLAPPING_PRIMARY_FORECASTS')
            intervals[at]=(f,pred)
    seen=set();events=[]
    for row in minutes:
        at=stamp(row['start']);end=stamp(row['end']);hour=at.replace(minute=0,second=0,microsecond=0)
        info=intervals.get(hour)
        if not info or not bar_tradeable(hour,spec):continue
        f,pred=info;available=stamp(f['earliest_actionable_at']);finish=stamp(f['forecast_end'])
        if at<available or at>=finish:continue
        bid,ask=adjusted_quotes(row,symbol,COSTS[0]);tick=10**-spec['digits']
        bid={k:math.floor(v/tick+1e-8)*tick for k,v in bid.items()}
        ask={k:math.ceil(v/tick-1e-8)*tick for k,v in ask.items()}
        for side,quote,key in [('long',ask,'p01'),('short',bid,'p99')]:
            identity=(f['day'],side)
            if identity in seen:continue
            level=float(pred[key]);outside=lambda price:price<level if side=='long'else price>level
            if outside(quote['o']):detected=at;kind='known_minute_open';price=quote['o']
            elif outside(quote['l'if side=='long'else 'h']):detected=end;kind='completed_minute_breach';price=quote['l'if side=='long'else 'h']
            else:continue
            if detected>=finish:continue
            seen.add(identity)
            record={'symbol':symbol,'side':side,'day':f['day'],'primary_origin':f['origin'],
              'primary_available_at':f['earliest_actionable_at'],'detected_at':detected.isoformat(),
              'detection_kind':kind,'trigger_price':price,'trigger_quantile':level,
              'quantile_timestamp':pred['timestamp'],'forecast_end':f['forecast_end'],
              'minute_row_sha256':digest(row),'primary_history_input_sha256':f['history_input_sha256']}
            record['trigger_id']=digest(record);events.append(record)
    return events


def prepare(symbol: str, study: dict, specs: dict, source: Path, primary_root: Path, output: Path):
    if symbol not in SYMBOLS:raise ValueError('EIGHT_APPROVED_ASSETS_ONLY')
    rows=load_quotes(source);identity=read_json(source/'source.json')
    if identity['symbol']!=symbol:raise ValueError('SOURCE_SYMBOL_MISMATCH')
    primary=read_primary(primary_root,symbol,study,identity['rows_sha256'])
    days=candidate_days(rows,primary,specs['specifications'][symbol])
    minute_rows,audit=minute_quotes(symbol,days,rows,output/'minute-cache')
    events=detect_triggers(symbol,primary,minute_rows,specs['specifications'][symbol])
    result={'symbol':symbol,'source_rows_sha256':identity['rows_sha256'],'primary_forecast_days':len(primary),
      'minute_days':len(days),'minute_source_audit':audit,'minute_rows_sha256':digest(minute_rows),
      'triggers':events,'long_triggers':sum(e['side']=='long'for e in events),
      'short_triggers':sum(e['side']=='short'for e in events),'orders_sent':0}
    write_json(output/'triggers.json',result)
    print(json.dumps({'event':'triggers_ready',**{k:result[k]for k in ('symbol','minute_days','long_triggers','short_triggers')}}),flush=True)
    return result


def forecast_trigger(rows: list[dict], event: dict):
    detected=stamp(event['detected_at']);finish=stamp(event['forecast_end'])
    history=[r for r in rows if stamp(r['end'])<=detected][-500:]
    if len(history)!=500:raise ValueError('500_COMPLETED_HOURS_REQUIRED')
    observed=stamp(history[-1]['end']);length=(finish-observed).total_seconds()/3600
    if not length.is_integer()or not 1<=length<=119 or (detected-observed).total_seconds()>96*3600:
        raise ValueError('BOUNDED_REMAINING_FORECAST_REQUIRED')
    input_rows=[{'timestamp':r['end'],'target':(r['bid']['c']+r['ask']['c'])/2}for r in history]
    from ensemble_forecasting.worker import execute_job
    started=time.monotonic()
    result=execute_job({'request':{'prediction_length':int(length),'horizon_mode':'frequency_periods','frequency':'1h',
      'calendar':'NONE','transform':'log','context_length':500,'failure_policy':'fail','quantiles':QUANTILES,
      'models':{m:{'enabled':True,'weight':1}for m in MODELS}},
      'model_checkpoints':{'toto':'Datadog/Toto-2.0-4m'},'runtime_mode':'production',
      'input':{'rows':input_rows,'frequency':'1h','timezone':'UTC'},
      'source':{'type':'ticker','provider':'dukascopy','symbol':INSTRUMENTS[event['symbol']][0]}},
      progress=lambda v:print(json.dumps({'event':'refreshed_model','symbol':event['symbol'],
          'trigger_id':event['trigger_id'],**v}),flush=True))
    seconds=time.monotonic()-started
    complete={r['model']for r in result['model_runs']if r['status']=='completed'}
    if result.get('failures')or complete!=set(MODELS):raise ValueError('FIVE_REAL_REFRESHED_MODELS_REQUIRED')
    predictions=[r for r in result['predictions']if detected<stamp(r['timestamp'])<=finish]
    expected=[];at=detected.replace(minute=0,second=0,microsecond=0)+timedelta(hours=1)
    while at<=finish:expected.append(at);at+=timedelta(hours=1)
    if [stamp(r['timestamp'])for r in predictions]!=expected:raise ValueError('EXACT_REMAINING_TIMESTAMPS_REQUIRED')
    for r in predictions:
        values=[float(r[f'p{round(q*100):02d}'])for q in QUANTILES]
        if any(not math.isfinite(v)or v<=0 for v in values)or values!=sorted(values):
            raise ValueError('INVALID_REFRESHED_QUANTILES')
    return {'status':'completed','symbol':event['symbol'],'side':event['side'],'day':event['day'],
      'trigger_id':event['trigger_id'],'trigger':event,'origin':detected.isoformat(),'forecast_end':finish.isoformat(),
      'observed_origin':observed.isoformat(),'history_rows':500,'history_start':history[0]['end'],
      'history_input_sha256':digest(input_rows),'same_input_as_primary':digest(input_rows)==event['primary_history_input_sha256'],
      'new_completed_hours_since_primary':sum(stamp(r['end'])>stamp(event['primary_origin'])for r in history),
      'inference_seconds':seconds,'earliest_actionable_at':(detected+timedelta(seconds=seconds)).isoformat(),
      'execution_availability':'First full H1 bar after measured inference; partial bars cannot enter or add.',
      'predictions':predictions,'model_runs':result['model_runs'],'prepared_series_hash':result.get('prepared_series_hash'),
      'ensemble_warnings':result.get('warnings',[]),'orders_sent':0}


def run_forecasts(symbol: str, source: Path, trigger_file: Path, output: Path):
    rows=load_quotes(source);identity=read_json(source/'source.json');triggers=read_json(trigger_file)
    if triggers['symbol']!=symbol or triggers['source_rows_sha256']!=identity['rows_sha256']:
        raise ValueError('REFRESHED_SOURCE_MISMATCH')
    output.mkdir(parents=True,exist_ok=True);records=[];shared={}
    for event in triggers['triggers']:
        key=(event['detected_at'],event['forecast_end']);path=output/(event['trigger_id']+'.json.gz')
        if path.exists():
            record=read_json(path)
            if record.get('status')=='completed'and record.get('trigger_id')==event['trigger_id']and record.get('source_rows_sha256')==identity['rows_sha256']:
                records.append(record);continue
        try:
            if key in shared:
                record={**shared[key],'side':event['side'],'trigger_id':event['trigger_id'],'trigger':event}
            else:record=forecast_trigger(rows,event);shared[key]=record
        except Exception as exc:
            error=str(exc)if isinstance(exc,ValueError)and re.fullmatch('[A-Z0-9_]+',str(exc))else getattr(exc,'code',type(exc).__name__)
            record={'status':'failed','symbol':symbol,'side':event['side'],'trigger_id':event['trigger_id'],'error_code':error,'orders_sent':0}
        record['source_rows_sha256']=identity['rows_sha256'];write_json(path,record);records.append(record)
        print(json.dumps({'event':'refreshed_forecast','symbol':symbol,'side':event['side'],'trigger':event['detected_at'],
            'status':record['status'],'seconds':record.get('inference_seconds'),'error':record.get('error_code')}),flush=True)
    summary={'symbol':symbol,'planned':len(records),'completed':sum(r['status']=='completed'for r in records),
        'failed_triggers':[r['trigger_id']for r in records if r['status']!='completed'],'orders_sent':0}
    write_json(output/'summary.json',summary)
    if summary['failed_triggers']:raise RuntimeError('INCOMPLETE_SECOND_FORECASTS')


def describe(result: dict, first: datetime, last: datetime):
    baskets=result['baskets'];trades=result['trades'];open_n=result['open_entries'];cycles=len(baskets)+bool(open_n)
    durations=lambda items:[(stamp(t['exit_at'])-stamp(t['entry_at'])).total_seconds()/3600 for t in items]
    bh=durations(baskets);lh=durations(trades)
    return {'maximum_ladder':round(result['max_open_lots']/.01),
      'mean_peak_ladder':(sum(b['entries']for b in baskets)+open_n)/cycles if cycles else None,
      'mean_basket_hours':sum(bh)/len(bh)if bh else None,'mean_leg_hours':sum(lh)/len(lh)if lh else None,
      'longest_closed_basket_hours':max(bh)if bh else None,
      'open_age_hours':(last-stamp(result['oldest_open_entry'])).total_seconds()/3600 if open_n else 0,
      'entry_days':len({stamp(t['entry_at']).astimezone(__import__('zoneinfo').ZoneInfo('Europe/Prague')).date()for t in [*trades,*result['open_basket']]}),
      'first':first.isoformat(),'last':last.isoformat()}


def run_replays(symbol: str, study: dict, specs: dict, sources: Path, primary_root: Path, triggers_root: Path, forecasts_root: Path, output: Path):
    rows=load_quotes(sources/symbol);identity=read_json(sources/symbol/'source.json')
    trigger=read_json(triggers_root/symbol/'triggers.json');events=trigger['triggers']
    refreshed=[read_json(p)for p in (forecasts_root/symbol).glob('*.json.gz')]
    expected={e['trigger_id']for e in events}
    if {r.get('trigger_id')for r in refreshed}!=expected or len(refreshed)!=len(expected)or any(
        r.get('status')!='completed'or r.get('source_rows_sha256')!=identity['rows_sha256']for r in refreshed):
        raise ValueError('ALL_REAL_SECOND_FORECASTS_REQUIRED')
    primary=read_primary(primary_root,symbol,study,identity['rows_sha256']);spec=specs['specifications'][symbol]
    currency=spec['profitCurrency'];conversion=Conversion(currency,load_quotes(sources/('USDJPY.sim'if currency=='JPY'else 'USDCAD.sim'))if currency!='USD'else [])
    first=datetime.combine(date.fromisoformat(study['start']),datetime.min.time(),UTC)+timedelta(hours=18)
    last=datetime.combine(date.fromisoformat(study['end'])+timedelta(days=1),datetime.min.time(),UTC)+timedelta(hours=17)
    split=datetime.combine(date.fromisoformat(study['development_end_exclusive']),datetime.min.time(),UTC)+timedelta(hours=18)
    reports=[]
    for side in ('long','short'):
        selected=[r for r in refreshed if r['side']==side];cases=[]
        for name,forecasts in [('primary_control',primary),('triggered_second_forecast',selected)]:
          for costs in (COSTS[0],COSTS[2]):
           for order in ('low_first','high_first'):
            for period,origin in [('annual',first),('fresh_later',split)]:
                r=replay(symbol,side,rows,forecasts,spec,conversion,origin,last,max(origin,split),RULES[0],costs,order)
                r['holding_and_ladder']=describe(r,origin,last)
                cases.append({'strategy':name,'period':period,**r})
        reports.append({'symbol':symbol,'side':side,'trigger_count':len(selected),
          'same_input_as_primary_count':sum(r['same_input_as_primary']for r in selected),
          'no_full_actionable_bar_count':sum(not any(stamp(p['timestamp'])-timedelta(hours=1)>=stamp(r['earliest_actionable_at'])for p in r['predictions'])for r in selected),
          'scenarios':cases,'orders_sent':0})
    write_json(output/'report.json.gz',{'symbol':symbol,'complete':True,'reports':reports,'source_rows_sha256':identity['rows_sha256'],'orders_sent':0})
    print(json.dumps({'event':'replay_complete','symbol':symbol,'cases':sum(len(r['scenarios'])for r in reports)}),flush=True)


def aggregate(symbols: list[str], study: dict, specs: dict, reports_root: Path, output: Path, baseline: dict|None=None):
    reports=[];missing=[]
    for symbol in symbols:
        paths=list(reports_root.rglob(symbol+'/report.json.gz'))
        if len(paths)!=1:missing.append(symbol);continue
        r=read_json(paths[0])
        if r.get('symbol')!=symbol or not r.get('complete'):missing.append(symbol);continue
        reports.extend(r['reports'])
    if missing:
        write_json(output/'incomplete.json',{'complete':False,'missing_symbols':missing,'orders_sent':0})
        raise ValueError('INCOMPLETE_TRIGGERED_YEAR_NO_RETURNS_CLAIM')
    if baseline is not None:
        for r in reports:
            original=next((v for v in baseline['reports']if v['symbol']==r['symbol']and v['side']==r['side']),None)
            if original is None:raise ValueError('PARENT_CONTROL_SIDE_MISSING')
            for current in r['scenarios']:
                if current['strategy']!='primary_control'or current['period']!='annual':continue
                prior=next((s for s in original['scenarios']if s['rule']=='baseline'and s['costs']==current['costs']and s['ordering']==current['ordering']),None)
                if prior is None or any(current[k]!=prior[k]for k in ('full','trades','baskets','open_basket')):
                    raise ValueError('PRIMARY_CONTROL_REGRESSION')
    result={'complete':True,'study':study,'cost_snapshot':specs,'reports':reports,'orders_sent':0}
    write_json(output/'results.json.gz',result)
    lines=['# FTMO triggered second forecast: annual results','',
      f"Period: {study['start']} 18:00 UTC through the day after {study['end']} at 17:00 UTC.",'',
      '0.01 lot per entry; independent long/short ladders. Primary P01/P99 breaches trigger one actual five-model second forecast per side/session. Minute opens are known at their start; minute high/low breaches are confirmed only after that minute ends. The original forecast must already be available. Refreshed models see the latest 500 completed genuine H1 closes, never an uncompleted crossing hour. Some early triggers have no new H1 observations; these are counted below. No missing observations are filled.','',
      'New entries/additions require the refreshed P01/P99 and are delayed until the first full H1 bar after measured inference. At most one addition per observed H1 bar. Longs average lower; shorts average higher. Spacing and targets: 10 pips FX / 10 points indices. Target is average execution entry ± distance. Baskets carry until target; open losses are included at the annual cutoff. There are no additions without an active refreshed forecast; target exits continue outside its horizon when the market is tradable.','',
      'Dukascopy proxy candles and frozen current FTMO costs are projected backward. Historical swaps, dividends, leverage, holidays and exact broker fills are not verified. The current rolling index specifications started Jan 30, 2026; the earlier index period is a proxy. 0.01-lot executable minimum/steps are unverified. Reference and doubled-spread/adverse-swap stress costs are tested under two assumed hourly high/low orders; ranges are sensitivities, not confidence intervals. FTMO 2-Step checks use a static $90,000 floor and midnight CE(S)T cash balance minus $5,000, with floating P&L and costs counted. No live orders were sent.','',
      '## Annual comparison','',
      '| Asset / side | Primary net | Second-forecast net | Second basket W/L | Basket win rate | Max equity DD | Worst daily loss | Max ladder | Mean basket hours | Open legs |',
      '|---|---:|---:|---|---:|---:|---:|---:|---:|---:|']
    def span(values,decimals=2,percent=False):
        v=[x for x in values if x is not None]
        if not v:return '—'
        if percent:v=[x*100 for x in v]
        a,b=min(v),max(v);fmt=lambda n:f'{n:,.{decimals}f}'+('%'if percent else '')
        return fmt(a)if round(a,decimals)==round(b,decimals)else fmt(a)+'–'+fmt(b)
    for r in reports:
        base=[s for s in r['scenarios']if s['strategy']=='primary_control'and s['period']=='annual']
        new=[s for s in r['scenarios']if s['strategy']=='triggered_second_forecast'and s['period']=='annual']
        lines.append('| '+r['symbol'].replace('.sim','')+' '+r['side']+' | $'+span([s['full']['net_equity_pnl']for s in base])+' | $'+span([s['full']['net_equity_pnl']for s in new])+' | '+span([s['full']['basket_wins']for s in new],0)+' / '+span([s['full']['basket_losses']for s in new],0)+' | '+span([s['full']['basket_win_rate']for s in new],1,True)+' | $'+span([s['full']['max_equity_drawdown']for s in new])+' | $'+span([s['max_ftmo_midnight_balance_daily_loss']for s in new])+' | '+span([s['holding_and_ladder']['maximum_ladder']for s in new],0)+' | '+span([s['holding_and_ladder']['mean_basket_hours']for s in new],1)+' | '+span([s['open_entries']for s in new],0)+' |')
    lines+=['','## Trigger coverage','',
      '| Asset / side | Triggered forecasts | Same H1 input as primary | No complete actionable H1 bar |','|---|---:|---:|---:|']
    for r in reports:lines.append(f"| {r['symbol']} {r['side']} | {r['trigger_count']} | {r['same_input_as_primary_count']} | {r['no_full_actionable_bar_count']} |")
    lines+=['','Full trade, basket, commission, swap, open-liability and fresh-flat final-three-month metrics are preserved in results.json.gz. No parameter selection or optimization was performed.']
    (output/'report.md').write_text('\n'.join(lines)+'\n')
    import csv
    flat=[]
    for r in reports:
      for s in r['scenarios']:
        flat.append({'symbol':r['symbol'],'side':r['side'],'strategy':s['strategy'],'period':s['period'],'costs':s['costs'],'ordering':s['ordering'],
          **s['full'],**s['holding_and_ladder'],'open_entries':s['open_entries'],'open_net_pnl':s['open_net_pnl'],
          'max_daily_loss':s['max_ftmo_midnight_balance_daily_loss'],'commission_paid':s['commission_paid'],'swap_accrued':s['swap_accrued'],
          'first_daily_breach':s['first_daily_limit_breach'],'first_static_breach':s['first_total_limit_breach']})
    with (output/'metrics.csv').open('w',newline='')as stream:
        writer=csv.DictWriter(stream,fieldnames=list(flat[0]));writer.writeheader();writer.writerows(flat)


def main():
    parser=argparse.ArgumentParser();sub=parser.add_subparsers(dest='command',required=True)
    for name in ('prepare','forecast','replay','aggregate'):
        p=sub.add_parser(name);p.add_argument('--output',type=Path,required=True)
        if name!='aggregate':p.add_argument('--symbol',choices=SYMBOLS,required=True)
        if name in ('prepare','replay','aggregate'):
            p.add_argument('--study',type=Path,required=True);p.add_argument('--specs',type=Path,required=True)
        if name in ('prepare','forecast'):p.add_argument('--source',type=Path,required=True)
        if name in ('prepare','replay'):p.add_argument('--primary',type=Path,required=True)
        if name=='forecast':p.add_argument('--triggers',type=Path,required=True)
        if name=='replay':
            p.add_argument('--sources',type=Path,required=True);p.add_argument('--triggers',type=Path,required=True);p.add_argument('--forecasts',type=Path,required=True)
        if name=='aggregate':
            p.add_argument('--symbols',required=True);p.add_argument('--reports',type=Path,required=True);p.add_argument('--baseline',type=Path)
    a=parser.parse_args()
    if a.command=='prepare':prepare(a.symbol,read_json(a.study),read_json(a.specs),a.source,a.primary,a.output)
    elif a.command=='forecast':run_forecasts(a.symbol,a.source,a.triggers,a.output)
    elif a.command=='replay':run_replays(a.symbol,read_json(a.study),read_json(a.specs),a.sources,a.primary,a.triggers,a.forecasts,a.output)
    else:
        symbols=a.symbols.split(',')
        if not symbols or len(set(symbols))!=len(symbols)or any(s not in SYMBOLS for s in symbols):raise ValueError('EIGHT_APPROVED_ASSETS_ONLY')
        aggregate(symbols,read_json(a.study),read_json(a.specs),a.reports,a.output,read_json(a.baseline)if a.baseline else None)


if __name__=='__main__':main()
