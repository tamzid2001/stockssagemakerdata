"""Read-only annual stock replay: one six-hour ensemble, P01, averaging down.

Raw execution prices preserve a literal $1 spacing. Only historical context
is rebased for splits already effective at that day's forecast origin.
"""
from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timedelta, timezone
import gzip
import hashlib
import json
import math
from pathlib import Path
import re
import time

from .alpaca_spy_strategy import MinuteBar, NEW_YORK
from .alpaca_spy_worker import AlpacaAPI
from .alpaca_spy_intraday_backtest import MODELS, QUANTILES, timestamp

SYMBOLS = ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'NFLX', 'AVGO', 'WMT')
HORIZON = 360


def schedule(start: date, end: date):
    import pandas_market_calendars as mcal
    if start > end or (end-start).days > 366 or end >= datetime.now(NEW_YORK).date():
        raise ValueError('COMPLETED_ONE_YEAR_RANGE_REQUIRED')
    return mcal.get_calendar('NYSE').schedule(start_date=start, end_date=end)


def plan(start: date, end: date, symbols=SYMBOLS, chunk_size=63):
    if len(symbols) != 10 or len(set(symbols)) != 10 or any(s not in SYMBOLS for s in symbols):
        raise ValueError('TEN_APPROVED_MEGACAP_SYMBOLS_REQUIRED')
    if not 1 <= chunk_size <= 63:
        raise ValueError('CHUNK_SIZE_INVALID')
    calendar = schedule(start, end)
    normal = calendar[(calendar.market_close-calendar.market_open).dt.total_seconds() >= HORIZON*60]
    if normal.empty: raise ValueError('NO_COMPLETE_SIX_HOUR_SESSIONS')
    days = [d.date().isoformat() for d in normal.index]
    return {'start': start.isoformat(), 'end': end.isoformat(), 'symbols': list(symbols),
        'days': days, 'planned_forecasts': len(days)*len(symbols),
        'excluded_early_closes': [d.date().isoformat() for d in calendar.index if d not in normal.index],
        'include': [{'symbol': s, 'chunk': i//chunk_size, 'start': group[0], 'end': group[-1]}
            for s in symbols for i in range(0, len(days), chunk_size)
            if (group := days[i:i+chunk_size])]}


def bars_and_splits(api, symbol, start, end, feed='sip'):
    if symbol not in SYMBOLS or feed not in ('sip', 'iex'):
        raise ValueError('SYMBOL_OR_FEED_INVALID')
    first = start-timedelta(days=16)
    params = {'timeframe': '1Min', 'start': first.isoformat()+'T00:00:00Z',
        'end': (end+timedelta(days=1)).isoformat()+'T00:00:00Z',
        'limit': 10000, 'sort': 'asc', 'adjustment': 'raw', 'feed': feed}
    bars = {}; tokens = set()
    while True:
        payload = api.request('GET', f'/v2/stocks/{symbol}/bars', data=True, params=params)
        for r in payload.get('bars') or []:
            b = MinuteBar(timestamp(r['t'])+timedelta(minutes=1), float(r['c']),
                float(r['h']), float(r['l']), float(r['o']))
            if not all(math.isfinite(v) and v > 0 for v in (b.open,b.high,b.low,b.close)) or not b.low <= min(b.open,b.close) <= max(b.open,b.close) <= b.high:
                raise ValueError('INVALID_OBSERVED_BAR')
            if b.end in bars and bars[b.end] != b:
                raise ValueError('CONFLICTING_OBSERVED_BAR')
            bars[b.end] = b
        token = payload.get('next_page_token')
        if not token: break
        if token in tokens: raise RuntimeError('PAGINATION_STALLED')
        tokens.add(token); params['page_token'] = token
    params = {'symbols': symbol, 'types': 'forward_split,reverse_split',
        'start': first.isoformat(), 'end': end.isoformat(), 'limit': 1000}
    splits = {}; tokens = set()
    while True:
        payload = api.request('GET', '/v1/corporate-actions', data=True, params=params)
        for kind in ('forward_splits','reverse_splits'):
            for r in (payload.get('corporate_actions') or {}).get(kind) or []:
                if r.get('symbol') != symbol: raise ValueError('SPLIT_SYMBOL_MISMATCH')
                ex = date.fromisoformat(r['ex_date']); old=float(r['old_rate']); new=float(r['new_rate'])
                if not all(math.isfinite(v) and v>0 for v in (old,new)): raise ValueError('INVALID_SPLIT_RATIO')
                key = (ex,old,new); splits[key] = {'ex_date':ex.isoformat(),'old_rate':old,'new_rate':new}
        token=payload.get('next_page_token')
        if not token:break
        if token in tokens:raise RuntimeError('SPLIT_PAGINATION_STALLED')
        tokens.add(token);params['page_token']=token
    return sorted(bars.values(),key=lambda b:b.end), sorted(splits.values(),key=lambda s:s['ex_date'])


def context_at(bars, origin, splits):
    history=[b for b in bars if b.end<=origin][-500:]
    result=[]
    for b in history:
        factor=1.
        for split in splits:
            effective=datetime.combine(date.fromisoformat(split['ex_date']),datetime.min.time(),NEW_YORK)
            if b.end.astimezone(NEW_YORK).date() < effective.date() <= origin.astimezone(NEW_YORK).date():
                factor *= split['old_rate']/split['new_rate']
        result.append(MinuteBar(b.end,b.close*factor,b.high*factor,b.low*factor,b.open*factor))
    return result


def forecast(history,symbol):
    from ensemble_forecasting.worker import execute_job
    result=execute_job({'request':{'prediction_length':HORIZON,'horizon_mode':'frequency_periods',
        'frequency':'1min','calendar':'NONE','transform':'log','context_length':500,
        'failure_policy':'fail','quantiles':QUANTILES,
        'models':{m:{'enabled':True,'weight':1}for m in MODELS}},
        'model_checkpoints':{'toto':'Datadog/Toto-2.0-4m'},'runtime_mode':'production',
        'input':{'rows':[{'timestamp':b.end.isoformat(),'target':b.close}for b in history],
                 'frequency':'1min','timezone':'America/New_York'},
        'source':{'type':'ticker','symbol':symbol,'provider':'alpaca'}})
    if result.get('failures') or {r['model']for r in result['model_runs']if r['status']=='completed'} != set(MODELS):
        raise RuntimeError('FIVE_MODEL_FORECAST_INCOMPLETE')
    return result


def replay(bars,predictions,available,opening,closing,spread=0.):
    if spread < 0 or not math.isfinite(spread):raise ValueError('SPREAD_INVALID')
    rows={timestamp(r['timestamp']):r for r in predictions}
    expected=[opening+timedelta(minutes=i)for i in range(1,HORIZON+1)]
    if sorted(rows)!=expected:raise ValueError('FORECAST_TIMESTAMPS_INVALID')
    for r in rows.values():
        if not all(math.isfinite(r[k]) and r[k]>0 for k in ('p01','p50')) or r['p01']>r['p50']:
            raise ValueError('FORECAST_LEVELS_INVALID')
    rising=rows[expected[-1]]['p50']>=rows[expected[0]]['p50']
    entries=[];trades=[];curve=[];cash=0.
    future=[b for b in bars if opening < b.end <= closing]
    if not future or future[-1].end != closing:raise ValueError('EXACT_EXCHANGE_CLOSE_MISSING')
    for b in future:
        start=b.end-timedelta(minutes=1);row=rows.get(b.end)
        if rising and row and available<=start:
            level=min(row['p01'],entries[-1][0]-1)if entries else row['p01']
            if b.low<level:
                price=min(b.open,level);entries.append((price,start if b.open<level else b.end));cash-=spread/2
        low=cash+sum(b.low-e-spread/2 for e,_ in entries)
        equity=cash+sum(b.close-e-spread/2 for e,_ in entries)
        curve.append({'at':b.end.isoformat(),'low':low,'close':equity,'open_entries':len(entries)})
        if b.end==closing:
            trades=[{'entry_at':t.isoformat(),'entry':e,'exit_at':closing.isoformat(),'exit':b.close,
                'pnl':b.close-e-spread}for e,t in entries]
    return {'rising_median':rising,'spread_per_share':spread,'trades':trades,'curve':curve,
        'pnl':sum(t['pnl']for t in trades),'entries':len(trades)}


def summarize(records,spread_index=0):
    cash=peak=drawdown=maxdaily=0.;trades=[];days=[];max_entries=0
    for r in sorted(records,key=lambda r:r['day']):
        result=r['scenarios'][spread_index]
        for p in result['curve']:
            drawdown=max(drawdown,peak-cash-p['low']);maxdaily=max(maxdaily,-p['low'])
            peak=max(peak,cash+p['close']);max_entries=max(max_entries,p['open_entries'])
        cash+=result['pnl'];trades+=result['trades']
        if result['entries']:days.append(result['pnl'])
    def streak(values):
        win=loss=bestwin=bestloss=0
        for p in values:
            win=win+1 if p>0 else 0;loss=loss+1 if p<0 else 0
            bestwin=max(bestwin,win);bestloss=max(bestloss,loss)
        return {'wins':bestwin,'losses':bestloss}
    wins=sum(t['pnl']>0 for t in trades);losses=sum(t['pnl']<0 for t in trades)
    return {'completed_forecasts':len(records),'entries':len(trades),'wins':wins,'losses':losses,
        'entry_win_rate':wins/len(trades)if trades else None,'net_per_one_share_per_entry':cash,
        'max_equity_drawdown':drawdown,'max_daily_loss':maxdaily,'max_open_entries':max_entries,
        'baskets':len(days),'basket_wins':sum(p>0 for p in days),'basket_losses':sum(p<0 for p in days),
        'entry_streaks':streak([t['pnl']for t in trades]),'basket_streaks':streak(days)}


def run(symbol,start,end,output,feed='sip'):
    output.mkdir(parents=True,exist_ok=True);calendar=schedule(start,end)
    calendar=calendar[(calendar.market_close-calendar.market_open).dt.total_seconds()>=HORIZON*60]
    api=AlpacaAPI('paper');records=[];failures=[]
    try:
        bars,splits=bars_and_splits(api,symbol,start,end,feed)
        with gzip.open(output/'observed-bars.jsonl.gz','wt')as f:
            for b in bars:f.write(json.dumps({'end':b.end.isoformat(),'o':b.open,'h':b.high,'l':b.low,'c':b.close})+'\n')
        (output/'splits.json').write_text(json.dumps(splits,indent=2)+'\n')
        for day,row in calendar.iterrows():
            opening=row.market_open.to_pydatetime();closing=row.market_close.to_pydatetime()
            record={'symbol':symbol,'day':day.date().isoformat(),'origin':opening.isoformat(),
                'horizon_minutes':HORIZON,'feed':feed,'price_adjustment':'raw, origin-relative split context only','orders_sent':0}
            try:
                history=context_at(bars,opening,splits)
                if len(history)!=500 or history[-1].end!=opening:raise ValueError('500_ORIGIN_HISTORY_ROWS_REQUIRED')
                record['history_sha256']=hashlib.sha256(json.dumps([(b.end.isoformat(),b.close)for b in history]).encode()).hexdigest()
                begin=time.monotonic();result=forecast(history,symbol);elapsed=time.monotonic()-begin
                available=opening+timedelta(seconds=elapsed)
                record.update(status='completed',inference_seconds=elapsed,earliest_actionable_at=available.isoformat(),
                    predictions=result['predictions'],model_runs=result['model_runs'],
                    scenarios=[replay(bars,result['predictions'],available,opening,closing,cost)for cost in (0.,.01,.05)])
                records.append(record)
            except Exception as exc:
                # Only static, bounded error codes; never provider bodies or secrets.
                code=str(exc)if re.fullmatch(r'[A-Z0-9_]{1,100}',str(exc))else type(exc).__name__
                record.update(status='failed',error=code);failures.append({'day':record['day'],'error':code})
            (output/(record['day']+'.json')).write_text(json.dumps(record,separators=(',',':'),default=str)+'\n')
            print(json.dumps({'event':'megacap_p01_day','symbol':symbol,'day':record['day'],'status':record['status'],'entries':record.get('scenarios',[{}])[0].get('entries')}),flush=True)
        report={'symbol':symbol,'planned_forecasts':len(calendar),'failures':failures,'complete':not failures,
            'results':[{'round_trip_spread_per_share':c,**summarize(records,i)}for i,c in enumerate((0.,.01,.05))]}
        (output/'summary.json').write_text(json.dumps(report,indent=2)+'\n')
        return report
    finally:api.client.close()


def aggregate(folder,study,output):
    records={};failed={}
    for p in folder.rglob('*.json'):
        if not re.fullmatch(r'\d{4}-\d{2}-\d{2}\.json',p.name):continue
        r=json.loads(p.read_text());key=(r['symbol'],r['day'])
        if key in records or key in failed:raise ValueError('DUPLICATE_STUDY_DAY')
        (records if r['status']=='completed' else failed)[key]=r
    result=[]
    for symbol in study['symbols']:
        complete=[records[(symbol,d)]for d in study['days']if(symbol,d)in records]
        missing=[d for d in study['days']if(symbol,d)not in records]
        result.append({'symbol':symbol,'planned_forecasts':len(study['days']),'complete':not missing,
            'missing_or_failed_days':missing,'results':[{'round_trip_spread_per_share':c,**summarize(complete,i)}for i,c in enumerate((0.,.01,.05))]})
    report={'study':study,'rules':{'forecast':'five models, 500 completed minute closes, one 360-minute forecast at09:30 NY per normal exchange session','entry':'below actionable P01 only if final forecast P50 >= first forecast P50','add':'one/minute, minimum $1 below last actual fill AND below current P01','exit':'actual exchange close, no TP or stop, no orders','size':'one share per entry; no martingale','costs':'0, $.01, $.05 per-share round-trip spread sensitivities, no slippage/commission','splits':'raw prices for execution; context adjusted only for splits effective by origin; provider metadata reconstructed retrospectively','early_closes':'excluded because a complete six-hour regular-session window does not exist'},'complete':all(r['complete']for r in result),'stocks':result}
    output.mkdir(parents=True,exist_ok=True);(output/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    lines=['# One-year mega-cap P01 backtest','',f"Period: {study['start']} to {study['end']}. Complete: {report['complete']}.",'','One share per entry; $1 downward averaging, non-declining median, EOD exit. Returns are dollars, not capital-normalized percentages.','','| Stock | Forecasts | Entries | Win rate | Net before costs | Max equity drawdown |','|---|---:|---:|---:|---:|---:|']
    for r in result:
        s=r['results'][0];wr=f"{100*s['entry_win_rate']:.2f}%"if s['entry_win_rate']is not None else '—'
        lines.append(f"| {r['symbol']} | {s['completed_forecasts']}/{r['planned_forecasts']} | {s['entries']} | {wr} | ${s['net_per_one_share_per_entry']:.2f} | ${s['max_equity_drawdown']:.2f} |")
    lines+=['','Missing or failed days remain explicit in report.json. No synthetic bars or silent four-model fallbacks. Drawdown uses minute OHLC lows and minute-close peaks; exact intrabar sequencing is unavailable. Splits are effective-date adjusted in context; foundation model training data and retrospectively revised provider history can affect historical results. Two stock spread sensitivities are in report.json.']
    (output/'report.md').write_text('\n'.join(lines)+'\n');return report


def main():
    parser=argparse.ArgumentParser();sub=parser.add_subparsers(dest='command',required=True)
    p=sub.add_parser('plan');p.add_argument('--start',type=date.fromisoformat,required=True);p.add_argument('--end',type=date.fromisoformat,required=True);p.add_argument('--output',type=Path,required=True)
    p=sub.add_parser('run');p.add_argument('--symbol',choices=SYMBOLS,required=True);p.add_argument('--start',type=date.fromisoformat,required=True);p.add_argument('--end',type=date.fromisoformat,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--feed',choices=('sip','iex'),default='sip')
    p=sub.add_parser('aggregate');p.add_argument('--artifacts',type=Path,required=True);p.add_argument('--plan',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if args.command=='plan':
        result=plan(args.start,args.end);args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'include':result['include']}))
    elif args.command=='run':
        result=run(args.symbol,args.start,args.end,args.output,args.feed)
        if not result['complete']:raise SystemExit(1)
    else:
        result=aggregate(args.artifacts,json.loads(args.plan.read_text()),args.output)
        if not result['complete']:raise SystemExit(1)

if __name__=='__main__':main()
