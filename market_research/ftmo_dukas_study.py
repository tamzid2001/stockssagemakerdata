"""Plan, forecast, replay and report the separate FTMO long/short study."""
from __future__ import annotations

import argparse
import csv
from dataclasses import asdict
from datetime import date, datetime, timedelta
import gzip
import json
from pathlib import Path
import time

from .ftmo_dukas_data import FTMO_SOURCE, INSTRUMENTS, UTC, digest, get_json, load_quotes, stamp
from .ftmo_dukas_engine import COSTS, RULES, Conversion, replay

MODELS=('prophet','toto','granite','chronos','timesfm')
QUANTILES=(.01,.10,.25,.50,.75,.90,.99)


def plan(start: date, end: date, symbols: list[str], chunk_size=75):
    if start>end or (end-start).days>365 or datetime.combine(end+timedelta(days=1),datetime.min.time(),UTC)+timedelta(hours=17)>=datetime.now(UTC):
        raise ValueError('COMPLETED_BOUNDED_YEAR_REQUIRED')
    if not symbols or len(symbols)>10 or len(set(symbols))!=len(symbols) or any(s not in INSTRUMENTS for s in symbols):
        raise ValueError('APPROVED_DISTINCT_SYMBOLS_REQUIRED')
    if not 1<=chunk_size<=75:raise ValueError('CHUNK_SIZE_INVALID')
    days=[(start+timedelta(days=i)).isoformat()for i in range((end-start).days+1)]
    split=min(end+timedelta(days=1),start+timedelta(days=274))
    return {'schema_version':'ftmo-dukas-23h-v1','start':start.isoformat(),'end':end.isoformat(),'days':days,'symbols':symbols,
        'timezone':'UTC','origin_hour':18,'horizon_hours':23,'lot_per_entry':.01,'carry_until_target':True,
        'development_end_exclusive':split.isoformat(),'selection':'prespecified nine rules; select on development only; fresh flat later replay',
        'planned_forecasts':len(days)*len(symbols),'forecasts':[
            {'symbol':symbol,'chunk':i//chunk_size,'start':group[0],'end':group[-1]}
            for symbol in symbols for i in range(0,len(days),chunk_size) if (group:=days[i:i+chunk_size])],
        'sources':[{'symbol':s}for s in dict.fromkeys([*symbols,'USDJPY.sim','USDCAD.sim'])],
        'replays':[{'symbol':s,'side':side}for s in symbols for side in ('long','short')],
        'rules':[asdict(r)for r in RULES],'cost_scenarios':[asdict(c)for c in COSTS],
        'spacing_and_target':{s:INSTRUMENTS[s][1]for s in symbols},
        'limits_as_diagnostics':{'account_usd':100000,'max_daily_loss_usd':5000,'max_total_loss_usd':10000},
        'orders_sent':0}


def snapshot_specs(symbols: list[str]):
    payload=get_json(FTMO_SOURCE);data=payload['data'];specs={}
    from zoneinfo import ZoneInfo
    offset=int(data['platformTimeOffset']['UTC']);zone=ZoneInfo(f'Etc/GMT{-offset:+d}')
    for symbol in symbols:
        matches=[s for s in data['symbols']if s.get('code')==symbol and s.get('active')]
        if len(matches)!=1:raise ValueError('EXACT_ACTIVE_FTMO_SYMBOL_REQUIRED')
        spec=dict(matches[0]);templates=[]
        for interval in data['tradingHours']:
            if interval['symbolCode']!=symbol:continue
            a,b=stamp(interval['start']).astimezone(zone),stamp(interval['end']).astimezone(zone)+timedelta(minutes=1)
            left=a.weekday()*1440+a.hour*60+a.minute
            right=left+int((b-a).total_seconds()/60)
            if not 0<right-left<=1440*7:raise ValueError('INVALID_FTMO_SESSION')
            templates.append([left,right])
        if not templates or spec['contractSize']<=0 or spec['leverageStandard']<=0 or spec['swapType']!='percentage':
            raise ValueError('FTMO_COST_SPECIFICATIONS_INCOMPLETE')
        spec['weekly_minutes']=sorted({tuple(r)for r in templates})
        spec['historical_costs_verified']=False
        spec['percentage_commission_side_basis_verified']=spec['commissionType']=='flat_USD'
        spec['minimum_lot_verified']=False
        specs[symbol]=spec
    return {'source':FTMO_SOURCE,'page':'https://ftmo.oanda.com/simulated-assets/',
        'captured_at':datetime.now(UTC).isoformat(),'platform_time_offset':data['platformTimeOffset'],
        'specifications':specs,'raw_response_sha256':digest(payload),
        'historical_daily_swap_rates_available':False,'assumed_server_timezone':'Europe/Helsinki',
        'assumed_interest_mode':'MT5 interest/current, annual percent, 360-day banking year',
        'assumed_triple_swap':'FX/metals Wednesday; indices Friday; crypto daily; alternate all-daily scenario retained',
        'commission_basis':'FX documented round-trip. Percentage rates have both per-side and round-trip cost scenarios.',
        'volume_note':'0.01 lot is the user-requested research size; current public endpoint omits minimum volume/step. The Jan 2026 launch table had 0.1 minimum units for indices/BTC. Execution feasibility is unverified.'}


def forecast_day(rows: list[dict], symbol: str, day: date):
    origin=datetime.combine(day,datetime.min.time(),UTC)+timedelta(hours=18)
    history=[r for r in rows if stamp(r['end'])<=origin][-500:]
    if len(history)!=500:raise ValueError('500_GENUINE_COMPLETED_HOURS_REQUIRED')
    observed_origin=stamp(history[-1]['end']);gap=(origin-observed_origin).total_seconds()/3600
    if not gap.is_integer()or not 0<=gap<=96:raise ValueError('HOURLY_HISTORY_TOO_STALE')
    input_rows=[{'timestamp':r['end'],'target':(r['bid']['c']+r['ask']['c'])/2}for r in history]
    from ensemble_forecasting.worker import execute_job
    started=time.monotonic()
    result=execute_job({'request':{'prediction_length':23+int(gap),'horizon_mode':'frequency_periods','frequency':'1h',
        'calendar':'NONE','transform':'log','context_length':500,'failure_policy':'fail','quantiles':QUANTILES,
        'models':{m:{'enabled':True,'weight':1}for m in MODELS}},
        'model_checkpoints':{'toto':'Datadog/Toto-2.0-4m'},'runtime_mode':'production',
        'input':{'rows':input_rows,'frequency':'1h','timezone':'UTC'},
        'source':{'type':'ticker','provider':'dukascopy','symbol':INSTRUMENTS[symbol][0]}})
    seconds=time.monotonic()-started
    complete={r['model']for r in result['model_runs']if r['status']=='completed'}
    if result.get('failures')or complete!=set(MODELS):raise ValueError('FIVE_REAL_MODELS_REQUIRED')
    predictions=[r for r in result['predictions']if origin<stamp(r['timestamp'])<=origin+timedelta(hours=23)]
    expected=[origin+timedelta(hours=i)for i in range(1,24)]
    if [stamp(r['timestamp'])for r in predictions]!=expected:raise ValueError('EXACT_23_HOUR_FORECAST_REQUIRED')
    for r in predictions:
        values=[float(r[f'p{round(q*100):02d}'])for q in QUANTILES]
        if any(not __import__('math').isfinite(v)or v<=0 for v in values)or values!=sorted(values):
            raise ValueError('INVALID_QUANTILE_ORDER_OR_PRICE')
    return {'status':'completed','symbol':symbol,'day':day.isoformat(),'origin':origin.isoformat(),
        'forecast_end':(origin+timedelta(hours=23)).isoformat(),'observed_origin':observed_origin.isoformat(),
        'history_gap_hours':gap,'history_rows':500,'history_input_sha256':digest(input_rows),
        'history_start':history[0]['end'],'context_gap_policy':'No filled hours. Value-sequence models use genuine observations; Prophet uses their real times.',
        'inference_seconds':seconds,'earliest_actionable_at':(origin+timedelta(seconds=seconds)).isoformat(),
        'execution_availability':'Next full hourly bar after actual measured inference; do not reuse its first partial hour.',
        'predictions':predictions,'model_runs':result['model_runs'],'ensemble_warnings':result.get('warnings',[]),
        'prepared_series_hash':result.get('prepared_series_hash'),'orders_sent':0}


def run_forecasts(symbol: str, start: date, end: date, source: Path, output: Path):
    output.mkdir(parents=True,exist_ok=True);rows=load_quotes(source);records=[]
    identity=json.loads((source/'source.json').read_text())
    if identity['symbol']!=symbol or identity['dukascopy_code']!=INSTRUMENTS[symbol][0]:
        raise ValueError('FORECAST_QUOTE_SYMBOL_MISMATCH')
    for i in range((end-start).days+1):
        day=start+timedelta(days=i);path=output/(day.isoformat()+'.json.gz')
        if path.exists():
            with gzip.open(path,'rt')as f:existing=json.load(f)
            if existing.get('status')=='completed'and existing.get('symbol')==symbol and existing.get('day')==day.isoformat():
                if existing.get('source_rows_sha256')!=identity['rows_sha256']:
                    raise ValueError('RESUME_FROZEN_QUOTES_CHANGED')
                records.append(existing);continue
        try:record=forecast_day(rows,symbol,day)
        except Exception as exc:
            record={'status':'failed','symbol':symbol,'day':day.isoformat(),'error_code':getattr(exc,'code',type(exc).__name__),
                'orders_sent':0}
            # Never persist arbitrary HTTP/model exception bodies that could contain credentials.
            if isinstance(exc,ValueError)and __import__('re').fullmatch('[A-Z0-9_]+',str(exc)):
                record['error_code']=str(exc)
        record['source_rows_sha256']=identity['rows_sha256']
        with gzip.open(path,'wt')as f:json.dump(record,f,separators=(',',':'))
        records.append(record);print(json.dumps({'symbol':symbol,'day':day.isoformat(),'status':record['status'],'error':record.get('error_code')}),flush=True)
    summary={'symbol':symbol,'planned':len(records),'completed':sum(r['status']=='completed'for r in records),
        'failed_days':[r['day']for r in records if r['status']!='completed'],'orders_sent':0}
    (output/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    if summary['failed_days']:raise RuntimeError('INCOMPLETE_FORECAST_CHUNK')


def read_forecasts(root: Path, symbol: str, study: dict):
    records={}
    for folder in root.rglob(symbol+'-c*'):
        if not folder.is_dir():continue
        for path in folder.glob('????-??-??.json.gz'):
            with gzip.open(path,'rt')as f:r=json.load(f)
            if r.get('symbol')!=symbol or r['day']in records:raise ValueError('FORECAST_IDENTITY_OR_DUPLICATE_FAILED')
            records[r['day']]=r
    missing=[day for day in study['days']if records.get(day,{}).get('status')!='completed']
    return [records[d]for d in study['days']if d in records],missing


def select_development(scenarios: list[dict], split: datetime):
    reference=[s for s in scenarios if s['costs']==COSTS[0].name]
    grouped={r.name:[s for s in reference if s['rule']==r.name]for r in RULES}
    base=grouped['baseline']
    if any(s['development']is None for s in base):return 'baseline'
    base_profit=min(s['development']['net_equity_pnl']for s in base)
    base_dd=max(s['development']['max_equity_drawdown']for s in base)
    base_losses=max(s['development']['losing_sessions']for s in base)
    base_win=min(s['development'].get('session_win_rate')or 0. for s in base)
    candidates=[]
    for name,values in grouped.items():
        if name=='baseline' or len(values)!=2:continue
        if not all(s['development']['closed_baskets']>=20 and s['development']['exposed_sessions']>=20 for s in values):continue
        if min(s['development']['net_equity_pnl']for s in values)<max(0.,base_profit*.5):continue
        if max(s['development']['max_equity_drawdown']for s in values)>base_dd:continue
        if max(s['development']['losing_sessions']for s in values)>base_losses:continue
        if min(s['development'].get('session_win_rate')or 0. for s in values)<base_win:continue
        if any(s[k]is not None and stamp(s[k])<split for s in values for k in ('first_daily_limit_breach','first_total_limit_breach','first_margin_breach')):continue
        candidates.append((max(s['development']['losing_sessions']for s in values),
            max(s['development']['max_equity_drawdown']for s in values),-min(s['development']['net_equity_pnl']for s in values),name))
    return min(candidates)[-1]if candidates else 'baseline'


def run_replay(symbol: str, side: str, study: dict, specs: dict, sources: Path, forecasts_root: Path, output: Path):
    forecasts,missing=read_forecasts(forecasts_root,symbol,study)
    output.mkdir(parents=True,exist_ok=True)
    if missing:
        result={'symbol':symbol,'side':side,'complete':False,'missing_forecast_days':missing,'scenarios':[],'orders_sent':0}
        (output/'report.json').write_text(json.dumps(result,indent=2)+'\n');raise ValueError('MISSING_FORECASTS_NO_OPTIMIZATION_REPORT')
    rows=load_quotes(sources/symbol);spec=specs['specifications'][symbol];currency=spec['profitCurrency']
    identity=json.loads((sources/symbol/'source.json').read_text())
    if identity['symbol']!=symbol or any(r.get('source_rows_sha256')!=identity['rows_sha256']for r in forecasts):
        raise ValueError('REPLAY_QUOTES_DO_NOT_MATCH_FORECAST_SOURCE')
    conversion=Conversion(currency,load_quotes(sources/('USDJPY.sim'if currency=='JPY'else 'USDCAD.sim'))if currency!='USD'else [])
    first=datetime.combine(date.fromisoformat(study['start']),datetime.min.time(),UTC)+timedelta(hours=18)
    last=datetime.combine(date.fromisoformat(study['end'])+timedelta(days=1),datetime.min.time(),UTC)+timedelta(hours=17)
    split=datetime.combine(date.fromisoformat(study['development_end_exclusive']),datetime.min.time(),UTC)+timedelta(hours=18)
    scenarios=[]
    for rule in RULES:
        for costs in COSTS:
            for ordering in ('low_first','high_first'):
                scenarios.append(replay(symbol,side,rows,forecasts,spec,conversion,first,last,split,rule,costs,ordering))
        print(json.dumps({'symbol':symbol,'side':side,'rule':rule.name,'completed_scenarios':len(scenarios)}),flush=True)
    selected=select_development(scenarios,split);later=[]
    if split<last:
        for name in dict.fromkeys(['baseline',selected]):
            rule=next(r for r in RULES if r.name==name)
            for costs in COSTS:
                for ordering in ('low_first','high_first'):
                    later.append(replay(symbol,side,rows,forecasts,spec,conversion,split,last,last,rule,costs,ordering))
    post_launch=[]
    launch=datetime(2026,1,30,18,tzinfo=UTC)
    if symbol in ('US500.sim','US30.sim','US100.sim','XAUUSD.sim','BTCUSD.sim') and first<launch<last:
        # Start flat after the launch; never pretend a pre-launch .sim basket existed.
        post_launch=[replay(symbol,side,rows,forecasts,spec,conversion,launch,last,last,RULES[0],COSTS[0],order)
                     for order in ('low_first','high_first')]
    # Preserve trade details for baseline and the dev-selected rule; all other trial metrics remain visible.
    for result in scenarios:
        if result['rule']not in ('baseline',selected):
            for key in ('trades','baskets','session_equity_changes','open_basket'):result.pop(key,None)
    result={'symbol':symbol,'side':side,'complete':True,'completed_forecasts':len(forecasts),
        'selected_from_development_only':selected,'scenarios':scenarios,'fresh_flat_later_validation':later,
        'fresh_post_launch_baseline':post_launch,
        'minimum_volume_feasibility_verified':False,'historical_ftmo_cost_accuracy_verified':False,'orders_sent':0}
    with gzip.open(output/'report.json.gz','wt')as f:json.dump(result,f,separators=(',',':'))


def aggregate(study: dict, root: Path, specs: dict, output: Path):
    output.mkdir(parents=True,exist_ok=True);reports=[];missing=[]
    for item in study['replays']:
        files=list(root.rglob(f"{item['symbol']}-{item['side']}/report.json.gz"))
        if len(files)!=1:
            missing.append(item);continue
        with gzip.open(files[0],'rt')as f:r=json.load(f)
        if not r.get('complete')or r.get('symbol')!=item['symbol']or r.get('side')!=item['side']:missing.append(item);continue
        reports.append(r)
    complete=not missing
    result={'complete':complete,'study':study,'cost_snapshot':specs,'missing_symbol_sides':missing,'reports':reports}
    with gzip.open(output/'results.json.gz','wt')as f:json.dump(result,f,separators=(',',':'))
    lines=['# FTMO Dukascopy hourly long/short study','',f"Period: {study['start']} 18:00 UTC through the day after {study['end']} at 17:00 UTC. Complete: {complete}.",'',
        'Long and short strategies are separate. 0.01 lot per addition, at most one addition per observed hourly bar. Long: below P01, average lower, exit at average execution entry + target. Short: above P99, average higher, exit at average execution entry − target. Carry until target; open positions remain marked to market at study end.','',
        'Spacing and target: 10 pips FX, 10 index points, $1 gold, $100 BTC. Daily five-model forecasts use 500 genuine completed hourly mid closes, cover 23 hours and become usable only after measured inference. Closed market hours are not filled.','',
        '## Cost and execution limits','',
        'These are Dukascopy price proxies under frozen CURRENT FTMO US specifications, not historical executable FTMO quotes or historical daily swap rates. Rolling indices, gold and BTC began on Jan 30, 2026. Percent commissions have separate per-side and round-trip interpretations; reference uses per-side. FX commission is verified $5/lot round-trip.','',
        'Bid/ask OHLC spreads are retained, widened to at least the user-supplied FTMO spread snapshot. The stress case doubles that floor, increases negative swaps 50%, and removes positive swaps. Interest/current swaps use annual percent/360, assumed triple Wednesday for FX/metals and Friday for indices; crypto and an alternate sensitivity accrue daily. Historical holidays, swap changes, index dividends, maintenance, percentage commission basis, minimum lot/step and rollover day require platform exports for exact replication.','',
        'The GMT+2/+3 server clock is modeled with Europe/Helsinki; risk-day reset uses Europe/Prague. FTMO daily loss is checked against midnight BALANCE, including current unrealized P&L, commissions and swaps. $100k / $5k daily / $10k total are diagnostics. Published volume caps and available margin block new additions; margin failures invalidate executable claims.','',
        'Two possible bid/ask hourly high/low orderings are replayed. They are sensitivities, not exact fills or confidence intervals. JPY/CAD P&L uses observed USDJPY/USDCAD conversion quotes; intrahour trade conversion uses the known hour-open rate.','',
        '## Full-year reference-cost comparison','',
        '| Symbol | Side | Baseline total net P&L | Baseline max DD | Dev-selected filter | Filter total net P&L | Filter max DD |','|---|---|---:|---:|---|---:|---:|']
    for r in reports:
        cases=[s for s in r['scenarios']if s['costs']==COSTS[0].name]
        base=[s for s in cases if s['rule']=='baseline'];chosen=[s for s in cases if s['rule']==r['selected_from_development_only']]
        def range_metric(values,key):
            numbers=[s['full'][key]for s in values];return f'${min(numbers):,.2f}–${max(numbers):,.2f}'
        lines.append(f"| {r['symbol']} | {r['side']} | {range_metric(base,'net_equity_pnl')} | {range_metric(base,'max_equity_drawdown')} | {r['selected_from_development_only']} | {range_metric(chosen,'net_equity_pnl')} | {range_metric(chosen,'max_equity_drawdown')} |")
    lines+=['','## Equity sessions and open liabilities: reference costs, high-first path','',
        '| Symbol | Side | Rule | Session W/L | Session win rate | Closed basket W/L | Entry W/L | Open net P&L | Commissions | Swaps |','|---|---|---|---|---:|---|---|---:|---:|---:|']
    for r in reports:
        for name in dict.fromkeys(['baseline',r['selected_from_development_only']]):
            s=next(s for s in r['scenarios']if s['costs']==COSTS[0].name and s['ordering']=='high_first'and s['rule']==name)
            m=s['full'];wr='—'if m['session_win_rate']is None else f"{100*m['session_win_rate']:.2f}%"
            lines.append(f"| {r['symbol']} | {r['side']} | {name} | {m['winning_sessions']}/{m['losing_sessions']} | {wr} | {m['basket_wins']}/{m['basket_losses']} | {m['entry_wins']}/{m['entry_losses']} | ${s['open_net_pnl']:.2f} | ${s['commission_paid']:.2f} | ${s['swap_accrued']:.2f} |")
    lines+=['','## Fresh flat later validation: frozen selection, reference costs','',
        '| Symbol | Side | Rule | Later net equity P&L | Maximum DD | Losing sessions |','|---|---|---|---:|---:|---:|']
    for r in reports:
        for name in dict.fromkeys(['baseline',r['selected_from_development_only']]):
            values=[s for s in r['fresh_flat_later_validation']if s['rule']==name and s['costs']==COSTS[0].name]
            if values:
                lines.append(f"| {r['symbol']} | {r['side']} | {name} | {range_metric(values,'net_equity_pnl')} | {range_metric(values,'max_equity_drawdown')} | {max(s['full']['losing_sessions']for s in values)} |")
    lines+=['','## Fresh post-launch baseline for rolling contracts','',
        '| Symbol | Side | Net equity P&L since Jan 30, 2026 | Maximum DD |','|---|---|---:|---:|']
    for r in reports:
        values=r['fresh_post_launch_baseline']
        if values:lines.append(f"| {r['symbol']} | {r['side']} | {range_metric(values,'net_equity_pnl')} | {range_metric(values,'max_equity_drawdown')} |")
    lines+=['','## Validation and selection','',
        'Nine candidates were fixed before replay. Selection uses only the first 274 origin days: at least 20 closed baskets and exposed sessions, positive net equity P&L retaining at least half of positive baseline profit, no worse drawdown, losing-session count or session win rate, and no development-period risk/margin breach. Minimize losing sessions, then drawdown, then maximize P&L. If none qualifies, keep baseline.','',
        'Later-period validation starts FLAT and replays baseline and the frozen selected filter independently. It never selects or tunes on the later results. Full-year alternative results remain research diagnostics; foundation model training data and revised source history can affect retrospective evaluation. Closed-target win rate must be read alongside losing equity sessions and open liabilities.','',
        f"Missing reports: {json.dumps(missing)}.",'',
        'Sources: https://ftmo.oanda.com/simulated-assets/ ; https://ftmo.oanda.com/blog/a-refined-trading-setup-for-the-us-market/ ; https://ftmo.oanda.com/blog/trading-updates/trading-update-jan-28-2026/ ; https://www.mql5.com/en/docs/constants/environment_state/marketinfoconstants ; https://jetta.dukascopy.com/v1 .','',
        'All 9 filters × 4 cost cases × 2 OHLC paths, fresh later validation, entry/basket W/L, session losses, commissions, swaps, exposure, breaches and retained baseline/selected trade logs are in results.json.gz. No exchange orders or broker login are used.']
    (output/'report.md').write_text('\n'.join(lines)+'\n')
    csv_rows=[]
    for report in reports:
        for s in report['scenarios']:
            csv_rows.append({'symbol':report['symbol'],'side':report['side'],'rule':s['rule'],'costs':s['costs'],'ordering':s['ordering'],
                'selected_on_development':s['rule']==report['selected_from_development_only'],**s['full'],
                **{k:s[k]for k in ('open_entries','open_lots','open_net_pnl','commission_paid','swap_accrued','max_open_lots','max_margin_usd',
                   'max_ftmo_midnight_balance_daily_loss','first_daily_limit_breach','first_total_limit_breach','first_margin_breach')}})
    if csv_rows:
        with (output/'all-scenarios.csv').open('w',newline='')as f:
            writer=csv.DictWriter(f,fieldnames=list(csv_rows[0]));writer.writeheader();writer.writerows(csv_rows)
    if not complete:raise ValueError('INCOMPLETE_STUDY_REPORTED_EXPLICITLY')


def main():
    parser=argparse.ArgumentParser();commands=parser.add_subparsers(dest='command',required=True)
    p=commands.add_parser('plan');p.add_argument('--start',type=date.fromisoformat,required=True);p.add_argument('--end',type=date.fromisoformat,required=True)
    p.add_argument('--symbols',default=','.join(INSTRUMENTS));p.add_argument('--output',type=Path,required=True)
    p=commands.add_parser('forecast');p.add_argument('--symbol',choices=INSTRUMENTS,required=True);p.add_argument('--start',type=date.fromisoformat,required=True);p.add_argument('--end',type=date.fromisoformat,required=True)
    p.add_argument('--source',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p=commands.add_parser('replay');p.add_argument('--symbol',choices=INSTRUMENTS,required=True);p.add_argument('--side',choices=('long','short'),required=True)
    for k in ('study','specs','sources','forecasts','output'):p.add_argument('--'+k,type=Path,required=True)
    p=commands.add_parser('aggregate')
    for k in ('study','specs','artifacts','output'):p.add_argument('--'+k,type=Path,required=True)
    args=parser.parse_args()
    if args.command=='plan':
        study=plan(args.start,args.end,[s.strip()for s in args.symbols.split(',')]);args.output.mkdir(parents=True,exist_ok=True)
        specs=snapshot_specs(study['symbols']);study['cost_snapshot_sha256']=digest(specs)
        for name,value in [('study-plan.json',study),('ftmo-cost-snapshot.json',specs)]:
            (args.output/name).write_text(json.dumps(value,indent=2)+'\n')
        print(json.dumps({'planned_forecasts':study['planned_forecasts'],'symbols':study['symbols'],'orders_sent':0}))
    elif args.command=='forecast':run_forecasts(args.symbol,args.start,args.end,args.source,args.output)
    elif args.command=='replay':
        study=json.loads(args.study.read_text());specs=json.loads(args.specs.read_text())
        if digest(specs)!=study['cost_snapshot_sha256']:raise ValueError('COST_SNAPSHOT_INTEGRITY_FAILED')
        run_replay(args.symbol,args.side,study,specs,args.sources,args.forecasts,args.output)
    else:aggregate(json.loads(args.study.read_text()),args.artifacts,json.loads(args.specs.read_text()),args.output)


if __name__=='__main__':main()
