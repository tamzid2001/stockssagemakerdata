"""Observed option-bar repricing after frozen average P10/P90 touches.

Separates independent-leg exits from closing the whole strangle on the first
target. Research-only; missing option observations never receive invented fills.
"""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path

import httpx
import pandas as pd

from .spy_exit_sweep import load_archive
from .spy_options import contract
from .spy_weekly import digest, iso, save

MODES=('matching_leg','both_legs')


def prices_by_minute(payload):
    result={}
    for row in payload['rows']:
        t=iso(pd.to_datetime(row['timestamp'],utc=True))
        values=[row[k] for k in ('open','high','low','close')]
        if any(type(v) not in (float,int) or not math.isfinite(v) or v<=0 for v in values):
            raise ValueError('INVALID_OPTION_PRICE_BAR')
        if t in result or row['low']>min(row['open'],row['close']) or row['high']<max(row['open'],row['close']):
            raise ValueError('INVALID_OPTION_BAR_ORDER_OR_DUPLICATE')
        result[t]=row
    return result


def target_comparison(week, underlying_minutes, legs, entry_start, entry_end, mode):
    if mode not in MODES:
        raise ValueError('INVALID_TARGET_EXIT_MODE')
    prices={name:prices_by_minute(data) for name,data in legs.items()}
    common=sorted(t for t in prices['call'].keys()&prices['put'].keys() if entry_start<=t<=entry_end)
    if not common:
        return {'status':'entry_unpriced','reason':'NO_COMMON_ENTRY_TRADE_MINUTE','mode':mode}
    entry=common[0]
    end=pd.Timestamp(week['end'])
    deadline=iso(end-pd.Timedelta(minutes=5))
    exit_latest=iso(end-pd.Timedelta(minutes=1))
    levels={'call':week['average_quantiles']['0.9'],'put':week['average_quantiles']['0.1']}
    first={}
    for row in sorted(underlying_minutes,key=lambda r:r['timestamp']):
        t=iso(pd.to_datetime(row['timestamp'],utc=True))
        # Do not use any entry-minute extreme whose order relative to the
        # independently observed option entry trade cannot be established.
        if t<=entry or t>=deadline:
            continue
        local=pd.Timestamp(t).tz_convert('America/New_York')
        if not (9*60+30<=local.hour*60+local.minute<16*60):
            continue
        for leg in ('call','put'):
            hit=row['high']>=levels[leg] if leg=='call' else row['low']<=levels[leg]
            if hit and leg not in first:
                first[leg]={'bar_start':t,'signal_available_at':iso(pd.Timestamp(t)+pd.Timedelta(minutes=1)),
                    'target':levels[leg],'underlying_high':row['high'],'underlying_low':row['low'],
                    'underlying_close':row['close'],'leg':leg}
    trigger=min(first.values(),key=lambda t:t['bar_start']) if first else None
    if trigger:
        trigger={**trigger,'same_minute_targets':[leg for leg,t in first.items() if t['bar_start']==trigger['bar_start']]}
    exit_plan={leg:(first.get(leg) if mode=='matching_leg' else trigger) for leg in ('call','put')}
    ledger=[]
    for leg in ('call','put'):
        decision=exit_plan[leg]
        earliest=decision['signal_available_at'] if decision else deadline
        # Bounded delayed-observation proxy, never a guaranteed target fill.
        latest=min(exit_latest,iso(pd.Timestamp(earliest)+pd.Timedelta(minutes=4)))
        eligible=prices[leg].keys() if mode=='matching_leg' else prices['call'].keys()&prices['put'].keys()
        candidates=sorted(t for t in eligible if earliest<=t<=latest)
        entry_price=prices[leg][entry]['open']
        record={'leg':leg,'entry_time':entry,'entry_bar_open':entry_price,'quantity_contracts':1,'assumed_multiplier':100,
            'matching_target':levels[leg],'matching_target_observed':leg in first,
            'matching_target_first_touch':first.get(leg),'exit_decision':decision,
            'exit_reason':'target_touch' if decision else 'friday_1555_time_exit',
            'exit_window_start':earliest,'exit_window_end':latest}
        if not candidates:
            record.update({'status':'exit_unpriced','exit_time':None,'net_pnl_proxy_before_costs':None})
        else:
            exit_time=candidates[0];exit_price=prices[leg][exit_time]['open']
            record.update({'status':'priced_trade_bar_proxy','exit_time':exit_time,'exit_bar_open':exit_price,
                'observation_delay_minutes':(pd.Timestamp(exit_time)-pd.Timestamp(earliest)).total_seconds()/60,
                'net_pnl_proxy_before_costs':(exit_price-entry_price)*100,
                'return_on_leg_premium_pct':(exit_price/entry_price-1)*100})
        ledger.append(record)
    complete=all(t['status']=='priced_trade_bar_proxy' for t in ledger)
    debit=sum(t['entry_bar_open']*100 for t in ledger)
    pnl=sum(t['net_pnl_proxy_before_costs'] for t in ledger) if complete else None
    return {'status':'priced_trade_bar_proxy' if complete else 'incomplete_exit_prices','mode':mode,'entry_time':entry,
        'target_touches':first,'legs':ledger,'entry_premiums_usd':debit,'net_pnl_proxy_before_costs':pnl,
        'return_on_premiums_pct':pnl/debit*100 if complete else None,
        'net_pnl_after_065_per_contract_per_fill':pnl-2.60 if complete else None,
        'target_exit_legs':sum(t['exit_reason']=='target_touch' for t in ledger),
        'timed_exit_legs':sum(t['exit_reason']=='friday_1555_time_exit' for t in ledger)}


def summarize(results):
    summaries={}
    for timing in ('friday_after_close','next_session_morning'):
        for mode in MODES:
            key=f'{timing}/{mode}'
            attempted=[r['scenarios'][key] for r in results if key in r.get('scenarios',{})]
            completed=[s for s in attempted if s['status']=='priced_trade_bar_proxy']
            pnl=sum(s['net_pnl_proxy_before_costs'] for s in completed)
            debit=sum(s['entry_premiums_usd'] for s in completed)
            wins=sum(s['net_pnl_proxy_before_costs']>0 for s in completed)
            losses=sum(s['net_pnl_proxy_before_costs']<0 for s in completed)
            summaries[key]={'attempted_weeks':len(results),'complete_weeks':len(completed),
                'incomplete_weeks':len(results)-len(completed),'profitable_pairs':wins,'losing_pairs':losses,
                'pair_win_rate':wins/len(completed) if completed else None,
                'total_entry_premiums_usd':debit,'net_pnl_proxy_before_costs':pnl if completed else None,
                'return_on_total_premiums_pct':pnl/debit*100 if debit else None,
                'net_pnl_after_065_per_contract_per_fill':pnl-2.60*len(completed) if completed else None,
                'target_exit_legs':sum(s['target_exit_legs'] for s in completed),
                'timed_exit_legs':sum(s['timed_exit_legs'] for s in completed),
                'profitable_legs':sum(t['net_pnl_proxy_before_costs']>0 for s in completed for t in s['legs']),
                'losing_legs':sum(t['net_pnl_proxy_before_costs']<0 for s in completed for t in s['legs']),
                'ranking_valid_for_full_sample':len(completed)==len(results)}
    return summaries


def run(archive, diagnostics, output):
    records,manifest=load_archive(archive)
    source=records['report-spy-source.json']
    diagnostics=json.loads(Path(diagnostics).read_text())
    if diagnostics['source_manifest_sha256']!=digest(manifest):
        raise ValueError('DIAGNOSTIC_SOURCE_MISMATCH')
    output=Path(output);output.mkdir(parents=True,exist_ok=True)
    results=[]
    with httpx.Client(timeout=90) as client:
        for week in diagnostics['weeks']:
            origin=pd.Timestamp(week['origin']);end=pd.Timestamp(week['end'])
            legs={};errors=[];contracts={}
            for leg,kind,q in (('call','C','0.9'),('put','P','0.1')):
                symbol,strike=contract(end,kind,week['average_quantiles'][q])
                contracts[leg]={'symbol':symbol,'strike':strike,'average_target':week['average_quantiles'][q]}
                request={'source':'alpaca','contractSymbol':symbol,'timeframe':'1Min','limit':5000,
                    'start':iso(origin),'end':iso(end-pd.Timedelta(minutes=1))}
                path=output/f'report-spy-option-path-{symbol}.json'
                if path.exists():
                    data=json.loads(path.read_text())
                    if data['request']!=request: raise ValueError('OPTION_CHECKPOINT_CONFLICT')
                else:
                    response=client.post('https://quantura.studio/api/market-data/options/history',json=request)
                    if response.status_code!=200:
                        errors.append({'contract':symbol,'http_status':response.status_code,'code':'HISTORY_UNAVAILABLE'});continue
                    data=response.json()
                    if not data.get('ok') or data.get('contractSymbol')!=symbol or data.get('provider')!='alpaca' or data.get('fallbackUsed'):
                        raise ValueError('UNEXPECTED_OPTION_HISTORY')
                    if len(data['rows'])>=5000: raise ValueError('POTENTIALLY_TRUNCATED_OPTION_PATH')
                    data.update({'request':request,'retrieved_at':iso(pd.Timestamp.now(tz='UTC')),'redistribution_status':'review_required'})
                    prices_by_minute(data)
                    save(path,data)
                legs[leg]=data
            if errors:
                results.append({'origin':week['origin'],'expiry':week['end'],'errors':errors});continue
            next_day=pd.Timestamp(week['initial_future_p50_time']).tz_convert('America/New_York').date()
            next_open=pd.Timestamp(str(next_day)+' 09:30',tz='America/New_York').tz_convert('UTC')
            windows={'friday_after_close':(origin+pd.Timedelta(minutes=5),origin+pd.Timedelta(minutes=14)),
                     'next_session_morning':(next_open,next_open+pd.Timedelta(minutes=10))}
            scenarios={f'{timing}/{mode}':target_comparison(week,source['rows'],legs,iso(a),iso(b),mode)
                       for timing,(a,b) in windows.items() for mode in MODES}
            results.append({'origin':week['origin'],'expiry':week['end'],'contracts':contracts,
                'source_sha256':{leg:digest(data) for leg,data in legs.items()},
                'source_rows':{leg:len(data['rows']) for leg,data in legs.items()},'scenarios':scenarios})
            print(json.dumps({'event':'option_target_week_checkpointed','origin':week['origin'],
                              'status':{k:v['status'] for k,v in scenarios.items()}}),flush=True)
    summary=summarize(results)
    report={'version':'spy_option_average_target_v1','source_run_id':manifest['run_id'],
        'source_manifest_sha256':digest(manifest),'diagnostics_sha256':digest(diagnostics),
        'analysis_sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in (Path(__file__),Path(__file__).with_name('spy_options.py'))},
        'weeks':results,'summary':summary,
        'methodology':{'size':'one 100-share call and one 100-share put each week; no recovery multiplier',
            'strikes':'nearest whole dollar to frozen average P90 call/P10 put; actual trade bars verify contract existence',
            'entry':'common actual option trade minute 16:05-16:14 Friday or 09:30-09:40 next NYSE session',
            'trigger':'underlying regular-session minute high reaches average P90 or low reaches average P10',
            'no_lookahead':'exclude entry-minute extremes; decision available only after full trigger minute ends',
            'target_exit':'first actual option trade-bar open after trigger-minute close, at most four extra minutes later',
            'paired_exit':'both legs must have an observed trade bar in the SAME minute',
            'unhit_target':'submit timed close at Friday 15:55 ET, at most 15:59; no intrinsic/expiry substitution',
            'unpriced_exit':'report incomplete; never silently carry to settlement or insert a zero-valued exit'},
        'limitations':['Historical option trade bars are not quotes, actionable bids/asks, or verified fills.',
            'First common minute is an ex-post trade-bar matching approximation; simultaneous execution is not established.',
            'Backend reports entitlement-default options feed; it is not verified as OPRA.',
            'IEX underlying prices are not consolidated SIP/NBBO. No weekend/overnight target monitoring.',
            'Friday entry uses completed-close forecasts with a research delay, not proven historical model availability.',
            'Next-session morning is Tuesday for the Labor Day week; forecasts remain frozen from Friday.',
            'Four weeks and current model checkpoints are retrospective research, not evidence of a repeatable live edge.',
            'Returns are on cumulative option premiums, not on underlying share capital or annualized returns.',
            'Untouched Friday time exit at 15:55 is an explicit research assumption, not user-selected execution evidence.']}
    save(output/'report-spy-option-targets.json',report)
    flat=[]
    for week in results:
        for scenario,data in week.get('scenarios',{}).items():
            for leg in data.get('legs',[]):
                flat.append({'origin':week['origin'],'expiry':week['expiry'],'scenario':scenario,
                    'contract':week['contracts'][leg['leg']]['symbol'],
                    **{k:v for k,v in leg.items() if not isinstance(v,dict)}})
    fields=list(dict.fromkeys(k for r in flat for k in r))
    with (output/'option_target_legs.csv').open('x',newline='') as target:
        writer=csv.DictWriter(target,fieldnames=fields);writer.writeheader();writer.writerows(flat)
    print(json.dumps({'summary':summary},indent=2))
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--archive',required=True);parser.add_argument('--diagnostics',required=True);parser.add_argument('--output',required=True)
    args=parser.parse_args();run(args.archive,args.diagnostics,args.output)
