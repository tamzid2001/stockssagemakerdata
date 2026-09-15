"""Frozen-forecast SPY strangle price-bar sensitivity; not executable option P&L.

Only uses the existing Quantura market-data provider abstraction. No brokerage
orders, modeled premiums, current option chain, or guessed missing prices.
"""
import argparse
import math
from pathlib import Path
import json

import httpx
import pandas as pd

from .spy_weekly import digest, iso, save


def contract(expiry, kind, level):
    strike=math.floor(level+.5)  # Nearest $1; explicit deterministic half-up rule.
    if kind not in ('C','P') or not 0<strike<10000:
        raise ValueError('INVALID_OPTION_REFERENCE')
    day=pd.Timestamp(expiry).strftime('%y%m%d')
    return f'SPY{day}{kind}{strike*1000:08d}',strike


def estimate(week, call, put, call_strike, put_strike, entry_start, entry_end):
    """First common actual trade bar in a bounded window; no stale/asof fills."""
    calls={r['timestamp']:r for r in call['rows']}
    puts={r['timestamp']:r for r in put['rows']}
    common=sorted(t for t in calls.keys()&puts.keys() if entry_start<=t<=entry_end)
    if not common:
        return {'available':False,'reason':'NO_SYNCHRONOUS_OPTION_TRADE_BARS','entry_window_start':entry_start}
    t=common[0]
    c,p=float(calls[t]['open']),float(puts[t]['open'])
    if any(not math.isfinite(v) or v<=0 for v in (c,p)):
        raise ValueError('INVALID_OPTION_PREMIUM')
    terminal=week['end_close']
    payoff=max(0.,terminal-call_strike)+max(0.,put_strike-terminal)
    total=c+p
    return {'available':True,'trade_bar_start':t,'call_trade_bar_open':c,'put_trade_bar_open':p,
        'combined_bar_premium_per_share':total,'gross_debit_one_standard_pair_usd':total*100,
        'underlying_friday_close':terminal,'cash_equivalent_intrinsic_per_share':payoff,
        'estimated_pnl_usd_before_costs':(payoff-total)*100,
        'estimated_return_pct_before_costs':(payoff/total-1)*100,
        'round_trip_065_per_contract_sensitivity_usd':(payoff-total)*100-2.60,
        'status':'trade_bar_proxy_not_verified_execution',
        'premium_breakeven_down':put_strike-total,'premium_breakeven_up':call_strike+total}


def run(diagnostics, output):
    source=json.loads(Path(diagnostics).read_text())
    output=Path(output);output.mkdir(parents=True,exist_ok=True)
    rows=[]
    with httpx.Client(timeout=90) as client:
        for week in source['weeks']:
            origin=pd.Timestamp(week['origin']); end=pd.Timestamp(week['end'])
            # Existing source stamps the opening minute; next_session label is
            # display only. Actual NYSE date is the initial future hourly P50.
            next_day=pd.Timestamp(week['initial_future_p50_time']).tz_convert('America/New_York').date()
            next_open=pd.Timestamp(str(next_day)+' 09:30',tz='America/New_York').tz_convert('UTC')
            legs=[];errors=[]
            for kind,q in (('C','0.9'),('P','0.1')):
                symbol,strike=contract(end,kind,week['average_quantiles'][q])
                path=output/f'report-spy-option-source-{symbol}.json'
                request={'source':'alpaca','contractSymbol':symbol,'timeframe':'1Min','limit':5000,
                    'start':iso(origin),'end':iso(next_open+pd.Timedelta(minutes=10))}
                if path.exists():
                    data=json.loads(path.read_text())
                    if data['request']!=request: raise ValueError('OPTION_CHECKPOINT_CONFLICT')
                else:
                    response=client.post('https://quantura.studio/api/market-data/options/history',json=request)
                    if response.status_code!=200:
                        errors.append({'contract':symbol,'http_status':response.status_code,'code':'HISTORY_UNAVAILABLE'})
                        continue
                    data=response.json()
                    if not data.get('ok') or data.get('contractSymbol')!=symbol or data.get('provider')!='alpaca' or data.get('fallbackUsed'):
                        raise ValueError('UNEXPECTED_OPTION_HISTORY')
                    data.update({'request':request,'retrieved_at':iso(pd.Timestamp.now(tz='UTC')),
                        'redistribution_status':'review_required','price_type':'trade_bar; exact entitled upstream feed not identified'})
                    save(path,data)
                legs.append((symbol,strike,data))
            if errors:
                rows.append({'origin':week['origin'],'available':False,'errors':errors});continue
            (cs,ck,c),(ps,pk,p)=legs
            windows={'friday_after_close':(origin+pd.Timedelta(minutes=5),origin+pd.Timedelta(minutes=14)),
                     'next_session_morning':(next_open,next_open+pd.Timedelta(minutes=10))}
            rows.append({'origin':week['origin'],'expiry':week['end'],'available':True,
                'call':cs,'call_strike':ck,'put':ps,'put_strike':pk,'call_source_sha256':digest(c),'put_source_sha256':digest(p),
                'call_minus_average_p90':ck-week['average_quantiles']['0.9'],
                'put_minus_average_p10':pk-week['average_quantiles']['0.1'],
                'both_otm_origin_reference':pk<week['last_actual_close']<ck,
                'both_otm_next_open_reference':pk<week['next_session_open']<ck,
                'estimates':{name:estimate(week,c,p,ck,pk,iso(a),iso(b)) for name,(a,b) in windows.items()}})
    summaries={}
    for name in ('friday_after_close','next_session_morning'):
        eligible=[r['estimates'][name] for r in rows if r.get('available') and r['estimates'][name]['available']]
        debit=sum(r['gross_debit_one_standard_pair_usd'] for r in eligible)
        pnl=sum(r['estimated_pnl_usd_before_costs'] for r in eligible)
        summaries[name]={'eligible_weeks':len(eligible),'attempted_weeks':len(rows),
            'positive_proxy_pnl_weeks':sum(r['estimated_pnl_usd_before_costs']>0 for r in eligible),
            'negative_proxy_pnl_weeks':sum(r['estimated_pnl_usd_before_costs']<0 for r in eligible),
            'total_premiums_proxy_usd':debit,'net_pnl_proxy_usd_before_costs':pnl,
            'return_on_total_premiums_pct':pnl/debit*100 if debit else None}
    report={'version':'spy_option_trade_bar_proxy_v1','source_diagnostics_sha256':digest(source),'weeks':rows,'summary':summaries,
        'methodology':{'strike_selection':'nearest $1 to frozen weekly average P90 call and P10 put; observed historical bars verify contracts',
            'size':'one standard 100-share call plus one standard 100-share put per week; no recovery multiplier',
            'friday_entry':'first common trade bar at/after 4:05pm ET, by 4:14pm; not an impossible zero-latency 4pm fill',
            'morning_entry':'first common trade bar 9:30-9:40am next NYSE session, using unchanged Friday forecast',
            'exit':'cash-equivalent Friday-close intrinsic proxy, not exercised shares or verified option closing fills'},
        'limitations':['Historical options BAR opens are not simultaneous bid/ask quotes or guaranteed fills.',
            'The backend reports entitlement-default, not certified OPRA; feed quality cannot be assumed.',
            'Underlying expiry reference is saved SPY IEX regular close, not verified official exercise/settlement price.',
            'No independently recorded historical model availability; four-week retrospective test, not a live performance record.',
            'Buying Friday and Monday uses different premiums; no Monday forecast retraining is implied.',
            'No intraday profit-taking tested for options. Exact average path levels need not be listed strikes.',
            'Costs, spreads, exercise charges, financing and taxes are excluded; pricing proxy only.']}
    save(output/'report-spy-options.json',report)
    print(json.dumps({'summary':summaries,'weeks':rows},indent=2))
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--diagnostics',required=True);parser.add_argument('--output',required=True)
    args=parser.parse_args();run(args.diagnostics,args.output)
