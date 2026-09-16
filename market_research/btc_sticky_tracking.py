"""Receipt-timed BTC P90 + sticky-direction taker/recursive-ladder research.

No account/order API. Ask snapshots are counterfactual fills, never executions.
Keep these versioned experiments separate from legacy flat-fee/maker reports.
"""
from decimal import Decimal, ROUND_CEILING
import gzip
import heapq
import json
import math

from .btc_hold_tracking import first_signals, prospective_observations
from .engine import digest
from .recovery_switch import statistics

VERSION = 'btc_sticky_next_ask_recursive_ladder_v1'


def direction_at(settlements, timestamp, current_market):
    """For binary outcomes, sticky-until-correct == opposite latest settlement.

    Every settlement updates the state even when P90 disagrees/no trade occurs.
    Later retrievals/revisions cannot change what was known at this timestamp.
    """
    known = {}
    for row in settlements:
        if (row.get('result') in ('yes','no') and row.get('resolution_status') == 'resolved'
                and row['market_id'] != current_market
                and row['close_at'] <= row['first_confirmed_at'] < timestamp):
            old = known.get(row['market_id'])
            if old is None or old['first_confirmed_at'] < row['first_confirmed_at']:
                known[row['market_id']] = row
    latest = max(known.values(), key=lambda r:(r['close_at'],r['market_id']), default=None)
    if latest is None:
        return None
    return {'side':'no' if latest['result']=='yes' else 'yes',
            'prior_market_id':latest['market_id'], 'prior_result':latest['result'],
            'confirmed_at':latest['first_confirmed_at'],
            'confirmation_clock':latest.get('confirmation_clock','recorded_receipt')}


def taker_fee(quantity, price, precision='0.0001', multiplier=1):
    """Current quadratic schedule, ONE aggregate fill + explicit balance rounding.

    Partial-fill rounding/accumulators require actual exchange fills and are not
    claimed here. Both direct (4dp) and non-direct (cent) sensitivities are run.
    """
    q,p,m = (Decimal(str(v)) for v in (quantity,price,multiplier))
    if precision not in ('0.0001','0.01') or not all(v.is_finite() for v in (q,p,m)) or q<=0 or not 0<p<1 or m<0:
        raise ValueError('INVALID_TAKER_FEE_INPUT')
    fee = (Decimal('.07')*m*q*p*(1-p)).quantize(Decimal('.000001'), rounding=ROUND_CEILING)
    return float((q*p+fee).quantize(Decimal(precision), rounding=ROUND_CEILING)-q*p)


def _settlement(settlements, market_id, as_of):
    return max((r for r in settlements if r['market_id']==market_id
        and r.get('resolution_status')=='resolved' and r.get('result') in ('yes','no')
        and r['close_at'] <= r['first_confirmed_at'] <= as_of),
        key=lambda r:r['first_confirmed_at'], default=None)


def simulate(signals, observations, settlements, *, as_of, policy, precision, multiplier):
    if policy not in ('fixed_one','recover_cycle','recursive_ladder'):
        raise ValueError('INVALID_STICKY_POLICY')
    tape = {(q['contract_id'],q['timestamp']):q for q in observations}
    trades,orders,misses,pending = [],[],[],[]
    size,cycle,realized = 1,0.,0.

    def recognize(before):
        nonlocal size,cycle,realized
        while pending and pending[0][0] < before:
            _,_,trade = heapq.heappop(pending)
            net = trade['net_pnl']; cycle += net; realized += net
            if cycle >= -1e-10:
                cycle,size = 0.,1
            elif net < 0:
                size = min(100, math.floor(size*2.5))
            trade.update(next_recovery_size=size, recovery_cycle_pnl=cycle,
                         confirmed_cumulative_pnl=realized)

    for signal in sorted(signals,key=lambda r:(r['signal_received_at'],r['market_id'])):
        side = signal['contract_id']
        q = tape.get((side,signal['signal_at']+60))
        if not q or not signal['signal_received_at']<q['received_at']<signal['market_end'] or not 0<q['ask']<1:
            misses.append({**signal,'reason':'MISSING_OR_UNTRADEABLE_NEXT_MINUTE_ASK'})
            continue
        recognize(q['received_at'])
        quantity = size if policy=='recover_cycle' else 1
        fills = [{'timestamp':q['timestamp'],'received_at':q['received_at'],
                  'price':q['ask'],'quantity':quantity,'rung':0}]
        if policy == 'recursive_ladder':
            anchor = Decimal(str(q['ask']))
            active_at, next_rung, used = q['timestamp'],1,1
            threshold = anchor-Decimal('.10')
            active = None
            for t in range(q['timestamp']+60,min(signal['market_end'],as_of+1),60):
                if threshold < Decimal('.10') or used >= 100:
                    break
                current = tape.get((side,t))
                if active is None:
                    active = {'market_id':signal['market_id'],'contract_id':side,'rung':next_rung,
                        'threshold':float(threshold),'armed_after_minute':active_at,
                        'quantity':min(2**next_rung,100-used),'status':'armed',
                        'execution_verified':False}
                    orders.append(active)
                if current is None:
                    # No price interpolation or assumed fills through a gap.
                    active['missing_minutes'] = active.get('missing_minutes',0)+1
                    active.pop('trigger_minute',None)
                    continue
                trigger = active.get('trigger_minute')
                if trigger is not None and t==trigger+60 and current['received_at']<signal['market_end']:
                    if 0<current['ask']<=float(threshold):
                        count=active['quantity']
                        fills.append({'timestamp':t,'received_at':current['received_at'],
                                      'price':current['ask'],'quantity':count,'rung':next_rung})
                        active.update(status='candidate_taker_fill',filled_at=current['received_at'],fill_price=current['ask'])
                        used += count; next_rung += 1; active_at=t
                        threshold=anchor-Decimal('.10')*next_rung
                        active=None
                        continue  # Never fill a second rung from the same candle.
                    active['unfilled_attempts']=active.get('unfilled_attempts',0)+1
                    active.pop('trigger_minute',None)
                if t>active_at and 0<current['ask']<=float(threshold):
                    active['trigger_minute']=t
            if active and as_of>=signal['market_end']:
                active['status']='expired_unfilled'
        for fill in fills:
            fill['fee']=taker_fee(fill['quantity'],fill['price'],precision,multiplier)
        count=sum(f['quantity'] for f in fills)
        cost=sum(f['quantity']*f['price'] for f in fills)
        fees=sum(f['fee'] for f in fills)
        trade={**signal,'trade_id':digest([VERSION,policy,precision,signal['market_id']]),
            'entry_at':q['received_at'],'entry_price':cost/count,'quantity':count,'fills':fills,
            'fees':fees,'status':'open','execution_verified':False,'known_pnl_before_entry':realized}
        resolution=_settlement(settlements,signal['market_id'],as_of)
        if resolution and resolution['close_at']>q['received_at']:
            payout=int(side.rsplit(':',1)[-1]==resolution['result'])
            gross=count*payout-cost
            trade.update(status='closed',exit_at=resolution['close_at'],exit_price=payout,
                exit_reason='authoritative_settlement',outcome_confirmed_at=resolution['first_confirmed_at'],
                gross_pnl=gross,net_pnl=gross-fees)
            heapq.heappush(pending,(resolution['first_confirmed_at'],trade['trade_id'],trade))
        else:
            marks=[v for (s,t),v in tape.items() if s==side and q['timestamp']<=t<=as_of]
            mark=max(marks,key=lambda v:v['timestamp']) if marks else None
            trade.update(mark_at=mark['timestamp'] if mark else None,
                         mark_net_pnl=count*mark['bid']-cost-fees if mark else None)
        trades.append(trade)
    recognize(as_of+1)
    summary=statistics(trades)
    summary.update(candidate_entries=len(trades),missed_entries=len(misses),
        ladder_rungs_armed=len(orders),ladder_rungs_filled=sum(o['status']=='candidate_taker_fill' for o in orders),
        total_contracts=sum(t['quantity'] for t in trades),max_position=max((t['quantity'] for t in trades),default=0),
        cap_positions=sum(t['quantity']==100 for t in trades))
    return {'policy':policy,'balance_precision':precision,'summary':summary,
            'trades':trades,'ladder_orders':orders,'missed_entries':misses}


def compare(forecasts, observations, settlements, fee_policy, *, as_of):
    tape=prospective_observations(observations,as_of)
    signals,ambiguous=first_signals(forecasts,tape,as_of)
    calls=[]; agreed=[]
    for signal in signals:
        direction=direction_at(settlements,signal['signal_received_at'],signal['market_id'])
        agreement=bool(direction and signal['contract_id'].endswith(':'+direction['side']))
        call={**signal,'direction':direction,'agreement':agreement}
        calls.append(call)
        if agreement: agreed.append(call)
    valid_fee=(fee_policy.get('fee_type')=='quadratic' and
        type(fee_policy.get('multiplier')) in (int,float) and
        math.isfinite(fee_policy['multiplier']) and fee_policy['multiplier']>=0)
    scenarios={}
    if valid_fee:
        for precision in ('0.0001','0.01'):
            for policy in ('fixed_one','recover_cycle','recursive_ladder'):
                scenarios[policy+'_'+precision]=simulate(agreed,tape,settlements,as_of=as_of,
                    policy=policy,precision=precision,multiplier=fee_policy['multiplier'])
    return {'version':VERSION,'paper_only':True,'live_orders_enabled':False,'generated_at':as_of,
        'fee_policy':fee_policy,'fee_status':'current_schedule_sensitivity' if valid_fee else 'unavailable_no_financial_results',
        'coverage':{'timely_minute_side_rows':len(tape),'settlement_events':len(settlements),
            'first_p90_signals':len(signals),'ambiguous_minutes':ambiguous,'agreements':len(agreed),
            'no_confirmed_prior':sum(c['direction'] is None for c in calls)},
        'signals':calls,'scenarios':scenarios,
        'methodology':{'direction':'opposite latest confirmed settled result, independent of forecasts/trades',
            'entry':'first unambiguous completed-minute bid >= P90, agreeing direction, next minute ask',
            'exit':'hold to authoritative settlement; no switching or stop loss',
            'recovery':'1 contract; floor(2.5x) after a loss until cycle P&L >= 0; max 100',
            'ladder':'separate 1/2/4/8 sizing, initial actual ask minus 10c per rung, floor 10c, total cap 100; sequential',
            'ladder_execution':'minute ask <= threshold triggers next-minute marketable-limit candidate capped at threshold; no same-minute cascade',
            'fees':'current series quadratic schedule sensitivity; one aggregate fill per rung, direct/non-direct rounding variants; partial fills unverified',
            'limitations':'No actual orders, orderbook depth, latency or verified fills; missing/late minutes excluded, no interpolation; realized drawdown is not bankroll drawdown'}}


def from_store(store, as_of):
    observations=store.values('observations')
    # Prefer new paired receipts, but retain historical evidence without changing
    # its timestamps. Backfilled archived rows remain excluded from live evidence.
    merged={(q['contract_id'],q['timestamp']):q for q in observations}
    for row in store.values('btc_minutes'):
        for side in ('yes','no'):
            q={'contract_id':row['market_id']+':'+side,'game_id':row['event_id'],
                'timestamp':row['timestamp'],'received_at':row['received_at'],
                'collection_mode':row['collection_mode'],'observed':True,
                'ask':row[side+'_ask'],'bid':row[side+'_bid']}
            key=q['contract_id'],q['timestamp']
            if key not in merged or q['received_at']<merged[key]['received_at']:
                merged[key]=q
    settlements=store.values('btc_settlements')
    # Existing checkpoints remain usable only where the original recorded
    # confirmation time exists. Never reconstruct missing knowledge from prices.
    from .engine import stamp
    for row in store.values('checkpoints'):
        market=row.get('market') if isinstance(row,dict) else None
        if not market:
            continue
        r=store._get('checkpoints','resolution:'+market['ticker']+':yes') or {}
        confirmed=r.get('first_confirmed_at')
        if r.get('resolution_status')=='resolved' and r.get('result') in ('yes','no') and type(confirmed) is int:
            settlements.append({'market_id':market['ticker'],'close_at':stamp(market['close_time']),
                'first_confirmed_at':confirmed,'result':r['result'],'resolution_status':'resolved',
                'confirmation_clock':'legacy_recorded_receipt'})
    result=compare(store.values('forecasts'),list(merged.values()),settlements,
                   store._get('checkpoints','btc_fee_policy') or {},as_of=as_of)
    result['collector']=store._get('checkpoints','btc_collector_health')
    result['minute_archive_rows']=len(store.values('btc_minutes'))
    return result


def compact(result):
    return {**{k:v for k,v in result.items() if k not in ('signals','scenarios')},
            'scenarios':{k:{'summary':v['summary'],'policy':v['policy'],'balance_precision':v['balance_precision']}
                         for k,v in result['scenarios'].items()}}


def export(store):
    """Only included in the authenticated encrypted checkpoint, never public logs."""
    import csv
    import io
    import time
    result=from_store(store,int(time.time()))
    with gzip.GzipFile(filename=str(store.directory/'btc_sticky_tracking.json.gz'),mode='wb',mtime=0) as out:
        out.write(json.dumps(result,allow_nan=False,separators=(',',':')).encode())
    fields=['market_id','event_id','timestamp','received_at','yes_ask','yes_bid','no_ask','no_bid',
            'collection_mode','timely','source','no_side_method','orderbook_depth_verified']
    with gzip.GzipFile(filename=str(store.directory/'btc_paired_minutes.csv.gz'),mode='wb',mtime=0) as out:
        with io.TextIOWrapper(out,encoding='utf-8',newline='') as text:
            writer=csv.DictWriter(text,fieldnames=fields,extrasaction='ignore');writer.writeheader()
            writer.writerows(sorted(store.values('btc_minutes'),key=lambda r:(r['market_id'],r['timestamp'])))
    return result
