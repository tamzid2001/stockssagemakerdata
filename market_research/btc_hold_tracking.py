"""Versioned first-P90 hold-to-settlement monitoring; no exchange execution.

Separate the preserved quote benchmark from receipt-timed prospective evidence
and conservative post-only candidate fills. Never manufacture fill certainty.
"""
from collections import Counter, defaultdict
import math
from .btc_hold import compare as hold
from .btc_hold_sizing import POLICIES, replay
from .btc_limits import limit_price
from .engine import digest

VERSION='btc_first_p90_hold_tracking_v1'
MAX_RECEIPT_DELAY_SECONDS=30


def _settle(row, observations, resolutions, as_of, fee_rate):
    r=resolutions.get(row['contract_id'],{})
    if (r.get('resolution_status')=='resolved' and r.get('settled_at',as_of+1)<=as_of
            and r.get('selected_side_payout') in (0,1) and r['settled_at']>row['entry_at']):
        price=r['selected_side_payout'];gross=price-row['entry_price'];fees=fee_rate*row['entry_price']
        row.update(status='closed',exit_at=r['settled_at'],exit_price=price,
                   exit_reason='authoritative_settlement',gross_pnl=gross,fees=fees,net_pnl=gross-fees)
    else:
        marks=[q for q in observations if q['contract_id']==row['contract_id'] and
               row['entry_at']<=q['timestamp']<=as_of]
        mark=max(marks,key=lambda q:q['timestamp']) if marks else None
        row.update(mark_net_pnl=(mark['bid'] if mark else row['entry_price'])-row['entry_price']*(1+fee_rate),
                   mark_at=mark['timestamp'] if mark else None)
    return row


def first_signal_limits(entries, observations, forecasts, resolutions, as_of):
    """First selected side only, no switching. Reprice -1c each received minute."""
    tape=defaultdict(dict)
    for q in observations: tape[q['contract_id']][q['timestamp']]=q
    ends={f['market_context']['contract_id']:max(r['timestamp'] for r in f['rows']) for f in forecasts}
    orders=[]; trades=[]
    for entry in entries:
        side=entry['contract_id'];end=ends[side];pending=None;position=None
        times=sorted(t for t in tape[side] if entry['signal_at']<=t<end and t<=as_of)
        for t in times:
            q=tape[side][t];received=q['received_at']
            if pending:
                if (t==pending['quote_at']+60 and received>pending['posted_at']
                        and q['ask']<pending['limit_price']):
                    pending.update(status='candidate_fill',filled_at=received,fill_price=pending['limit_price'],
                                   execution_verified=False)
                    position={'trade_id':pending['order_id'],'game_id':entry['game_id'],'contract_id':side,
                        'signal_at':entry['signal_at'],'entry_at':received,'entry_price':pending['limit_price'],
                        'quantity':1.,'status':'open','execution_verified':False}
                    pending=None;break
                pending.update(status='cancelled_reprice' if t==pending['quote_at']+60 else 'cancelled_missing_minute',
                               cancelled_at=received)
                pending=None
            price=limit_price('buy',q)
            if price is None or received>=end: continue
            pending={'order_id':digest([VERSION,side,entry['signal_at'],received]),'contract_id':side,
                'market':entry['game_id'],'signal_at':entry['signal_at'],'quote_at':t,'posted_at':received,
                'limit_price':price,'quantity':1.,'post_only':True,'reduce_only':False,'status':'resting',
                'bid_at_post':q['bid'],'ask_at_post':q['ask'],'execution_verified':False}
            orders.append(pending)
        if pending and as_of>=end: pending.update(status='expired_unfilled',cancelled_at=end)
        if position: trades.append(_settle(position,observations,resolutions,as_of,.01))
    return trades,orders


def first_signals(forecasts, observations, as_of):
    """Latch the first side even if its subsequent order never fills."""
    groups=defaultdict(list);tape=defaultdict(dict)
    for f in forecasts: groups[f['market_context']['market_id']].append(f)
    for q in observations: tape[q['contract_id']][q['timestamp']]=q
    signals=[];ambiguous=0
    for market,pair in sorted(groups.items()):
        if len(pair)!=2 or len({f['available_at'] for f in pair})!=1: continue
        sides={f['market_context']['contract_id']:f for f in pair}
        if len(sides)!=2: continue
        curves={s:{r['timestamp']:r['quantiles'] for r in f['rows']} for s,f in sides.items()}
        end=min(max(curve) for curve in curves.values())
        times=sorted(set.intersection(*(set(tape[s]) for s in sides)))
        for t in times:
            if not pair[0]['available_at']<t<end or t>as_of: continue
            candidates=[s for s in sides if t in curves[s] and tape[s][t]['bid']>=curves[s][t]['0.9']]
            if len(candidates)>1: ambiguous+=1;continue
            if len(candidates)!=1: continue
            side=candidates[0];q=tape[side][t]
            if q['received_at']>=end: continue
            signals.append({'game_id':pair[0]['market_context']['event_id'],'market_id':market,
                'contract_id':side,'signal_at':t,'signal_received_at':q['received_at'],
                'forecast_id':sides[side]['forecast_id'],'signal_bid':q['bid'],
                'signal_p90':curves[side][t]['0.9'],'market_end':end})
            break
    return signals,ambiguous


def prospective_observations(observations, as_of):
    """Shared receipt-timed minute tape; never promote backfills to live evidence."""
    return [q for q in observations if q.get('collection_mode')=='live' and q.get('observed')
            and type(q.get('received_at')) is int and type(q.get('timestamp')) is int and q['timestamp']%60==0
            and all(type(q.get(k)) in (int,float) and math.isfinite(q[k]) for k in ('bid','ask'))
            and 0<=q['bid']<=q['ask']<=1
            and q['timestamp']<=q['received_at']<=min(as_of,q['timestamp']+MAX_RECEIPT_DELAY_SECONDS)]


def compare(forecasts, observations, resolutions, *, as_of):
    origins=defaultdict(set)
    for f in forecasts: origins[f['market_context']['market_id']].add(f['origin'])
    if any(len(v)!=1 for v in origins.values()): raise ValueError('ONE_BTC_ORIGIN_PER_MARKET_REQUIRED')
    lengths={f.get('history_count') for f in forecasts}
    if lengths- {1,2} or len(lengths)>1: raise ValueError('SEPARATE_BTC_HORIZON_COHORTS_REQUIRED')
    history=next(iter(lengths),None)
    benchmark=hold(forecasts,observations,resolutions,as_of=as_of)
    confirmed={s:r.get('first_confirmed_at',r.get('checked_at')) for s,r in resolutions.items()}
    timely=prospective_observations(observations,as_of)
    # Historical/backfilled quotes never appear in the prospective subset.
    signals,ambiguous=first_signals(forecasts,timely,as_of)
    by_quote={(q['contract_id'],q['timestamp']):q for q in timely}
    prospective=[]
    for signal in signals:
        q=by_quote.get((signal['contract_id'],signal['signal_at']+60))
        if not q or not signal['signal_received_at']<q['received_at']<signal['market_end'] or not 0<q['ask']<1: continue
        t={**signal,'trade_id':digest([VERSION,'quote',signal['market_id'],signal['signal_at']]),
            'entry_at':q['received_at'],'entry_price':q['ask'],'quantity':1.,'status':'open'}
        prospective.append(_settle(t,timely,resolutions,as_of,.01))
    limit_trades,orders=first_signal_limits(signals,timely,forecasts,resolutions,as_of)
    scenarios={}
    for label,trades,step in (('legacy_quote_benchmark',benchmark['trades'],0.),
            ('prospective_quote_integer',prospective,1.),('post_only_candidate_integer',limit_trades,1.)):
        scenarios[label]={p:replay(trades,confirmed,as_of=as_of,policy=p,quantity_step=step) for p in POLICIES}
    return {'version':VERSION,'paper_only':True,'live_orders_enabled':False,'as_of':as_of,
        'history_minutes':history,'horizon_minutes':15-history if history else None,
        'scenarios':scenarios,'orders':orders,'signals':signals,
        'coverage':{'forecast_markets':len(origins),'observations':len(observations),
            'timely_prospective_observations':len(timely),'excluded_untimed_or_late_observations':len(observations)-len(timely),
            'first_signal_markets':len(signals),'ambiguous_minutes_excluded':ambiguous,
            'quote_entries_missed':len(signals)-len(prospective),
            'order_counts':dict(Counter(o['status'] for o in orders))},
        'live_readiness':{'ready':False,'maker_fills_verified':False,'fee_schedule_verified':False,
            'requires':['actual order acknowledgements and fills','market-specific fee/quantity/tick validation',
                        'capital and exposure limits','out-of-sample validation']},
        'methodology':{'signal':'first unambiguous at-or-above-P90 completed-minute bid; not forced fresh crossing',
            'exit':'official settlement only; no P10 switch, take-profit or trailing stop',
            'entry_limit':'ask minus $0.01 rounded down to cent; same side, reprice until close',
            'fill':'strict next-minute ask trade-through, candidate only, never proof of execution',
            'order_ledger_quantity':'unit-size order candidates; sizing scenarios are separate analytical overlays, not verified queued sizes',
            'receipt_delay_limit_seconds':MAX_RECEIPT_DELAY_SECONDS,
            'fee_assumption':'1% entry notional; no settlement sell fee; not verified maker fees',
            'sizing':'across successive markets separately within each horizon; no cross-cohort pooling',
            'drawdown':'realized dollar drawdown; open marks separately, no bankroll ROI claim'}}
