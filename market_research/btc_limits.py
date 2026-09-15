"""BTC post-only limit PAPER proxy, never an exchange order client.

Minute quotes cannot prove maker fills/queue depth. A strict later quote
trade-through is a candidate fill, not a verified execution. Touches don't fill.
An exit must fill before the opposite entry can be posted. Unfilled orders,
repricing, missed signals and settlement are part of the report, not discarded.
"""
from collections import Counter, defaultdict
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

from .engine import digest
from .recovery_switch import statistics

VERSION = 'btc_post_only_minus_one_cent_v1'


def limit_price(action, quote):
    if action == 'buy':
        value = (Decimal(str(quote['ask'])) - Decimal('.01')).quantize(Decimal('.01'), rounding=ROUND_FLOOR)
        return float(value) if 0 < value < Decimal(str(quote['ask'])) else None
    value = (Decimal(str(quote['bid'])) + Decimal('.01')).quantize(Decimal('.01'), rounding=ROUND_CEILING)
    return float(value) if Decimal(str(quote['bid'])) < value < 1 else None


def simulate(forecasts, observations, resolutions, *, as_of, fee_rate=0.):
    if type(fee_rate) not in (int, float) or not 0 <= fee_rate <= .1:
        raise ValueError('INVALID_FEE_ASSUMPTION')
    grouped = defaultdict(list)
    for f in forecasts:
        if f['available_at'] <= as_of:
            grouped[(f['market_context']['market_id'], f['origin'])].append(f)
    tape = defaultdict(dict)
    for q in observations:
        if q.get('observed') and q['timestamp'] % 60 == 0 and q['timestamp'] <= as_of and 0 <= q['bid'] <= q['ask'] <= 1:
            tape[q['contract_id']][q['timestamp']] = q
    trades, orders, signals, per_game = [], [], [], {}
    for (market, origin), pair in sorted(grouped.items()):
        if len(pair) != 2 or len({f['market_context']['contract_id'] for f in pair}) != 2 or len({f['available_at'] for f in pair}) != 1:
            continue
        sides = {f['market_context']['contract_id']: f for f in pair}
        curves = {s: {r['timestamp']: r['quantiles'] for r in f['rows']} for s, f in sides.items()}
        end = min(max(r['timestamp'] for r in f['rows']) for f in pair)
        available = pair[0]['available_at']
        times = sorted({t for s in sides for t in tape[s] if available < t < end and t <= as_of})
        position = pending = intent = None
        size = 1.
        ledger, book = [], []
        ambiguous = missed = increases = 0

        def post(t, action, side, why, next_side=None):
            nonlocal pending, missed
            quote = tape[side].get(t)
            price = limit_price(action, quote) if quote else None
            if price is None:
                missed += 1
                return
            pending = {'order_id': digest([VERSION, market, origin, t, action, side, len(book)]),
                'market': market, 'contract_id': side, 'action': action, 'posted_at': t,
                'limit_price': price, 'quantity': position['quantity'] if action == 'sell' else size,
                'post_only': True, 'reduce_only': action == 'sell', 'status': 'resting',
                'reason': why, 'next_side': next_side, 'quote_bid_at_post': quote['bid'],
                'quote_ask_at_post': quote['ask'], 'forecast_id': sides[side]['forecast_id']}
            book.append(pending)

        def close(t, price, reason):
            nonlocal position, size, increases
            gross = (price-position['entry_price'])*position['quantity']
            fees = fee_rate*(position['entry_price']+(0 if reason=='authoritative_settlement' else price))*position['quantity']
            position.update(status='closed',exit_at=t,exit_price=price,exit_reason=reason,
                            gross_pnl=gross,fees=fees,net_pnl=gross-fees)
            pnl = sum(t['net_pnl'] for t in ledger if t['status']=='closed')
            if pnl >= -1e-12:
                size=1.
            elif reason == 'p10_exit_and_opposite':
                old=size;size=min(size*2.5,100.);increases += size>old
            position.update(game_realized_net_pnl=pnl,next_quantity=size)
            position=None

        for t in times:
            quotes={s:tape[s][t] for s in sides if t in tape[s]}
            if pending and t > pending['posted_at']:
                q=quotes.get(pending['contract_id'])
                crossed=bool(q and (q['ask'] < pending['limit_price'] if pending['action']=='buy' else q['bid'] > pending['limit_price']))
                if crossed and t-pending['posted_at'] == 60:
                    order=pending;pending=None
                    order.update(status='candidate_fill',filled_at=t,fill_price=order['limit_price'],
                                 execution_verified=False)
                    if order['action']=='buy':
                        position={'trade_id':order['order_id'],'game_id':market,'variant':VERSION,
                            'contract_id':order['contract_id'],'entry_at':t,'entry_price':order['limit_price'],
                            'quantity':order['quantity'],'status':'open','entry_reason':order['reason'],
                            'signal_at':order['posted_at'],'execution_verified':False}
                        ledger.append(position);intent=None
                    else:
                        next_side=order['next_side'];close(t,order['limit_price'],order['reason'])
                        intent=(next_side,order['reason'])
                else:
                    pending.update(status='cancelled_reprice' if q and t-pending['posted_at']==60 else 'cancelled_missing_minute',cancelled_at=t)
                    pending=None
            active={s:curves[s].get(t) for s in sides}
            if position:
                held=position['contract_id'];other=next(s for s in sides if s!=held)
                reason=None
                if held in quotes and active[held] and quotes[held]['bid']<=active[held]['0.1']:
                    reason='p10_exit_and_opposite'
                elif other in quotes and active[other] and quotes[other]['bid']>=active[other]['0.9']:
                    reason='opposite_p90_switch'
                if reason and not intent:
                    intent=(other,reason)
                    signals.append({'market':market,'timestamp':t,'reason':reason,'held_side':held,'target_side':other})
                if intent and not pending:
                    post(t,'sell',held,intent[1],intent[0])
            else:
                candidates={}
                for side in sides:
                    q,level=quotes.get(side),active[side]
                    if not q or not level:continue
                    if q['bid']>=level['0.9']:candidates[side]='p90_entry'
                    if q['bid']<=level['0.1']:
                        candidates.setdefault(next(s for s in sides if s!=side),'opposite_p10_entry')
                if len(candidates)==1 and not intent:
                    intent=next(iter(candidates.items()))
                elif len(candidates)>1 and not intent:
                    ambiguous+=1
                if intent and not pending:
                    post(t,'buy',intent[0],intent[1])
        if pending and as_of>=end:
            pending.update(status='expired_at_market_close',cancelled_at=end)
        if position:
            r=resolutions.get(position['contract_id'],{})
            if r.get('resolution_status')=='resolved' and r.get('settled_at',as_of+1)<=as_of and r.get('selected_side_payout') in (0,1):
                close(r['settled_at'],r['selected_side_payout'],'authoritative_settlement')
            else:
                marks=[q for t,q in tape[position['contract_id']].items() if position['entry_at']<=t<=min(as_of,end)]
                bid=max(marks,key=lambda q:q['timestamp'])['bid'] if marks else position['entry_price']
                position['mark_net_pnl']=(bid-position['entry_price']-fee_rate*(bid+position['entry_price']))*position['quantity']
        trades.extend(ledger);orders.extend(book)
        per_game[market]={**statistics(ledger),'order_counts':dict(Counter(o['status'] for o in book)),
                          'ambiguous_entries':ambiguous,'invalid_or_missing_limit_quotes':missed,'multiplier_increases':increases}
    return {'version':VERSION,'paper_only':True,'execution_verified':False,'as_of':as_of,
        'summary':{**statistics(trades),'markets_evaluated':len(per_game),'markets_traded':len({t['game_id'] for t in trades}),
                   'order_counts':dict(Counter(o['status'] for o in orders))},
        'per_game':per_game,'trades':trades,'orders':orders,'signals':signals,
        'configuration':{'fee_rate_assumption':fee_rate,'base_shares':1,'max_shares':100,'multiplier':2.5,
            'entry_limit':'current ask minus $0.01, rounded down to cent; must be non-marketable',
            'exit_limit':'current bid plus $0.01, rounded up to cent; reduce-only; must be non-marketable',
            'reprice':'cancel/replace on each completed minute while intent remains active',
            'fill_proxy':'strict trade-through on next consecutive minute quote; touches and missing minutes never fill',
            'switch':'exit candidate fill must precede posting opposite buy; no concurrent YES/NO holdings',
            'sizing':'held P10 exit multiplies if cumulative game net PnL remains negative; reset on recovery'},
        'limitations':['Quote trade-through is NOT proof of maker execution or available size; win rate/return are conditional paper proxies.',
            'Unfilled limits can prevent trading a market or holding any position at settlement. No forced fills.',
            'Zero maker fees are a sensitivity assumption, not a verified historical schedule. Settlement has no exit commission.',
            'Signals use completed minute quotes, not intraminute extrema or webhooks.']}
