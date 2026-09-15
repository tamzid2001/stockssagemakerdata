"""Across-market hold sizing using only settlements known before each entry."""
from copy import deepcopy
import heapq
import math
from .recovery_switch import statistics

POLICIES = ('fixed_one', 'reset_after_win', 'recover_cycle')


def replay(trades, confirmations, *, as_of, policy, quantity_step=0., multiplier=2.5, cap=100):
    if policy not in POLICIES or quantity_step not in (0., 1.):
        raise ValueError('INVALID_HOLD_SIZING_POLICY')
    if not all(math.isfinite(v) for v in (multiplier, cap)) or not 1 <= multiplier <= 10 or not 1 <= cap <= 100:
        raise ValueError('INVALID_HOLD_SIZING_LIMITS')
    source=sorted(deepcopy(trades),key=lambda t:(t['entry_at'],t['trade_id']))
    if len({t['game_id'] for t in source}) != len(source):
        raise ValueError('ONE_HOLD_ENTRY_PER_MARKET_REQUIRED')
    pending=[]; ledger=[]; size=1.; cycle=0.; realized=0.; completed=0

    def recognize(before):
        nonlocal size, cycle, realized, completed
        while pending and pending[0][0] < before:
            _,_,row=heapq.heappop(pending)
            net=row['net_pnl']; realized+=net; cycle+=net; completed+=1
            if policy=='fixed_one':
                size=1.
            elif policy=='reset_after_win':
                if net < -1e-12: size=min(cap,size*multiplier)
                elif net > 1e-12: size=1.
            elif cycle >= -1e-12:
                size=1.; cycle=0.
            elif net < -1e-12:
                size=min(cap,size*multiplier)
            if quantity_step: size=max(1., math.floor(size))
            row.update(next_size_when_confirmed=size, cumulative_confirmed_net_pnl=realized,
                       recovery_cycle_net_pnl=cycle)

    unconfirmed=0
    for row in source:
        if row['entry_at'] > as_of: continue
        if row['quantity'] != 1: raise ValueError('UNIT_HOLD_LEDGER_REQUIRED')
        recognize(row['entry_at'])
        row.update(quantity=size, sizing_policy=policy, known_pnl_before_entry=realized,
                   known_closed_trades_before_entry=completed)
        if row['status']=='closed':
            if row['exit_at'] <= row['entry_at']: raise ValueError('NONCAUSAL_HOLD_EXIT')
            known=confirmations.get(row['contract_id'])
            for field in ('gross_pnl','fees','net_pnl'): row[field] *= size
            # Legacy outcomes lacking receipt time may be scored descriptively,
            # but never change a subsequent order's quantity using hindsight.
            if type(known) is int and row['exit_at'] <= known <= as_of:
                row['outcome_confirmed_at']=known
                heapq.heappush(pending,(known,row['trade_id'],row))
            else: unconfirmed+=1
        elif row['status']=='open': row['mark_net_pnl'] *= size
        else: raise ValueError('INVALID_HOLD_TRADE_STATUS')
        ledger.append(row)
    recognize(as_of+1)
    summary=statistics(ledger)
    summary.update(cap_entries=sum(t['quantity']==cap for t in ledger),
        outcomes_without_causal_confirmation=unconfirmed,
        max_entry_notional=max((t['quantity']*t['entry_price'] for t in ledger),default=0))
    return {'summary':summary,'trades':ledger,'configuration':{'base_shares':1,'multiplier':multiplier,
        'cap':cap,'policy':policy,'quantity_step':quantity_step,'rounding':'floor' if quantity_step else 'legacy_fractional',
        'scope':'successive markets in this immutable lineage only',
        'result_timing':'recorded confirmation strictly before entry',
        'partial_win':'retain size until recovery; never multiply because of a partial win',
        'return_basis':'net_closed_pnl / closed_entry_notional, not bankroll ROI'}}
