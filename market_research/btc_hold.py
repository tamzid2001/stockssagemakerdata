"""First P90 entry only, then official settlement; never reset or cherry-pick."""
from .recovery_switch import simulate, statistics


def compare(forecasts, observations, resolutions, *, as_of, fee_rate=.01):
    baseline=simulate(forecasts,observations,resolutions,as_of=as_of,p90_touch=True,switch_on_other_p90=True)
    first={}
    for trade in sorted(baseline['trades'],key=lambda t:(t['entry_at'],t['trade_id'])):
        first.setdefault(trade['game_id'],trade)
    ledger=[]
    for t in first.values():
        row={k:t[k] for k in ('trade_id','game_id','contract_id','signal_at','entry_at','entry_price')}
        row.update(quantity=1.,variant='first_p90_hold_to_settlement_v1',status='open')
        r=resolutions.get(t['contract_id'],{})
        if r.get('resolution_status')=='resolved' and r.get('settled_at',as_of+1)<=as_of and r.get('selected_side_payout') in (0,1):
            payout=r['selected_side_payout'];gross=payout-row['entry_price'];fees=fee_rate*row['entry_price']
            row.update(status='closed',exit_at=r['settled_at'],exit_price=payout,
                       exit_reason='authoritative_settlement',gross_pnl=gross,fees=fees,net_pnl=gross-fees)
        else:
            marks=[q for q in observations if q.get('observed') and q['contract_id']==t['contract_id'] and
                   q['timestamp']%60==0 and row['entry_at']<=q['timestamp']<=as_of and 0<=q['bid']<=q['ask']<=1]
            mark=max(marks,key=lambda q:q['timestamp']) if marks else None
            row['mark_net_pnl']=(mark['bid'] if mark else row['entry_price'])-row['entry_price']-fee_rate*row['entry_price']
            row['mark_at']=mark['timestamp'] if mark else None
        ledger.append(row)
    return {'version':'first_p90_hold_to_settlement_v1','paper_only':True,'as_of':as_of,
            'summary':statistics(ledger),'trades':ledger,
            'configuration':{'entry_fee_assumption':fee_rate,'settlement_exit_fee':0,'base_shares':1,
                'switch':False,'take_profit':False,'execution':'first P90 benchmark next-minute ask; NOT maker fills',
                'sizing':'one trade per market; no within-market follow-up trade to multiply'}}
