"""Prospective coin paper fills in a separate journal; no order-write methods.

Uses an observed ask for entry and a later observed bid for the five-cent exit,
otherwise the official binary settlement. Depth and queue priority are unmodeled.
Fees are a conservative whole-cent general-taker schedule assumption (M=1).
"""
from decimal import Decimal, ROUND_CEILING
import time

from .kalshi_execution import money, order_payload, STOP_BID


def modeled_fee(quantity, price):
    cost = money(quantity) * money(price)
    raw = Decimal('.07') * money(quantity) * money(price) * (1-money(price))
    # Round total cost+fee upward; label it modeled, never exchange-account P&L.
    return (cost+raw).quantize(Decimal('.01'), rounding=ROUND_CEILING)-cost


class PaperTrader:
    def __init__(self, config, broker, journal):
        if broker.enabled:
            raise RuntimeError('PAPER_REQUIRES_READ_ONLY_BROKER')
        self.config, self.broker, self.journal = config, broker, journal

    def enter(self, signal, quote, now):
        from .engine import stamp
        if (quote['timestamp'] != signal['signal_at']+60 or not quote.get('timely')
                or not quote['timestamp'] <= quote['received_at'] <= now <= quote['timestamp']+30
                or now >= signal['market_end'] or self.journal.used(signal['market_id'])):
            raise RuntimeError('MISSED_OR_DUPLICATE_PAPER_ENTRY')
        state = self.journal.state()
        if state.get('active'):
            raise RuntimeError('PRIOR_TRADE_UNRECONCILED')
        market = self.broker.market(signal['market_id'])
        if (market.get('status') != 'active' or market.get('market_type') != 'binary'
                or stamp(market['close_time'])-stamp(market['open_time']) != 900
                or stamp(market['close_time']) != signal['market_end']):
            raise RuntimeError('MARKET_NOT_TRADEABLE')
        side=signal['contract_id'].rsplit(':',1)[1]
        ask=money(quote[side+'_ask']);quantity=state['size']
        if not 0 < ask <= money(self.config.max_ask):
            raise RuntimeError('PAPER_ASK_OUT_OF_RANGE')
        intent=order_payload(signal['market_id'], side, quantity, ask, market['exchange_index'], self.config)
        entry={'ticker':signal['market_id'],'side':side,'intent':intent,'signal':signal,
               'quote':quote,'created_at':now,'target_contracts':intent['count'],
               'filled':str(quantity),'cost':str(ask*quantity),
               'fees':str(modeled_fee(quantity,ask)), 'status':'paper_held',
               'paper_only':True,'execution_model':'observed_ask_later_bid_no_depth',
               'fee_model':'general_taker_M1_total_cost_ceiling_cent'}
        self.journal.begin(entry)
        return entry

    def reconcile(self):
        entry=self.journal.state().get('active')
        if not entry:return 'flat'
        if not entry.get('paper_only'):
            raise RuntimeError('PAPER_JOURNAL_CONTAINS_NON_PAPER_POSITION')
        market=self.broker.market(entry['ticker']);now=time.time()
        quantity=money(entry['filled']);side=entry['side']
        if market.get('status') in ('settled','finalized') and market.get('result') in ('yes','no'):
            proceeds=quantity if market['result']==side else Decimal(0)
            exit_fee=Decimal(0);kind='settled';price=proceeds/quantity
        else:
            # Do not simulate an executable zero bid or a same-observation exit.
            raw=market.get(side+'_bid_dollars')
            bid=money(raw) if raw is not None else None
            if (market.get('status')!='active' or now<=entry['created_at']
                    or bid is None or not 0<bid<=STOP_BID):
                return 'paper_held_to_settlement'
            proceeds=quantity*bid;exit_fee=modeled_fee(quantity,bid);kind='stopped';price=bid
        net=proceeds-money(entry['cost'])-money(entry['fees'])-exit_fee
        self.journal.finish({**entry,'status':kind,'net_pnl':str(net),
            'paper_exit_at':now,'paper_exit_price':str(price),'paper_exit_fee':str(exit_fee),
            'official_result':market.get('result') if kind=='settled' else None},net)
        return 'paper_'+kind
