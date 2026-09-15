"""First-P90 fixed 10-cent averaging-down ladder: private paper research only.

Reuse the existing first-side and minus-one-cent entry policy. Only after its
candidate fill can lower limits be posted. No opposite-side switch, cross-market sizing,
synthetic quotes, depth assumptions disguised as execution, or broker orders.
"""
from collections import Counter, defaultdict
from decimal import Decimal
import math

from .btc_hold_tracking import first_signals, first_signal_limits, prospective_observations
from .engine import digest
from .recovery_switch import statistics

VERSION = 'btc_first_p90_hold_10c_doubling_ladder_v1'


def compact_report(report):
    """Only bounded aggregates enter minute reports/Firestore; ledgers stay encrypted."""
    return {**{k: report[k] for k in ('version', 'as_of', 'paper_only', 'live_orders_enabled',
                                     'history_minutes', 'horizon_minutes', 'coverage', 'configuration')},
            'scenarios': {fee: {mode: run['summary'] for mode, run in modes.items()}
                          for fee, modes in report['scenarios'].items()}}


def ladder_prices(entry_price):
    """Fixed 10c spacing from filled entry; include the 10c floor once."""
    if type(entry_price) not in (int, float) or not math.isfinite(entry_price) or not 0 < entry_price < 1:
        raise ValueError('INVALID_LADDER_ENTRY_PRICE')
    price = Decimal(str(entry_price))
    if price != price.quantize(Decimal('.01')):
        raise ValueError('CENT_LADDER_ENTRY_REQUIRED')
    floor, step, levels = Decimal('.10'), Decimal('.10'), []
    while price > floor:
        price = max(floor, price - step)
        levels.append(float(price))
    return levels


def _known_resolutions(resolutions, as_of):
    return {side: r for side, r in resolutions.items()
            if type(r.get('settled_at')) is int
            and type(r.get('first_confirmed_at', r.get('checked_at'))) is int
            and r['settled_at'] <= r.get('first_confirmed_at', r.get('checked_at')) <= as_of}


def _position(first, lots, tape, resolutions, as_of, fee_rate, mode):
    quantity = sum(lot['quantity'] for lot in lots)
    cost = sum(lot['entry_price'] * lot['quantity'] for lot in lots)
    row = {k: first[k] for k in ('game_id', 'contract_id', 'signal_at', 'entry_at')}
    row.update(trade_id=digest([VERSION, first['trade_id'], fee_rate, mode]),
               quantity=quantity, entry_price=cost / quantity, entry_notional=cost,
               initial_entry_price=first['entry_price'], averaging_fills=quantity - 1,
               filled_rungs=len(lots)-1,
               last_entry_at=lots[-1]['entry_at'], status='open', execution_verified=False)
    outcome = resolutions.get(first['contract_id'], {})
    if (outcome.get('resolution_status') == 'resolved' and outcome.get('selected_side_payout') in (0, 1)
            and first['entry_at'] < outcome['settled_at'] <= as_of):
        payout = outcome['selected_side_payout']
        row.update(status='closed', exit_at=outcome['settled_at'], exit_price=payout,
                   exit_reason='authoritative_settlement', gross_pnl=payout * quantity - cost,
                   fees=fee_rate * cost, net_pnl=payout * quantity - cost * (1 + fee_rate),
                   outcome_confirmed_at=outcome.get('first_confirmed_at', outcome.get('checked_at')))
    else:
        marks = [q for q in tape if first['entry_at'] <= q['received_at'] <= as_of]
        mark = max(marks, key=lambda q: q['timestamp'])['bid'] if marks else first['entry_price']
        row['mark_net_pnl'] = mark * quantity - cost * (1 + fee_rate)
    return row


def compare(forecasts, observations, resolutions, *, as_of):
    if type(as_of) is not int or as_of < 0:
        raise ValueError('VALID_LADDER_AS_OF_REQUIRED')
    history = {f.get('history_count') for f in forecasts}
    if len(history) > 1 or history - {1, 2}:
        raise ValueError('SEPARATE_BTC_HORIZON_COHORTS_REQUIRED')
    origins = defaultdict(set)
    for f in forecasts:
        origins[f['market_context']['market_id']].add(f['origin'])
    if any(len(values) != 1 for values in origins.values()):
        raise ValueError('ONE_BTC_ORIGIN_PER_MARKET_REQUIRED')
    timely = prospective_observations(observations, as_of)
    by_side = defaultdict(dict)
    for q in timely:
        key = q['contract_id'], q['timestamp']
        previous = by_side[key[0]].get(key[1])
        if previous is not None and previous != q:
            raise ValueError('CONFLICTING_LADDER_MINUTE_QUOTE')
        by_side[key[0]][key[1]] = q
    timely = [q for values in by_side.values() for q in values.values()]
    outcomes = _known_resolutions(resolutions, as_of)
    signals, ambiguous = first_signals(forecasts, timely, as_of)
    initial, original_orders = first_signal_limits(signals, timely, forecasts, outcomes, as_of)
    orders = [{**o, 'role': 'initial_entry'} for o in original_orders]
    ends = {s['contract_id']: s['market_end'] for s in signals}
    entries, lot_ledger, per_market = [], [], []
    for first in initial:
        side, end = first['contract_id'], ends[first['contract_id']]
        quotes = sorted(by_side[side].values(), key=lambda q: q['timestamp'])
        fill_quote = next(q for q in quotes if q['received_at'] == first['entry_at'])
        lots = [dict(lot_id=first['trade_id'], game_id=first['game_id'], contract_id=side,
                     entry_at=first['entry_at'], entry_price=first['entry_price'], quantity=1,
                     rung=0, execution_verified=False)]
        book = []
        committed_quantity = 1
        for rung, price in enumerate(ladder_prices(first['entry_price']), 1):
            resting = price < fill_quote['ask']
            requested_quantity = 2 ** rung
            quantity = min(requested_quantity, 100 - committed_quantity) if resting else 0
            if resting:
                committed_quantity += quantity
            status = ('skipped_marketable_at_activation' if not resting else
                      'skipped_position_cap' if not quantity else 'resting')
            book.append(dict(order_id=digest([VERSION, first['trade_id'], rung]),
                market=first['game_id'], contract_id=side, role='average_down', rung=rung,
                posted_at=first['entry_at'] if status == 'resting' else None,
                evaluated_at=first['entry_at'], quote_at_post=fill_quote['timestamp'],
                quote_ask_at_post=fill_quote['ask'], limit_price=price, quantity=quantity,
                requested_quantity=requested_quantity, cap_limited=resting and quantity < requested_quantity,
                post_only=True, reduce_only=False, execution_verified=False,
                status=status))
        previous = fill_quote
        for q in quotes:
            if not first['entry_at'] < q['received_at'] < end or not q['timestamp'] < end:
                continue
            active = [o for o in book if o['status'] == 'resting']
            if q['timestamp'] != previous['timestamp'] + 60:
                # Unknown fills during an unobserved gap are not invented. Stop
                # counting this book; disclose the uncertainty separately.
                for order in active:
                    order.update(status='cancelled_data_gap_unknown_fill', cancelled_at=q['received_at'])
            else:
                for order in active:
                    if q['ask'] < order['limit_price']:
                        order.update(status='candidate_fill', filled_at=q['received_at'],
                                     fill_price=order['limit_price'])
                        lots.append(dict(lot_id=order['order_id'], game_id=first['game_id'],
                            contract_id=side, entry_at=q['received_at'], entry_price=order['limit_price'],
                            quantity=order['quantity'], rung=order['rung'], execution_verified=False))
            previous = q
        if as_of >= end:
            for order in book:
                if order['status'] == 'resting':
                    order.update(status='expired_unfilled', expired_at=end)
        orders.extend(book)
        lot_ledger.extend(lots)
        entries.append((first, lots, quotes))
        per_market.append(dict(game_id=first['game_id'], contract_id=side,
            initial_entry_price=first['entry_price'], planned_ladder_prices=ladder_prices(first['entry_price']),
            candidate_filled_contracts=sum(l['quantity'] for l in lots),
            average_entry_price=sum(l['entry_price']*l['quantity'] for l in lots) / sum(l['quantity'] for l in lots),
            filled_entry_notional=sum(l['entry_price']*l['quantity'] for l in lots),
            maximum_committed_notional=first['entry_price'] + sum(o['limit_price']*o['quantity'] for o in book if o['posted_at'] is not None),
            maximum_committed_contracts=committed_quantity,
            order_counts=dict(Counter(o['status'] for o in book))))
    scenarios = {}
    for label, rate in (('entry_fee_1pct', .01), ('zero_fee_sensitivity', 0.)):
        scenarios[label] = {}
        for mode in ('first_entry_only', 'average_down_10c'):
            trades = [_position(first, lots if mode == 'average_down_10c' else lots[:1], quotes,
                                outcomes, as_of, rate, mode) for first, lots, quotes in entries]
            summary = statistics(trades)
            summary.update(closed_markets=summary['closed_trades'],
                filled_contracts=sum(t['quantity'] for t in trades),
                additional_contracts=sum(t['averaging_fills'] for t in trades),
                max_filled_entry_notional=max((t['entry_notional'] for t in trades), default=0),
                markets_averaged_down=sum(t['averaging_fills'] > 0 for t in trades),
                average_contracts_per_market=sum(t['quantity'] for t in trades) / len(trades) if trades else None)
            scenarios[label][mode] = {'summary': summary, 'trades': trades}
    n = next(iter(history), None)
    return {'version': VERSION, 'as_of': as_of, 'paper_only': True, 'live_orders_enabled': False,
        'history_minutes': n, 'horizon_minutes': 15 - n if n else None, 'scenarios': scenarios,
        'orders': orders, 'lots': lot_ledger, 'per_market': per_market,
        'coverage': {'forecast_markets': len(origins), 'first_signal_markets': len(signals),
            'initial_candidate_fill_markets': len(initial), 'observations': len(observations),
            'timely_observations': len(timely), 'ambiguous_signal_minutes': ambiguous,
            'initial_order_counts': dict(Counter(o['status'] for o in original_orders)),
            'ladder_order_counts': dict(Counter(o['status'] for o in orders if o['role'] == 'average_down'))},
        'configuration': {'base_contracts': 1, 'rung_multiplier': 2, 'step_cents': 10, 'floor_cents': 10,
            'maximum_contracts_per_market': 100, 'cross_market_multiplier': None,
            'quantity_policy': '1 initial, then 2**rung; cap total filled plus resting at 100; final rung may be partial',
            'anchor': 'initial candidate-filled minus-one-cent entry, not forecast P90 threshold',
            'floor_policy': 'append 10c floor once; final step may be less than 10c',
            'activation': 'only after initial candidate fill, never on the signal or fill minute retrospectively',
            'rungs': 'fixed prices; no replenishment; skip marketable rungs at activation',
            'exit': 'official known settlement only; never switch side or stop at P10',
            'quote_policy': 'completed live minutes received within 30s; no intraminute or backfilled fills',
            'fill': 'strict later ask trade-through with consecutive observed minutes, candidate only',
            'data_gap': 'cancel remaining ladder simulation and disclose unknown possible fills',
            'fees': '1% entry notional assumption and separate zero-fee sensitivity; no settlement sell fee'},
        'limitations': ['Paper candidate fills, not verified order acceptance, maker fills or available size.',
            'No averaging before initial fill, repeated rungs, opposite positions, or cross-market recovery multiplier.',
            'Market win rate counts each settled market once; averaging cannot change which side wins settlement.',
            'Averaging increases exposure; lower average cost does not establish a profitable strategy.',
            'Quote gaps and unfilled orders remain explicit. Hypothetical fills during gaps are unknown.',
            'Return divides net closed P&L by cumulative closed entry notional, not bankroll ROI.',
            'Strategy selected after observing this tape; retrospective results require forward validation.']}
