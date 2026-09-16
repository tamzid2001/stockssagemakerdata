"""Thirty-minute-origin, first-row contrarian replay. No exchange order APIs.

Freeze P10/P90 in row one. Classify only the first completed minute after
publication, then execute at the following minute book. A SELL selects the
complement of the same binary contract (never an arbitrary soccer team).
"""
from collections import defaultdict
import math

from .engine import digest
from .recovery_switch import statistics

VERSION = 'first_row_p10_buy_p90_sell_v1'


def game_groups(contracts):
    """Keep provider identities; Kalshi events can contain 1–3 binary books.

    A NO position remains the complement of its own market, not an inferred
    different team. Legacy Polymarket pairing remains unchanged.
    """
    from .p1_worker import game_groups as polymarket_groups
    result = polymarket_groups([c for c in contracts if c.get('source') != 'kalshi'])
    events = defaultdict(dict)
    invalid = set()
    for c in contracts:
        if c.get('source') != 'kalshi':
            continue
        event, ticker, side = c.get('eventId'), c.get('marketId'), c.get('side')
        if (not event or not ticker or c.get('providerSymbol') != ticker or
                side not in {'yes', 'no'} or c.get('contractId') != f'{ticker}:{side}' or
                not ticker.startswith(event + '-')):
            if event:
                invalid.add(event)
            continue
        previous = events[event].get(c['contractId'])
        if previous is not None and previous != c:
            invalid.add(event)
        events[event][c['contractId']] = c
    for event, unique in events.items():
        books = defaultdict(list)
        for c in unique.values():
            books[c['marketId']].append(c)
        if event in invalid or not 1 <= len(books) <= 3:
            continue
        if all(len(p) == 2 and {c['side'] for c in p} == {'yes', 'no'} for p in books.values()):
            result[event] = sorted(unique.values(), key=lambda c: c['contractId'])
    return result


def classify(ask, p10, p90):
    if not all(math.isfinite(v) and 0 <= v <= 1 for v in (ask, p10, p90)) or p10 > p90:
        raise ValueError('INVALID_FIRST_ROW_VALUES')
    return 'buy' if ask < p10 else 'sell' if ask > p90 else 'neutral'


def simulate(forecasts, observations, resolutions, *, as_of, fee_rate=.01, multiplier=2.5):
    if not 0 <= fee_rate <= .1 or not 1 <= multiplier <= 10:
        raise ValueError('INVALID_FIRST_ROW_COSTS')
    groups = defaultdict(list)
    for f in forecasts:
        if f.get('strategy') == VERSION:
            groups[(f['market_context']['event_id'], f['origin'])].append(f)
    tape = defaultdict(dict)
    for q in observations:
        if q.get('observed') and q['timestamp'] % 60 == 0 and q['timestamp'] <= as_of:
            tape[q['contract_id']][q['timestamp']] = q
    games = defaultdict(list)
    for (game, _), pair in groups.items():
        if (len(pair) != pair[0]['expected_side_count'] or
                len({f['available_at'] for f in pair}) != 1 or
                len({f['market_context']['contract_id'] for f in pair}) != len(pair)):
            continue
        games[game].append(pair)
    trades, signals, audit = [], [], []
    for game, pairs in sorted(games.items()):
        size, cycle, held = 1, 0., None
        ids = {f['market_context']['contract_id'] for pair in pairs for f in pair}
        ends = [resolutions[s]['settled_at'] for s in ids if resolutions.get(s, {}).get('settled_at')]
        end = min(ends) if ends else as_of + 1

        def close(timestamp, price, reason):
            nonlocal held, size, cycle
            exit_fee = 0 if reason == 'authoritative_settlement' else fee_rate * price * held['quantity']
            held.update(status='closed', exit_at=timestamp, exit_price=price, exit_reason=reason)
            held['fees'] += exit_fee
            held['gross_pnl'] = (price-held['entry_price']) * held['quantity']
            held['net_pnl'] = held['gross_pnl'] - held['fees']
            cycle += held['net_pnl']
            if cycle >= -1e-10:
                size, cycle = 1, 0.
            elif held['net_pnl'] < 0:
                size = min(100, math.floor(size * multiplier))
            held.update(next_quantity=size, recovery_cycle_pnl=cycle)
            trades.append(held)
            held = None

        for pair in sorted(pairs, key=lambda p: (p[0]['available_at'], p[0]['origin'])):
            published = pair[0]['available_at']
            t = published // 60 * 60 + 60
            entry_at = t + 60
            if entry_at >= end or entry_at > as_of:
                continue
            by_side = {f['market_context']['contract_id']: f for f in pair}
            books = {s: tape[s].get(t) for s in by_side}
            if any(q is None for q in books.values()):
                audit.append(dict(game_id=game, timestamp=t, status='missing_first_minute_pair'))
                continue
            votes = defaultdict(list)
            for side, f in by_side.items():
                first = f['rows'][0]
                q = books[side]
                signal = classify(q['ask'], first['quantiles']['0.1'], first['quantiles']['0.9'])
                record = dict(game_id=game, contract_id=side, timestamp=t, forecast_id=f['forecast_id'],
                              forecast_timestamp=first['timestamp'], published_at=published,
                              ask=q['ask'], p10=first['quantiles']['0.1'], p90=first['quantiles']['0.9'], signal=signal)
                signals.append(record)
                target = side if signal == 'buy' else None
                if signal == 'sell':
                    mates = [s for s, other in by_side.items() if s != side and
                             other['market_context']['market_id'] == f['market_context']['market_id']]
                    if len(mates) == 1:
                        target = mates[0]
                if target:
                    votes[target].append(record)
            if not votes:
                continue
            # Conflicting binary votes are not resolved using later outcomes.
            if len(votes) > 1:
                audit.append(dict(game_id=game, timestamp=t, status='ambiguous_targets', targets=sorted(votes)))
                continue
            target = next(iter(votes))
            if held and held['contract_id'] == target:
                continue  # Hold; no repeated entry or averaging.
            quote = tape[target].get(entry_at)
            old_quote = tape[held['contract_id']].get(entry_at) if held else None
            if not quote or not 0 < quote['ask'] < 1 or (held and not old_quote):
                audit.append(dict(game_id=game, timestamp=entry_at, status='missing_execution_book'))
                continue
            if held:
                close(entry_at, old_quote['bid'], 'opposite_first_row_signal')
            held = dict(trade_id=digest([VERSION, game, entry_at, target]), game_id=game,
                        contract_id=target, signal_at=t, entry_at=entry_at, entry_price=quote['ask'],
                        quantity=size, fees=fee_rate * quote['ask'] * size, status='open',
                        forecast_id=by_side[target]['forecast_id'], execution_verified=False)
        if held:
            resolution = resolutions.get(held['contract_id'], {})
            if (resolution.get('resolution_status') == 'resolved' and
                    resolution.get('selected_side_payout') in (0, 1) and end <= as_of):
                close(end, resolution['selected_side_payout'], 'authoritative_settlement')
            else:
                recent = [q for t, q in tape[held['contract_id']].items() if held['entry_at'] <= t <= as_of]
                mark = max(recent, key=lambda q: q['timestamp']) if recent else None
                held['mark_net_pnl'] = ((mark['bid'] - held['entry_price']) * held['quantity'] - held['fees']) if mark else 0
                trades.append(held)
    summary = statistics(trades)
    summary.update(recovery_signals=sum(s['signal'] != 'neutral' for s in signals),
                   multiplier_increases=sum(t.get('next_quantity', 1) > t['quantity'] for t in trades))
    return dict(version=VERSION, paper_only=True, trades=trades, signals=signals, audit=audit,
                summary={VERSION: summary}, fee_rate_assumption=fee_rate,
                execution_policy='First completed minute after publication; NEXT minute ask/bid; settlement hold or signal switch.',
                conflict_policy='Skip disagreeing target sides; no invented team selection or later-quote substitution.')


def from_store(store):
    coverage = store._get('checkpoints', 'recovery_coverage') or {}
    resolutions = {}
    for row in store.values('checkpoints'):
        if row.get('contract_id') and row.get('resolution_status'):
            resolutions[row['contract_id']] = row
    forecasts = store.values('forecasts')
    report = simulate(forecasts, store.values('observations'), resolutions, as_of=coverage.get('as_of', 0))
    report.update(forecast_count=len(forecasts), coverage=coverage)
    return report
