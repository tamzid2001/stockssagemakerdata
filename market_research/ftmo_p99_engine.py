"""Buy-only, equity-risk-sized research replay. This module cannot place orders.

Minute OHLC cannot identify tick order: both low-first and high-first paths are
reported. Limit orders observed during a minute become eligible next minute.
"""
from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime, timedelta
import math

from .ftmo_dukas_data import INSTRUMENTS, UTC, stamp
from .ftmo_dukas_engine import Costs, PRAGUE, SERVER, adjusted_quotes, commission, swap_quote, swap_weight


@dataclass(frozen=True)
class Candidate:
    grid: float
    sizing: str = 'equal'

    @property
    def name(self):
        return f'grid-{self.grid:g}-{self.sizing}'


def candidates(symbol):
    if symbol in ('EURUSD.sim', 'GBPUSD.sim', 'USDCAD.sim'):
        grids = (.0005, .001, .002, .005, .01, 1.)
    elif symbol in ('USDJPY.sim', 'GBPJPY.sim'):
        grids = (.05, .1, .2, .5, 1., 2.)
    elif symbol == 'BTCUSD.sim':
        grids = (1., 25., 50., 100., 250., 500.)
    elif symbol == 'XAUUSD.sim':
        grids = (.25, .5, 1., 2., 5., 10.)
    else:
        grids = (1., 5., 10., 20., 50., 100.)
    return tuple(Candidate(g, s) for g in grids for s in ('equal', 'larger_deeper', 'smaller_deeper'))


def floor_lots(lots, step=.01):
    return round(math.floor(max(0., lots) / step + 1e-10) * step, 8)


def trail_distance(entries, grid):
    prices = [e['entry'] for e in entries if e['lots'] > 0]
    return grid if len(prices) < 2 else .75 * (max(prices) - min(prices))


class KnownConversion:
    """Only current opens or completed closes; never an unfinished H1 close."""
    def __init__(self, currency, rows):
        if currency not in ('USD', 'JPY', 'CAD'):
            raise ValueError('UNSUPPORTED_USD_CONVERSION')
        self.currency = currency
        self.rows = rows
        self.times = [stamp(r['start']) for r in rows]
        self.ends = [stamp(r['end']) for r in rows]

    def usd(self, amount, at):
        if self.currency == 'USD':
            return amount
        i = bisect_right(self.times, at) - 1
        if i < 0 or (at - self.times[i]).total_seconds() > 72 * 3600:
            raise ValueError('USD_CONVERSION_QUOTE_UNAVAILABLE')
        field = 'c' if self.ends[i] <= at else 'o'
        row = self.rows[i]
        return amount / row['ask' if amount >= 0 else 'bid'][field]


def minute_tradeable(at, spec):
    local = at.astimezone(SERVER)
    minute = local.weekday() * 1440 + local.hour * 60 + local.minute
    return any(a <= minute and minute + 1 <= b for a, b in spec['weekly_minutes'])


def loss_per_lot(entry, stop, spec, conversion, at, costs, risk_span=0.):
    """USD loss from ask fill to bid stop plus both fees and seven-day swap reserve."""
    price_loss = -conversion.usd(-max(0., entry - stop, risk_span) * spec['contractSize'], at)
    fees = commission(spec, 1., entry, costs) + commission(spec, 1., max(stop, 1e-12), costs)
    swap = swap_quote(spec, 'long', 1., entry, 7, costs)
    return price_loss + fees + max(0., -conversion.usd(swap, at))


def planned_levels(entry, stop, p90, grid, tick):
    """The first entry is immediate; additions must be below P90 and above P01."""
    if not all(math.isfinite(v) and v > 0 for v in (entry, stop, p90, grid, tick)):
        raise ValueError('INVALID_RISK_PLAN_PRICE')
    if entry <= stop:
        return []
    skip = max(1, math.floor((entry - p90) / grid) + 1)
    first_add = entry - skip * grid
    n = max(0, math.ceil((first_add - stop) / grid - 1e-10))
    if n > 50_000:
        raise ValueError('GRID_HAS_TOO_MANY_PLANNED_LEVELS')
    return [entry] + [first_add - i * grid for i in range(n) if first_add - i * grid > stop + tick / 2]


def sized_lots(price, stop, p90, equity, entries, candidate, spec, conversion, at, costs, risk_fraction=.01,
               *, risk_equity=None, risk_span=0.):
    """Reserve the whole remaining ladder within 1% equity, rounding DOWN to lot step.

    scale = (risk_budget - existing_stop_risk) / sum(weight[k] * USD_loss_per_lot[k]).
    lots[k] = floor_to_step(scale * weight[k]); this returns the next entry size.
    """
    levels = planned_levels(price, stop, p90, candidate.grid, 10 ** -spec['digits'])
    if not levels or equity <= 0:
        return 0.
    risk = sum(e['lots'] * loss_per_lot(e['entry'], stop, spec, conversion, at, costs, risk_span) for e in entries)
    available = max(0., (equity if risk_equity is None else risk_equity) * risk_fraction - risk)
    count = len(levels)
    def weight(i):
        x = i / max(1, count - 1)
        return 1 + x if candidate.sizing == 'larger_deeper' else 2 - x if candidate.sizing == 'smaller_deeper' else 1.
    if candidate.sizing not in ('equal', 'larger_deeper', 'smaller_deeper'):
        raise ValueError('UNSUPPORTED_LOT_PROFILE')
    weights = [weight(i) for i in range(count)]
    denominator = sum(w * loss_per_lot(p, stop, spec, conversion, at, costs, risk_span) for p, w in zip(levels, weights))
    if denominator <= 0:
        return 0.
    scale = min(available / denominator,
                max(0., spec['maxTotalVolume'] - sum(e['lots'] for e in entries)) / sum(weights))
    lots = min(scale * weights[0], spec['maxTradeVolume'])
    used_margin = sum(conversion.usd(e['lots'] * spec['contractSize'] * price, at) / spec['leverageStandard'] for e in entries)
    margin_per_lot = conversion.usd(spec['contractSize'] * price, at) / spec['leverageStandard']
    lots = min(lots, max(0., equity - used_margin) / (margin_per_lot + commission(spec, 1., price, costs)))
    return floor_lots(lots, spec.get('volumeStep', .01))


def replay(symbol, rows, forecasts, spec, conversion, first, last, candidate, costs=Costs('reference_percentage_per_side'),
           ordering='low_first', initial_balance=100_000., risk_fraction=.01, detail=False, *,
           entry_quantile='p99', entry_comparison='above', averaging_gate='p90'):
    if (ordering not in ('low_first', 'high_first') or not 0 < risk_fraction <= .01
            or entry_quantile not in ('p90', 'p99') or entry_comparison not in ('above', 'below', 'any')
            or averaging_gate not in ('p90', 'grid')):
        raise ValueError('INVALID_REPLAY_CONFIGURATION')
    forecasts = sorted([f for f in forecasts if f['status'] == 'completed'], key=lambda f: f['earliest_actionable_at'])
    events = [(stamp(f['earliest_actionable_at']), f) for f in forecasts]
    index = 0
    cash = initial_balance
    active = []
    ledger = []
    baskets = []
    current = None
    pending = None
    signal_origin = consumed_origin = None
    stop = p90 = None
    risk_equity = risk_span = None
    trailing = peak_bid = None
    basket_start = None
    basket_legs = []
    fees = swaps = 0.
    max_lots = max_margin = max_dd = max_daily = max_risk = 0.
    peak_equity = initial_balance
    daily_base = initial_balance
    daily_key = None
    first_daily_breach = first_total_breach = first_margin_breach = None
    cancelled = blocked = signals = 0
    hourly_curve = []
    hour_key = None
    last_bid = last_at = None
    tick = 10 ** -spec['digits']
    rolled = None

    def equity(bid, at):
        return cash + sum(conversion.usd((bid - e['entry']) * e['lots'] * spec['contractSize'], at)
                          - commission(spec, e['lots'], bid, costs) for e in active)

    def mark(bid, at):
        nonlocal peak_equity, max_dd, max_daily, daily_base, daily_key
        nonlocal first_daily_breach, first_total_breach, first_margin_breach, max_margin, max_lots
        key = at.astimezone(PRAGUE).date()
        # Daily balance reset, not peak equity: floating P&L is included in equity.
        if key != daily_key:
            daily_key = key
            daily_base = cash
        value = equity(bid, at)
        max_dd = max(max_dd, peak_equity - value)
        peak_equity = max(peak_equity, value)
        max_daily = max(max_daily, daily_base - value)
        if daily_base - value > 5000 and first_daily_breach is None:
            first_daily_breach = at.isoformat()
        if value < initial_balance - 10000 and first_total_breach is None:
            first_total_breach = at.isoformat()
        lots = sum(e['lots'] for e in active)
        margin = conversion.usd(lots * spec['contractSize'] * bid, at) / spec['leverageStandard']
        max_margin = max(max_margin, margin)
        max_lots = max(max_lots, lots)
        if margin > value and first_margin_breach is None:
            first_margin_breach = at.isoformat()
        return value

    def close_quantity(leg, qty, bid, at, reason):
        nonlocal cash, fees
        qty = min(qty, leg['lots'])
        fee = commission(spec, qty, bid, costs)
        pnl = conversion.usd((bid - leg['entry']) * qty * spec['contractSize'], at) - fee
        cash += pnl
        fees += fee
        leg['net_pnl'] += pnl
        leg['lots'] = round(leg['lots'] - qty, 8)
        if detail:
            leg['exits'].append({'at': at.isoformat(), 'price': bid, 'lots': qty, 'reason': reason, 'net_pnl': pnl})
        if leg['lots'] <= 1e-8:
            active.remove(leg)
            ledger.append({**leg, 'exit_at': at.isoformat(), 'duration_hours': (at - stamp(leg['at'])).total_seconds() / 3600})

    def finish(bid, at, reason):
        nonlocal pending, trailing, peak_bid, basket_start, basket_legs, signal_origin
        if basket_start is not None:
            for leg in list(active):
                close_quantity(leg, leg['lots'], bid, at, reason)
            baskets.append({'at': at.isoformat(), 'started_at': basket_start.isoformat(), 'reason': reason,
                            'legs': len(basket_legs), 'net_pnl': sum(e['net_pnl'] for e in basket_legs),
                            'fixed_final_p01': stop, 'fixed_final_quantile_span': risk_span,
                            'equity_at_entry': risk_equity, 'risk_budget_usd': risk_equity * risk_fraction,
                            'duration_hours': (at - basket_start).total_seconds() / 3600})
        pending = trailing = peak_bid = basket_start = signal_origin = None
        basket_legs = []

    def fill(price, bid, at):
        nonlocal fees, cash, basket_start, pending, max_risk, stop, risk_equity, risk_span
        if not active:
            tail = current['predictions'][-1]
            stop = math.floor(tail['p01'] / tick + 1e-8) * tick
            risk_span = tail['p99'] - tail['p01']
            risk_equity = equity(bid, at)
        sizing_p90 = p90 if averaging_gate == 'p90' and len(basket_legs) < 2 else price + candidate.grid
        lots = sized_lots(price, stop, sizing_p90, equity(bid, at), active, candidate, spec, conversion, at, costs, risk_fraction,
                          risk_equity=risk_equity, risk_span=risk_span)
        minimum = spec.get('minimumVolume', .01)
        if lots < minimum:
            return False
        fee = commission(spec, lots, price, costs)
        fees += fee
        cash -= fee
        leg = {'at': at.isoformat(), 'entry': price, 'lots': lots, 'initial_lots': lots,
               'net_pnl': -fee, 'exits': []}
        active.append(leg)
        basket_legs.append(leg)
        if basket_start is None:
            basket_start = at
        pending = None
        risk = sum(e['lots'] * loss_per_lot(e['entry'], stop, spec, conversion, at, costs, risk_span) for e in active)
        max_risk = max(max_risk, risk)
        return True

    for row in rows:
        at = row['_at'] if '_at' in row else stamp(row['start'])
        end = row['_end'] if '_end' in row else stamp(row['end'])
        if end <= first or at >= last:
            continue
        if (end - at).total_seconds() != 60:
            raise ValueError('GENUINE_MINUTE_REPLAY_REQUIRED')
        # Empty-account minutes between daily decisions cannot change equity.
        # Avoid expensive quote/cost/path work without skipping a pending signal.
        if not active and (not signal_origin or signal_origin == consumed_origin) and (index >= len(events) or events[index][0] > at):
            last_bid, last_at = row['bid']['c'], end
            key = end.replace(minute=0, second=0, microsecond=0)
            if detail and key != hour_key:
                hourly_curve.append({'at': end.isoformat(), 'equity': cash, 'cash': cash, 'lots': 0.})
                hour_key = key
            continue
        bid, ask = adjusted_quotes(row, symbol, costs)
        bid = {k: math.floor(v / tick + 1e-8) * tick for k, v in bid.items()}
        ask = {k: math.ceil(v / tick - 1e-8) * tick for k, v in ask.items()}
        tradeable = minute_tradeable(at, spec)
        mark(bid['o'], at)
        # Refresh P90 after inference. An active basket retains its triggering
        # forecast's FINAL P01 stop and FINAL P99-P01 risk span throughout.
        while index < len(events) and events[index][0] <= at:
            _, current = events[index]
            index += 1
            if pending is not None:
                cancelled += 1
            pending = None
            if len(basket_legs) < 2:
                p90 = current['predictions'][0]['p90']
            if not active:
                stop = math.floor(current['predictions'][-1]['p01'] / tick + 1e-8) * tick
            threshold = current['predictions'][0][entry_quantile]
            qualifies = (True if entry_comparison == 'any' else
                         current['cutoff_close'] > threshold if entry_comparison == 'above' else
                         current['cutoff_close'] < threshold)
            signal_origin = current['origin'] if qualifies else None
            if signal_origin:
                signals += 1
        if not current:
            last_bid, last_at = bid['c'], end
            continue
        # Swap is charged at the first observed quote after each server midnight,
        # including intervening dates. It applies only to already-held positions.
        server_day = at.astimezone(SERVER).date()
        if rolled is not None and server_day != rolled and active:
            d = rolled
            while d < server_day:
                for leg in active:
                    charge = conversion.usd(swap_quote(spec, 'long', leg['lots'], bid['o'], swap_weight(symbol, d, costs), costs), at)
                    cash += charge
                    swaps += charge
                    leg['net_pnl'] += charge
                d += timedelta(days=1)
        rolled = server_day
        if tradeable and active and bid['o'] <= max(stop, trailing if trailing is not None else stop):
            finish(bid['o'], at, 'gap_stop')
        if tradeable and not active and signal_origin and signal_origin != consumed_origin:
            consumed_origin = signal_origin
            if not fill(ask['o'], bid['o'], at):
                blocked += 1
        if tradeable and active and pending is not None and at >= pending['eligible_at']:
            limit = pending['price']
            if ask['o'] <= limit and (averaging_gate == 'grid' or len(basket_legs) >= 2 or ask['o'] < p90) and ask['o'] > stop:
                if not fill(ask['o'], bid['o'], at):
                    pending = None
                    blocked += 1
        path = ('o', 'l', 'h', 'c') if ordering == 'low_first' else ('o', 'h', 'l', 'c')
        previous_bid = bid['o']
        for field in path:
            px = bid[field]
            point_at = at if field == 'o' else end
            if tradeable and active:
                threshold = max(stop, trailing if trailing is not None else stop)
                # On a continuous descending segment an already-resting buy
                # limit above the stop is reached BEFORE that stop. A gap open
                # is different: the stop was handled before any new addition.
                if pending is not None and field != 'o' and ask[field] <= pending['price'] and (averaging_gate == 'grid' or len(basket_legs) >= 2 or pending['price'] < p90):
                    spread = max(0., ask[field] - bid[field])
                    limit_bid = pending['price'] - spread
                    if limit_bid > threshold and previous_bid > threshold:
                        price = pending['price']
                        mark(limit_bid, point_at)
                        if not fill(price, limit_bid, point_at):
                            pending = None
                            blocked += 1
                if px <= threshold:
                    # Continuous path stop fills at the stop; opens/gaps use actual bid.
                    exit_price = px if field == 'o' or previous_bid <= threshold else threshold
                    mark(exit_price, point_at)
                    finish(exit_price, point_at, 'trailing_stop' if trailing is not None and trailing >= stop else 'p01_stop')
            mark(px, point_at)
            if tradeable and active and equity(px, point_at) > initial_balance + sum(b['net_pnl'] for b in baskets):
                # Basket profitability includes commissions and accrued swap.
                peak_bid = px if peak_bid is None else max(peak_bid, px)
                proposed = math.floor((peak_bid - trail_distance(active, candidate.grid)) / tick + 1e-8) * tick
                trailing = proposed if trailing is None else max(trailing, proposed)
            previous_bid = px
        if tradeable and active and pending is None:
            lowest = min(e['entry'] for e in active)
            gate = min(lowest - candidate.grid, p90 - tick) if averaging_gate == 'p90' and len(basket_legs) < 2 else lowest - candidate.grid
            level = math.floor(gate / tick + 1e-8) * tick
            # Breach knowledge is available only at minute completion. Never fill
            # the same historical low used to create this order.
            if stop < level and (averaging_gate == 'grid' or len(basket_legs) >= 2 or level < p90) and ask['l'] <= level:
                pending = {'price': level, 'eligible_at': end, 'observed_at': end}
        value = mark(bid['c'], end)
        key = end.replace(minute=0, second=0, microsecond=0)
        if detail and key != hour_key:
            hourly_curve.append({'at': end.isoformat(), 'equity': value, 'cash': cash, 'lots': sum(e['lots'] for e in active)})
            hour_key = key
        last_bid, last_at = bid['c'], end
    if last_bid is None:
        raise ValueError('NO_MINUTE_OBSERVATIONS_IN_PERIOD')
    ending = equity(last_bid, last_at)
    wins = sum(b['net_pnl'] > 0 for b in baskets)
    losses = sum(b['net_pnl'] < 0 for b in baskets)
    streak_w = streak_l = longest_w = longest_l = 0
    for b in baskets:
        streak_w = streak_w + 1 if b['net_pnl'] > 0 else 0
        streak_l = streak_l + 1 if b['net_pnl'] < 0 else 0
        longest_w, longest_l = max(longest_w, streak_w), max(longest_l, streak_l)
    all_entries = ledger + active
    entry_weight = sum(e['initial_lots'] for e in all_entries)
    result = {'symbol': symbol, 'candidate': candidate.name, 'grid': candidate.grid, 'sizing': candidate.sizing,
              'ordering': ordering, 'costs': costs.name, 'initial_balance': initial_balance,
              'ending_equity': ending, 'net_pnl': ending - initial_balance, 'return_pct': 100 * (ending / initial_balance - 1),
              'closed_baskets': len(baskets), 'basket_wins': wins, 'basket_losses': losses,
              'basket_breakeven': len(baskets) - wins - losses,
              'basket_win_rate': wins / len(baskets) if baskets else None,
              'closed_entries': len(ledger), 'entry_wins': sum(e['net_pnl'] > 0 for e in ledger),
              'entry_losses': sum(e['net_pnl'] < 0 for e in ledger),
              'average_entry': sum(e['entry'] * e['initial_lots'] for e in all_entries) / entry_weight if entry_weight else None,
              'average_basket_hours': sum(b['duration_hours'] for b in baskets) / len(baskets) if baskets else None,
              'average_ladder': sum(b['legs'] for b in baskets) / len(baskets) if baskets else None,
              'max_ladder': max([b['legs'] for b in baskets] + [len(basket_legs)], default=0),
              'max_win_streak': longest_w, 'max_loss_streak': longest_l, 'max_equity_drawdown': max_dd,
              'max_daily_loss': max_daily, 'first_daily_breach': first_daily_breach,
              'first_total_breach': first_total_breach, 'first_margin_breach': first_margin_breach,
              'max_lots': max_lots, 'max_margin': max_margin, 'max_reserved_stop_risk': max_risk,
              'fees': fees, 'swap_pnl': swaps, 'fixed_final_p01_stop': stop, 'fixed_final_quantile_span': risk_span,
              'cancelled_limits': cancelled,
              'risk_or_volume_blocked_entries': blocked,
              ('daily_buy_forecast_signals' if entry_comparison == 'any' else f'{entry_comparison}_{entry_quantile}_forecast_signals'): signals,
              'entry_signal_quantile': entry_quantile,
              'entry_signal_comparison': entry_comparison,
              'averaging_gate': averaging_gate,
              'open_entries': len(active), 'open_lots': sum(e['lots'] for e in active),
              'open_age_hours': (last_at - basket_start).total_seconds() / 3600 if basket_start else None,
              'open_net_pnl': sum(e['net_pnl'] for e in basket_legs) + ending - cash,
              'risk_fraction': risk_fraction, 'orders_sent': 0}
    if detail:
        result.update(entries=ledger, baskets=baskets, open_basket=active, hourly_equity=hourly_curve)
    return result
