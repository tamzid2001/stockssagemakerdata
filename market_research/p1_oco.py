"""Per-game OCO paper books. Never sends exchange orders.

Six independent exit experiments, not six simultaneous real positions. Only
observed asks/bids after order placement can imply fills; same-minute opposing
fills are ambiguous and excluded. Limit precision is 0.0001 probability units
(one hundredth of one cent), not an assertion of exchange tick availability.
"""
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING

EXITS = (.1, .25, .5, .75, .9, .99)
QUANTILES = (.01, .1, .2, .25, .3, .4, .5, .6, .7, .75, .8, .9, .99)
TICK = Decimal("0.0001")


def price(value, *, up=False):
    number = Decimal(str(value))
    if not number.is_finite() or not 0 <= number <= 1:
        raise ValueError("INVALID_PROBABILITY_PRICE")
    return float(number.quantize(TICK, rounding=ROUND_CEILING if up else ROUND_FLOOR))


def initial_book():
    return {"last_timestamp": 0, "forecast_set": None, "needs_forecast": True,
            "levels": {str(q): {"orders": {}, "position": None, "trades": 0,
                        "wins": 0, "net_pnl": 0., "ambiguous": 0} for q in EXITS}}


def exit_limit(target, cost):
    # Always strictly greater than average entry cost. No fabricated fill if
    # the requested profitable price cannot exist within [0,1].
    minimum = Decimal(str(cost)) + TICK
    return None if minimum > 1 else price(max(Decimal(str(target)), minimum), up=True)


def replace_orders(book, forecasts, quotes, now, quantity=1.):
    if len(forecasts) != 2 or set(forecasts) != set(quotes) or quantity <= 0:
        raise ValueError("EXACTLY_TWO_SIDES_REQUIRED")
    if any(not q.observed or q.timestamp > now or now-q.timestamp > 120 for q in quotes.values()):
        raise ValueError("FRESH_BOTH_SIDE_QUOTES_REQUIRED")
    if any(f["available_at"] > now or f["rows"][-1]["timestamp"] <= now for f in forecasts.values()):
        raise ValueError("PROSPECTIVE_FORECAST_REQUIRED")
    version = "|".join(forecasts[k]["forecast_id"] for k in sorted(forecasts))
    for level, ledger in book["levels"].items():
        if ledger["position"]:
            p = ledger["position"]
            p["exit_limit"] = exit_limit(forecasts[p["side"]]["rows"][-1]["quantiles"][level], p["average_cost"])
            p["exit_placed_at"] = now
            continue
        # Require new paired forecasts after an exit; no stale re-entry loop.
        if ledger.get("exited_forecast_set") == version:
            continue
        ledger["orders"] = {}
        for side, f in forecasts.items():
            limit = price(f["rows"][-1]["quantiles"]["0.01"])
            if 0 < limit < quotes[side].ask:
                ledger["orders"][side] = {"limit":limit,"placed_at":now,"expires":f["rows"][-1]["timestamp"],
                    "exit_target":f["rows"][-1]["quantiles"][level],"quantity":quantity,"forecast_id":f["forecast_id"]}
    book["forecast_set"] = version
    book["needs_forecast"] = False


def advance_book(book, quotes, now, fee_rate=.01):
    if now <= book["last_timestamp"]:
        return []
    if not 0 <= fee_rate < 1:
        raise ValueError("INVALID_FEE")
    events = []
    for level, ledger in book["levels"].items():
        position = ledger["position"]
        if position:
            q = quotes.get(position["side"])
            limit = position["exit_limit"]
            if q and q.observed and q.timestamp == now and now > position["exit_placed_at"] and limit is not None and q.bid >= limit:
                gross = (limit-position["average_cost"])*position["quantity"]
                fees = fee_rate*(limit+position["average_cost"])*position["quantity"]
                net = gross-fees
                events.append({"kind":"exit","timestamp":now,"level":level,"side":position["side"],
                    "entry_at":position["entry_at"],"entry":position["average_cost"],"exit":limit,
                    "quantity":position["quantity"],"gross_pnl":gross,"fees":fees,"net_pnl":net})
                ledger.update(position=None,orders={},trades=ledger["trades"]+1,wins=ledger["wins"]+int(net>0),net_pnl=ledger["net_pnl"]+net,exited_forecast_set=book["forecast_set"])
                book["needs_forecast"] = True
            continue  # No same-bar exit and re-entry; hold beyond horizon.
        hits = [(side, order) for side, order in ledger["orders"].items()
                if side in quotes and quotes[side].observed and quotes[side].timestamp == now
                and order["placed_at"] < now <= order["expires"] and quotes[side].ask <= order["limit"]]
        # OHLC/one-minute snapshots cannot establish which opposing order filled
        # first. Exclude ambiguity instead of pretending atomic exchange OCO.
        if len(hits) > 1:
            ledger["orders"] = {}; ledger["ambiguous"] += 1
            book["needs_forecast"] = True
            events.append({"kind":"ambiguous_fill_excluded","timestamp":now,"level":level})
        elif hits:
            side, order = hits[0]
            ledger["position"] = {"side":side,"average_cost":order["limit"],"quantity":order["quantity"],"entry_at":now,
                "exit_limit":exit_limit(order["exit_target"],order["limit"]),"exit_placed_at":now}
            ledger["orders"] = {}  # First fill cancels ALL opposing buy limits.
            events.append({"kind":"entry","timestamp":now,"level":level,"side":side,"price":order["limit"],"quantity":order["quantity"]})
        else:
            ledger["orders"] = {side:o for side,o in ledger["orders"].items() if o["expires"] >= now}
    book["last_timestamp"] = now
    return events


def summary(books):
    result = {}
    for level in map(str,EXITS):
        ledgers = [book["levels"][level] for book in books]
        trades = sum(l["trades"] for l in ledgers); wins = sum(l["wins"] for l in ledgers)
        result[level] = {"closed_trades":trades,"net_wins":wins,"win_rate":wins/trades if trades else None,
            "net_pnl":sum(l["net_pnl"] for l in ledgers),"open_positions":sum(bool(l["position"]) for l in ledgers),
            "ambiguous_fills_excluded":sum(l["ambiguous"] for l in ledgers)}
    return result
