"""Minute-tranche P1 accumulation, independent fixed-quantile paper exits.

Only later genuine observed quotes imply fills. No exchange orders, synthetic
quotes, hindsight exit selection or guaranteed liquidity. One held side/game.
"""
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING
import math
from .engine import digest

VERSION = "p1_minute_accumulation_v3"
EXITS = (.1, .25, .5, .75, .9, .99)
QUANTILES = (.01, .1, .2, .25, .3, .4, .5, .6, .7, .75, .8, .9, .99)
TICK = Decimal("0.0001")


def price(value, *, up=False):
    n = Decimal(str(value))
    if not n.is_finite() or not 0 <= n <= 1:
        raise ValueError("INVALID_PROBABILITY_PRICE")
    return float(n.quantize(TICK, rounding=ROUND_CEILING if up else ROUND_FLOOR))


def initial_book():
    return {"version":VERSION,"sequence":0,"last_timestamp":0,"forecast_set":None,
        "needs_forecast":True,"active_forecasts":{},"last_armed_minute":-1,
        "levels":{str(q):{"orders":{},"position":None,"trades":0,"wins":0,
            "net_pnl":0.,"gross_pnl":0.,"fees":0.,"closed_cost_basis":0.,"entries":0,"additions":0,
            "max_quantity":0.,"ambiguous":0} for q in EXITS}}


def exit_limit(target, cost):
    # No invented cost+tick quantile. Wait for a newer valid quantile if needed.
    limit=price(target,up=True)
    return limit if Decimal(str(target))>Decimal(str(cost)) and Decimal(str(limit))>Decimal(str(cost)) else None


def sequence(book, events):
    for e in events:
        book["sequence"]+=1
        e.update(sequence=book["sequence"],strategy=VERSION)
    return events


def arm_minute(book, quotes, now, quantity=1., max_quantity=100.):
    """One NEW tranche per side per minute, including still-unfilled tranches.

    Uses terminal P1 of the active 30-minute curve; every order records that
    target date. Orders expire with that curve, not 30 minutes after placement.
    Aggregate same-side pending + filled quantity cannot exceed the cap.
    """
    if not all(math.isfinite(v) for v in (quantity,max_quantity)) or not 0<quantity<=max_quantity<=10000:
        raise ValueError("INVALID_POSITION_LIMITS")
    minute=now//60
    if minute<=book["last_armed_minute"] or not book["active_forecasts"]:
        return []
    events=[]
    for level,ledger in book["levels"].items():
        if ledger.get("exited_forecast_set")==book["forecast_set"]:
            continue
        p=ledger["position"]
        for side,f in book["active_forecasts"].items():
            if p and p["side"]!=side:
                continue
            q=quotes.get(side)
            if not q or not q.observed or q.timestamp>now or now-q.timestamp>120:
                continue
            if not f["available_at"]<=now<f["expires"] or not 0<f["p1"]<q.ask:
                continue
            pending=sum((Decimal(str(o["quantity"])) for o in ledger["orders"].values() if o["side"]==side and o["expires"]>=now),Decimal(0))
            remaining=Decimal(str(max_quantity))-Decimal(str(p["quantity"] if p else 0))-pending
            size=float(min(Decimal(str(quantity)),remaining))
            if size<=0:
                continue
            identifier=digest([book["forecast_set"],level,side,minute])
            order={"id":identifier,"side":side,"limit":f["p1"],"placed_at":now,
                "expires":f["expires"],"target_timestamp":f["expires"],
                "exit_target":f["quantiles"][level],"quantity":size,"forecast_id":f["forecast_id"]}
            ledger["orders"][identifier]=order
            events.append({"kind":"buy_limit_placed","timestamp":now,"level":level,"side":side,
                "order":order,"quote_ask":q.ask,"quote_timestamp":q.timestamp,
                "purpose":"average_in" if p else "initial_entry"})
    book["last_armed_minute"]=minute
    return sequence(book,events)


def replace_orders(book, forecasts, quotes, now, quantity=1., max_quantity=100.):
    if book.get("version")!=VERSION:
        raise ValueError("INCOMPATIBLE_P1_BOOK_VERSION")
    if len(forecasts) not in {2,6} or set(forecasts)!=set(quotes):
        raise ValueError("COMPLETE_MONEYLINE_GROUP_REQUIRED")
    if any(not q.observed or q.timestamp>now or now-q.timestamp>120 for q in quotes.values()):
        raise ValueError("FRESH_BOTH_SIDE_QUOTES_REQUIRED")
    targets={f["rows"][-1]["timestamp"] for f in forecasts.values()}
    if len(targets)!=1 or min(targets)<=now or any(f["available_at"]>now for f in forecasts.values()):
        raise ValueError("PROSPECTIVE_FORECAST_REQUIRED")
    if now<book["last_timestamp"]:
        raise ValueError("OUT_OF_ORDER_FORECAST")
    for f in forecasts.values():
        values=[price(f["rows"][-1]["quantiles"][str(q)]) for q in (.01,*EXITS)]
        if values!=sorted(values):
            raise ValueError("CROSSING_FORECAST_QUANTILES")
    version=digest({s:f["forecast_id"] for s,f in forecasts.items()})
    if book["forecast_set"]==version:
        return arm_minute(book,quotes,now,quantity,max_quantity)
    events=[]
    for level,ledger in book["levels"].items():
        for order in ledger["orders"].values():
            events.append({"kind":"buy_limit_cancelled","reason":"new_forecast","timestamp":now,"level":level,"order":order})
        ledger["orders"]={}
        p=ledger["position"]
        if p:
            f=forecasts[p["side"]]
            p.update(exit_target=f["rows"][-1]["quantiles"][level],exit_placed_at=now,exit_forecast_id=f["forecast_id"])
            p["exit_limit"]=exit_limit(p["exit_target"],p["average_cost"])
            events.append({"kind":"exit_limit_updated","timestamp":now,"level":level,"side":p["side"],
                "target":p["exit_target"],"limit":p["exit_limit"],"average_cost":p["average_cost"],"forecast_id":f["forecast_id"]})
    book.update(forecast_set=version,needs_forecast=False,
        active_forecasts={s:{"forecast_id":f["forecast_id"],"available_at":now,
            "expires":f["rows"][-1]["timestamp"],"p1":price(f["rows"][-1]["quantiles"]["0.01"]),
            "quantiles":f["rows"][-1]["quantiles"]} for s,f in forecasts.items()})
    # A replacement during the same minute does not authorize another tranche.
    return sequence(book,events)+arm_minute(book,quotes,now,quantity,max_quantity)


def advance_book(book, quotes, now, fee_rate=.01):
    if now<=book["last_timestamp"]:
        return []
    if not math.isfinite(fee_rate) or not 0<=fee_rate<1:
        raise ValueError("INVALID_FEE")
    events=[]
    for level,ledger in book["levels"].items():
        for identifier,o in list(ledger["orders"].items()):
            if o["expires"]<now:
                events.append({"kind":"buy_limit_expired","timestamp":now,"level":level,"order":o})
                del ledger["orders"][identifier]
        p=ledger["position"]
        hits=[o for o in ledger["orders"].values() if o["side"] in quotes
            and quotes[o["side"]].observed and quotes[o["side"]].timestamp==now
            and o["placed_at"]<now<=o["expires"] and quotes[o["side"]].ask<=o["limit"]]
        q=quotes.get(p["side"]) if p else None
        sell=bool(p and q and q.observed and q.timestamp==now and now>p["exit_placed_at"]
            and p["exit_limit"] is not None and q.bid>=p["exit_limit"])
        if len({o["side"] for o in hits})>1 or (sell and hits):
            ledger["orders"]={};ledger["ambiguous"]+=1;book["needs_forecast"]=True
            events.append({"kind":"ambiguous_fill_excluded","timestamp":now,"level":level})
        elif sell:
            gross=p["exit_limit"]*p["quantity"]-p["cost_basis"]
            fees=p["entry_fees"]+fee_rate*p["exit_limit"]*p["quantity"]
            net=gross-fees
            events.append({"kind":"exit","timestamp":now,"level":level,"side":p["side"],
                "entry_at":p["entry_at"],"entry":p["average_cost"],"exit":p["exit_limit"],
                "quantity":p["quantity"],"cost_basis":p["cost_basis"],"additions":p["additions"],
                "gross_pnl":gross,"fees":fees,"net_pnl":net,"quote_bid":q.bid,"forecast_id":p["exit_forecast_id"]})
            ledger.update(position=None,orders={},trades=ledger["trades"]+1,wins=ledger["wins"]+int(net>0),
                net_pnl=ledger["net_pnl"]+net,gross_pnl=ledger["gross_pnl"]+gross,fees=ledger["fees"]+fees,
                closed_cost_basis=ledger["closed_cost_basis"]+p["cost_basis"],
                exited_forecast_set=book["forecast_set"])
            book["needs_forecast"]=True
        elif hits:
            for o in sorted(hits,key=lambda o:(o["placed_at"],o["id"])):
                side=o["side"]
                if p and p["side"]!=side:
                    raise ValueError("OPPOSING_POSITION_ORDER")
                cost=float(Decimal(str(o["limit"]))*Decimal(str(o["quantity"])))
                previous=p["average_cost"] if p else None
                size=float(Decimal(str(p["quantity"] if p else 0))+Decimal(str(o["quantity"])))
                total=float(Decimal(str(p["cost_basis"] if p else 0))+Decimal(str(cost)))
                kind="average_in" if p else "entry"
                if p:
                    p.update(quantity=size,cost_basis=total,average_cost=total/size,
                        entry_fees=p["entry_fees"]+cost*fee_rate,additions=p["additions"]+1,exit_placed_at=now)
                    ledger["additions"]+=1
                else:
                    p={"side":side,"quantity":size,"cost_basis":total,"average_cost":total/size,
                        "entry_fees":cost*fee_rate,"entry_at":now,"additions":0,
                        "exit_target":o["exit_target"],"exit_placed_at":now,"exit_forecast_id":o["forecast_id"]}
                    ledger["entries"]+=1
                p["exit_limit"]=exit_limit(p["exit_target"],p["average_cost"])
                ledger["position"]=p;ledger["max_quantity"]=max(ledger["max_quantity"],size)
                del ledger["orders"][o["id"]]
                events.append({"kind":kind,"timestamp":now,"level":level,"side":side,"order":o,
                    "price":o["limit"],"added_quantity":o["quantity"],"quantity":size,
                    "previous_average":previous,"average_cost":p["average_cost"],"cost_basis":total,
                    "entry_fees":p["entry_fees"],"exit_limit":p["exit_limit"],"quote_ask":quotes[side].ask})
            for identifier,o in list(ledger["orders"].items()):
                if o["side"]!=p["side"]:
                    events.append({"kind":"buy_limit_cancelled","reason":"opposite_side_filled", "timestamp":now,"level":level,"order":o})
                    del ledger["orders"][identifier]
        p=ledger["position"]
        mark=quotes.get(p["side"]) if p else None
        if p and mark and mark.observed and mark.timestamp==now:
            p.update(mark_bid=mark.bid,mark_timestamp=now,
                mark_net_pnl=mark.bid*p["quantity"]*(1-fee_rate)-p["cost_basis"]-p["entry_fees"])
    book["last_timestamp"]=now
    return sequence(book,events)


def summary(books):
    books=list(books); result={}
    for level in map(str,EXITS):
        ledgers=[b["levels"][level] for b in books]
        positions=[l["position"] for l in ledgers if l["position"]]
        trades=sum(l["trades"] for l in ledgers);wins=sum(l["wins"] for l in ledgers)
        cost=sum(l["closed_cost_basis"] for l in ledgers)
        result[level]={"closed_trades":trades,"net_wins":wins,"win_rate":wins/trades if trades else None,
            "net_pnl":sum(l["net_pnl"] for l in ledgers),"gross_pnl":sum(l["gross_pnl"] for l in ledgers),
            "closed_cost_basis":cost,"net_return_on_closed_cost":sum(l["net_pnl"] for l in ledgers)/cost if cost else None,
            "closed_trade_fees":sum(l["fees"] for l in ledgers),"initial_entries":sum(l["entries"] for l in ledgers),
            "averaging_fills":sum(l["additions"] for l in ledgers),"open_positions":len(positions),
            "open_shares":sum(p["quantity"] for p in positions),"open_cost_basis":sum(p["cost_basis"] for p in positions),
            "open_mark_net_pnl":sum(p["mark_net_pnl"] for p in positions) if all("mark_net_pnl" in p for p in positions) else None,
            "max_position_shares":max((l["max_quantity"] for l in ledgers),default=0),
            "pending_buy_orders":sum(len(l["orders"]) for l in ledgers),
            "ambiguous_fills_excluded":sum(l["ambiguous"] for l in ledgers)}
    return result
