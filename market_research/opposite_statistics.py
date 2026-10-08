"""Per-series paper/live accounting, recovered from encrypted execution evidence.

Returns use actual closed-trade cash flows, never the research backtest. Realized
drawdown is the closed-trade net curve; sampled bid equity is tracked separately.
No exchange calls, credentials, account balances or Firestore writes are needed.
"""
from decimal import Decimal
from math import isfinite

SCHEMA = 1
D = Decimal


def amount(value):
    number = D(str(value))
    if not number.is_finite():
        raise ValueError("STATISTICS_NONFINITE_AMOUNT")
    return number


def fresh():
    return dict(trades=0, wins=0, losses=0, breakevens=0, net="0", fees="0",
                cost="0", payout="0", contracts="0", entry_price_sum="0",
                min_entry=None, max_entry=None, max_contracts="0", gross_profit="0",
                gross_loss="0", peak_net="0", max_realized_drawdown="0",
                current_win_streak=0, current_loss_streak=0, max_win_streak=0,
                max_loss_streak=0, duration_seconds=0, max_duration_seconds=0,
                win_duration_seconds=0, loss_duration_seconds=0,
                duration_sources={}, first_entry_at=None, last_settled_at=None)


def entry_time(entry):
    """Live timestamps are fill *confirmation* times, not matching-engine times."""
    if entry.get("entry_at") is not None:
        return float(entry["entry_at"]), "paper_ask_receipt"
    receipts = [float(f["reconciled_at"]) for f in entry.get("fills", [])
                if amount(f.get("filled", 0)) > 0 and f.get("reconciled_at") is not None]
    if receipts:
        return min(receipts), "live_first_fill_confirmation"
    return float(entry["created_at"]), "legacy_entry_intent_proxy"


def add_closed(stats, entry):
    quantity = amount(entry["filled"])
    if entry.get("status") != "settled" or quantity <= 0:
        return
    cost, fees, net = (amount(entry[k]) for k in ("cost", "fees", "net_pnl"))
    payout = quantity if entry["official_result"] == entry["side"] else D(0)
    if cost < 0 or fees < 0 or payout - cost - fees != net:
        raise ValueError("STATISTICS_SETTLEMENT_INVALID")
    price = cost / quantity
    started, source = entry_time(entry)
    ended = float(entry["settled_at"])
    if not isfinite(started) or not isfinite(ended) or ended < started:
        raise ValueError("STATISTICS_DURATION_INVALID")
    duration = ended - started
    stats["trades"] += 1
    outcome = "wins" if net > 0 else "losses" if net < 0 else "breakevens"
    stats[outcome] += 1
    for key, value in (("net", net), ("fees", fees), ("cost", cost),
                       ("payout", payout), ("contracts", quantity), ("entry_price_sum", price),
                       ("gross_profit", max(net, D(0))), ("gross_loss", max(-net, D(0)))):
        stats[key] = str(amount(stats[key]) + value)
    stats["min_entry"] = str(min(price, amount(stats["min_entry"]))) if stats["min_entry"] is not None else str(price)
    stats["max_entry"] = str(max(price, amount(stats["max_entry"]))) if stats["max_entry"] is not None else str(price)
    stats["max_contracts"] = str(max(quantity, amount(stats["max_contracts"])))
    stats["peak_net"] = str(max(amount(stats["peak_net"]), amount(stats["net"])))
    stats["max_realized_drawdown"] = str(max(amount(stats["max_realized_drawdown"]),
                                          amount(stats["peak_net"]) - amount(stats["net"])))
    for side, winning in (("win", net > 0), ("loss", net < 0)):
        current, maximum = "current_" + side + "_streak", "max_" + side + "_streak"
        stats[current] = stats[current] + 1 if winning else 0
        stats[maximum] = max(stats[maximum], stats[current])
    stats["duration_seconds"] += duration
    stats["max_duration_seconds"] = max(stats["max_duration_seconds"], duration)
    if net > 0:
        stats["win_duration_seconds"] += duration
    elif net < 0:
        stats["loss_duration_seconds"] += duration
    stats["duration_sources"][source] = stats["duration_sources"].get(source, 0) + 1
    stats["first_entry_at"] = min(started, stats["first_entry_at"]) if stats["first_entry_at"] is not None else started
    stats["last_settled_at"] = ended


def matches_totals(stats, totals):
    return (all(stats[k] == totals.get(k, 0) for k in ("trades", "wins", "losses"))
            and all(amount(stats[k]) == amount(totals.get(k, 0)) for k in ("net", "fees")))


def economic_record(entry):
    return tuple(str(entry.get(k)) for k in ("ticker", "side", "filled", "cost", "fees",
                  "net_pnl", "official_result", "settled_at", "created_at", "entry_at"))


def bootstrap(state, archived_groups, now):
    """Rebuild once; deduplicate immutable archives and current journal by market/mode."""
    if state.get("statistics", {}).get("schema") == SCHEMA:
        return
    closed = {}
    for ticker, group in archived_groups:
        for mode, entry in group.items():
            if entry.get("status") != "settled" or amount(entry.get("filled", 0)) <= 0:
                continue
            key = ticker, mode
            if entry.get("ticker") != ticker or mode not in ("paper", "live"):
                raise ValueError("STATISTICS_ARCHIVE_IDENTITY_INVALID")
            if key in closed and economic_record(closed[key]) != economic_record(entry):
                raise ValueError("STATISTICS_ARCHIVE_CONFLICT")
            closed[key] = entry
    for ticker, group in state.get("entries", {}).items():
        for mode, entry in group.items():
            if entry.get("status") != "settled" or amount(entry.get("filled", 0)) <= 0:
                continue
            key = ticker, mode
            if key in closed and economic_record(closed[key]) != economic_record(entry):
                raise ValueError("STATISTICS_ARCHIVE_CONFLICT")
            closed[key] = entry
    modes = {mode: fresh() for mode in ("paper", "live")}
    for (ticker, mode), entry in sorted(closed.items(), key=lambda r: (r[1]["settled_at"], r[0])):
        add_closed(modes[mode], entry)
    complete = {m: matches_totals(modes[m], state.get("totals", {}).get(m, {})) for m in modes}
    state["statistics"] = dict(schema=SCHEMA, initialized_at=now, modes=modes,
                               history_complete=complete, equity={})
    for group in state.get("entries", {}).values():
        for entry in group.values():
            if entry.get("status") == "settled":
                entry["statistics_recorded"] = SCHEMA


def record_settlement(state, entry, mode, now):
    # Called in the same journal write as settlement totals: recovery cannot
    # apply only half an accounting update or count the fill twice.
    if state.get("statistics", {}).get("schema") != SCHEMA:
        bootstrap(state, [], now)
    if entry.get("statistics_recorded") == SCHEMA:
        return
    add_closed(state["statistics"]["modes"][mode], entry)
    entry["statistics_recorded"] = SCHEMA


def number(value, places=6):
    return round(float(amount(value)), places) if value is not None else None


def ratio(top, bottom, multiplier=1):
    return number(amount(top) / amount(bottom) * multiplier) if amount(bottom) else None


def performance(state, minutes, now, modes=("paper", "live")):
    """Minute bid marks are displayed only when every open position has a fresh bid."""
    if state.get("statistics", {}).get("schema") != SCHEMA:
        bootstrap(state, [], now)
    report = {}
    latest = {}
    for row in minutes:
        if (row.get("collection_mode") != "live" or row["received_at"] > now or row["timestamp"] > now):
            continue
        previous = latest.get(row["market_id"])
        if previous is None or row["timestamp"] > previous["timestamp"]:
            latest[row["market_id"]] = row
    for mode in modes:
        stats = state["statistics"]["modes"][mode]
        totals = state.get("totals", {}).get(mode, {})
        complete = state["statistics"]["history_complete"][mode] and matches_totals(stats, totals)
        trades, wins, losses = (totals.get(k, 0) for k in ("trades", "wins", "losses"))
        entries = [group[mode] for group in state.get("entries", {}).values() if mode in group]
        active = [e for e in entries if e.get("status") != "settled"]
        opened = [e for e in active if amount(e.get("filled", 0)) > 0]
        cost = sum((amount(e["cost"]) for e in opened), D(0))
        fees = sum((amount(e["fees"]) for e in opened), D(0))
        quantity = sum((amount(e["filled"]) for e in opened), D(0))
        value = D(0)
        ages = []
        unmarked = 0
        for entry in opened:
            quote = latest.get(entry["ticker"])
            bid = quote.get(entry["side"] + "_bid") if quote else None
            if quote is None or now - quote["timestamp"] > 120 or bid is None or not 0 <= amount(bid) <= 1:
                unmarked += 1
            else:
                value += amount(entry["filled"]) * amount(bid)
                ages.append(now - quote["timestamp"])
        net = amount(totals.get("net", 0))
        equity_value = net - cost - fees + value if not unmarked else None
        sample = state["statistics"]["equity"].get(mode)
        if equity_value is not None:
            if sample is None:
                # Baseline starts at the first complete snapshot; it does not
                # fabricate historic mark-to-market drawdown from settlement totals.
                sample = dict(started_at=now, peak=str(equity_value), max_drawdown="0", samples=0)
                state["statistics"]["equity"][mode] = sample
            sample["peak"] = str(max(amount(sample["peak"]), equity_value))
            sample["max_drawdown"] = str(max(amount(sample["max_drawdown"]), amount(sample["peak"]) - equity_value))
            sample["samples"] += 1
            sample["last_at"] = now
        n = stats["trades"]
        row = dict(closed_trades=trades, wins=wins, losses=losses, breakevens=trades-wins-losses,
                   win_rate_pct=ratio(wins, trades, 100), net_pnl_usd=number(net),
                   fees_usd=number(totals.get("fees", 0)), history_complete=complete,
                   reconstructed_closed_trades=n, realized_drawdown_basis="closed_trade_net_at_confirmed_settlement",
                   open_positions=len(opened), pending_intents=sum(bool(e.get("pending")) for e in active),
                   entry_attempts_in_buffer=sum(e.get("attempt", 0) for e in entries),
                   rejected_entries_in_buffer=sum(e.get("status") == "entry_rejected" for e in active),
                   unfilled_entries=sum(amount(e.get("filled", 0)) == 0 for e in active),
                   open_contracts=number(quantity), open_entry_cost_usd=number(cost), open_fees_usd=number(fees),
                   open_average_entry_cents=ratio(cost, quantity, 100),
                   unmarked_positions=unmarked, oldest_bid_age_seconds=max(ages, default=None),
                   unrealized_bid_pnl_usd=number(value-cost-fees) if not unmarked else None,
                   strategy_cash_pnl_usd=number(net-cost-fees),
                   sampled_bid_equity_pnl_usd=number(equity_value),
                   max_sampled_bid_equity_drawdown_usd=number(sample["max_drawdown"]) if sample else None,
                   bid_equity_tracking_started_at=sample["started_at"] if sample else None,
                   bid_equity_samples=sample["samples"] if sample else 0,
                   accounting="authenticated_fills_and_settlements" if mode == "live" else "ask_snapshot_and_modeled_fees")
        derived = dict(gross_pnl_usd=number(amount(stats["net"])+amount(stats["fees"])),
                       total_entry_cost_usd=number(stats["cost"]), settlement_payout_usd=number(stats["payout"]),
                       return_on_closed_entry_spend_pct=ratio(stats["net"], amount(stats["cost"])+amount(stats["fees"]), 100),
                       avg_net_pnl_per_trade_usd=ratio(stats["net"], n),
                       avg_entry_cents=ratio(stats["entry_price_sum"], n, 100),
                       volume_weighted_entry_cents=ratio(stats["cost"], stats["contracts"], 100),
                       min_entry_cents=number(amount(stats["min_entry"])*100) if stats["min_entry"] is not None else None,
                       max_entry_cents=number(amount(stats["max_entry"])*100) if stats["max_entry"] is not None else None,
                       avg_contracts_per_trade=ratio(stats["contracts"], n), max_contracts_per_trade=number(stats["max_contracts"]),
                       avg_entry_cost_usd=ratio(stats["cost"], n),
                       max_realized_drawdown_usd=number(stats["max_realized_drawdown"]),
                       current_win_streak=stats["current_win_streak"], current_loss_streak=stats["current_loss_streak"],
                       max_win_streak=stats["max_win_streak"], max_loss_streak=stats["max_loss_streak"],
                       profit_factor=ratio(stats["gross_profit"], stats["gross_loss"]),
                       avg_win_usd=ratio(stats["gross_profit"], stats["wins"]), avg_loss_usd=ratio(stats["gross_loss"], stats["losses"]),
                       avg_trade_duration_seconds=ratio(stats["duration_seconds"], n),
                       max_trade_duration_seconds=stats["max_duration_seconds"] if n else None,
                       avg_winning_duration_seconds=ratio(stats["win_duration_seconds"], stats["wins"]),
                       avg_losing_duration_seconds=ratio(stats["loss_duration_seconds"], stats["losses"]))
        row.update({key: val if complete else None for key, val in derived.items()})
        row["duration_sources"] = stats["duration_sources"] if complete else None
        report[mode] = row
    return report


def log_lines(series, report):
    def display(value, suffix=""):
        return "n/a" if value is None else f"{value:.2f}{suffix}"
    return [f"{series} {mode.upper()} | trades={r['closed_trades']} W/L={r['wins']}/{r['losses']} "
            f"win={display(r['win_rate_pct'], '%')} net=${display(r['net_pnl_usd'])} "
            f"fees=${display(r['fees_usd'])} return/spend={display(r['return_on_closed_entry_spend_pct'], '%')} "
            f"DD realized=${display(r['max_realized_drawdown_usd'])} "
            f"DD sampled bid=${display(r['max_sampled_bid_equity_drawdown_usd'])} "
            f"streak W/L={r['current_win_streak']}/{r['current_loss_streak']} "
            f"max W/L={r['max_win_streak']}/{r['max_loss_streak']} "
            f"avg entry={display(r['avg_entry_cents'], 'c')} "
            f"avg duration={display(r['avg_trade_duration_seconds'], 's')} "
            f"open={r['open_positions']} open cost=${display(r['open_entry_cost_usd'])} "
            f"unrealized bid=${display(r['unrealized_bid_pnl_usd'])} pending={r['pending_intents']} "
            f"history={'complete' if r['history_complete'] else 'incomplete'}"
            for mode, r in report.items()]
