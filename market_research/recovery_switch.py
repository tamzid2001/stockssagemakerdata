"""Buy on a P90 upward cross; one position per game.

Signals use an already-published, time-aligned forecast. Fills use the NEXT
genuine quote, never the bar used to detect a signal. No fixed-price stop or
forced horizon liquidation. This is private paper research, not exchange orders.
"""
import csv
import gzip
import json
import math
from collections import defaultdict

from .engine import digest
from .quantile_paths import model_participation

VERSION = "in_game_p90_switch_v1"
BTC_MINUTE_POLICY = "btc_p90_or_opposite_p10_hold_switch_v1"
VARIANTS = (("p90_cross_p10_opposite", "0.9"),)
EXIT_FRACTIONS = (.01, .05, .10, .20, .30, .40, .50, .60, .70, .80, .90, 1.0)


def statistics(trades):
    closed = sorted((t for t in trades if t.get("status") == "closed"),
                    key=lambda t: (t["exit_at"], t["game_id"], t["trade_id"]))
    wins = losses = even = win_streak = loss_streak = longest_win = longest_loss = 0
    pnl = peak = drawdown = 0.
    for trade in closed:
        value = trade["net_pnl"]
        pnl += value
        peak = max(peak, pnl)
        drawdown = max(drawdown, peak - pnl)
        if value > 1e-12:
            wins += 1; win_streak += 1; loss_streak = 0
        elif value < -1e-12:
            losses += 1; loss_streak += 1; win_streak = 0
        else:
            even += 1; win_streak = loss_streak = 0
        longest_win = max(longest_win, win_streak)
        longest_loss = max(longest_loss, loss_streak)
    cost = sum(t["entry_price"] * t["quantity"] for t in closed)
    opened = [t for t in trades if t.get("status") == "open"]
    return {"closed_trades": len(closed), "wins": wins, "losses": losses, "breakeven": even,
            "win_rate": wins / len(closed) if closed else None,
            "gross_pnl": sum(t["gross_pnl"] for t in closed), "net_pnl": pnl,
            "fees": sum(t["fees"] for t in closed), "closed_entry_notional": cost,
            "net_return_on_closed_entry_notional": pnl / cost if cost else None,
            "longest_winning_streak": longest_win, "longest_losing_streak": longest_loss,
            "current_winning_streak": win_streak, "current_losing_streak": loss_streak,
            "realized_equity_max_drawdown": drawdown,
            "max_quantity_used": max((t["quantity"] for t in trades), default=0),
            "open_positions": len(opened),
            "open_mark_net_pnl": sum(t["mark_net_pnl"] for t in opened),
            "open_entry_notional": sum(t["entry_price"] * t["quantity"] for t in opened)}


def simulate(forecasts, observations, resolutions, *, as_of, multiplier=2.5, max_shares=100, fee_rate=.01,
             switch_on_other_p90=False, p90_touch=False, exit_policy=None, btc_minute_policy=False):
    if type(btc_minute_policy) is not bool or (btc_minute_policy and
            (exit_policy is not None or not switch_on_other_p90 or not p90_touch or max_shares > 100)):
        raise ValueError("INVALID_BTC_MINUTE_POLICY")
    if not all(math.isfinite(v) for v in (multiplier, max_shares, fee_rate)) or not (
            1 <= multiplier <= 10 and 1 <= max_shares <= 10000 and 0 <= fee_rate <= .1):
        raise ValueError("INVALID_RECOVERY_CONFIGURATION")
    if exit_policy is not None and (not isinstance(exit_policy, dict)
            or set(exit_policy) != {"kind", "fraction"}
            or exit_policy["kind"] not in ("take_profit", "trailing_stop")
            or type(exit_policy["fraction"]) not in (int, float)
            or exit_policy["fraction"] not in EXIT_FRACTIONS):
        raise ValueError("INVALID_EXIT_COMPARISON_POLICY")
    variants = VARIANTS if exit_policy is None else (
        (f"p90_{exit_policy['kind']}_{round(exit_policy['fraction'] * 100):02d}pct", "0.9"),)
    if btc_minute_policy:
        variants = ((BTC_MINUTE_POLICY, "0.9"),)
    groups = defaultdict(list)
    for f in forecasts:
        if f.get("strategy") == VERSION and f["available_at"] <= as_of:
            c = f["market_context"]
            groups[(c["event_id"], f["origin"])].append(f)
    games = defaultdict(list)
    for (game, _), pair in groups.items():
        if len(pair) != pair[0]["expected_side_count"] or len({f["market_context"]["contract_id"] for f in pair}) != len(pair):
            continue
        if len({f["available_at"] for f in pair}) != 1:
            continue
        if btc_minute_policy and (len(pair) != 2 or
                len({f['market_context']['market_id'] for f in pair}) != 1):
            continue
        games[game].append(pair)
    tape = defaultdict(dict)
    for q in observations:
        if q.get("observed") and q["timestamp"] % 60 == 0 and q["timestamp"] <= as_of and 0 <= q["bid"] <= q["ask"] <= 1:
            tape[q["contract_id"]][q["timestamp"]] = q
    trades, signals, per_game, pending_orders = [], [], {}, []
    for game, pairs in sorted(games.items()):
        pairs.sort(key=lambda pair: (pair[0]["available_at"], pair[0]["origin"]))
        sides = {f["market_context"]["contract_id"] for p in pairs for f in p}
        game_start = max(f["game_start"] for p in pairs for f in p)
        # No post-resolution quote is a tradable observation.
        ends = [r["settled_at"] for s in sides if (r := resolutions.get(s, {})).get("settled_at")]
        market_end = min(ends) if ends else as_of + 1
        end = min(as_of, market_end)
        times = sorted({t for s in sides for t in tape[s] if game_start < t <= as_of and t < market_end})
        curves = {f["forecast_id"]: {r["timestamp"]: r["quantiles"] for r in f["rows"]} for p in pairs for f in p}
        for variant, high in variants:
            size, position, pending, active, pair_index = 1., None, None, None, 0
            memory, ledger = {}, []
            fresh_cross_required = False
            increases = ambiguous = expired = signals_count = low_signals_count = 0

            def close(t, bid, reason):
                nonlocal position, size, increases, fresh_cross_required
                value = position
                value.update(status="closed", exit_at=t, exit_price=bid, exit_reason=reason)
                if pending and reason != "authoritative_settlement":
                    value.update(exit_signal_at=pending["signal_at"], exit_signal_p10=pending.get("exit_signal_p10"))
                value["gross_pnl"] = (bid - value["entry_price"]) * value["quantity"]
                # The legacy benchmark is immutable. The new policy does not
                # treat settlement payout as an exchange sell-order execution.
                exit_fee_base = 0 if btc_minute_policy and reason == 'authoritative_settlement' else bid
                value["fees"] = fee_rate * (exit_fee_base + value["entry_price"]) * value["quantity"]
                value["net_pnl"] = value["gross_pnl"] - value["fees"]
                value['game_realized_net_pnl'] = sum(t['net_pnl'] for t in ledger if t.get('status') == 'closed')
                if value['game_realized_net_pnl'] < -1e-12:
                    escalate = not btc_minute_policy or reason == 'p10_exit_and_opposite'
                    next_size = min(size * multiplier, max_shares) if escalate else size
                    increases += next_size > size
                    size = next_size
                else:
                    size = 1.
                value["next_quantity"] = size
                if reason in ("take_profit", "trailing_stop"):
                    fresh_cross_required = True
                position = None

            for t in times:
                quotes = {s: tape[s][t] for s in sides if t in tape[s]}
                # Execute a signal from a strictly earlier observed minute.
                if pending:
                    if t > pending["signal_at"] + 120:
                        expired += 1; pending = None
                    elif t > pending["signal_at"]:
                        target = pending.get("target_side")
                        held = position["contract_id"] if position else None
                        can_exit = held is None or held in quotes
                        can_enter = target is None or (target in quotes and 0 < quotes[target]["ask"] < 1)
                        if can_exit and can_enter:
                            if position:
                                close(t, quotes[held]["bid"], pending["reason"])
                            if target:
                                position = {"trade_id": digest([VERSION, game, variant, fee_rate if btc_minute_policy else None, t, target])
                                            if btc_minute_policy else digest([VERSION, game, variant, t, target]),
                                    "game_id": game, "variant": variant, "contract_id": target,
                                    "signal_at": pending["signal_at"], "previous_quote_at": pending.get("previous_quote_at"),
                                    "entry_reason": pending["reason"], "trigger_contract_id": pending.get("trigger_contract_id", target),
                                    "signal_p90": pending.get("signal_p90"), "signal_bid": pending.get("signal_bid"),
                                    "signal_forecast_id": pending["forecast_id"], "entry_at": t,
                                    "entry_price": quotes[target]["ask"], "quantity": size, "status": "open"}
                                if exit_policy:
                                    position["peak_completed_minute_bid"] = quotes[target]["bid"]
                                if btc_minute_policy:
                                    position['signal_p10'] = pending.get('signal_p10', pending.get('exit_signal_p10'))
                                ledger.append(position)
                                fresh_cross_required = False
                            pending = None
                # Percentage exits remain active through gaps between forecast
                # windows. Only real completed-minute bids update the peak;
                # candle highs, stream ticks and future settlement never do.
                if exit_policy and position and not pending and position["contract_id"] in quotes:
                    bid = quotes[position["contract_id"]]["bid"]
                    position["peak_completed_minute_bid"] = max(position["peak_completed_minute_bid"], bid)
                    kind, fraction = exit_policy["kind"], exit_policy["fraction"]
                    level = (position["entry_price"] * (1 + fraction) if kind == "take_profit"
                             else position["peak_completed_minute_bid"] * (1 - fraction))
                    hit = bid >= level if kind == "take_profit" else bid <= level
                    if hit and t > position["entry_at"]:
                        pending = {"signal_at": t, "target_side": None, "reason": kind,
                                   "forecast_id": position["signal_forecast_id"], "exit_threshold": level}
                        position["exit_signal_threshold"] = level
                while pair_index < len(pairs) and pairs[pair_index][0]["available_at"] < t:
                    active = pairs[pair_index]; pair_index += 1
                    memory = {}  # A revised curve cannot manufacture a crossing.
                if not active:
                    continue
                recoveries = []
                low_entries = []
                active_by_side = {f["market_context"]["contract_id"]: f for f in active}
                for s, f in active_by_side.items():
                    q, levels = quotes.get(s), curves[f["forecast_id"]].get(t)
                    if not q or not levels:
                        continue
                    before = memory.get(s)
                    if before and t - before["timestamp"] != 60:
                        before = None
                    previous_quote_at = before["timestamp"] if before else None
                    crossed = (q["bid"] >= levels[high] if p90_touch and not fresh_cross_required else
                               bool(before and before["bid"] <= before["high"] and q["bid"] > levels[high]))
                    if crossed:
                        recoveries.append({"target_side": s, "signal_at": t, "previous_quote_at": previous_quote_at,
                                           "signal_p90": levels[high], "signal_bid": q["bid"],
                                           "forecast_id": f["forecast_id"], "reason": "p90_entry"})
                        signals.append({"game_id": game, "variant": variant, "timestamp": t,
                                        "contract_id": s, "previous_quote_at": previous_quote_at, "forecast_id": f["forecast_id"],
                                        "previous_bid": before["bid"] if before else None, "previous_p90": before["high"] if before else None,
                                        "signal_bid": q["bid"], "signal_p90": levels[high]})
                        signals_count += 1
                    memory[s] = {"timestamp": t, "bid": q["bid"], "high": levels[high]}
                    if btc_minute_policy and not position and q['bid'] <= levels['0.1']:
                        mates = [side for side, candidate in active_by_side.items() if side != s
                                 and candidate['market_context']['market_id'] == f['market_context']['market_id']]
                        if len(mates) == 1:
                            low_entries.append({'target_side': mates[0], 'signal_at': t,
                                'trigger_contract_id': s, 'forecast_id': f['forecast_id'],
                                'reason': 'opposite_p10_entry', 'signal_bid': q['bid'],
                                'signal_p90': None, 'signal_p10': levels['0.1']})
                            low_signals_count += 1
                if pending:
                    continue
                if btc_minute_policy and not position:
                    # P90 and the paired P10 may agree on one side. Conflicting
                    # sides are explicitly skipped, never chosen by settlement.
                    choices = {r['target_side']: r for r in low_entries + recoveries}
                    recoveries = list(choices.values())
                if len(recoveries) > 1:
                    ambiguous += 1  # Never choose the side using future results.
                    recoveries = []
                other = next((r for r in recoveries if not position or r["target_side"] != position["contract_id"]), None)
                if other and not position:
                    pending = other
                elif position:
                    s = position["contract_id"]
                    f = active_by_side.get(s)
                    levels = curves[f["forecast_id"]].get(t) if f else None
                    if s in quotes and levels and quotes[s]["bid"] <= levels["0.1"]:
                        market_id = f["market_context"]["market_id"]
                        mates = [side for side, candidate in active_by_side.items()
                                 if side != s and candidate["market_context"]["market_id"] == market_id]
                        if len(mates) != 1:
                            raise ValueError("UNIQUE_OPPOSITE_BINARY_SIDE_REQUIRED")
                        pending = {"signal_at": t, "reason": "p10_exit_and_opposite", "target_side": mates[0],
                                   "trigger_contract_id": s, "exit_signal_p10": levels["0.1"], "forecast_id": f["forecast_id"]}
                    elif other and switch_on_other_p90:
                        candidate = active_by_side[other['target_side']]
                        if candidate['market_context']['market_id'] == f['market_context']['market_id']:
                            pending = {**other, 'reason': 'opposite_p90_switch'}
            if position:
                r = resolutions.get(position["contract_id"], {})
                if r.get("resolution_status") == "resolved" and r.get("settled_at", as_of + 1) <= as_of and r["settled_at"] > position["entry_at"] and r.get("selected_side_payout") in (0, 1):
                    close(r["settled_at"], r["selected_side_payout"], "authoritative_settlement")
                else:
                    marks = [q for t, q in tape[position["contract_id"]].items() if position["entry_at"] <= t <= end]
                    mark = max(marks, key=lambda q: q["timestamp"]) if marks else None
                    bid = mark["bid"] if mark else position["entry_price"]
                    position["mark_at"] = mark["timestamp"] if mark else position["entry_at"]
                    position["mark_net_pnl"] = (bid - position["entry_price"] - fee_rate * (bid + position["entry_price"])) * position["quantity"]
            if pending:
                if market_end <= as_of:
                    expired += 1
                else:
                    pending_orders.append({"game_id": game, "variant": variant, **pending})
            trades.extend(ledger)
            per_game.setdefault(game, {})[variant] = {**statistics(ledger), "recovery_signals": signals_count,
                "ambiguous_signals_excluded": ambiguous, "expired_pending_orders": expired,
                "multiplier_increases": increases, "next_quantity": size,
                **({'opposite_p10_entry_signals': low_signals_count} if btc_minute_policy else {})}
    summary = {variant: statistics([t for t in trades if t["variant"] == variant]) for variant, _ in variants}
    for variant, s in summary.items():
        s.update({key: sum(g[variant][key] for g in per_game.values()) for key in
                  ("recovery_signals", "ambiguous_signals_excluded", "expired_pending_orders", "multiplier_increases")})
        if btc_minute_policy:
            s['opposite_p10_entry_signals'] = sum(g[variant]['opposite_p10_entry_signals'] for g in per_game.values())
    return {"version": BTC_MINUTE_POLICY if btc_minute_policy else VERSION, "as_of": as_of, "paper_only": True, "summary": summary,
            "per_game": per_game, "trades": sorted(trades, key=lambda t: (t["entry_at"], t["game_id"], t["variant"])),
            "signals": signals, "pending_orders": pending_orders, "model_participation": model_participation(forecasts),
            "configuration": {"base_shares": 1, "loss_multiplier": multiplier, "max_shares": max_shares,
                "sizing": "multiply_next_trade_until_cumulative_game_net_pnl_recovers", "fee_rate_assumption": fee_rate,
                "fixed_price_stop": None, "horizon_exit": False, "execution": "next_genuine_bid_ask_within_120_seconds",
                "quote_basis": "completed_one_minute_bid_ask", "switch_on_other_p90": switch_on_other_p90,
                "p90_trigger": "at_or_above" if p90_touch else "cross_from_at_or_below_to_above",
                **({'flat_entry': 'P90_touch_or_opposite_P10_touch; conflicting_sides_skipped',
                    'sizing': 'multiply_after_held_P10_exit_only_while_game_net_pnl_negative; otherwise retain; reset_on_cumulative_recovery',
                    'settlement_fee_rate': 0, 'exit_priority': 'first_observed_minute; held_P10_wins_same_minute_tie',
                    'maker_fills_verified': False, 'execution_warning': 'Quote benchmark, not resting-order fills or a maker fee claim.'}
                   if btc_minute_policy else {}),
                **({"exit_policy": exit_policy, "percentage_basis": "relative_price_not_probability_points",
                    "percentage_exit_action": "flat_until_fresh_p90_cross",
                    "exit_priority": "pending_order_then_percentage_exit_then_p10_then_opposite_p90",
                    "take_profit_target_above_one": "unreachable_not_clamped",
                    "trailing_peak": "completed_minute_bid_since_fill"} if exit_policy else {})}}


def from_store(store):
    configuration = store.values("configuration")[0]
    coverage = store._get("checkpoints", "recovery_coverage") or {}
    outcomes = {r["contract_id"]: r for r in store.values("checkpoints") if isinstance(r, dict) and r.get("resolution_status")}
    result = simulate(store.values("forecasts"), store.values("observations"), outcomes,
                      as_of=coverage.get("as_of", 0), multiplier=configuration["loss_multiplier"],
                      max_shares=configuration["max_shares"], switch_on_other_p90=True, p90_touch=True)
    return {**result, "coverage": coverage, "forecast_count": len(store.values("forecasts")),
            "failures": [r for r in store.values("checkpoints") if isinstance(r, dict) and r.get("status") == "failed"]}


def export(store):
    result = from_store(store)
    with (store.directory / "recovery_summary.json").open("w") as out:
        json.dump(result, out, allow_nan=False, indent=2)
    for name, rows in (("recovery_trades.csv.gz", result["trades"]), ("recovery_signals.csv.gz", result["signals"])):
        with gzip.open(store.directory / name, "wt", newline="") as out:
            writer = csv.DictWriter(out, fieldnames=sorted({k for row in rows for k in row}) or ["trade_id"])
            writer.writeheader(); writer.writerows(rows)
    from .p1_oco import QUANTILES
    with gzip.open(store.directory / "recovery_forecast_quantiles.csv.gz", "wt", newline="") as out:
        writer = csv.writer(out)
        writer.writerow(["forecast_id", "game_id", "contract_id", "side", "symbol", "game_start",
                         "input_cutoff", "available_at", "history_count", "history_phase", "timestamp", *[f"p{int(q*100):02d}" for q in QUANTILES]])
        for f in sorted(store.values("forecasts"), key=lambda r: (r["origin"], r["forecast_id"])):
            c = f["market_context"]
            for row in f["rows"]:
                writer.writerow([f["forecast_id"], c["event_id"], c["contract_id"], c["side"], c.get("symbol"),
                                 f["game_start"], f["origin"], f["available_at"], f["history_count"], f.get("history_phase"), row["timestamp"],
                                 *[row["quantiles"][str(q)] for q in QUANTILES]])
    return result
