"""Aggregate an encrypted BTC interval replay without exposing trade rows.

The command is intended for an authenticated GitHub Actions runner. It reads
the immutable, encrypted replay records and emits only price buckets and
portfolio statistics. Raw quotes, forecasts, and per-market trades never leave
the encrypted research store.
"""

from __future__ import annotations

import argparse
from collections import Counter
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
import heapq
import json
import math
import os
from pathlib import Path
import re
import statistics
import tempfile

from .engine import digest
from .btc_sticky_tracking import taker_fee
from .recovery_cloud import Campaign, decode_catalog, validate_campaign
from .recovery_switch import statistics as trade_statistics


REPORT_KEY = re.compile(r"report-[a-f0-9]{32}")
ORIGIN_KEY = re.compile(r"origin-(?:[1-9]|1[0-2])-[a-f0-9]{32}")


def _finite(value: object) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("NONFINITE_TRADE_VALUE")
    return number


def _cent_bucket(price: object) -> int:
    value = Decimal(str(price))
    if not value.is_finite() or not Decimal("0") < value < Decimal("1"):
        raise ValueError("INVALID_ENTRY_PRICE")
    # cost / quantity in saved replay rows can introduce binary float noise
    # (e.g. 0.58 becomes 0.57999999999999996). Preserve sub-cent prices while
    # removing noise far below the exchange's recorded price precision.
    value = value.quantize(Decimal("0.000000001"))
    return int((value * 100).to_integral_value(rounding=ROUND_FLOOR))


def _ordered_closed(trades: list[dict]) -> list[dict]:
    closed = []
    for trade in trades:
        if trade.get("status") != "closed":
            continue
        net = _finite(trade["net_pnl"])
        quantity = _finite(trade["quantity"])
        entry = _finite(trade["entry_price"])
        if quantity <= 0:
            raise ValueError("INVALID_TRADE_QUANTITY")
        closed.append({**trade, "net_pnl": net, "quantity": quantity, "entry_price": entry})
    return sorted(
        closed,
        key=lambda row: (
            int(row.get("outcome_confirmed_at", row.get("exit_at", 0))),
            int(row.get("entry_at", 0)),
            str(row.get("market_id", "")),
        ),
    )


def streaks(trades: list[dict]) -> dict:
    best_win = best_loss = current_win = current_loss = 0
    for trade in _ordered_closed(trades):
        if trade["net_pnl"] > 0:
            current_win += 1
            current_loss = 0
            best_win = max(best_win, current_win)
        elif trade["net_pnl"] < 0:
            current_loss += 1
            current_win = 0
            best_loss = max(best_loss, current_loss)
        else:
            current_win = current_loss = 0
    return {"longest_winning_streak": best_win, "longest_losing_streak": best_loss}


def recovery_cycles(trades: list[dict]) -> dict:
    """Measure wins needed after a cycle first becomes negative.

    This follows the persisted recovery-sized P&L. A cycle starts on the first
    loss while flat and closes only when cumulative cycle P&L returns to zero or
    above. Intervening losses remain part of the same cycle.
    """

    active = False
    pnl = 0.0
    wins = losses = trade_count = consecutive_wins = 0
    completed: list[dict] = []
    for trade in _ordered_closed(trades):
        net = trade["net_pnl"]
        if not active:
            if net >= 0:
                continue
            active = True
            pnl = 0.0
            wins = losses = trade_count = consecutive_wins = 0
        pnl += net
        trade_count += 1
        wins += net > 0
        losses += net < 0
        consecutive_wins = consecutive_wins + 1 if net > 0 else 0
        if pnl >= -1e-10:
            completed.append(
                {
                    "wins_to_recover": wins,
                    "losses_in_cycle": losses,
                    "trades_in_cycle": trade_count,
                    "ending_pnl": round(pnl, 6),
                    "final_consecutive_wins": consecutive_wins,
                }
            )
            active = False
            pnl = 0.0
    win_counts = [row["wins_to_recover"] for row in completed]
    modes = statistics.multimode(win_counts) if win_counts else []
    return {
        "completed_cycles": len(completed),
        "open_unrecovered_cycle": active,
        "open_cycle_pnl": round(pnl, 6) if active else 0.0,
        "wins_to_recover_mean": round(statistics.mean(win_counts), 4) if win_counts else None,
        "wins_to_recover_median": statistics.median(win_counts) if win_counts else None,
        "wins_to_recover_mode": modes,
        "wins_to_recover_min": min(win_counts) if win_counts else None,
        "wins_to_recover_max": max(win_counts) if win_counts else None,
        "distribution": dict(sorted(Counter(win_counts).items())),
        "final_consecutive_wins_distribution": dict(sorted(Counter(
            row["final_consecutive_wins"] for row in completed
        ).items())),
    }


def bankroll(trades: list[dict]) -> dict:
    """Cash required to replay this sequence from its recorded starting point.

    Debit ask cost and recorded entry fees at entry; credit settlement proceeds
    only at the recorded outcome confirmation. Ties debit before credit, matching
    the simulation's strictly-before settlement recognition. This is a sample
    funding requirement, not a guarantee for a future losing sequence.
    """
    events = []
    maximum_entry = Decimal(0)
    maximum_quantity = Decimal(0)
    for index, trade in enumerate(_ordered_closed(trades)):
        price = Decimal(str(trade["entry_price"]))
        quantity = Decimal(str(trade["quantity"]))
        fees = Decimal(str(trade.get("fees", 0)))
        payout_price = Decimal(str(trade["exit_price"]))
        entered = int(trade["entry_at"])
        confirmed = int(trade["outcome_confirmed_at"])
        if not fees.is_finite() or fees < 0 or payout_price not in (0, 1) or confirmed < entered:
            raise ValueError("INVALID_SETTLEMENT_CASH_FLOW")
        debit = price * quantity + fees
        payout = quantity * payout_price
        if abs(payout - debit - Decimal(str(trade["net_pnl"]))) > Decimal("0.000001"):
            raise ValueError("TRADE_CASH_FLOW_MISMATCH")
        events.extend(((entered, 0, index, -debit), (confirmed, 1, index, payout)))
        maximum_entry = max(maximum_entry, debit)
        maximum_quantity = max(maximum_quantity, quantity)
    cash = low = peak = drawdown = Decimal(0)
    for _, _, _, change in sorted(events):
        cash += change
        low = min(low, cash)
        peak = max(peak, cash)
        drawdown = max(drawdown, peak - cash)

    def cents(value):
        # Epsilon removes binary-float serialization noise at exact-cent bounds.
        return float(value.quantize(Decimal("0.000000001")).quantize(
            Decimal("0.01"), rounding=ROUND_CEILING
        ))

    return {
        "historical_minimum_initial_cash": cents(-low),
        "maximum_single_entry_cash_with_fees": cents(maximum_entry),
        "maximum_cash_drawdown_including_locked_positions": cents(drawdown),
        "maximum_contracts": float(maximum_quantity),
        "ending_cash_change": round(float(cash), 6),
        "assumptions": "USD; profits reinvested; recorded fees; settlement credited at recorded confirmation; no extra execution buffer",
        "scope": "Closed trades in this origin only; not a guaranteed minimum for future trading",
    }


def replay_recovery_scenario(trades: list[dict], fee_policy: dict, *,
                             starting_contracts: int, max_increases: int | None) -> dict:
    """Resize an immutable entry sequence using only confirmed prior outcomes."""
    if fee_policy.get("fee_type") != "quadratic" or fee_policy.get("multiplier") != 1:
        raise ValueError("UNVERIFIED_FEE_POLICY")
    if (type(starting_contracts) is not int or not 1 <= starting_contracts <= 100 or
            (max_increases is not None and (type(max_increases) is not int or not 0 <= max_increases <= 6))):
        raise ValueError("INVALID_RECOVERY_SENSITIVITY")
    source = sorted(_ordered_closed(trades), key=lambda row: (int(row["entry_at"]), str(row["market_id"])))
    if len(source) != len(trades) or len({row["market_id"] for row in source}) != len(source):
        raise ValueError("COMPLETE_UNIQUE_TRADE_LEDGER_REQUIRED")
    pending: list[tuple[int, str, float]] = []
    size, increases, cycle = starting_contracts, 0, 0.0
    resized = []
    for row in source:
        entry_at = int(row["entry_at"])
        while pending and pending[0][0] < entry_at:
            _, _, net = heapq.heappop(pending)
            cycle += net
            if cycle >= -1e-10:
                cycle, size, increases = 0.0, starting_contracts, 0
            elif net < 0 and (max_increases is None or increases < max_increases):
                size = min(100, math.floor(size * 2.5))
                increases += 1
        price = _finite(row["entry_price"])
        payout = _finite(row["exit_price"])
        if not 0 < price < 1 or payout not in (0, 1) or int(row["outcome_confirmed_at"]) < entry_at:
            raise ValueError("INVALID_HISTORICAL_TRADE")
        fee = taker_fee(size, price, "0.0001", fee_policy["multiplier"])
        net = size * (payout - price) - fee
        if starting_contracts == 1 and max_increases is None and (size != row["quantity"] or
                not math.isclose(fee, row["fees"], abs_tol=1e-7) or
                not math.isclose(net, row["net_pnl"], abs_tol=1e-7)):
            raise ValueError("BASELINE_REPLAY_MISMATCH")
        revised = {**row, "quantity": size, "fees": fee,
                   "gross_pnl": size * (payout - price), "net_pnl": net}
        resized.append(revised)
        heapq.heappush(pending, (int(row["outcome_confirmed_at"]), str(row["market_id"]), net))
    summary = trade_statistics(resized)
    maximum_allowed = starting_contracts
    for _ in range(max_increases or 0):
        maximum_allowed = min(100, math.floor(maximum_allowed * 2.5))
    funding = bankroll(resized)
    return {
        "starting_contracts": starting_contracts,
        "maximum_loss_escalations_per_recovery_cycle": max_increases,
        "maximum_contracts_allowed_by_escalations": 100 if max_increases is None else maximum_allowed,
        "closed_trades": summary["closed_trades"],
        "wins": summary["wins"], "losses": summary["losses"],
        "net_pnl": round(summary["net_pnl"], 6),
        "return_on_entry_notional": summary["net_return_on_closed_entry_notional"],
        "realized_equity_max_drawdown": round(summary["realized_equity_max_drawdown"], 6),
        "maximum_contracts_used": summary["max_quantity_used"],
        "historical_minimum_initial_cash": funding["historical_minimum_initial_cash"],
        "maximum_cash_drawdown_including_locked_positions": funding["maximum_cash_drawdown_including_locked_positions"],
    }


def capped_recovery_scenarios(trades: list[dict], fee_policy: dict) -> list[dict]:
    """Prior published 1-contract sensitivity; keep the baseline verified."""
    return [replay_recovery_scenario(trades, fee_policy, starting_contracts=1, max_increases=limit)
            for limit in (1, 2, 3, None)]


def price_buckets(trades: list[dict]) -> list[dict]:
    buckets: dict[int, dict] = {}
    for trade in _ordered_closed(trades):
        cent = _cent_bucket(trade["entry_price"])
        row = buckets.setdefault(
            cent,
            {
                "entry_bucket_cents": cent,
                "label": f"{cent}c-<{cent + 1}c",
                "wins": 0,
                "losses": 0,
                "breakeven": 0,
                "net_pnl": 0.0,
                "entry_notional": 0.0,
                "entry_price_total": 0.0,
                "trades": 0,
            },
        )
        net = trade["net_pnl"]
        row["wins" if net > 0 else "losses" if net < 0 else "breakeven"] += 1
        row["net_pnl"] += net
        row["entry_notional"] += trade["entry_price"] * trade["quantity"]
        row["entry_price_total"] += trade["entry_price"]
        row["trades"] += 1
    output = []
    for cent in sorted(buckets):
        row = buckets[cent]
        decisive = row["wins"] + row["losses"]
        output.append(
            {
                **{key: value for key, value in row.items() if key != "entry_price_total"},
                "win_rate": row["wins"] / decisive if decisive else None,
                "average_entry_price": row["entry_price_total"] / row["trades"],
                "net_pnl": round(row["net_pnl"], 6),
                "entry_notional": round(row["entry_notional"], 6),
            }
        )
    return output


def summarize_origin(origin: int, scenario: dict) -> dict:
    trades = scenario.get("trades")
    if not isinstance(trades, list):
        raise ValueError("RECOVERY_TRADES_REQUIRED")
    ordered = _ordered_closed(trades)
    wins = sum(row["net_pnl"] > 0 for row in ordered)
    losses = sum(row["net_pnl"] < 0 for row in ordered)
    return {
        "observed_minutes": origin,
        "forecast_minutes": 15 - origin,
        "closed_trades": len(ordered),
        "wins": wins,
        "losses": losses,
        "breakeven": len(ordered) - wins - losses,
        "win_rate": wins / (wins + losses) if wins + losses else None,
        "net_pnl": round(sum(row["net_pnl"] for row in ordered), 6),
        **streaks(ordered),
        "recovery": recovery_cycles(ordered),
        "bankroll": bankroll(ordered),
        "price_buckets": price_buckets(ordered),
    }


def aggregate(origin_scenarios: dict[int, dict], campaign_id: str, report_key: str,
              fee_policy: dict | None = None) -> dict:
    origins = [summarize_origin(origin, origin_scenarios[origin]) for origin in sorted(origin_scenarios)]
    combined = [trade for origin in sorted(origin_scenarios) for trade in origin_scenarios[origin]["trades"]]
    result = {
        "schema_version": 2,
        "paper_only": True,
        "campaign_id": campaign_id,
        "report_key": report_key,
        "strategy": "first_p90_plus_sticky_direction_hold_to_settlement",
        "sizing": "1 contract; floor(2.5x) after losses until cycle P&L recovers; 100-contract cap",
        "origins": origins,
        "combined_price_buckets": price_buckets(combined),
        "combined_bucket_warning": "Origins reuse markets and are not independent trades; do not sum their P&L as one portfolio.",
    }
    if fee_policy is not None and 1 in origin_scenarios:
        result["origin_1_recovery_escalation_sensitivity"] = capped_recovery_scenarios(
            origin_scenarios[1]["trades"], fee_policy)
        result["origin_1_start_size_sensitivity"] = [
            replay_recovery_scenario(origin_scenarios[1]["trades"], fee_policy,
                                     starting_contracts=start, max_increases=limit)
            for start in (1, 5, 10, 20, 30) for limit in (3, 4)]
    return result


def _record(campaign: Campaign, key: str) -> dict:
    if not (REPORT_KEY.fullmatch(key) or ORIGIN_KEY.fullmatch(key)):
        raise ValueError("INVALID_REPORT_KEY")
    snapshot = campaign.lease.ref.collection("btc_horizon_report").document(key).get()
    if not snapshot.exists:
        raise ValueError("REPORT_NOT_FOUND")
    metadata = snapshot.to_dict()
    with tempfile.TemporaryDirectory(prefix="btc-safe-report-") as folder:
        encrypted = Path(folder) / "record.enc"
        campaign.download(metadata["archive"], encrypted)
        value = decode_catalog(encrypted)
    if digest(value) != metadata["content_sha256"]:
        raise ValueError("REPORT_HASH_MISMATCH")
    return value


def load(campaign_id: str, report_key: str) -> dict:
    campaign = Campaign(validate_campaign(campaign_id), "aggregate-report-reader")
    compact = _record(campaign, report_key)
    if compact.get("version") != "interval_archived_minutes_p90_sticky_v2":
        raise ValueError("UNSUPPORTED_REPORT_VERSION")
    if compact.get("configuration", {}).get("series") != "KXBTC15M":
        raise ValueError("BTC_REPORT_REQUIRED")
    scenarios = {}
    for origin, strategies in compact.get("origins", {}).items():
        pointer = strategies["p90_sticky"]["recover_cycle"]["report_key"]
        full = _record(campaign, pointer)
        scenarios[int(origin)] = full["p90_sticky"]["recover_cycle"]
    if set(scenarios) != set(range(1, 13)):
        raise ValueError("ALL_MINUTE_ORIGINS_REQUIRED")
    result = aggregate(scenarios, campaign_id, report_key, compact.get("fee_policy"))
    result["coverage"] = compact.get("coverage", {})
    result["as_of"] = compact.get("as_of")
    result["complete"] = compact.get("complete")
    result["fee_policy"] = compact.get("fee_policy")
    result["limitations"] = compact.get("limitations", [])
    return result


def markdown(report: dict) -> str:
    rows = [
        "# BTC 15-minute P90 + sticky-direction report",
        "",
        "| Observed -> forecast | W/L | Win rate | Net P&L | Longest W/L streak | Median wins to recover |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for item in report["origins"]:
        recovery = item["recovery"]
        median = recovery["wins_to_recover_median"]
        rows.append(
            f"| {item['observed_minutes']} -> {item['forecast_minutes']} min | "
            f"{item['wins']}/{item['losses']} | {item['win_rate']:.2%} | "
            f"${item['net_pnl']:.2f} | {item['longest_winning_streak']}/{item['longest_losing_streak']} | "
            f"{median if median is not None else 'n/a'} |"
        )
    rows.extend(["", "Exact 1-cent winner/loser buckets are included in the sanitized JSON artifact."])
    if scenarios := report.get("origin_1_recovery_escalation_sensitivity"):
        rows.extend(["", "## First minute to 14-minute forecast: recovery escalation limits", "",
                     "| Maximum increases | Max contracts used | Net P&L | Return on entries | Realized drawdown | Historical minimum cash |",
                     "|---:|---:|---:|---:|---:|---:|"])
        for item in scenarios:
            label = "unlimited (100-contract cap)" if item["maximum_loss_escalations_per_recovery_cycle"] is None else str(item["maximum_loss_escalations_per_recovery_cycle"])
            rows.append(f"| {label} | {item['maximum_contracts_used']} | ${item['net_pnl']:.2f} | "
                        f"{item['return_on_entry_notional']:.2%} | ${item['realized_equity_max_drawdown']:.2f} | "
                        f"${item['historical_minimum_initial_cash']:.2f} |")
    return "\n".join(rows) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--report-key", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = load(args.campaign_id, args.report_key)
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if args.output.exists():
        raise ValueError("OUTPUT_ALREADY_EXISTS")
    args.output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    os.chmod(args.output, 0o600)
    summary = markdown(result)
    if path := os.getenv("GITHUB_STEP_SUMMARY"):
        with open(path, "a", encoding="utf-8") as target:
            target.write(summary)
    print(summary, flush=True)


if __name__ == "__main__":
    main()
