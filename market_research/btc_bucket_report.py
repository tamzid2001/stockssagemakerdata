"""Aggregate an encrypted BTC interval replay without exposing trade rows.

The command is intended for an authenticated GitHub Actions runner. It reads
the immutable, encrypted replay records and emits only price buckets and
portfolio statistics. Raw quotes, forecasts, and per-market trades never leave
the encrypted research store.
"""

from __future__ import annotations

import argparse
from collections import Counter
from decimal import Decimal, ROUND_FLOOR
import json
import math
import os
from pathlib import Path
import re
import statistics
import tempfile

from .engine import digest
from .recovery_cloud import Campaign, decode_catalog, validate_campaign


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
    wins = losses = trade_count = 0
    completed: list[dict] = []
    for trade in _ordered_closed(trades):
        net = trade["net_pnl"]
        if not active:
            if net >= 0:
                continue
            active = True
            pnl = 0.0
            wins = losses = trade_count = 0
        pnl += net
        trade_count += 1
        wins += net > 0
        losses += net < 0
        if pnl >= -1e-10:
            completed.append(
                {
                    "wins_to_recover": wins,
                    "losses_in_cycle": losses,
                    "trades_in_cycle": trade_count,
                    "ending_pnl": round(pnl, 6),
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
    }


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
        "price_buckets": price_buckets(ordered),
    }


def aggregate(origin_scenarios: dict[int, dict], campaign_id: str, report_key: str) -> dict:
    origins = [summarize_origin(origin, origin_scenarios[origin]) for origin in sorted(origin_scenarios)]
    combined = [trade for origin in sorted(origin_scenarios) for trade in origin_scenarios[origin]["trades"]]
    return {
        "schema_version": 1,
        "paper_only": True,
        "campaign_id": campaign_id,
        "report_key": report_key,
        "strategy": "first_p90_plus_sticky_direction_hold_to_settlement",
        "sizing": "1 contract; floor(2.5x) after losses until cycle P&L recovers; 100-contract cap",
        "origins": origins,
        "combined_price_buckets": price_buckets(combined),
        "combined_bucket_warning": "Origins reuse markets and are not independent trades; do not sum their P&L as one portfolio.",
    }


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
    return aggregate(scenarios, campaign_id, report_key)


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
