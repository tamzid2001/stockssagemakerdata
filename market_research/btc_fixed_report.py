"""Read-only aggregate report from the exact completed September BTC cohort."""
import argparse
import csv
from decimal import Decimal, ROUND_FLOOR
import json
import math
from pathlib import Path
import re
import tempfile
from .btc_bucket_report import _record
from .btc_bucket_report import bankroll
from .btc_sticky_tracking import taker_fee
from .engine import digest
from .recovery_cloud import Campaign, decode_catalog
from .recovery_switch import statistics

CAMPAIGN = "p90-bb99f9b990315428acfc2bb3"
REPORT = "report-74c449f9d0c09a63da121bf36b528549"

def game_record(campaign, ticker):
    if not re.fullmatch(r"KXBTC15M-[A-Z0-9-]{8,100}", ticker):
        raise ValueError("INVALID_ORIGINAL_MARKET_ID")
    snapshot = campaign.lease.ref.collection("btc_horizon_report").document("game-"+ticker).get()
    if not snapshot.exists:
        raise ValueError("ORIGINAL_GAME_NOT_FOUND")
    metadata = snapshot.to_dict()
    with tempfile.TemporaryDirectory() as folder:
        path = Path(folder)/"game.enc"
        campaign.download(metadata["archive"], path)
        value = decode_catalog(path)
    if digest(value) != metadata["content_sha256"] or value["market_id"] != ticker:
        raise ValueError("ORIGINAL_GAME_HASH_MISMATCH")
    return value

def budget_quantity(price, precision, multiplier, include_fees):
    """Floor to the documented 0.01-contract grid; never exceed $1 budget."""
    p = Decimal(str(price))
    if not p.is_finite() or not 0 < p < 1:
        raise ValueError("INVALID_OPPOSITE_PRICE")
    units = int((Decimal(1)/p*100).to_integral_value(rounding=ROUND_FLOOR))
    if include_fees:
        low, high = 0, units
        while low < high:
            mid = (low+high+1)//2
            q = Decimal(mid)/100
            debit = q*p + Decimal(str(taker_fee(float(q),float(p),precision,multiplier)))
            if debit <= 1:
                low = mid
            else:
                high = mid-1
        units = low
    if units <= 0:
        raise ValueError("BUDGET_BELOW_MINIMUM_QUANTITY")
    return float(Decimal(units)/100)

def opposite_replay(original, games, *, price_policy, include_fees, precision, multiplier):
    if price_policy not in ("recorded_opposite_ask", "ideal_one_minus_original_ask"):
        raise ValueError("INVALID_OPPOSITE_PRICE_POLICY")
    trades, misses = [], []
    for source in original["trades"]:
        if source["status"] != "closed" or source["quantity"] != 1 or len(source["fills"]) != 1:
            raise ValueError("CLOSED_FIXED_ONE_BASELINE_REQUIRED")
        ticker = source["market_id"]
        side = source["contract_id"].rsplit(":",1)[1]
        opposite = "no" if side == "yes" else "yes"
        at = source["fills"][0]["timestamp"]
        tape = {(r["contract_id"],r["timestamp"]):r for r in games[ticker]["tape"]}
        same, other = tape.get((ticker+":"+side,at)), tape.get((ticker+":"+opposite,at))
        if not same or not math.isclose(same["ask"],source["entry_price"],abs_tol=1e-8) or same["received_at"] != source["entry_at"]:
            raise ValueError("ORIGINAL_ENTRY_TAPE_MISMATCH")
        if not other or not (at <= other["received_at"] <= at+30 and source["signal_received_at"] < other["received_at"] < source["market_end"]):
            misses.append({"reason":"MISSING_TIMELY_OPPOSITE_ASK"})
            continue
        if not math.isclose(other["ask"],1-same["bid"],abs_tol=1e-8):
            raise ValueError("BINARY_OPPOSITE_ASK_MISMATCH")
        price = other["ask"] if price_policy == "recorded_opposite_ask" else float(Decimal(1)-Decimal(str(same["ask"])))
        if not 0 < price < 1:
            misses.append({"reason":"UNTRADEABLE_OPPOSITE_PRICE"})
            continue
        q = budget_quantity(price,precision,multiplier,include_fees)
        fee = taker_fee(q,price,precision,multiplier)
        payout = 1-source["exit_price"]
        gross = q*(payout-price)
        trades.append({**source,"contract_id":ticker+":"+opposite,"source_contract_id":source["contract_id"],
            "trade_id":digest(["opposite_fixed_dollar_v1",source["trade_id"],price_policy,include_fees,precision]),
            "entry_at":other["received_at"],"entry_price":price,"quantity":q,"exit_price":payout,
            "gross_pnl":gross,"net_pnl":gross-fee,"fees":fee,"execution_verified":False,
            "fills":[{"timestamp":at,"received_at":other["received_at"],"price":price,"quantity":q,"fee":fee}]})
    summary = statistics(trades)
    count = len(trades)
    fees = summary["fees"]
    cost = summary["closed_entry_notional"]
    return {"price_policy":price_policy,"budget_includes_fees":include_fees,"balance_precision":precision,
        "quantity_grid":0.01,"entry_budget_usd":1,"original_signals":len(original["trades"]),"missed_entries":len(misses),
        "trades":count,"wins":summary["wins"],"losses":summary["losses"],"win_rate_pct":100*summary["win_rate"] if count else None,
        "average_entry_cents":100*sum(t["entry_price"] for t in trades)/count if count else None,
        "average_contracts":sum(t["quantity"] for t in trades)/count if count else None,
        "maximum_contracts":max((t["quantity"] for t in trades),default=None),
        "gross_pnl_usd":summary["gross_pnl"],"net_pnl_usd":summary["net_pnl"],"fees_usd":fees,
        "entry_notional_usd":cost,"total_cash_debited_usd":cost+fees,
        "net_return_on_entry_notional_pct":100*summary["net_pnl"]/cost if cost else None,
        "net_return_on_cash_debited_pct":100*summary["net_pnl"]/(cost+fees) if cost+fees else None,
        "realized_max_drawdown_usd":summary["realized_equity_max_drawdown"],
        "longest_win_streak":summary["longest_winning_streak"],"longest_loss_streak":summary["longest_losing_streak"],
        **{k:v for k,v in bankroll(trades).items() if k != "assumptions" and k != "scope"}}

def summarize(scenario):
    trades = [t for t in scenario["trades"] if t["status"] == "closed"]
    if any(t["quantity"] != 1 for t in trades):
        raise ValueError("ONE_CONTRACT_REQUIRED")
    summary = scenario["summary"]
    count = len(trades)
    cost = sum(t["entry_price"] for t in trades)
    fees = sum(t["fees"] for t in trades)
    net = sum(t["net_pnl"] for t in trades)
    if count != summary["closed_trades"] or abs(cost-summary["closed_entry_notional"]) > 1e-8 or abs(net-summary["net_pnl"]) > 1e-8:
        raise ValueError("ORIGINAL_LEDGER_TOTAL_MISMATCH")
    prices = [t["entry_price"] for t in trades]
    return {"trades": count, "wins": summary["wins"], "losses": summary["losses"],
            "win_rate_pct": 100*summary["win_rate"] if count else None,
            "average_entry_cents": 100*cost/count if count else None,
            "average_entry_with_fee_cents": 100*(cost+fees)/count if count else None,
            "minimum_entry_cents": 100*min(prices) if count else None,
            "maximum_entry_cents": 100*max(prices) if count else None,
            "net_pnl_usd": net, "gross_pnl_usd": summary["gross_pnl"], "fees_usd": fees,
            "net_return_on_entry_notional_pct": 100*net/cost if cost else None,
            "realized_max_drawdown_usd": summary["realized_equity_max_drawdown"],
            "longest_win_streak": summary["longest_winning_streak"],
            "longest_loss_streak": summary["longest_losing_streak"],
            "first_entry_at": min((t["entry_at"] for t in trades), default=None),
            "last_entry_at": max((t["entry_at"] for t in trades), default=None),
            "open_positions": summary["open_positions"]}

def load():
    campaign = Campaign(CAMPAIGN, "fixed-one-read-only-report")
    compact = _record(campaign, REPORT)
    if compact["coverage"]["analyzed_markets"] != 291 or compact["coverage"]["origin_status_counts"]["evaluated"] != 3492 or not compact["complete"]:
        raise ValueError("EXACT_COMPLETED_COHORT_REQUIRED")
    fee = compact["fee_policy"]
    if fee.get("fee_type") not in ("quadratic","quadratic_with_maker_fees") or type(fee.get("multiplier")) not in (int,float) or not math.isfinite(fee["multiplier"]) or fee["multiplier"] < 0:
        raise ValueError("ORIGINAL_FEE_POLICY_REQUIRED")
    rows, opposite_rows, games = [], [], {}
    for origin in range(1, 13):
        pointer = compact["origins"][str(origin)]["p90_sticky"]["fixed_one"]["report_key"]
        full = _record(campaign, pointer)
        for strategy in ("first_p90", "p90_sticky"):
            rows.append({"observed_minutes": origin, "forecast_minutes": 15-origin,
                         "strategy": strategy, **summarize(full[strategy]["fixed_one"])})
        baseline = full["p90_sticky"]["fixed_one"]
        for t in baseline["trades"]:
            if t["market_id"] not in games:
                games[t["market_id"]] = game_record(campaign,t["market_id"])
        for precision in ("0.0001","0.01"):
            for price_policy, include_fees in (("recorded_opposite_ask",False),("recorded_opposite_ask",True),("ideal_one_minus_original_ask",False)):
                opposite_rows.append({"observed_minutes":origin,"forecast_minutes":15-origin,
                    **opposite_replay(baseline,games,price_policy=price_policy,include_fees=include_fees,precision=precision,multiplier=fee["multiplier"])})
    return {"campaign_id": CAMPAIGN, "report_key": REPORT, "source_run": 35487987854,
            "as_of": compact["as_of"], "coverage": compact["coverage"],
            "fee_policy": fee, "rows": rows,"opposite_fixed_dollar_rows":opposite_rows,
            "limitations": compact["limitations"]+[
                "Opposite tests retain the original sticky + P90 signals and next-minute entry times; only side and sizing change.",
                "$1 per entry before fees unless budget_includes_fees is true; floor to 0.01 contracts, no recovery multiplier.",
                "Ideal one minus original ask is the opposite bid, not a verified ask fill. Recorded opposite ask is the spread-aware alternative.",
                "Fractional sizes assume market support; original market-specific fractional eligibility and orderbook depth are not verified.",
                "Best timing is selected in sample across 12 overlapping timings, not an independently validated future strategy."]}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    report = load()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    (output/"btc-fixed-one.json").write_text(json.dumps(report, indent=2))
    with (output/"btc-fixed-one.csv").open("w") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(report["rows"][0]))
        writer.writeheader(); writer.writerows(report["rows"])
    with (output/"btc-opposite-fixed-dollar.csv").open("w") as stream:
        writer = csv.DictWriter(stream,fieldnames=list(report["opposite_fixed_dollar_rows"][0]))
        writer.writeheader();writer.writerows(report["opposite_fixed_dollar_rows"])
    print(json.dumps(report))
