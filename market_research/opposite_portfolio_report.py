"""Verified shared-cash replay of the five selected original cohorts. Read only."""
import argparse
from collections import defaultdict
from decimal import Decimal
import json
from pathlib import Path

from .btc_bucket_report import _record, bankroll
from .btc_fixed_report import game_record, opposite_replay, validate_cohort
from .interval_fixed_dollar_report import original_cohorts
from .opposite_strategies import STRATEGIES, VERSION
from .recovery_cloud import Campaign

D = lambda v: Decimal(str(v))

def portfolio(trades, games):
    """Shared cash and bid equity, at recorded entry/quote/confirmation times."""
    if len({t["trade_id"] for t in trades}) != len(trades):
        raise ValueError("DUPLICATE_PORTFOLIO_TRADE")
    events=defaultdict(list)
    for t in trades:
        if t["status"] != "closed":raise ValueError("CLOSED_PORTFOLIO_REQUIRED")
        events[int(t["entry_at"])].append((0,"entry",t))
        events[int(t["outcome_confirmed_at"])].append((2,"settle",t))
        for q in games[t["market_id"]]["tape"]:
            if (q["contract_id"] == t["contract_id"] and
                    t["entry_at"] <= q["received_at"] < t["outcome_confirmed_at"]):
                events[int(q["received_at"])].append((1,"quote",{**q,"trade_id":t["trade_id"]}))
    cash=realized=peak_r=peak_e=dd_r=dd_e=Decimal(0)
    held={};curve=[];maximum_open=0
    for at in sorted(events):
        # Cash funding uses debit-before-credit ties, as in the original replay.
        # Equity/realized metrics sample the complete simultaneous event group.
        for _,kind,row in sorted(events[at],key=lambda e:(e[0],str(e[2].get("trade_id","")),e[2].get("timestamp",0))):
            key=row["trade_id"]
            if kind=="entry":
                cost=D(row["quantity"])*D(row["entry_price"])+D(row["fees"])
                cash-=cost
                quotes=[q for q in games[row["market_id"]]["tape"] if q["contract_id"]==row["contract_id"]
                        and q["received_at"]<=at and q["timestamp"]<=at]
                if not quotes:raise ValueError("ENTRY_BID_UNAVAILABLE")
                bid=D(max(quotes,key=lambda q:(q["received_at"],q["timestamp"]))["bid"])
                held[key]={"quantity":D(row["quantity"]),"bid":bid}
            elif kind=="quote" and key in held:
                held[key]["bid"]=D(row["bid"])
            elif kind=="settle":
                cash+=D(row["quantity"])*D(row["exit_price"])
                realized+=D(row["net_pnl"])
                held.pop(key)
        maximum_open=max(maximum_open,len(held))
        equity=cash+sum((r["quantity"]*r["bid"] for r in held.values()),Decimal(0))
        peak_r=max(peak_r,realized);peak_e=max(peak_e,equity)
        dd_r=max(dd_r,peak_r-realized);dd_e=max(dd_e,peak_e-equity)
        curve.append({"at":at,"cash_change":float(cash),"realized_pnl":float(realized),
                      "bid_equity_change":float(equity),"open_positions":len(held)})
    if held or abs(cash-realized)>D("0.000001"):raise ValueError("PORTFOLIO_CASH_RECONCILIATION_FAILED")
    return {**bankroll(trades),"net_pnl_usd":float(realized),"trades":len(trades),
            "wins":sum(t["net_pnl"]>0 for t in trades),"losses":sum(t["net_pnl"]<0 for t in trades),
            "win_rate_pct":100*sum(t["net_pnl"]>0 for t in trades)/len(trades) if trades else None,
            "realized_max_drawdown_usd":float(dd_r),"observed_bid_equity_max_drawdown_usd":float(dd_e),
            "maximum_simultaneous_positions":maximum_open,
            "first_entry_at":min((t["entry_at"] for t in trades),default=None),
            "last_settlement_at":max((t["outcome_confirmed_at"] for t in trades),default=None),
            "curve":curve}

def load():
    selected=original_cohorts();all_trades=[];games={};assets=[]
    for series,n in STRATEGIES.items():
        source=selected[series];campaign=Campaign(source["campaign_id"],"five-portfolio-read-only")
        compact=_record(campaign,source["report_key"])
        validate_cohort(compact,series,source["analyzed_markets"])
        if compact["as_of"] != source["as_of"]:raise ValueError("COHORT_CUTOFF_MISMATCH")
        key=compact["origins"][str(n)]["p90_sticky"]["fixed_one"]["report_key"]
        original=_record(campaign,key)["p90_sticky"]["fixed_one"]
        for trade in original["trades"]:
            ticker=trade["market_id"]
            if ticker not in games:games[ticker]=game_record(campaign,ticker,series)
        fee=compact["fee_policy"]
        if fee["fee_type"] not in ("quadratic","quadratic_with_maker_fees"):
            raise ValueError("UNSUPPORTED_ORIGINAL_FEE_POLICY")
        result=opposite_replay(original,games,price_policy="recorded_opposite_ask",include_fees=False,
                               precision="0.0001",multiplier=fee["multiplier"],with_ledger=True)
        all_trades.extend(result.pop("ledger"))
        assets.append({"series":series,"observed_minutes":n,"forecast_minutes":15-n,
                       "source_run":source["source_run"],"fee_policy":fee,**result})
        print(json.dumps({"series":series,"trades":result["trades"],"net":result["net_pnl_usd"]}),flush=True)
    result=portfolio(all_trades,games)
    if abs(result["net_pnl_usd"]-sum(r["net_pnl_usd"] for r in assets))>1e-6:
        raise ValueError("ASSET_TOTAL_MISMATCH")
    return {"version":VERSION,"as_of":next(iter(selected.values()))["as_of"],"assets":assets,
            "portfolio":result,"limitations":[
                "The selected timings were chosen in sample; this is not a forecast of future returns.",
                "$1 entry notional before modeled fees, 0.01-contract grid, no recovery multiplier; hold to settlement.",
                "Recorded next-minute opposite asks are simulated fills; depth and fractional eligibility were not execution-verified.",
                "Equity is marked at observed minute bids, carrying the latest bid through gaps; intraminute drawdown is unknown.",
                "Cash requires recorded settlement confirmation; the sample minimum uses profits reinvested and no extra buffer.",
                "Current-market one-second live retries can differ from archived next-minute prices and modeled fees."]}

def main():
    p=argparse.ArgumentParser();p.add_argument("--output",type=Path,required=True);a=p.parse_args()
    result=load();a.output.mkdir(parents=True,exist_ok=True)
    (a.output/"portfolio.json").write_text(json.dumps(result,indent=2,allow_nan=False))
    print(json.dumps({k:v for k,v in result["portfolio"].items() if k!="curve"}),flush=True)

if __name__=="__main__":main()
