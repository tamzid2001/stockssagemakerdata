"""Read-only signal/quote/fill attribution. Never claims a lease or sends orders."""
import argparse
import json
import os
from pathlib import Path
import time

from .kalshi_execution import money
from .opposite_dollar_trader import CloudJournal, PREFIX, STORAGE_TIMEOUT, storage_retry
from .opposite_strategies import DollarConfig, STRATEGIES

def entry_rows(value):
    groups=value.get("entries",{})
    if value.get("ticker"):groups={value["ticker"]:groups}
    tape={(r["value"].get("market_id"),r["value"].get("timestamp")):r["value"]
          for r in value.get("rows",[]) if r.get("kind")=="btc_minutes"}
    result=[]
    for ticker,group in groups.items():
        for mode,entry in group.items():
            quantity=money(entry.get("filled","0"))
            if not quantity:continue
            signal=entry.get("signal",{});source=entry.get("source_side")
            sticky=signal.get("sticky",{}).get("side")
            bid=signal.get("signal_bid");p90=signal.get("signal_p90")
            opposite={"yes":"no","no":"yes"}.get(source)
            quote=tape.get((ticker,signal.get("signal_at",0)+60),{})
            initial=quote.get(str(entry.get("side"))+"_ask")
            average=money(entry["cost"])/quantity*100
            complement=(1-money(bid))*100 if bid is not None else None
            limits=[money(fill["intent"]["price"])*100 for fill in entry.get("fills",[])
                    if fill.get("intent",{}).get("price") is not None]
            correct=source==sticky and entry.get("side")==opposite and opposite is not None
            reason="insufficient_signal_evidence"
            if not correct:reason="side_agreement_not_verified"
            elif bid is not None and money(bid)<=money(".5"):reason="signal_side_bid_at_or_below_50c"
            elif initial is not None and money(initial)>=money(".5"):reason="opposite_ask_at_or_above_50c_next_minute"
            elif average>=50:reason="fill_average_above_50c_after_next_minute_quote"
            else:reason="opposite_fill_average_below_50c"
            result.append({"ticker":ticker,"mode":mode,"status":entry.get("status"),
                "created_at":entry.get("created_at"),"signal_at":signal.get("signal_at"),
                "source_side":source,"sticky_side":sticky,"trade_side":entry.get("side"),
                "opposite_side_verified":correct,
                "signal_bid_cents":float(money(bid)*100) if bid is not None else None,
                "signal_p90_cents":float(money(p90)*100) if p90 is not None else None,
                "signal_crossed_p90":money(bid)>=money(p90) if bid is not None and p90 is not None else None,
                "signal_complement_cents":float(complement) if complement is not None else None,
                "next_minute_opposite_ask_cents":float(money(initial)*100) if initial is not None else None,
                "average_fill_cents":round(float(average),6),"contracts":float(quantity),
                "attempts":entry.get("attempt",0),"recorded_order_receipts":len(entry.get("fills",[])),
                "first_yes_book_limit_cents":float(limits[0]) if limits else None,
                "first_opposite_side_limit_cents":float(100-limits[0] if entry.get("side")=="no" else limits[0]) if limits else None,
                "difference_from_signal_complement_cents":round(float(average-complement),6) if complement is not None else None,
                "classification":reason})
    return result

def audit(series,subaccount,archive_limit):
    reports=[]
    for selected in STRATEGIES if series=="all" else (series,):
        journal=CloudJournal(DollarConfig(selected,subaccount),os.environ["KALSHI_API_KEY_ID"],"read-only-entry-audit")
        value,generation=journal.read(journal.name)
        rows=entry_rows(value);seen={(r["ticker"],r["mode"]) for r in rows}
        prefix=PREFIX+journal.account+"/markets/"+selected+"-"
        archives=list(journal.bucket.list_blobs(prefix=prefix,timeout=STORAGE_TIMEOUT,retry=storage_retry()))
        archives.sort(key=lambda blob:blob.updated.timestamp() if blob.updated else 0,reverse=True)
        for blob in archives[:archive_limit]:
            archived,_=journal.read(blob.name)
            for row in entry_rows(archived):
                identity=(row["ticker"],row["mode"])
                if identity not in seen:rows.append(row);seen.add(identity)
        reports.append({"series":selected,"journal_generation":generation,
            "worker_code_sha":value.get("health",{}).get("code_sha"),
            "worker_health_at":value.get("health",{}).get("at"),
            "archive_objects_read":min(len(archives),archive_limit),
            "entries":sorted(rows,key=lambda row:(row.get("created_at") or 0,row["mode"]),reverse=True)})
    return {"read_only":True,"orders_sent":0,"storage_writes":0,"at":int(time.time()),"reports":reports}

def main():
    parser=argparse.ArgumentParser();parser.add_argument("--series",choices=["all",*STRATEGIES],default="all")
    parser.add_argument("--subaccount",type=int,default=0);parser.add_argument("--archive-limit",type=int,default=12)
    parser.add_argument("--output",type=Path)
    args=parser.parse_args()
    if not 0<=args.archive_limit<=32:raise ValueError("INVALID_ARCHIVE_LIMIT")
    value=audit(args.series,args.subaccount,args.archive_limit)
    text=json.dumps(value,indent=2)
    if args.output:args.output.write_text(text+"\n")
    print(text)

if __name__=="__main__":main()
