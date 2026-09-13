"""Hourly prospective, provider-isolated ensemble catalog; encrypted artifacts."""
import argparse
import json
import os
import time
from .corpus import annotate_forecast, export_corpus, lagged_window, matching_actuals
from .engine import normalize_quotes
from .forecast import default_research_models, forecast_window
from .local_store import LocalStore
from .provider import KalshiProvider, QuanturaProvider
from .worker import paired_contracts


def run(source, maximum):
    provider=KalshiProvider() if source=="kalshi" else QuanturaProvider()
    run_id=os.environ.get("GITHUB_RUN_ID",str(time.time_ns()))
    configuration={"provider":source,"lag_minutes":15,"horizon_minutes":75,"minimum_observations":500,"strategy":None,"models":list(default_research_models())}
    store=LocalStore(source+"-hourly-"+run_id,run_id);store.claim(configuration)
    deadline=time.time()+45*60
    contracts={};cursor="0";seen=set();discovery={}
    while cursor not in seen and time.time()<deadline:
        seen.add(cursor)
        rows,discovery=provider.discover("live",20,cursor)
        contracts.update({c["contractId"]:c for c in rows})
        cursor=discovery.get("next_cursor")
        if not cursor: break
    eligible=paired_contracts(list(contracts.values()))
    counts={"eligible_sides":len(eligible),"processed":0,"successful":0,"failed":0,"partial":bool(cursor) or len(eligible)>maximum}
    forecasts=[];failures=[]
    try:
        for c in eligible[:maximum]:
            if time.time()>deadline or store.at_capacity: counts["partial"]=True;break
            counts["processed"]+=1
            try:
                decision=int(time.time())//60*60
                quotes=normalize_quotes(provider.history(c,decision-7*86400,decision),decision)
                window,cutoff=lagged_window(quotes,decision,15)
                if len(window)<500: raise ValueError("FIVE_HUNDRED_OBSERVATIONS_REQUIRED")
                f=forecast_window(window,75)
                if {m["id"] for m in f["models"] if m["status"]=="completed"}!={"prophet","toto","granite","chronos","timesfm"}: raise ValueError("FIVE_MODEL_ENSEMBLE_REQUIRED")
                annotate_forecast(f,c,decision,cutoff,15)
                f["available_at"]=int(time.time());f["actuals"]=matching_actuals(f,quotes)
                f["expired_before_publication"]=f["rows"][-1]["timestamp"]<=f["available_at"]
                store.save(c["contractId"],{},f,[],c)
                forecasts.append(f);counts["successful"]+=1
            except (ValueError,RuntimeError) as error:
                counts["failed"]+=1
                failures.append({"contract_id":c["contractId"],"code":str(error) if str(error).isupper() else "HISTORY_OR_MODEL_UNAVAILABLE"})
                print(json.dumps({"event":"screener_side_skipped",**failures[-1]}),flush=True)
    finally:
        counts["coverage_pct"]=100*counts["successful"]/len(eligible) if eligible else None
        export_corpus(store,forecasts,configuration)
        store.report(run_id,{"configuration":configuration,"coverage":counts,"failures":failures,"commit":os.environ.get("GITHUB_SHA")})
        store.release()
        print(json.dumps({"event":"hourly_screener","source":source,**counts}),flush=True)
        if path:=os.environ.get("GITHUB_STEP_SUMMARY"):
            with open(path,"a") as output: output.write(f"## {source} hourly ensemble screener\n\nInput cutoff: 15 minutes; forecast: 75 minutes; 500 real observations required.\n\nCoverage: `{json.dumps(counts)}`\n\nNot a trading win rate. GitHub CPU runtime may limit full live-market coverage.\n")
    if not forecasts: raise RuntimeError("NO_QUALIFYING_FORECASTS")


if __name__=="__main__":
    parser=argparse.ArgumentParser();parser.add_argument("--provider",choices=["kalshi","polymarket_us"],required=True);parser.add_argument("--max-contracts",type=int,default=500)
    args=parser.parse_args()
    if not 2<=args.max_contracts<=500: parser.error("Bounded maximum 2–500 required")
    run(args.provider,args.max_contracts)
