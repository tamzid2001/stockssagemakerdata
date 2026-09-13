"""Real-history replay of the minute P1 engine, local encrypted checkpoints only."""
import argparse
from dataclasses import asdict
import json
import math
import os
import time
from .corpus import annotate_forecast
from .engine import digest, history_window, normalize_quotes, stamp, validate_forecast
from .forecast import forecast_window
from .local_store import LocalStore
from .p1_oco import VERSION, QUANTILES, replace_orders
from .p1_worker import MODELS, game_groups, replay_data, export_report
from .provider import QuanturaProvider, historical_range


def process_game(store,provider,pair,as_of,horizon,max_origins,deadline,forecaster=forecast_window):
    game=pair[0].get("eventId",pair[0]["marketId"]);key="p1-game:"+game
    metadata={"marketId":game,"sides":[{**s,"live":True} for s in pair]}
    data={}
    for side in pair:
        start,end=historical_range(side,as_of)
        start=max(start-7*86400,stamp(side["availableFrom"]) if side.get("availableFrom") else 0)
        raw=store.archived_history(side,start,end)
        if raw is None:
            raw=provider.history(side,start,end);store.archive_history(side,start,end,raw)
        data[side["contractId"]]=normalize_quotes(raw,end)
    counts={s:len(rows) for s,rows in data.items()}
    if any(n<32 for n in counts.values()):
        return {"status":"insufficient_history","observations":counts,"complete":True,"forecasts":0}
    common=sorted(set.intersection(*({q.timestamp for q in rows} for rows in data.values())))
    if not common:return {"status":"unaligned_history","complete":True,"forecasts":0,"observations":counts}
    first=max(rows[31].timestamp for rows in data.values())
    if pair[0].get("eventStart"):first=max(first,stamp(pair[0]["eventStart"]))
    first=(first+59)//60*60;end=min(rows[-1].timestamp for rows in data.values())
    book=store.load(key).get("state") or {};attempted=0
    for decision in range(first,end,horizon*60):
        book=store.load(key).get("state") or {}
        if decision<=book.get("completed_decision",0):continue
        if attempted>=max_origins or time.monotonic()>=deadline or store.at_capacity:break
        if decision<book.get("last_timestamp",0) and book.get("pending_pair",{}).get("decision")!=decision:
            book.update(completed_decision=decision,skipped_busy_origins=book.get("skipped_busy_origins",0)+1)
            store.save(key,book,None,[],metadata)
            continue
        attempted+=1;pending=book.get("pending_pair")
        book,_=replay_data(store,key,metadata,{s:[q for q in rows if q.timestamp<=decision] for s,rows in data.items()})
        try:
            if not pending or pending["decision"]!=decision:
                prior=[t for t in common if t<=decision]
                if not prior or decision-prior[-1]>120:raise ValueError("STALE_HISTORY")
                origin=prior[-1];windows={s:history_window(rows,origin) for s,rows in data.items()}
                if any(len(w)<32 for w in windows.values()):raise ValueError("THIRTY_TWO_GENUINE_OBSERVATIONS_REQUIRED")
                started=time.monotonic();forecasts={}
                for side in pair:
                    f=forecaster(windows[side["contractId"]],horizon,MODELS,QUANTILES)
                    if {m["id"] for m in f["models"] if m["status"]=="completed"}!=set(MODELS):raise ValueError("FIVE_MODEL_ENSEMBLE_REQUIRED")
                    validate_forecast(f,origin,horizon);annotate_forecast(f,side,decision,origin,0)
                    f.update(strategy=VERSION,mode="historical_paper_with_measured_latency",
                        history_count=len(windows[side["contractId"]]),input_snapshot=[asdict(q) for q in windows[side["contractId"]]])
                    forecasts[side["contractId"]]=f
                latency=time.monotonic()-started;available=decision+max(1,math.ceil(latency/60))*60
                for side in pair:
                    f=forecasts[side["contractId"]];f.update(available_at=available,group_inference_seconds=latency)
                    store.save(side["contractId"],{},f,[],side)
                pending={"decision":decision,"forecasts":forecasts,"available_at":available}
                book["pending_pair"]=pending;store.save(key,book,None,[],metadata)
            available=pending["available_at"]
            if available>end:raise ValueError("GAME_ENDED_BEFORE_PUBLICATION")
            elapsed={s:[q for q in rows if q.timestamp<=available] for s,rows in data.items()}
            book,_=replay_data(store,key,metadata,elapsed)
            events=replace_orders(book,pending["forecasts"],{s:rows[-1] for s,rows in elapsed.items() if rows},available)
            store.save(key,book,None,[{**e,"game_id":game,"contract_id":e.get("side")} for e in events],metadata)
            future={s:[q for q in rows if q.timestamp<=min(decision+horizon*60,end)] for s,rows in data.items()}
            book,_=replay_data(store,key,metadata,future)
        except (ValueError,RuntimeError) as error:
            book=store.load(key).get("state") or book
            book["failed_origins"]=book.get("failed_origins",0)+1
            store.checkpoint("failed:"+digest([game,decision]),{"game_id":game,"decision":decision,
                "code":str(error) if str(error).isupper() else "MODEL_OR_DATA_UNAVAILABLE"})
        book.pop("pending_pair",None)
        book.update(completed_decision=decision,origin_count=book.get("origin_count",0)+1)
        store.save(key,book,None,[],metadata)
    complete=not any(t>book.get("completed_decision",0) for t in range(first,end,horizon*60))
    return {"status":"processed","complete":complete,"observations":counts,
        "origins":book.get("origin_count",0),"failed_origins":book.get("failed_origins",0)}


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--strategy",choices=["p1_oco"],default="p1_oco")
    p.add_argument("--provider",choices=["polymarket_us"],default="polymarket_us")
    p.add_argument("--horizon",type=int,choices=[5,15,30],default=30)
    p.add_argument("--max-contracts",type=int,default=100)
    p.add_argument("--max-origins",type=int,default=1000)
    p.add_argument("--duration-minutes",type=int,default=345)
    p.add_argument("--series",default="")
    p.add_argument("--lag-minutes",type=int,choices=[0],default=0)
    p.add_argument("--roll-minutes",type=int,default=30,help="Legacy dispatcher alias; P1 always rolls by its horizon")
    p.add_argument("--loss-multiplier",type=float,default=2.5,help="Legacy dispatcher alias; P1 uses fixed tranches, no multiplier")
    p.add_argument("--max-shares",type=float,choices=[100],default=100)
    args=p.parse_args()
    if not 2<=args.max_contracts<=500 or not 1<=args.max_origins<=10000 or not 1<=args.duration_minutes<=345:p.error("Invalid replay limits")
    configuration={"version":VERSION,"mode":"historical","horizon":args.horizon,"roll_minutes":args.horizon,
        "lag_minutes":0,"minimum_history":32,"maximum_history":500,"models":list(MODELS),
        "base_shares":1,"max_shares":100,"fee_rate":.01,"paper_only":True,
        "max_contracts":args.max_contracts,"code_sha":os.environ.get("QUANTURA_CODE_SHA","local")}
    store=LocalStore("p1-replay-"+digest(configuration)[:20],os.environ.get("GITHUB_RUN_ID","local"))
    old=store.values("configuration")
    if old and configuration not in old:raise RuntimeError("RESTORE_ORIGINAL_CODE_AND_CONFIGURATION")
    store.claim(configuration);provider=QuanturaProvider();coverage={};failures=[];fatal=None
    deadline=time.monotonic()+min(args.duration_minutes*60,max(0,float(os.environ.get("QUANTURA_JOB_STARTED_AT",time.time()))+350*60-time.time()))
    export_report(store,configuration,coverage,failures)
    try:
        catalog=store._get("checkpoints","p1_catalog")
        if catalog:
            contracts,meta=store.load_catalog(catalog["id"]);coverage=meta["coverage"];as_of=meta["as_of"]
        else:
            as_of=int(time.time());contracts,coverage=provider.discover("historical",10,"0")
            store.checkpoint("p1_catalog",{"id":store.save_catalog(contracts,coverage,as_of)})
        groups=game_groups(contracts);selected={};remaining=args.max_contracts
        for game,pair in groups.items():
            if len(pair)<=remaining:selected[game]=pair;remaining-=len(pair)
        results={};interrupted=False
        coverage.update(eligible_games=len(groups),selected_games=len(selected),partial=len(selected)<len(groups))
        for game,pair in selected.items():
            if time.monotonic()>=deadline or store.at_capacity:interrupted=True;break
            try:results[game]=process_game(store,provider,pair,as_of,args.horizon,args.max_origins,deadline)
            except (ValueError,RuntimeError) as error:
                failures.append({"game_id":game,"code":str(error) if str(error).isupper() else "PROVIDER_OR_MODEL_UNAVAILABLE"})
                results[game]={"complete":True,"status":"failed"}
            coverage["game_results"]=results;export_report(store,configuration,coverage,failures)
            from .p1_outcomes import collect
            collect(store,provider,pair)
        required=interrupted or any(not r["complete"] for r in results.values())
        coverage.update(resume_required=required,game_results=results)
        report=export_report(store,configuration,coverage,failures)
        print(json.dumps({"event":"p1_replay_report",**report}),flush=True)
        if os.environ.get("GITHUB_OUTPUT"):
            with open(os.environ["GITHUB_OUTPUT"],"a") as out:out.write(f"resume_required={str(required).lower()}\n")
        if not report["forecast_count"]:raise RuntimeError("NO_QUALIFYING_P1_FORECASTS")
    except Exception as error:fatal=type(error).__name__;raise
    finally:export_report(store,configuration,coverage,failures,fatal);store.release()


if __name__=="__main__":main()
