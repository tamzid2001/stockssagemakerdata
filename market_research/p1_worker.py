"""Live Polymarket P1 OCO paper execution using existing providers/models/store."""
import argparse
import json
import os
import time

from .corpus import annotate_forecast
from .engine import digest, history_window, normalize_quotes
from .forecast import default_research_models, forecast_window
from .p1_oco import QUANTILES, advance_book, initial_book, replace_orders, summary
from .provider import QuanturaProvider
from .store import Store
from .worker import Heartbeat, paired_contracts


def process_game(store, provider, pair, heartbeat, *, now=None, forecaster=forecast_window):
    now = int(time.time()) if now is None else now
    key = "p1-game:" + pair[0]["marketId"]
    saved = store.load(key)
    book = saved.get("state") or initial_book()
    data = {}
    for side in pair:
        data[side["contractId"]] = normalize_quotes(provider.history(side, now-7*86400, now),now)
    events = []
    # Replay observed quotes since the last durable checkpoint, without using
    # a forecast generated later to fill an earlier hypothetical order.
    by_time = {}
    for side, quotes in data.items():
        for quote in quotes:
            if quote.timestamp > book["last_timestamp"]:
                by_time.setdefault(quote.timestamp,{})[side] = quote
    for timestamp, quotes in sorted(by_time.items()):
        changes = advance_book(book,quotes,timestamp)
        events.extend(changes)
        if changes:
            store.save(key,book,None,[{**e,"market_id":pair[0]["marketId"]} for e in changes],{"sides":pair,"marketId":pair[0]["marketId"]})
    created = 0
    try:
        if all(s.get("live") for s in pair) and (book["needs_forecast"] or now-book.get("forecast_at",0) >= 1800):
            windows = {side:history_window(quotes,quotes[-1].timestamp) for side,quotes in data.items() if quotes}
            if len(windows)!=2 or any(len(window)<500 for window in windows.values()):
                raise ValueError("FIVE_HUNDRED_OBSERVATIONS_REQUIRED")
            forecasts = {}
            for side in pair:
                heartbeat.check()
                f = forecaster(windows[side["contractId"]],30,tuple(default_research_models()),QUANTILES)
                participants = {m["id"] for m in f["models"] if m["status"]=="completed"}
                if participants != {"prophet","toto","granite","chronos","timesfm"}:
                    raise ValueError("FIVE_MODEL_ENSEMBLE_REQUIRED")
                annotate_forecast(f,side,now,now,0)
                f["available_at"] = int(time.time())
                f["strategy"] = "p1_oco_v1"
                forecasts[side["contractId"]] = f
                store.save(side["contractId"],{},f,[],side)
                created += 1
            published = int(time.time())
            # Inference takes time: refresh both quotes before arming limits.
            latest = {}
            for side in pair:
                quotes = normalize_quotes(provider.history(side,published-600,published),published)
                if quotes: latest[side["contractId"]] = quotes[-1]
            replace_orders(book,forecasts,latest,published)
            book["forecast_at"] = published
    finally:
        # Persist even if forecasting fails after previously open positions exit.
        store.save(key,book,None,[],{"sides":pair,"marketId":pair[0]["marketId"]})
    return book,created,events


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--once",action="store_true")
    parser.add_argument("--max-contracts",type=int,default=500)
    args=parser.parse_args()
    if not 2<=args.max_contracts<=500 or args.max_contracts%2: parser.error("Even bounded contract count required")
    configuration={"version":"p1_oco_v1","horizon":30,"lag":0,"history_minimum":500,"exits":[.1,.25,.5,.75,.9,.99],"models":list(default_research_models()),"paper_only":True}
    run_id=os.environ.get("GITHUB_RUN_ID",str(time.time_ns()))+"-"+os.environ.get("GITHUB_RUN_ATTEMPT","1")
    store=Store("polymarket-p1-oco-"+digest(configuration)[:20],run_id)
    store.claim(configuration)
    heartbeat=Heartbeat(store); heartbeat.thread.start()
    provider=QuanturaProvider()
    deadline=min(time.time()+345*60,float(os.environ.get("QUANTURA_JOB_STARTED_AT",time.time()))+350*60)
    coverage={}; books={}; failures=[]
    try:
        while time.time()<deadline-120:
            heartbeat.check()
            contracts,coverage=provider.discover("live",100,"0")
            grouped={}
            for c in paired_contracts(contracts): grouped.setdefault(c["marketId"],[]).append(c)
            selected=dict(list(grouped.items())[:args.max_contracts//2])
            # Closed games' open positions remain visible/censored, never
            # silently treated as wins, losses or settled cash receipts.
            for tracked in store.tracked():
                if tracked.get("sides") and tracked["marketId"] not in selected:
                    selected[tracked["marketId"]]=[{**s,"live":False} for s in tracked["sides"]]
            coverage.update(eligible_games=len(grouped),selected_games=len(selected),processed_games=0,forecasts=0,failed_games=0,partial=len(grouped)>args.max_contracts//2)
            for game,pair in selected.items():
                if time.time()>deadline-120: coverage["runtime_limited"]=True; break
                try:
                    book,count,_=process_game(store,provider,pair,heartbeat)
                    books[game]=book;coverage["forecasts"]+=count;coverage["processed_games"]+=1
                except (ValueError,RuntimeError) as error:
                    heartbeat.check();coverage["failed_games"]+=1
                    retained=store.load("p1-game:"+game).get("state")
                    if retained: books[game]=retained
                    failures.append({"market_id":game,"code":str(error) if str(error).isupper() else "PROVIDER_OR_MODEL_UNAVAILABLE"})
                    print(json.dumps({"event":"p1_game_skipped",**failures[-1]}),flush=True)
            report={"configuration":configuration,"coverage":coverage,"levels":summary(books.values()),"failures":failures[-500:],"statistics_scope":"persisted_per_game_paper_books","limitations":["Quote-triggered simulated fills, not exchange executions or queue priority.","Six separate exit experiments; one side per game per experiment.","Missing quotes and inference latency can skip fills.","Unknown settlement and remaining positions are not counted as closed wins.","Sell limits above cost do not guarantee profit after fees."]}
            store.report(run_id+"-"+str(time.time_ns()),report)
            print(json.dumps({"event":"p1_paper_cycle","paper_only":True,**coverage,"levels":report["levels"]}),flush=True)
            if args.once: break
            heartbeat.stop.wait(60)
        heartbeat.check();heartbeat.close();store.release()
        if args.once and not coverage.get("forecasts"):
            raise RuntimeError("NO_QUALIFYING_P1_FORECASTS")
        if os.environ.get("GITHUB_OUTPUT"):
            with open(os.environ["GITHUB_OUTPUT"],"a") as out: out.write("handoff_ready=true\n")
    finally:
        heartbeat.close()


if __name__=="__main__": main()
