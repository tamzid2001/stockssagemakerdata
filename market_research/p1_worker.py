"""Artifact-checkpointed P1 accumulation paper worker. No exchange orders/DB writes."""
import argparse
from dataclasses import asdict
import json
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor

from .corpus import annotate_forecast
from .engine import digest, history_window, normalize_quotes, validate_forecast
from .forecast import default_research_models, forecast_window
from .p1_oco import VERSION, EXITS, QUANTILES, advance_book, arm_minute, initial_book, replace_orders, summary
from .provider import QuanturaProvider
from .local_store import LocalStore
from .worker import Heartbeat

MODELS = ("prophet", "toto", "granite", "chronos", "timesfm")
_LOCKS = {}
_LOCK_GUARD = threading.Lock()


def game_lock(store, key):
    with _LOCK_GUARD:
        return _LOCKS.setdefault((getattr(store,"session","test"),key),threading.RLock())


def replay_data(store, key, metadata, data, quantity=1., max_quantity=100.):
    """Single atomic book writer shared by forecast and minute-observer paths."""
    with game_lock(store,key):
        book=store.load(key).get("state") or initial_book()
        events=[]; by_time={}
        live=all(s.get("live") for s in metadata["sides"])
        if not live:
            # A terminal quote is not proof a closed exchange would fill an order.
            for ledger in book["levels"].values():ledger["orders"]={}
            store.save(key,book,None,[],metadata)
            return book,events
        for side,quotes in data.items():
            for q in quotes:
                if q.timestamp>book["last_timestamp"]:
                    by_time.setdefault(q.timestamp,{})[side]=q
        for timestamp,quotes in sorted(by_time.items()):
            if book["active_forecasts"] and hasattr(store,"db"):
                with store.lock,store.db:
                    for side,q in quotes.items():
                        store._put("observations",digest([key,side,timestamp]),
                            {"game_id":metadata["marketId"],"contract_id":side,**asdict(q)},True)
            changes=advance_book(book,quotes,timestamp)
            changes+=arm_minute(book,quotes,timestamp,quantity,max_quantity)
            tagged=[{**e,"game_id":metadata["marketId"],"contract_id":e.get("side")} for e in changes]
            if tagged:store.save(key,book,None,tagged,metadata)
            events.extend(tagged)
        store.save(key,book,None,[],metadata)
        return book,events


class MinuteObserver:
    """Poll established paper books independently of sequential model inference."""
    def __init__(self,store,quantity=1.,max_quantity=100.):
        self.store,self.quantity,self.max_quantity=store,quantity,max_quantity
        self.stop=threading.Event()
        self.thread=threading.Thread(target=self.run,daemon=True)

    def observe(self,record):
        key=record["contract_id"];c=record["contract"]
        if not all(s.get("live") for s in c["sides"]):return
        now=int(time.time());provider=QuanturaProvider()
        data={s["contractId"]:normalize_quotes(provider.history(s,now-3600,now),now) for s in c["sides"]}
        replay_data(self.store,key,c,data,self.quantity,self.max_quantity)

    def run(self):
        with ThreadPoolExecutor(max_workers=4) as executor:
            while not self.stop.is_set():
                started=time.monotonic()
                records=[r for r in self.store.values("contracts") if r.get("contract_id","").startswith("p1-game:") and r.get("state",{}).get("active_forecasts")]
                futures=[executor.submit(self.observe,r) for r in records]
                for future in futures:
                    try:future.result()
                    except Exception as error:
                        self.store.checkpoint("minute_observer_failure",{"timestamp":int(time.time()),"error_type":type(error).__name__})
                        print(json.dumps({"event":"p1_minute_observer_failed","error_type":type(error).__name__}),flush=True)
                self.stop.wait(max(1,60-(time.monotonic()-started)))

    def close(self):
        self.stop.set();self.thread.join(timeout=300)
        if self.thread.is_alive():raise RuntimeError("MINUTE_OBSERVER_DID_NOT_STOP")


def game_groups(contracts):
    """One two-side moneyline, or all six YES/NO sides of a soccer game."""
    events = {}
    for c in contracts:
        events.setdefault(c.get("eventId", c["marketId"]), {})[c["contractId"]] = c
    result = {}
    for event, unique in events.items():
        rows = list(unique.values())
        groups = {}
        for c in rows:
            groups.setdefault(c["marketId"], []).append(c)
        paired = all(len(p)==2 and {s["side"] for s in p}=={"long","short"} for p in groups.values())
        if paired and (len(rows)==2 or (len(rows)==6 and len(groups)==3 and all(
                c.get("marketType")=="SPORTS_MARKET_TYPE_DRAWABLE_OUTCOME" for c in rows))):
            result[event] = sorted(rows, key=lambda c:c["contractId"])
    return result


def process_game(store, provider, pair, heartbeat, *, now=None, forecaster=forecast_window,
                 minimum_history=32, quantity=1., max_quantity=100., horizon=30):
    now = int(time.time()) if now is None else now
    game = pair[0].get("eventId", pair[0]["marketId"])
    key = "p1-game:"+game
    metadata = {"sides":pair,"marketId":game}
    events = []; created = 0

    try:
        data = {s["contractId"]:normalize_quotes(provider.history(s,now-7*86400,now),now) for s in pair}
        book,changes=replay_data(store,key,metadata,data,quantity,max_quantity)
        events.extend(changes)
        due = book["needs_forecast"] or now-book.get("forecast_at",0)>=horizon*60
        if all(s.get("live") for s in pair) and due:
            common = set.intersection(*({q.timestamp for q in rows if q.observed} for rows in data.values()))
            if not common or now-max(common)>120:
                raise ValueError("FRESH_ALIGNED_SIDE_QUOTES_REQUIRED")
            origin = max(common)
            windows = {side:history_window(rows,origin) for side,rows in data.items()}
            counts = {side:len(w) for side,w in windows.items()}
            with game_lock(store,key):
                current=store.load(key).get("state") or book
                current["history_counts"]=counts
                store.save(key,current,None,[],metadata)
            if any(n<minimum_history for n in counts.values()):
                raise ValueError("THIRTY_TWO_GENUINE_OBSERVATIONS_REQUIRED")
            forecasts = {}
            for side in pair:
                heartbeat.check()
                f = forecaster(windows[side["contractId"]],horizon,MODELS,QUANTILES)
                if {m["id"] for m in f["models"] if m["status"]=="completed"} != set(MODELS):
                    raise ValueError("FIVE_MODEL_ENSEMBLE_REQUIRED")
                validate_forecast(f,origin,horizon)
                annotate_forecast(f,side,now,origin,0)
                f.update(available_at=int(time.time()),strategy=VERSION,mode="prospective_paper",
                    expected_side_count=len(pair),paper_publication_required=True,
                    history_count=len(windows[side["contractId"]]),
                    input_snapshot=[asdict(q) for q in windows[side["contractId"]]])
                forecasts[side["contractId"]] = f
                store.save(side["contractId"],{},f,[],side)
                created += 1
            # Execute OLD outstanding orders against observations received during
            # inference before installing new forecasts. Never backdate a fill.
            observed_at = int(time.time())
            refresh = {s["contractId"]:normalize_quotes(provider.history(s,observed_at-3600,observed_at),observed_at) for s in pair}
            with game_lock(store,key):
                # Reload: the minute observer may have filled/averaged/exited
                # while models ran. Never overwrite its newer state.
                book,changes=replay_data(store,key,metadata,refresh,quantity,max_quantity)
                events.extend(changes)
                published = int(time.time())
                latest = {s:rows[-1] for s,rows in refresh.items() if rows}
                changes = replace_orders(book,forecasts,latest,published,quantity,max_quantity)
                book["forecast_at"] = published
                book["forecast_ids"] = {s:f["forecast_id"] for s,f in forecasts.items()}
                tagged=[{**e,"game_id":game,"contract_id":e.get("side")} for e in changes]
                store.save(key,book,None,tagged,metadata)
                store.checkpoint("path-publication:"+digest(book["forecast_ids"]),
                    {"path_forecast_ids":list(book["forecast_ids"].values()),"available_at":published})
                events.extend(tagged)
        return book,created,events
    finally:
        with game_lock(store,key):
            current=store.load(key).get("state") or initial_book()
            current["last_attempt_at"]=now
            store.save(key,current,None,[],metadata)


def export_report(store, configuration, coverage, failures, error=None):
    from .quantile_paths import from_store as paths_from_store
    books = [r["state"] for r in store.values("contracts") if r.get("contract_id","").startswith("p1-game:")]
    forecasts = store.values("forecasts")
    from .p1_outcomes import from_store
    report = {"schema_version":2,"configuration":configuration,"coverage":coverage,
        "forecast_count":len(forecasts),"levels":summary(books),"failures":failures[-500:],
        "outcome_cohorts":from_store(store)["cohorts"],
        "low_high_paper_experiments":paths_from_store(store,int(time.time()))["summary"],
        "fatal_error":error,"statistics_scope":"checkpoint_lineage_cumulative",
        "paper_only":True,"generated_at":int(time.time()),
        "limitations":["Quote-triggered simulated fills, not exchange executions or queue/liquidity confirmation.",
            "Six independent fixed exit quantiles; do not sum their returns as one strategy.",
            "One new P1 tranche per observed minute while ask>P1; terminal P1 of the active horizon; pending+filled shares capped.",
            "Sell targets below/equal to weighted average cost are suspended, not fabricated.",
            "No stop loss or assumed settlement: open positions are censored, with available bid marks shown.",
            "Closed-trade win rate alone omits open losses; above-cost exits can still lose after fees.",
            "Selected horizon refresh cadence is a target; sequential CPU inference and provider delays create gaps.",
            "Model input is up to 500 genuine observed minutes, not synthetic filled intervals.",
            "Private research only; underlying provider redistribution rights require review."]}
    store.report(os.environ.get("GITHUB_RUN_ID","local")+"-"+str(time.time_ns()),report)
    return report


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--once",action="store_true")
    parser.add_argument("--max-contracts",type=int,default=500)
    parser.add_argument("--minimum-history",type=int,choices=[32],default=32)
    parser.add_argument("--horizon",type=int,choices=[5,15,30],default=30)
    parser.add_argument("--quantity",type=float,default=1.)
    parser.add_argument("--max-quantity",type=float,default=100.)
    args=parser.parse_args()
    if not 2<=args.max_contracts<=500 or not 0<args.quantity<=args.max_quantity<=10000:
        parser.error("Bounded positive contract/position limits required")
    configuration={"version":VERSION,"horizon":args.horizon,"lag":0,"history_target":500,
        "history_minimum":args.minimum_history,"history_phase":"both","exits":list(EXITS),
        "models":list(MODELS),"paper_only":True,"base_shares":args.quantity,"max_shares":args.max_quantity,
        "fee_rate":.01,"entry_cadence_seconds":60,"quantile_target":"terminal_forecast_horizon",
        "code_sha":os.environ.get("QUANTURA_CODE_SHA","local")}
    run_id=os.environ.get("GITHUB_RUN_ID",str(time.time_ns()))+"-"+os.environ.get("GITHUB_RUN_ATTEMPT","1")
    store=LocalStore("polymarket-p1-"+digest(configuration)[:20],run_id)
    existing=store.values("configuration")
    if existing and configuration not in existing:
        raise RuntimeError("RESTORE_ORIGINAL_CODE_AND_CONFIGURATION")
    store.claim(configuration)
    heartbeat=Heartbeat(store); heartbeat.thread.start()
    observer=MinuteObserver(store,args.quantity,args.max_quantity);observer.thread.start()
    provider=QuanturaProvider()
    deadline=min(time.time()+345*60,float(os.environ.get("QUANTURA_JOB_STARTED_AT",time.time()))+350*60)
    coverage={};failures=[]; fatal=None
    # Create a recoverable baseline even if discovery or model initialization fails.
    export_report(store,configuration,coverage,failures)
    try:
        if tuple(default_research_models()) != MODELS:
            raise RuntimeError("FIVE_CONFIGURED_MODELS_REQUIRED")
        while time.time()<deadline-120 and not store.at_capacity:
            heartbeat.check()
            contracts,coverage=provider.discover("live",100,"0")
            grouped=game_groups(contracts)
            due=lambda item:store.load("p1-game:"+item[0]).get("state",{}).get("last_attempt_at",0)
            selected={};remaining=args.max_contracts
            for game,pair in sorted(grouped.items(),key=due):
                if len(pair)<=remaining:
                    selected[game]=pair;remaining-=len(pair)
            for record in store.values("contracts"):
                c=record.get("contract") or {}
                if c.get("sides") and c["marketId"] not in selected:
                    selected[c["marketId"]]=grouped.get(c["marketId"],[{**s,"live":False} for s in c["sides"]])
            coverage.update(eligible_games=len(grouped),selected_games=len(selected),processed_games=0,
                forecasts=0,failed_games=0,partial=sum(len(p) for p in grouped.values())>args.max_contracts)
            for game,pair in selected.items():
                if time.time()>deadline-120 or store.at_capacity:
                    coverage["runtime_limited"]=True;break
                try:
                    _,count,_=process_game(store,provider,pair,heartbeat,minimum_history=args.minimum_history,
                        quantity=args.quantity,max_quantity=args.max_quantity,horizon=args.horizon)
                    coverage["forecasts"]+=count;coverage["processed_games"]+=1
                except (ValueError,RuntimeError) as error:
                    heartbeat.check();coverage["failed_games"]+=1
                    code=str(error) if str(error).isupper() and len(str(error))<100 else "PROVIDER_OR_MODEL_UNAVAILABLE"
                    failure={"game_id":game,"code":code,"timestamp":int(time.time()),
                        "history_counts":store.load("p1-game:"+game).get("state",{}).get("history_counts")}
                    failures.append(failure)
                    print(json.dumps({"event":"p1_game_skipped",**failure}),flush=True)
                export_report(store,configuration,coverage,failures)
            from .p1_outcomes import collect
            collect(store,provider,[s for pair in selected.values() if not all(s.get("live") for s in pair) for s in pair])
            report=export_report(store,configuration,coverage,failures)
            print(json.dumps({"event":"p1_paper_cycle",**coverage,"levels":report["levels"]}),flush=True)
            if args.once:break
            heartbeat.stop.wait(60)
        if args.once and not coverage.get("forecasts"):
            raise RuntimeError("NO_QUALIFYING_P1_FORECASTS")
        if store.at_capacity:
            raise RuntimeError("ARTIFACT_CAPACITY_REACHED_REQUIRES_NEW_SHARD")
        if os.environ.get("GITHUB_OUTPUT"):
            with open(os.environ["GITHUB_OUTPUT"],"a") as out:out.write("handoff_ready=true\n")
    except Exception as error:
        fatal=type(error).__name__
        raise
    finally:
        export_report(store,configuration,coverage,failures,fatal)
        observer.close();heartbeat.close();store.release()


if __name__=="__main__":main()
