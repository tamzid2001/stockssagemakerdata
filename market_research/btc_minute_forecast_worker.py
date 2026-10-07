"""Continuous BTC binary-market minute forecasts and fixed-one paper reports.

The collector runs independently of inference. Every forecast uses the first
N genuine minute receipts of its own 15-minute market, and records actual live
publication time. No order APIs, exchange credentials, or Firestore writes.
"""
import argparse
from dataclasses import asdict
import gzip
import json
import math
import os
from pathlib import Path
import signal
import tempfile
import threading
import time

from .btc_minute_archive import MinuteCollector, safe_failure
from .btc_hold_tracking import first_signals, prospective_observations
from .btc_sticky_tracking import direction_at, simulate
from .engine import Quote, digest, stamp
from .forecast import forecast_window
from .interval_markets import KalshiIntervalProvider
from .interval_studies import validate_pair_member
from .local_store import LocalStore
from .p1_oco import QUANTILES
from .recovery_cloud import BUCKET, encode_catalog, decode_catalog

VERSION = "btc_all_live_minutes_fixed_one_v1"
ORIGINS = tuple(range(1, 15))
KINDS = {"btc_minutes", "btc_minute_revisions", "btc_minute_coverage", "btc_lifecycle",
         "btc_settlements", "checkpoints", "btc_live_origins"}
PREFIX = "private-research/btc-minute-forecasts-v1/"

def rows(store):
    with store.lock:
        return [{"kind":kind, "id":key, "value":json.loads(gzip.decompress(raw))}
                for kind,key,raw in store.db.execute("SELECT kind,id,data FROM records").fetchall() if kind in KINDS]

class CloudState:
    """Generation-fenced encrypted checkpoint in the existing private bucket."""
    def __init__(self):
        from .artifact import key_bytes
        key_bytes()  # Reject missing/malformed encryption keys before collection.
        from google.cloud import storage
        from google.oauth2 import service_account
        raw = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")
        if not raw:
            raise ValueError("RESEARCH_STORAGE_IDENTITY_REQUIRED")
        info = json.loads(raw)
        credentials = service_account.Credentials.from_service_account_info(info)
        self.bucket = storage.Client(project=info["project_id"], credentials=credentials).bucket(BUCKET)
        self.checkpoint = self.bucket.blob(PREFIX + "latest.enc")
        self.generation = 0

    def restore(self, store):
        from google.api_core.exceptions import NotFound
        try:
            self.checkpoint.reload()
        except NotFound:
            return
        self.generation = int(self.checkpoint.generation)
        if self.checkpoint.size > 16*1024*1024:
            raise ValueError("CHECKPOINT_TOO_LARGE")
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"state.enc"
            self.checkpoint.download_to_filename(path, if_generation_match=self.generation, checksum="auto")
            value = decode_catalog(path)
        if value.get("version") != VERSION or value.get("series") != "KXBTC15M":
            raise ValueError("BTC_CHECKPOINT_CONFIGURATION_CONFLICT")
        with store.lock,store.db:
            for row in value["rows"]:
                if row["kind"] not in KINDS:
                    raise ValueError("INVALID_BTC_CHECKPOINT_KIND")
                store._put(row["kind"],row["id"],row["value"],True)

    def save(self, store):
        value = {"version":VERSION,"series":"KXBTC15M","rows":rows(store),"saved_at":int(time.time())}
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"state.enc";encode_catalog(value,path)
            if path.stat().st_size > 16*1024*1024:
                raise ValueError("CHECKPOINT_TOO_LARGE")
            # Concurrent/stale writers cannot overwrite another generation.
            self.checkpoint.upload_from_filename(path, if_generation_match=self.generation, checksum="auto")
            self.generation = int(self.checkpoint.generation)

    def archive(self, ticker, value):
        from google.api_core.exceptions import PreconditionFailed
        checksum = digest(value)
        blob = self.bucket.blob(PREFIX + "markets/" + ticker + "/" + checksum + ".enc")
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"market.enc";encode_catalog(value,path)
            try:
                blob.upload_from_filename(path, if_generation_match=0, checksum="auto")
            except PreconditionFailed:
                # Identical cleartext is encrypted with a fresh nonce. Authenticate
                # the existing object before treating it as an idempotent archive.
                existing = Path(folder)/"existing.enc";blob.reload()
                blob.download_to_filename(existing, if_generation_match=int(blob.generation), checksum="auto")
                if digest(decode_catalog(existing)) != checksum:
                    raise ValueError("MARKET_ARCHIVE_CONFLICT")
            blob.reload()
            return {"object":blob.name,"generation":str(blob.generation),"content_sha256":checksum}

def minute_window(minutes, ticker, opened, n, now, side):
    # Extended origins 13/14 still require every real completed minute.
    if n not in ORIGINS:
        raise ValueError("INVALID_LIVE_MINUTE_ORIGIN")
    index = {r["timestamp"]:r for r in minutes if r["market_id"]==ticker and r.get("collection_mode")=="live" and r["received_at"]<=now}
    required = [opened+i*60 for i in range(1,n+1)]
    if any(t not in index for t in required):
        raise ValueError("MISSING_FIRST_N_COMPLETED_MINUTES")
    return [Quote(t,index[t][side+"_ask"],index[t][side+"_bid"]) for t in required]

def forecast_pair(market,n,minutes,forecaster=forecast_window,clock=time.time):
    opened,end = stamp(market["open_time"]),stamp(market["close_time"])
    start = clock();origin = opened+n*60
    if not origin<=start<min(origin+60,end):
        raise ValueError("LIVE_ORIGIN_DEADLINE_MISSED")
    windows = {side:minute_window(minutes,market["ticker"],opened,n,start,side) for side in ("yes","no")}
    models = ("granite","chronos","timesfm") if n==1 else ("prophet","granite","chronos","timesfm")
    pair = []
    for side in ("yes","no"):
        f = forecaster(windows[side],15-n,models,QUANTILES,failure_policy="fail",**({"single_point_research":True} if n==1 else {}))
        validate_pair_member(f,origin,15-n,models)
        f.update(market_context={"market_id":market["ticker"],"event_id":market["event_ticker"],
                 "contract_id":market["ticker"]+":"+side,"side":side,"provider":"kalshi","series":"KXBTC15M"},
                 history_count=n,input_snapshot=[asdict(q) for q in windows[side]],execution_code_sha=os.getenv("QUANTURA_CODE_SHA","local"))
        f["forecast_id"] = digest([f["forecast_id"],market["ticker"],side,n,VERSION])
        pair.append(f)
    published = math.ceil(clock())
    # Whole YES/NO pairs become usable together, never a retrospectively shifted clock.
    usable = published<min(origin+60,end)
    pair = [{**f,"available_at":published,"publication_clock":"actual_live_inference_completion",
             "live_origin_usable":usable} for f in pair]
    return {"market":market["ticker"],"history_minutes":n,"horizon_minutes":15-n,
            "started_at":start,"published_at":published,"status":"published" if usable else "published_late_untradeable",
            "forecasts":pair,"paper_only":True,"execution_verified":False}

def paper_report(store,now):
    tape = prospective_observations([{"contract_id":r["market_id"]+":"+side,"game_id":r["event_id"],
        "timestamp":r["timestamp"],"received_at":r["received_at"],"collection_mode":r["collection_mode"],
        "observed":True,"ask":r[side+"_ask"],"bid":r[side+"_bid"]} for r in store.values("btc_minutes") for side in ("yes","no")],now)
    settlements = store.values("btc_settlements")
    fee = store._get("checkpoints","btc_fee_policy") or {}
    multiplier = fee.get("multiplier")
    valid_fee = fee.get("fee_type") in ("quadratic","quadratic_with_maker_fees") and type(multiplier) in (int,float) and math.isfinite(multiplier) and multiplier>=0
    origins = {}
    records = store.values("btc_live_origins")
    for n in ORIGINS:
        forecasts = [f for r in records if r["history_minutes"]==n and r["status"]=="published" for f in r["forecasts"]]
        signals,_ = first_signals(forecasts,tape,now)
        sticky = [s for s in signals if (d:=direction_at(settlements,s["signal_received_at"],s["market_id"])) and s["contract_id"].endswith(":"+d["side"])]
        origins[str(n)] = {label:simulate(selected,tape,settlements,as_of=now,policy="fixed_one",precision="0.0001",multiplier=multiplier if valid_fee else 0)
                          for label,selected in (("first_p90",signals),("p90_sticky",sticky))}
        if not valid_fee:
            for scenario in origins[str(n)].values():
                scenario["financial_status"] = "gross_only_fee_unavailable"
                scenario["summary"].update(net_pnl=None,fees=None,net_return_on_closed_entry_notional=None)
                for trade in scenario["trades"]:
                    trade.update(net_pnl=None,fees=None)
    return {"version":VERSION,"as_of":now,"origins":origins,"fee_policy":fee,
            "fee_status":"current_schedule_sensitivity" if valid_fee else "gross_only_fee_unavailable",
            "quantity_per_entry":1,"paper_only":True,"live_orders_enabled":False,
            "timely_side_observations":len(tape),"origin_evaluations":len(records),
            "publication_status_counts":{status:sum(r["status"]==status for r in records) for status in sorted({r["status"] for r in records})}}

def finalize(store,cloud,now,report):
    totals = store._get("checkpoints","archived_totals") or {}
    finalized = 0
    for life in sorted(store.values("btc_lifecycle"),key=lambda r:r["close_at"]):
        if life.get("resolution_status")!="resolved" or now<life["close_at"]+300:
            continue
        ticker = life["market_id"]
        selected = [row for row in rows(store) if row["kind"]!="checkpoints" and
                    (row["value"].get("market_id")==ticker or row["value"].get("market")==ticker)]
        outcomes = {n:{strategy:[t for t in data["trades"] if t["market_id"]==ticker]
                       for strategy,data in strategies.items()} for n,strategies in report["origins"].items()}
        cloud.archive(ticker,{"version":VERSION,"market":life["market"],"rows":selected,"paper_trades":outcomes,"fee_policy":report["fee_policy"]})
        # Only after the encrypted immutable archive is durable may the local
        # market be retired. Totals keep each origin/strategy independent.
        with store.lock,store.db:
            for n,strategies in outcomes.items():
                for strategy,trades in strategies.items():
                    key=n+":"+strategy;total=totals.setdefault(key,{"trades":0,"wins":0,"losses":0,"net_pnl":0.,"fees":0.,"entry_notional":0.,"unknown_fee_trades":0,"unknown_fee_gross_pnl":0.})
                    for trade in trades:
                        if trade["status"]!="closed":raise ValueError("SETTLED_MARKET_HAS_OPEN_PAPER_TRADE")
                        if report["fee_status"] != "current_schedule_sensitivity":
                            total["unknown_fee_trades"]+=1;total["unknown_fee_gross_pnl"]+=trade["gross_pnl"]
                            continue
                        total["trades"]+=1;total["wins"]+=trade["net_pnl"]>0;total["losses"]+=trade["net_pnl"]<0
                        total["net_pnl"]+=trade["net_pnl"];total["fees"]+=trade["fees"];total["entry_notional"]+=trade["entry_price"]
            for row in selected:
                if row["kind"]!="btc_settlements":store.db.execute("DELETE FROM records WHERE kind=? AND id=?",(row["kind"],row["id"]))
            store._put("checkpoints","finalized:"+ticker,{"market_id":ticker,"close_at":life["close_at"]})
            store._put("checkpoints","archived_totals",totals)
        finalized+=1
    # Keep settlement seeds and discovery tombstones bounded; market originals
    # remain encrypted in durable storage, not removed from the research archive.
    with store.lock,store.db:
        for kind in ("btc_settlements","checkpoints"):
            candidates=[(key,json.loads(gzip.decompress(raw))) for key,raw in store.db.execute("SELECT id,data FROM records WHERE kind=?",(kind,)) if kind=="btc_settlements" or key.startswith("finalized:")]
            for key,_ in sorted(candidates,key=lambda r:r[1].get("close_at",0),reverse=True)[128:]:store.db.execute("DELETE FROM records WHERE kind=? AND id=?",(kind,key))
    return finalized

def publish_checkpoint(store,cloud,output,now):
    report=paper_report(store,now)
    retired=finalize(store,cloud,now,report)
    cloud.save(store)
    report=paper_report(store,now)
    report["archived_totals"]=store._get("checkpoints","archived_totals") or {}
    report["totals_scope"]="origins are current buffer only; archived_totals are retired markets, independent per origin/strategy"
    (output/"report-btc-live-minute-summary.json").write_text(json.dumps(report,allow_nan=False))
    print(json.dumps({"event":"btc_minute_checkpoint","at":now,"finalized_markets":retired,
        "coverage":report["publication_status_counts"],"collector":store._get("checkpoints","btc_collector_health"),
        "firestore_writes":0}),flush=True)

def run(duration):
    output=Path(os.environ.get("QUANTURA_RESEARCH_DIR","/tmp/btc-live-minutes"));output.mkdir(parents=True,exist_ok=True)
    cloud=CloudState();store=LocalStore(VERSION,"btc-all-minutes",output,capacity_bytes=64*1024*1024)
    cloud.restore(store)
    provider=KalshiIntervalProvider("KXBTC15M",timeout=5,attempts=2)
    collector=MinuteCollector(store,provider);collector.start()
    end=time.monotonic()+duration*60;last_save=0;failed=False
    try:
        while time.monotonic()<end:
            now=int(time.time());minutes=store.values("btc_minutes")
            for life in store.values("btc_lifecycle"):
                if life["open_at"]>=now or life["close_at"]<=now or life.get("resolution_status")=="resolved":continue
                for n in ORIGINS:
                    # Earlier inference may have consumed the rest of this
                    # minute; never start with a stale loop timestamp.
                    now=int(time.time())
                    due=life["open_at"]+n*60;key=digest([life["market_id"],n])
                    if due>now or store._get("btc_live_origins",key):continue
                    if now>=due+60:
                        record={"market":life["market_id"],"history_minutes":n,"status":"missed_origin","forecasts":[],"reason":"NO_TIMELY_COMPLETE_ORIGIN"}
                    else:
                        try:
                            # Missing bars can still arrive before the deadline.
                            minute_window(minutes,life["market_id"],life["open_at"],n,now,"yes")
                        except ValueError:continue
                        def timeout(_signum,_frame):raise TimeoutError("LIVE_INFERENCE_DEADLINE")
                        previous=signal.signal(signal.SIGALRM,timeout)
                        try:
                            remaining=min(due+60,life["close_at"])-time.time()
                            if remaining<=0:raise TimeoutError("LIVE_INFERENCE_DEADLINE")
                            signal.setitimer(signal.ITIMER_REAL,remaining)
                            record=forecast_pair(life["market"],n,minutes)
                        except (ValueError,RuntimeError,OSError) as error:
                            record={"market":life["market_id"],"history_minutes":n,"status":"failed","forecasts":[],**safe_failure(error)}
                        finally:signal.setitimer(signal.ITIMER_REAL,0);signal.signal(signal.SIGALRM,previous)
                    with store.lock,store.db:store._put("btc_live_origins",key,record,True)
                    print(json.dumps({k:v for k,v in record.items() if k!="forecasts"}),flush=True)
            now=int(time.time())
            if now-last_save>=300:
                publish_checkpoint(store,cloud,output,now);last_save=now
            threading.Event().wait(.5)
    except BaseException:
        failed=True;raise
    finally:
        collector.stop()
        if collector.thread.is_alive():raise RuntimeError("COLLECTOR_CHECKPOINT_STILL_WRITING")
        if not failed:publish_checkpoint(store,cloud,output,int(time.time()))
        store.db.close()

if __name__=="__main__":
    parser=argparse.ArgumentParser();parser.add_argument("--duration-minutes",type=int,default=330)
    args=parser.parse_args()
    if not 1<=args.duration_minutes<=330:parser.error("INVALID_DURATION")
    run(args.duration_minutes)
