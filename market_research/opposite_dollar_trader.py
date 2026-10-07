"""Concurrent selected-series workers: $1 opposite sticky/P90, hold to settlement.

One fenced encrypted GCS journal per series. Live and paper entries share the
same causal signal but keep distinct accounting. Durable intent precedes each
IOC; ambiguous delivery is reconciled before any subsequent order. No Firestore.
"""
import argparse
from collections import Counter
import gzip
from dataclasses import asdict
from decimal import Decimal, ROUND_FLOOR
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import queue
import re
import signal
import tempfile
import time

from .engine import digest, stamp
from .kalshi_execution import KalshiExecution, money, order_payload, reconciled_order
from .opposite_strategies import DollarConfig, STRATEGIES, VERSION, portfolio_fingerprint
from .store import claim_transition

PREFIX = "private-research/five-opposite-trader-v1/"

def approved(config, env):
    sha=env.get("QUANTURA_CODE_SHA", "")
    return (env.get("QUANTURA_KALSHI_DOLLAR_LIVE_ENABLED")=="true"
            and re.fullmatch(r"[a-f0-9]{40}",sha) is not None
            and env.get("QUANTURA_KALSHI_DOLLAR_APPROVED_SHA")==sha
            and env.get("QUANTURA_KALSHI_DOLLAR_APPROVED_CONFIG")==portfolio_fingerprint(config.subaccount))

class DollarBroker(KalshiExecution):
    def __init__(self,config,*,requested_live=False,client=None,env=None):
        values=dict(os.environ if env is None else env)
        if requested_live and not approved(config,values):raise RuntimeError("DOLLAR_LIVE_APPROVAL_REQUIRED")
        # Reuse the established signed V2 client without authorizing a legacy strategy.
        local={**values,"QUANTURA_KALSHI_LIVE_ENABLED":"true" if requested_live else "false",
               "QUANTURA_KALSHI_APPROVED_CONFIG":config.fingerprint,
               "QUANTURA_KALSHI_APPROVED_SHA":values.get("QUANTURA_CODE_SHA", "")}
        super().__init__(config,requested_live=requested_live,client=client,env=local)

class CloudJournal:
    def __init__(self,config,key_id,holder):
        from google.cloud import storage
        from google.oauth2 import service_account
        from .artifact import key_bytes
        from .recovery_cloud import BUCKET
        key_bytes()
        info=json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT_JSON"])
        credentials=service_account.Credentials.from_service_account_info(info)
        self.bucket=storage.Client(project=info["project_id"],credentials=credentials).bucket(BUCKET)
        self.account=hashlib.sha256(f"{key_id}:{config.subaccount}".encode()).hexdigest()
        self.config,self.holder=config,holder
        self.name=PREFIX+self.account+"/"+config.series_ticker+".enc"
        self.generation=0;self.state={};self.fence=None

    def read(self,name):
        from google.api_core.exceptions import NotFound
        from .recovery_cloud import decode_catalog
        blob=self.bucket.blob(name)
        try:blob.reload()
        except NotFound:return {},0
        generation=int(blob.generation)
        if blob.size>16*1024*1024:raise RuntimeError("TRADER_JOURNAL_TOO_LARGE")
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/"state.enc"
            blob.download_to_filename(path,if_generation_match=generation,checksum="auto")
            return decode_catalog(path),generation

    def write(self,name,value,generation):
        from .recovery_cloud import encode_catalog
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/"state.enc";encode_catalog(value,path)
            if path.stat().st_size>16*1024*1024:raise RuntimeError("TRADER_JOURNAL_TOO_LARGE")
            blob=self.bucket.blob(name)
            blob.upload_from_filename(path,if_generation_match=generation,checksum="auto")
            return int(blob.generation)

    def claim(self):
        old,generation=self.read(self.name)
        if old and (old.get("version")!=VERSION or old.get("config")!=asdict(self.config)):
            raise RuntimeError("TRADER_CONFIGURATION_CONFLICT")
        lease=claim_transition(old.get("lease",{}),self.holder,time.time(),ttl=120)
        self.state={"version":VERSION,"config":asdict(self.config),"entries":{},"decisions":{},
                    "rows":[],"totals":{},**old,"lease":lease}
        self.generation=self.write(self.name,self.state,generation);self.fence=lease["fence"]

    def verify(self):
        value,generation=self.read(self.name);lease=value.get("lease",{})
        if (generation!=self.generation or lease.get("holder")!=self.holder
                or lease.get("fence")!=self.fence or lease.get("expires",0)<=time.time()):
            raise RuntimeError("TRADER_FENCE_LOST")

    def save(self):
        lease=self.state["lease"]
        if lease["holder"]!=self.holder or lease["fence"]!=self.fence or lease["expires"]<=time.time():
            raise RuntimeError("TRADER_FENCE_LOST")
        self.state["lease"]={**lease,"expires":time.time()+120,"updated_at":time.time()}
        self.generation=self.write(self.name,self.state,self.generation)

    def release(self):
        self.verify();self.state["lease"]["expires"]=0
        self.generation=self.write(self.name,self.state,self.generation)

    def archive(self,ticker,value):
        from google.api_core.exceptions import PreconditionFailed
        checksum=digest(value);name=PREFIX+self.account+"/markets/"+ticker+"/"+checksum+".enc"
        try:generation=self.write(name,value,0)
        except PreconditionFailed:
            existing,generation=self.read(name)
            if digest(existing)!=checksum:raise RuntimeError("TRADER_ARCHIVE_CONFLICT")
        return {"object":name,"generation":str(generation),"content_sha256":checksum}

class AccountGate:
    """Serialize admission across all five workers; reconcile exact portfolio ownership."""
    def __init__(self,journal,broker):self.journal,self.broker=journal,broker;self.generation=None
    def __enter__(self):
        from google.api_core.exceptions import PreconditionFailed
        name=PREFIX+self.journal.account+"/order-gate.enc"
        old,generation=self.journal.read(name)
        lease=claim_transition(old.get("lease",{}),self.journal.holder,time.time(),ttl=20)
        self.name,self.lease=name,lease
        try:self.generation=self.journal.write(name,{"lease":lease},generation)
        except PreconditionFailed:raise RuntimeError("LEASE_HELD") from None
        try:
            return self.admit()
        except BaseException:
            self.__exit__()
            raise

    def admit(self):
        self.journal.verify()
        orders,positions=self.broker.account()
        if orders:raise RuntimeError("ACCOUNT_RESTING_ORDER_UNVERIFIED")
        actual={}
        for p in positions:
            count=money(p["position_fp"])
            if not count:continue
            if p["ticker"] in actual:raise RuntimeError("DUPLICATE_ACCOUNT_POSITION")
            actual[p["ticker"]]=count
        expected={}
        for series in STRATEGIES:
            state,_=self.journal.read(PREFIX+self.journal.account+"/"+series+".enc")
            if state and (state.get("version")!=VERSION or state.get("config")!=asdict(DollarConfig(series,self.journal.config.subaccount))):
                raise RuntimeError("ACCOUNT_JOURNAL_CONFIGURATION_UNVERIFIED")
            for ticker,group in state.get("entries",{}).items():
                entry=group.get("live")
                if not entry or entry.get("status")=="settled":continue
                if not ticker.startswith(series+"-") or entry.get("ticker")!=ticker:
                    raise RuntimeError("ACCOUNT_POSITION_IDENTITY_UNVERIFIED")
                filled=money(entry.get("filled","0"))
                pending=entry.get("pending")
                if pending:
                    order=self.broker.find_order(pending["intent"],pending.get("order_id"))
                    if order is None:raise RuntimeError("ACCOUNT_INTENT_UNRESOLVED")
                    terminal=reconciled_order(order,pending["intent"],entry["side"])
                    filled+=money(terminal["filled"])
                if filled and (time.time()<entry["market_end"] or ticker in actual):
                    expected[ticker]=filled*(1 if entry["side"]=="yes" else -1)
        if expected!=actual:raise RuntimeError("ACCOUNT_POSITION_UNVERIFIED")
        return self
    def verify(self):
        self.journal.verify()
        _,generation=self.journal.read(self.name)
        if generation!=self.generation or self.lease["expires"]<=time.time():
            raise RuntimeError("ACCOUNT_ORDER_GATE_LOST")
    def __exit__(self,*_):
        if self.generation is not None:
            self.journal.write(self.name,{"lease":{**self.lease,"expires":0}},self.generation)

def quantity_remaining(entry,ask):
    remaining=Decimal("1.00")-money(entry.get("cost","0"))
    p=money(ask)
    if remaining<0:raise RuntimeError("ENTRY_BUDGET_EXCEEDED")
    if not 0<p<1:raise RuntimeError("CURRENT_ASK_UNAVAILABLE")
    return (remaining/p).quantize(Decimal(".01"),rounding=ROUND_FLOOR)

def market_ask(market,side,now,end):
    if (market.get("market_type")!="binary" or market.get("status")!="active"
            or stamp(market["close_time"])-stamp(market["open_time"])!=900
            or stamp(market["close_time"])!=end or now>=end):
        raise RuntimeError("MARKET_NOT_TRADEABLE")
    ask=money(market[side+"_ask_dollars"])
    if not 0<ask<1:raise RuntimeError("CURRENT_ASK_UNAVAILABLE")
    yes_price=ask if side=="yes" else 1-ask
    ranges=market.get("price_ranges")
    if not isinstance(ranges,list) or not any(money(r["step"])>0 and
            money(r["start"])<=yes_price<=money(r["end"]) and
            (yes_price-money(r["start"]))%money(r["step"])==0 for r in ranges):
        raise RuntimeError("PRICE_GRID_UNVERIFIED")
    return ask

class DollarTrader:
    def __init__(self,config,broker,journal,mode,gate_factory=AccountGate):
        self.config,self.broker,self.journal,self.mode=config,broker,journal,mode
        self.gate_factory=gate_factory

    def begin(self,signal,quote,now):
        ticker=signal["market_id"]
        if (ticker in self.journal.state["entries"] or quote["timestamp"]!=signal["signal_at"]+60
                or not quote.get("timely") or not quote["timestamp"]<=quote["received_at"]<=now<=quote["timestamp"]+30
                or now>=signal["market_end"]):raise RuntimeError("MISSED_OR_DUPLICATE_DOLLAR_ENTRY")
        original=signal["contract_id"].rsplit(":",1)[1]
        sticky=signal.get("sticky") or {}
        if sticky.get("side")!=original or sticky.get("confirmed_at",now)>=signal["signal_received_at"]:
            raise RuntimeError("OFFICIAL_STICKY_AGREEMENT_REQUIRED")
        side="no" if original=="yes" else "yes"
        entry={"ticker":ticker,"side":side,"source_side":original,"signal":signal,
               "market_end":signal["market_end"],"created_at":now,"status":"entering",
               "filled":"0","cost":"0","fees":"0","attempt":0,"last_attempt":0,"pending":None}
        self.journal.state["entries"][ticker]={m:{**entry} for m in ("paper","live")
                if self.mode=="both" or self.mode==m}
        self.journal.save()

    def reconcile(self,ticker,fee_policy,now):
        group=self.journal.state["entries"][ticker]
        market=None
        for mode,entry in group.items():
            if entry["status"]=="settled":continue
            pending=entry.get("pending")
            if pending:
                order=self.broker.find_order(pending["intent"],pending.get("order_id"))
                if order is None:continue  # Unknown POST delivery cannot authorize another order.
                terminal=reconciled_order(order,pending["intent"],entry["side"])
                for key in ("filled","cost","fees"):
                    entry[key]=str(money(entry[key])+money(terminal[key]))
                if money(entry["cost"])>1:raise RuntimeError("ENTRY_BUDGET_EXCEEDED")
                entry.setdefault("fills",[]).append({**terminal,"intent":pending["intent"],"reconciled_at":now})
                entry["pending"]=None;self.journal.save()
            market=market or self.broker.market(ticker)
            if now>=entry["market_end"]:
                self.settle(entry,mode,market,now);continue
            if entry["status"] in ("held","entry_rejected") or now-entry["last_attempt"]<1:continue
            ask=market_ask(market,entry["side"],now,entry["market_end"])
            quantity=quantity_remaining(entry,ask)
            if quantity<=0:
                entry["status"]="held";self.journal.save();continue
            if fee_policy.get("fee_type") not in ("quadratic","quadratic_with_maker_fees"):
                raise RuntimeError("CURRENT_FEE_POLICY_UNAVAILABLE")
            from .btc_sticky_tracking import taker_fee
            fee=money(taker_fee(float(quantity),float(ask),"0.0001",fee_policy["multiplier"]))
            if mode=="paper":
                entry.update(filled=str(quantity),cost=str(quantity*ask),fees=str(fee),status="held",
                             paper_only=True,execution_verified=False,entry_at=now,
                             fill_model="current_top_ask_snapshot_no_depth_or_queue_guarantee")
                self.journal.save();continue
            with self.gate_factory(self.journal,self.broker) as gate:
                # Admission may take network round trips. Refresh the market price
                # after admission, then size only the unspent $1 remainder.
                market=self.broker.market(ticker)
                ask=market_ask(market,entry["side"],time.time(),entry["market_end"])
                quantity=quantity_remaining(entry,ask)
                if quantity<=0:
                    entry["status"]="held";self.journal.save();continue
                fee=money(taker_fee(float(quantity),float(ask),"0.0001",fee_policy["multiplier"]))
                if self.broker.balance(market["exchange_index"])<quantity*ask+fee+Decimal(".01"):
                    continue
                attempt=entry["attempt"]+1
                intent=order_payload(ticker,entry["side"],quantity,ask,market["exchange_index"],self.config,
                                     attempt=attempt,fractional_remainder=True)
                prepared_at=time.time()
                entry.update(attempt=attempt,last_attempt=prepared_at,pending={"intent":intent,"prepared_at":prepared_at})
                self.journal.save();gate.verify()
                try:
                    value=self.broker.submit(intent)
                    ack=value.get("order",value)
                    if not isinstance(ack.get("order_id"),str):raise RuntimeError("ACK_ORDER_ID_UNAVAILABLE")
                    entry["pending"]["order_id"]=ack["order_id"]
                    self.journal.save()
                except RuntimeError as error:
                    code=str(error)
                    if code=="KALSHI_HTTP_429":
                        entry["pending"]=None;entry["last_attempt"]=now+2;self.journal.save()
                    elif code in ("KALSHI_HTTP_400","KALSHI_HTTP_401","KALSHI_HTTP_403"):
                        entry["pending"]=None;entry["status"]="entry_rejected";self.journal.save();raise
                    else:
                        # Persisted intent is retained across all ambiguous outcomes.
                        entry["delivery_status"]="unknown_reconcile_before_retry";self.journal.save()

    def settle(self,entry,mode,market,now):
        if entry.get("pending"):return
        if market.get("status") not in ("settled","finalized") or market.get("result") not in ("yes","no"):return
        quantity=money(entry["filled"]);payout=quantity if market["result"]==entry["side"] else Decimal(0)
        if mode=="live" and quantity:
            rows=self.broker.pages("/portfolio/settlements","settlements",ticker=entry["ticker"])
            matches=[r for r in rows if r["ticker"]==entry["ticker"] and r.get("exchange_index")==market["exchange_index"]]
            if not matches:return
            if len(matches)!=1:raise RuntimeError("AMBIGUOUS_ACCOUNT_SETTLEMENT")
            row=matches[0];other="no" if entry["side"]=="yes" else "yes"
            revenue=money(row["revenue_dollars"]) if "revenue_dollars" in row else money(row["revenue"])/100
            if (row["market_result"]!=market["result"] or money(row[entry["side"]+"_count_fp"])!=quantity
                    or money(row[other+"_count_fp"])!=0 or revenue!=payout
                    or money(row[entry["side"]+"_total_cost_dollars"])!=money(entry["cost"])
                    or money(row["fee_cost"])!=money(entry["fees"])):
                raise RuntimeError("SETTLEMENT_RECONCILIATION_MISMATCH")
            entry["settlement"]=row
        net=payout-money(entry["cost"])-money(entry["fees"])
        entry.update(status="settled",settled_at=now,official_result=market["result"],net_pnl=str(net))
        totals=self.journal.state["totals"].setdefault(mode,{"trades":0,"wins":0,"losses":0,"net":"0","fees":"0"})
        if quantity:
            totals.update(trades=totals["trades"]+1,wins=totals["wins"]+int(net>0),losses=totals["losses"]+int(net<0),
                          net=str(money(totals["net"])+net),fees=str(money(totals["fees"])+money(entry["fees"])))
        self.journal.save()

def numerical_pair(market,n,rows,output):
    """Numerical child receives no exchange key; publication uses actual completion."""
    for name in ("KALSHI_API_KEY_ID","KALSHI_PRIVATE_KEY","FIREBASE_SERVICE_ACCOUNT_JSON","QUANTURA_RESEARCH_ARTIFACT_KEY"):
        os.environ.pop(name,None)
    from .kalshi_live_worker import forecast_pair
    forecast_pair(market,n,rows,output)

def complete_context(rows,ticker,opened,n,now):
    index={r["timestamp"]:r for r in rows if r["market_id"]==ticker and r.get("collection_mode")=="live"
           and r.get("timely") and r["received_at"]<=now}
    times=[opened+i*60 for i in range(1,n+1)]
    return [index[t] for t in times] if all(t in index for t in times) else None

def retire_markets(journal,store,now):
    """Archive genuine receipts before dropping completed markets from the buffer."""
    from .btc_minute_forecast_worker import rows as store_rows
    source=store_rows(store);decisions=journal.state["decisions"]
    for life in store.values("btc_lifecycle"):
        ticker=life["market_id"];group=journal.state["entries"].get(ticker,{})
        if (life.get("resolution_status")!="resolved" or life["close_at"]>=now-7200
                or any(e["status"]!="settled" for e in group.values())):continue
        selected=[r for r in source if r["kind"]!="checkpoints" and r["value"].get("market_id")==ticker]
        journal.archive(ticker,{"version":VERSION,"ticker":ticker,"entries":group,
            "decision":decisions.get(ticker),"rows":selected,"execution_code_sha":os.getenv("QUANTURA_CODE_SHA")})
        journal.state["entries"].pop(ticker,None);decisions.pop(ticker,None)
        with store.lock,store.db:
            for r in selected:
                store.db.execute("DELETE FROM records WHERE kind=? AND id=?",(r["kind"],r["id"]))
            store._put("checkpoints","finalized:"+ticker,{"market_id":ticker,"close_at":life["close_at"]})
        # Keep the latest confirmed outcome even during long overnight closures.
        settlements=[r for r in selected if r["kind"]=="btc_settlements"]
        latest=max(settlements,key=lambda r:r["value"]["first_confirmed_at"],default=None)
        if latest:
            with store.lock,store.db:
                store._put(latest["kind"],latest["id"],latest["value"],True)
    with store.lock,store.db:
        checkpoints=[(key,json.loads(gzip.decompress(raw))) for key,raw in
                     store.db.execute("SELECT id,data FROM records WHERE kind='checkpoints'") if key.startswith("finalized:")]
        for key,_ in sorted(checkpoints,key=lambda r:r[1]["close_at"],reverse=True)[128:]:
            store.db.execute("DELETE FROM records WHERE kind='checkpoints' AND id=?",(key,))
        settlements=store.values("btc_settlements")
        keep={digest([r["market_id"],r["result"],r["first_confirmed_at"]]) for r in
              sorted(settlements,key=lambda r:r["close_at"],reverse=True)[:16]}
        for key, in store.db.execute("SELECT id FROM records WHERE kind='btc_settlements'").fetchall():
            if key not in keep:store.db.execute("DELETE FROM records WHERE kind='btc_settlements' AND id=?",(key,))
    journal.state["rows"]=store_rows(store)

def run(config,mode,duration):
    from .btc_minute_archive import MinuteCollector, safe_failure
    from .btc_hold_tracking import first_signals, prospective_observations
    from .btc_sticky_tracking import direction_at
    from .interval_markets import KalshiIntervalProvider
    from .kalshi_live_worker import observations
    from .local_store import LocalStore
    broker=DollarBroker(config,requested_live=mode in ("live","both"))
    if mode=="readiness":
        orders,positions=broker.account()
        limits=broker.request("GET","/account/limits")
        journal=CloudJournal(config,broker.key_id,os.getenv("GITHUB_RUN_ID","local")+":readiness:"+config.series_ticker)
        journal.claim()
        try:journal.verify()
        finally:journal.release();broker.client.close()
        print(json.dumps({"readiness":"ok","resting_orders":len(orders),"positions":len(positions),
                          "api_tier":limits.get("usage_tier"),"encrypted_journal":"verified",
                          "orders_sent":0,"config":asdict(config)}));return
    journal=CloudJournal(config,broker.key_id,os.getenv("GITHUB_RUN_ID","local")+":"+config.series_ticker)
    journal.claim();trader=DollarTrader(config,broker,journal,mode)
    for decision in journal.state["decisions"].values():
        if decision.get("status")=="forecasting":decision["status"]="retry_inference"
    stopped=False
    def stop(*_):
        nonlocal stopped
        stopped=True
    signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
    deadline=min(time.time()+duration*60,float(os.getenv("QUANTURA_JOB_STARTED_AT",time.time()))+345*60)
    context=multiprocessing.get_context("spawn");child=None;result_queue=None;computing=None
    last_save=last_health=0
    try:
        with tempfile.TemporaryDirectory() as folder:
            store=LocalStore(VERSION,config.series_ticker,folder,capacity_bytes=64*1024*1024)
            with store.lock,store.db:
                for r in journal.state.get("rows",[]):store._put(r["kind"],r["id"],r["value"],True)
            collector=MinuteCollector(store,KalshiIntervalProvider(config.series_ticker,timeout=3,attempts=2))
            collector.start()
            try:
                while not stopped and time.time()<deadline:
                    now=int(time.time());minutes=store.values("btc_minutes");lives=store.values("btc_lifecycle")
                    decisions=journal.state["decisions"]
                    if child:
                        try:result=result_queue.get_nowait()
                        except queue.Empty:result=None
                        if result is not None:
                            child.join(timeout=1)
                            if child.is_alive():child.terminate();child.join()
                            decisions[computing].update(result=result,status="forecasted" if "forecasts" in result else "forecast_failed")
                            journal.save();child=None
                        elif not child.is_alive() or now>=compute_end-120:
                            child.terminate();child.join();child=None
                            decisions[computing]["status"]="forecast_missed_deadline";journal.save()
                    for life in lives:
                        ticker=life["market_id"];opened,end=life["open_at"],life["close_at"]
                        if life.get("resolution_status")=="resolved" or now>=end:continue
                        if (ticker not in decisions or decisions[ticker].get("status")=="retry_inference") and opened+config.history_minutes*60<=now:
                            rows=complete_context(minutes,ticker,opened,config.history_minutes,now)
                            if rows and not child and now<end-120:
                                decisions[ticker]={"status":"forecasting","started_at":now,"market":life["market"]}
                                journal.save();computing=ticker;compute_end=end;result_queue=context.Queue()
                                child=context.Process(target=numerical_pair,args=(life["market"],config.history_minutes,rows,result_queue));child.start()
                            elif now>=end-120:
                                decisions[ticker]={"status":"missing_timely_opening_context"};journal.save()
                        decision=decisions.get(ticker,{})
                        if decision.get("status")=="forecasted":
                            tape=prospective_observations(observations([r for r in minutes if r["market_id"]==ticker]),now)
                            signals,_=first_signals(decision["result"]["forecasts"],tape,now)
                            if signals:
                                first=signals[0]
                                direction=direction_at(store.values("btc_settlements"),first["signal_received_at"],ticker)
                                decision.update(signal=first,direction=direction,
                                    status="entry_wait" if direction and first["contract_id"].endswith(":"+direction["side"]) else "first_signal_disagreed")
                                journal.save()
                        if decision.get("status")=="entry_wait" and ticker not in journal.state["entries"]:
                            first=decision["signal"];quote=next((r for r in minutes if r["market_id"]==ticker and r["timestamp"]==first["signal_at"]+60),None)
                            if quote and now<=quote["timestamp"]+30:
                                trader.begin({**first,"sticky":decision["direction"]},quote,now);decision["status"]="entered";journal.save()
                            elif now>first["signal_at"]+90:
                                decision["status"]="missed_next_minute_entry";journal.save()
                    fee=store._get("checkpoints","btc_fee_policy") or {}
                    statuses={}
                    for ticker,group in list(journal.state["entries"].items()):
                        if all(e["status"]=="settled" for e in group.values()):continue
                        try:trader.reconcile(ticker,fee,int(time.time()))
                        except (RuntimeError,ValueError,KeyError,OSError) as error:
                            statuses[ticker]=safe_failure(error)
                            if str(error) in ("TRADER_FENCE_LOST","ENTRY_BUDGET_EXCEEDED"):
                                raise
                    if time.time()-last_save>=30:
                        retire_markets(journal,store,now)
                        journal.state["health"]={"at":now,"mode":mode,"statuses":list(statuses.values()),
                            "collector":store._get("checkpoints","btc_collector_health"),"orders_enabled":broker.enabled}
                        journal.save();last_save=time.time()
                    if time.time()-last_health>=60:
                        health=store._get("checkpoints","btc_collector_health") or {}
                        print(json.dumps({"event":"opposite_dollar_heartbeat","series":config.series_ticker,
                            "mode":mode,"at":now,"orders_enabled":broker.enabled,"error_codes":[r["error_code"] for r in statuses.values()],
                            "collector_status":health.get("status","starting"),
                            "collector_error_codes":[r.get("error_code") for r in health.get("errors",[])],
                            "forecast_statuses":dict(Counter(d.get("status") for d in decisions.values())),
                            "genuine_minute_receipts":len(minutes),
                            "decisions":len(decisions),"active_markets":sum(any(e["status"]!="settled" for e in g.values()) for g in journal.state["entries"].values()),
                            "firestore_writes":0}),flush=True);last_health=time.time()
                    time.sleep(max(0,1-(time.time()-now)))
            finally:
                collector.stop()
                if child and child.is_alive():child.terminate();child.join()
                from .btc_minute_forecast_worker import rows as store_rows
                journal.state["rows"]=store_rows(store);journal.save()
    finally:
        journal.release();broker.client.close()

def main():
    parser=argparse.ArgumentParser();parser.add_argument("--series",choices=list(STRATEGIES),required=True)
    parser.add_argument("--mode",choices=("config","readiness","paper","live","both"),default="config")
    parser.add_argument("--subaccount",type=int,default=0);parser.add_argument("--duration-minutes",type=int,default=300)
    args=parser.parse_args();config=DollarConfig(args.series,args.subaccount)
    if not 1<=args.duration_minutes<=300:raise ValueError("INVALID_DURATION")
    if args.mode=="config":print(json.dumps({"config":asdict(config),"observed_minutes":config.history_minutes,
                "forecast_minutes":15-config.history_minutes,"portfolio_fingerprint":portfolio_fingerprint(config.subaccount)}));return
    run(config,args.mode,args.duration_minutes)

if __name__=="__main__":main()
