"""Read-only exchange-event tape for BTC first32/60/90/120-quote research.

No polling-generated observations, no resampling, no exchange orders. A gap or
late subscription disqualifies first-N claims. The minute studies remain intact.
"""
import argparse
import asyncio
import base64
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
import json
import os
import re
import time
import uuid

from .engine import digest, stamp
from .kalshi_btc import KalshiBTCProvider, SERIES
from .local_store import LocalStore

VERSION = 'btc_exchange_bbo_events_v1'
COUNTS = (32, 60, 90, 120)
WS_PATH = '/trade-api/ws/v2'
WS_URL = 'wss://external-api-ws.kalshi.com' + WS_PATH


def auth_headers(now_ms=None):
    """Same RSA-PSS contract as the existing server-side Kalshi signer."""
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding, rsa
    key_id=os.getenv('KALSHI_PROD_API_KEY') or os.getenv('KALSHI_API_KEY_ID')
    pem=os.getenv('KALSHI_PRIVATE_KEY','').replace('\\n','\n')
    if not key_id or not pem:raise ValueError('KALSHI_STREAM_CREDENTIALS_REQUIRED')
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,200}',key_id):raise ValueError('INVALID_KALSHI_KEY_ID')
    try:key=serialization.load_pem_private_key(pem.encode(),password=None)
    except (ValueError,TypeError):raise ValueError('INVALID_KALSHI_SIGNING_KEY') from None
    if not isinstance(key,rsa.RSAPrivateKey):raise ValueError('KALSHI_RSA_KEY_REQUIRED')
    timestamp=str(int(time.time()*1000) if now_ms is None else now_ms)
    signature=key.sign((timestamp+'GET'+WS_PATH).encode(),
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()),salt_length=hashes.SHA256().digest_size),hashes.SHA256())
    return {'KALSHI-ACCESS-KEY':key_id,'KALSHI-ACCESS-TIMESTAMP':timestamp,
            'KALSHI-ACCESS-SIGNATURE':base64.b64encode(signature).decode()}


def number(value, *, price=False):
    try:n=Decimal(str(value))
    except InvalidOperation:raise ValueError('INVALID_BOOK_NUMBER') from None
    if not n.is_finite() or (price and not 0<=n<=1):raise ValueError('INVALID_BOOK_NUMBER')
    return n


@dataclass
class Book:
    market: dict
    connection_id: str
    levels: dict = field(default_factory=lambda:{'yes':{},'no':{}})
    seq: int | None = None
    sid: int | None = None
    snapshot_received_ms: int | None = None
    last_exchange_ms: int | None = None
    continuity_lost: bool = False
    previous: dict = field(default_factory=dict)
    rows: dict = field(default_factory=lambda:{'yes':[],'no':[]})
    stats: Counter = field(default_factory=Counter)

    @property
    def opened_ms(self):return stamp(self.market['open_time'])*1000

    @property
    def end_ms(self):return stamp(self.market['close_time'])*1000

    def bbo(self):
        if any(not self.levels[s] for s in ('yes','no')):return {}
        y,n=(max(self.levels[s]) for s in ('yes','no'))
        if y+n>1:raise ValueError('CROSSED_EXCHANGE_BOOK')
        ys,ns=self.levels['yes'][y],self.levels['no'][n]
        return {'yes':(y,1-n,ys,ns),'no':(n,1-y,ns,ys)}

    def accept(self,message,received_ms):
        """Apply one sequence-checked message; return changed side BBO quotes.

        A quote update means best bid/ask price OR displayed size changed. Deep
        book-only deltas are applied but not falsely counted as another BBO quote.
        Multiple changes with the same exchange millisecond remain separate.
        """
        kind=message.get('type');payload=message.get('msg',{})
        if kind not in ('orderbook_snapshot','orderbook_delta'):return []
        if payload.get('market_ticker')!=self.market['ticker']:raise ValueError('BOOK_MARKET_MISMATCH')
        seq,sid=message.get('seq'),message.get('sid')
        if type(seq) is not int or type(sid) is not int or min(seq,sid)<1:
            raise ValueError('BOOK_SEQUENCE_REQUIRED')
        if self.seq is not None:
            if seq<=self.seq:
                self.continuity_lost=True
                raise ValueError('BOOK_SEQUENCE_REPLAY_OR_REORDER')
            if seq!=self.seq+1 or sid!=self.sid:
                self.continuity_lost=True
                raise ValueError('BOOK_SEQUENCE_GAP')
        if kind=='orderbook_snapshot':
            if self.snapshot_received_ms is not None:
                self.continuity_lost=True
                raise ValueError('UNREQUESTED_BOOK_RESNAPSHOT')
            for side in ('yes','no'):
                levels={}
                for p,s in payload.get(side+'_dollars_fp',[]):
                    p,s=number(p,price=True),number(s)
                    if s<=0 or p in levels:raise ValueError('INVALID_BOOK_SNAPSHOT')
                    levels[p]=s
                self.levels[side]=levels
            self.snapshot_received_ms=received_ms
            # Initial state is NOT an observed exchange quote event.
            self.previous=self.bbo()
            self.seq,self.sid=seq,sid
            self.stats['snapshots']+=1
            return []
        if self.snapshot_received_ms is None:raise ValueError('BOOK_SNAPSHOT_REQUIRED')
        timestamp=payload.get('ts_ms')
        if type(timestamp) is not int:raise ValueError('EXCHANGE_MILLISECOND_TIMESTAMP_REQUIRED')
        if timestamp>received_ms or (self.last_exchange_ms is not None and timestamp<self.last_exchange_ms):
            self.continuity_lost=True
            raise ValueError('NONCAUSAL_EXCHANGE_TIMESTAMP')
        side=payload.get('side')
        if side not in ('yes','no'):raise ValueError('INVALID_BOOK_SIDE')
        p=number(payload.get('price_dollars'),price=True)
        change=number(payload.get('delta_fp'))
        size=self.levels[side].get(p,Decimal(0))+change
        if size<0:raise ValueError('NEGATIVE_BOOK_SIZE')
        if size:self.levels[side][p]=size
        else:self.levels[side].pop(p,None)
        self.seq,self.sid,self.last_exchange_ms=seq,sid,timestamp
        self.stats['exchange_deltas']+=1
        current=self.bbo();output=[]
        for side,state in current.items():
            if state==self.previous.get(side):continue
            if not self.opened_ms<=timestamp<self.end_ms:continue
            bid,ask,bid_size,ask_size=state
            row={'provider':'kalshi','market_id':self.market['ticker'],'side':side,
                'connection_id':self.connection_id,'subscription_id':sid,'sequence':seq,
                'exchange_timestamp_ms':timestamp,'received_at_ms':received_ms,
                'bid':str(bid),'ask':str(ask),'bid_size':str(bid_size),'ask_size':str(ask_size),
                'ordinal':len(self.rows[side])+1,'observed':True,'source':'orderbook_delta',
                'synthetic':False,'unit':'decimal_probability','first_from_open_eligible':self.eligible}
            self.rows[side].append(row);output.append(row)
        if not output:self.stats['non_counted_deltas']+=1
        self.previous=current
        return output

    @property
    def eligible(self):
        return (self.snapshot_received_ms is not None and self.snapshot_received_ms<=self.opened_ms
                and not self.continuity_lost)

    def first(self,side,count):
        if side not in ('yes','no') or type(count) is not int or count not in COUNTS:
            raise ValueError('INVALID_QUOTE_COUNT_ARM')
        if not self.eligible:raise ValueError('COMPLETE_OPENING_QUOTE_SEQUENCE_REQUIRED')
        if len(self.rows[side])<count:raise ValueError('INSUFFICIENT_EXCHANGE_QUOTES')
        rows=self.rows[side][:count]
        return {'version':VERSION,'market':self.market,'side':side,'quote_count':count,
            'rows':rows,'source_hash':digest(rows),'timebase':'exchange_event_order',
            'cutoff_exchange_ms':rows[-1]['exchange_timestamp_ms'],
            'input_available_ms':max(r['received_at_ms'] for r in rows),
            'elapsed_from_open_seconds':(rows[-1]['exchange_timestamp_ms']-self.opened_ms)/1000,
            'input_span_seconds':(rows[-1]['exchange_timestamp_ms']-rows[0]['exchange_timestamp_ms'])/1000,
            'remaining_market_seconds':(self.end_ms-rows[-1]['exchange_timestamp_ms'])/1000,
            'forecast_status':'not_run_event_time_model_validation_required',
            'redistribution_status':'review_required'}


def save_update(store,book,rows):
    with store.lock,store.db:
        for row in rows:
            store._put('exchange_quotes',digest([book.connection_id,row['side'],row['sequence']]),row,True)
        if book.eligible:
            for side in ('yes','no'):
                for count in COUNTS:
                    key=digest([VERSION,book.market['ticker'],book.connection_id,side,count])
                    if len(book.rows[side])>=count and store._get('quote_windows',key) is None:
                        store._put('quote_windows',key,book.first(side,count),True)
        store._put('quote_coverage',book.connection_id,{
            'market':book.market['ticker'],'connection_id':book.connection_id,
            'eligible_from_open':book.eligible,'sequence':book.seq,
            'counts':{s:len(r) for s,r in book.rows.items()},'stats':dict(book.stats),
            'gap_detected':book.continuity_lost,'last_exchange_ms':book.last_exchange_ms})


async def capture(market,store,deadline):
    import websockets
    connection_id=uuid.uuid4().hex;book=Book(market,connection_id)
    try:
        async with websockets.connect(WS_URL,additional_headers=auth_headers(),
                open_timeout=15,close_timeout=5,ping_interval=20,ping_timeout=20,
                max_size=2**20,max_queue=256) as socket:
            await socket.send(json.dumps({'id':1,'cmd':'subscribe','params':{
                'channels':['orderbook_delta'],'market_tickers':[market['ticker']]}}))
            while time.monotonic()<deadline and time.time()*1000<book.end_ms:
                try:raw=await asyncio.wait_for(socket.recv(),timeout=min(1,max(.01,deadline-time.monotonic())))
                except asyncio.TimeoutError:continue  # quiet second produces NO observation
                received_ms=time.time_ns()//1_000_000
                message=json.loads(raw)
                if message.get('type')=='error':raise ValueError('KALSHI_SUBSCRIPTION_REJECTED')
                if message.get('type')=='orderbook_delta' and book.last_exchange_ms is None and book.snapshot_received_ms is None:
                    raise ValueError('BOOK_SNAPSHOT_REQUIRED')
                rows=book.accept(message,received_ms)
                save_update(store,book,rows)
                if store.at_capacity:raise ValueError('QUOTE_TAPE_LOCAL_CAPACITY_REACHED')
    except Exception:
        book.continuity_lost=True
        save_update(store,book,[])
        raise
    finally:
        save_update(store,book,[])


async def collect(store,minutes):
    provider=KalshiBTCProvider();deadline=time.monotonic()+minutes*60;attempted=set()
    while time.monotonic()<deadline:
        response=await asyncio.to_thread(provider.get,'/markets',{'series_ticker':SERIES,'limit':100})
        now=time.time()
        markets=sorted((m for m in response.get('markets',[]) if provider.valid_market(m)
                       and m['ticker'] not in attempted and now+5<=stamp(m['open_time'])<=now+900),
                       key=lambda m:m['open_time'])
        if not markets:
            await asyncio.sleep(min(10,max(.01,deadline-time.monotonic())));continue
        market=markets[0];attempted.add(market['ticker'])
        # Subscribe before opening. Never reconnect mid-market and claim its
        # first32 quotes; wait for the next eligible market after interruption.
        try:await capture(market,store,deadline)
        except Exception as error:
            print(json.dumps({'event':'btc_quote_capture_failed','market':market['ticker'],
                              'error_type':type(error).__name__}),flush=True)
        print(json.dumps({'event':'btc_quote_capture_checkpoint','markets_attempted':len(attempted),
                          'quote_count':store.db.execute("SELECT count(*) FROM records WHERE kind='exchange_quotes'").fetchone()[0]}),flush=True)


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--duration-minutes',type=int,choices=(30,60,120),default=60)
    args=parser.parse_args()
    auth_headers()  # credential validation before touching data or connecting
    store=LocalStore('btc-exchange-quotes',os.getenv('GITHUB_RUN_ID','local'),capacity_bytes=256*1024*1024)
    store.claim({'version':VERSION,'counts':list(COUNTS),'paper_only':True,'orders_enabled':False,
                 'code_sha':os.getenv('QUANTURA_CODE_SHA','local'),
                 'run_id':os.getenv('GITHUB_RUN_ID','local')})
    try:asyncio.run(collect(store,args.duration_minutes))
    finally:store.release()
    count=store.db.execute("SELECT count(*) FROM records WHERE kind='quote_windows'").fetchone()[0]
    print(json.dumps({'event':'btc_quote_collection_complete','frozen_windows':count,'forecasts':0,'trades':0}),flush=True)
    if not count:raise RuntimeError('NO_COMPLETE_OPENING_QUOTE_WINDOWS')


if __name__=='__main__':main()
