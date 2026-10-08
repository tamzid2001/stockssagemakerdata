"""Authenticated, bounded Dukascopy native minute archives for research.

Requester Pays S3 access is the provider's documented bulk route. No browser
challenge is bypassed and no AWS credential is stored in a research artifact.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
import gzip
import hashlib
import json
import lzma
import math
from pathlib import Path
import struct

from .ftmo_dukas_data import INSTRUMENTS, UTC, digest, hourly_from_minutes, stamp

BUCKET = 'cfg-public-proper-wallaby'
REGION = 'eu-west-1'
RECORD = struct.Struct('>5If')  # seconds, open, CLOSE, LOW, HIGH, volume
CATALOG = Path(__file__).resolve().parents[1]/'quantura_site/functions_explore/src/dukascopyInstruments.json'


def instrument(symbol):
    if symbol not in INSTRUMENTS:
        raise ValueError('APPROVED_FTMO_INSTRUMENT_REQUIRED')
    catalog = json.loads(CATALOG.read_text())
    matches = [r for r in catalog['instruments'] if r['code']==INSTRUMENTS[symbol][0]]
    if len(matches)!=1 or type(matches[0]['priceScale']) is not int:
        raise ValueError('VERIFIED_INSTRUMENT_PRICE_SCALE_REQUIRED')
    item = matches[0]
    return {**item, 'archive_symbol': ''.join(c for c in item['code'] if c.isalnum()),
            'catalog_sha256': digest(catalog)}


def client(profile=None):
    import boto3
    from botocore.config import Config
    return boto3.Session(profile_name=profile).client('s3', region_name=REGION,
            config=Config(connect_timeout=10, read_timeout=35, max_pool_connections=16,
                          retries={'mode':'standard','total_max_attempts':5}))


def decode_candles(compressed, day, scale):
    if not compressed:
        raise ValueError('EMPTY_CANDLE_ARCHIVE')
    decompressor = lzma.LZMADecompressor(memlimit=64*1024*1024)
    try:
        raw = decompressor.decompress(compressed, max_length=1440*RECORD.size+1)
    except lzma.LZMAError:
        raise ValueError('INVALID_CANDLE_COMPRESSION') from None
    if not decompressor.eof or decompressor.unused_data or len(raw)>1440*RECORD.size or len(raw)%RECORD.size:
        raise ValueError('INVALID_CANDLE_ARCHIVE_LENGTH')
    if type(scale) is not int or not 0<=scale<=8:
        raise ValueError('INVALID_VERIFIED_PRICE_SCALE')
    origin = datetime.combine(day, datetime.min.time(), UTC)
    rows = []
    prior = -1
    for seconds, opened, closed, low, high, volume in RECORD.iter_unpack(raw):
        if seconds%60 or not prior<seconds<86400 or not math.isfinite(volume) or volume<0:
            raise ValueError('INVALID_NATIVE_MINUTE_RECORD')
        prior = seconds
        observed = volume>0 or len({opened,closed,low,high})>1
        # The native archive includes stale flat, zero-volume placeholders.
        # They are retained only for pairing, then removed unless either side
        # has evidence of a genuine observation during that minute.
        if not (0<low<=min(opened,closed)<=max(opened,closed)<=high) and (observed or any((opened,closed,low,high))):
            raise ValueError('INVALID_NATIVE_CANDLE_OHLC')
        rows.append({'start':(origin+timedelta(seconds=seconds)).isoformat(),
                     'o':opened/10**scale, 'h':high/10**scale, 'l':low/10**scale,
                     'c':closed/10**scale, 'observed':observed})
    return rows


def get_archive(s3, key):
    try:
        response = s3.get_object(Bucket=BUCKET, Key=key, RequestPayer='requester', ChecksumMode='ENABLED')
    except Exception as exc:
        code = getattr(exc, 'response', {}).get('Error', {}).get('Code')
        if code in ('NoSuchKey','404'):
            return None, {'key':key, 'absent':True}
        raise RuntimeError('AUTHENTICATED_DUKASCOPY_ARCHIVE_FAILED') from None
    stream = response['Body']
    try:
        if not 0<response['ContentLength']<=512_000:
            raise ValueError('NATIVE_MINUTE_ARCHIVE_SIZE_INVALID')
        payload = stream.read(512_001)
        if len(payload)!=response['ContentLength']:
            raise ValueError('TRUNCATED_NATIVE_MINUTE_ARCHIVE')
    finally:
        stream.close()
    etag = response.get('ETag','').strip('"')
    if len(etag)==32 and hashlib.md5(payload, usedforsecurity=False).hexdigest()!=etag:
        raise ValueError('ARCHIVE_ETAG_INTEGRITY_FAILED')
    return payload, {'key':key, 'version_id':response.get('VersionId'), 'etag':etag,
                     'compressed_sha256':hashlib.sha256(payload).hexdigest(), 'bytes':len(payload)}


def download_day(s3, item, day):
    sides = {}
    audit = []
    for side in ('BID','ASK'):
        key=f"{item['archive_symbol']}/{day.year}/{day.month-1:02d}/{day.day:02d}/{side}_candles_min_1.bi5"
        payload, record = get_archive(s3, key)
        decoded = decode_candles(payload,day,item['priceScale']) if payload is not None else []
        record['native_records'] = len(decoded)
        record['genuine_side_minutes'] = sum(r['observed'] for r in decoded)
        sides[side] = {r['start']:r for r in decoded}
        audit.append(record)
    if bool(sides['BID'])!=bool(sides['ASK']):
        raise ValueError('MISSING_ONE_SIDE_OF_NATIVE_ARCHIVE')
    rows = []
    unmatched = 0
    placeholders = 0
    for at in sorted(sides['BID'].keys()|sides['ASK'].keys()):
        bid,ask = sides['BID'].get(at),sides['ASK'].get(at)
        if bid is None or ask is None:
            unmatched += 1
            continue
        if not bid['observed'] and not ask['observed']:
            placeholders += 1
            continue
        if min(bid['l'],ask['l'])<=0 or ask['o']<bid['o'] or ask['c']<bid['c']:
            raise ValueError('INVALID_PAIRED_NATIVE_QUOTE')
        rows.append({'start':at,'end':(stamp(at)+timedelta(minutes=1)).isoformat(),
                     'bid':{k:bid[k] for k in ('o','h','l','c')}, 'ask':{k:ask[k] for k in ('o','h','l','c')}})
    if unmatched:
        raise ValueError('UNMATCHED_NATIVE_MINUTE_SIDES')
    return rows, {'day':day.isoformat(), 'files':audit, 'paired_observed_minutes':len(rows),
                   'excluded_placeholder_minutes':placeholders}


def verify_access(day, profile=None):
    s3=client(profile)
    rows,audit=download_day(s3,instrument('EURUSD.sim'),day)
    if not rows:
        raise ValueError('LATEST_REPLAY_DAY_HAS_NO_SOURCE_OBSERVATIONS')
    return {'source':f's3://{BUCKET}', 'region':REGION, 'day':day.isoformat(),
            'paired_observed_minutes':len(rows), 'audit':audit, 'credentials_persisted':False}


def download(symbol, start, end, output, *, workers=8, profile=None, warmup_days=1100):
    if start>end or (end-start).days>=365 or not 1<=workers<=8 or not 700<=warmup_days<=1500:
        raise ValueError('BOUNDED_SOURCE_REQUEST_REQUIRED')
    output.mkdir(parents=True,exist_ok=True)
    item=instrument(symbol)
    s3=client(profile)
    first_day=start-timedelta(days=warmup_days)
    first=datetime.combine(first_day,datetime.min.time(),UTC)
    last=datetime.combine(end+timedelta(days=1),datetime.min.time(),UTC)+timedelta(hours=17)
    days=[first_day+timedelta(days=i) for i in range((last.date()-first_day).days+1)]
    replay_start=datetime.combine(start,datetime.min.time(),UTC)
    hourly=[]
    audit=[]
    minute_count=0
    hasher=hashlib.sha256(b'[')
    with gzip.open(output/'minutes.jsonl.gz','wt') as stream, ThreadPoolExecutor(max_workers=workers) as pool:
        # Ordered consumption gives deterministic close semantics even when
        # network requests complete in another order. No tick archives fetched.
        for minutes,record in _bounded_days(pool,s3,item,days,workers):
            audit.append(record)
            minutes=[r for r in minutes if first<=stamp(r['start']) and stamp(r['end'])<=last]
            bid=hourly_from_minutes([{**r['bid'],'start':r['start']} for r in minutes])
            ask=hourly_from_minutes([{**r['ask'],'start':r['start']} for r in minutes])
            if len(bid)!=len(ask):
                raise ValueError('PAIRED_HOUR_AGGREGATION_MISMATCH')
            for b,a in zip(bid,ask):
                if b['start']!=a['start']:
                    raise ValueError('PAIRED_HOUR_TIME_MISMATCH')
                at=stamp(b['start'])
                if at+timedelta(hours=1)<=last:
                    hourly.append({'start':b['start'],'end':(at+timedelta(hours=1)).isoformat(),
                                   'bid':{k:b[k] for k in ('o','h','l','c')},'ask':{k:a[k] for k in ('o','h','l','c')}})
            for row in minutes:
                if stamp(row['start'])<replay_start:
                    continue
                if minute_count:
                    hasher.update(b',')
                hasher.update(json.dumps(row,sort_keys=True,separators=(',',':')).encode())
                stream.write(json.dumps(row,separators=(',',':'))+'\n')
                minute_count+=1
            if len(audit)%100==0:
                print(json.dumps({'event':'authenticated_archive_progress','symbol':symbol,'days':len(audit),
                                  'total_days':len(days),'replay_minutes':minute_count}),flush=True)
    hasher.update(b']')
    if not hourly or not minute_count:
        raise ValueError('NO_AUTHENTICATED_PAIRED_OBSERVATIONS')
    with gzip.open(output/'quotes.jsonl.gz','wt') as stream:
        for row in hourly:
            stream.write(json.dumps(row,separators=(',',':'))+'\n')
    common={'symbol':symbol,'dukascopy_code':item['code'],'source':f's3://{BUCKET}',
            'region':REGION,'request_payer':'requester','metadata':item,'downloaded_at':datetime.now(UTC).isoformat(),
            'timestamp_convention':'UTC bucket start; conservatively available at bucket end',
            'warmup_start':first.isoformat(),'end':last.isoformat(),'orders_sent':0,'credentials_persisted':False,
            'coverage':{'archive_days':len(audit),
                        'absent_archive_days':[r['day'] for r in audit if all(f.get('absent') for f in r['files'])],
                        'days_without_genuine_minutes':[r['day'] for r in audit if not r['paired_observed_minutes']],
                        'excluded_placeholder_minutes':sum(r['excluded_placeholder_minutes'] for r in audit),
                        'missing_intervals_filled':False}}
    (output/'source.json').write_text(json.dumps({**common,'timeframe':'H1','paired_hours':len(hourly),
              'rows_sha256':digest(hourly),'files':audit},indent=2)+'\n')
    (output/'minutes-source.json').write_text(json.dumps({**common,'timeframe':'M1','paired_minutes':minute_count,
              'rows_sha256':hasher.hexdigest(),'audit':audit},indent=2)+'\n')
    print(json.dumps({'event':'authenticated_source_complete','symbol':symbol,'paired_hours':len(hourly),
                      'paired_minutes':minute_count,'days':len(days),'orders_sent':0}),flush=True)


def _bounded_days(pool,s3,item,days,workers):
    # Python 3.12 map eagerly schedules everything. Keep at most eight daily
    # payloads resident; one slow day cannot retain a year's minute candles.
    from collections import deque
    queue=deque()
    iterator=iter(days)
    for _ in range(workers):
        try:queue.append(pool.submit(download_day,s3,item,next(iterator)))
        except StopIteration:break
    while queue:
        yield queue.popleft().result()
        try:queue.append(pool.submit(download_day,s3,item,next(iterator)))
        except StopIteration:pass


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--symbol',choices=INSTRUMENTS,required=True)
    parser.add_argument('--start',type=date.fromisoformat,required=True)
    parser.add_argument('--end',type=date.fromisoformat,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--profile')
    args=parser.parse_args()
    download(args.symbol,args.start,args.end,args.output,profile=args.profile)
