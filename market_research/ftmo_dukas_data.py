"""Frozen, first-party Dukascopy hourly bid/ask observations for research.

No trading API, tick-by-tick downloads, filled gaps, or broker quote claims.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import gzip
import hashlib
import json
import math
from pathlib import Path
import time
import urllib.error
import urllib.request

UTC = timezone.utc
BASE = 'https://jetta.dukascopy.com/v1'
FTMO_SOURCE = 'https://ftmo.oanda.com/wp-json/ftmo/symbols'
INSTRUMENTS = {
    'EURUSD.sim': ('EUR-USD', .0010, .00004),
    'USDJPY.sim': ('USD-JPY', .10, .022),
    'GBPUSD.sim': ('GBP-USD', .0010, .00021),
    'GBPJPY.sim': ('GBP-JPY', .10, .106),
    'USDCAD.sim': ('USD-CAD', .0010, .00019),
    'US500.sim': ('USA500.IDX-USD', 10., .5),
    'US30.sim': ('USA30.IDX-USD', 10., 4.),
    'US100.sim': ('USATECH.IDX-USD', 10., 1.8),
    'XAUUSD.sim': ('XAU-USD', 1., 2.4),
    'BTCUSD.sim': ('BTC-USD', 100., 3.),
}


def stamp(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('EXPLICIT_UTC_OFFSET_REQUIRED')
    return result.astimezone(UTC)


def digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def get_json(url: str, attempts=5):
    if not (url.startswith(BASE+'/') or url == FTMO_SOURCE):
        raise ValueError('FIRST_PARTY_RESEARCH_ENDPOINT_REQUIRED')
    request = urllib.request.Request(url, headers={'Accept': 'application/json',
        'User-Agent': 'Quantura/1.0 (+https://quantura.studio)',
        'Referer': 'https://ftmo.oanda.com/simulated-assets/' if url == FTMO_SOURCE else 'https://www.dukascopy.com/'})
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(request, timeout=35) as response:
                data = response.read(12_000_001)
            if len(data) > 12_000_000:
                raise ValueError('PROVIDER_PAYLOAD_TOO_LARGE')
            result = json.loads(data)
            if not isinstance(result, dict):
                raise ValueError('PROVIDER_OBJECT_REQUIRED')
            return result
        except urllib.error.HTTPError as exc:
            if exc.code not in (408, 429, 500, 502, 503, 504) or attempt == attempts-1:
                raise RuntimeError(f'PROVIDER_HTTP_{exc.code}') from None
            retry = exc.headers.get('Retry-After', '')
            delay = max(60 if exc.code == 429 else 2**attempt, int(retry) if retry.isdigit() else 0)
            if delay > 180:
                raise RuntimeError('PROVIDER_RETRY_AFTER_EXCEEDS_JOB_BUDGET') from None
            print(json.dumps({'event': 'provider_retry', 'status': exc.code, 'wait_seconds': delay}), flush=True)
            time.sleep(delay)
        except (TimeoutError, urllib.error.URLError):
            if attempt == attempts-1:
                raise RuntimeError('PROVIDER_NETWORK_FAILED') from None
            time.sleep(2**attempt)
    raise RuntimeError('PROVIDER_DOWNLOAD_FAILED')


def decode(payload: dict, scale: int, interval_minutes=60) -> list[dict]:
    keys = ('times', 'opens', 'highs', 'lows', 'closes', 'volumes')
    if type(scale) is not int or not 0 <= scale <= 8 or any(not isinstance(payload.get(k), list) for k in keys):
        raise ValueError('INVALID_CANDLE_ENCODING')
    count = len(payload['times'])
    if count > 200_000 or any(len(payload[k]) != count for k in keys):
        raise ValueError('CANDLE_ARRAY_LENGTH_MISMATCH')
    if not count:
        return []
    current = float(payload['timestamp']); multiplier = float(payload['multiplier']); shift = float(payload['shift'])
    prices = [float(payload[k]) for k in ('open', 'high', 'low', 'close')]
    if not all(math.isfinite(v) for v in (current, multiplier, shift, *prices)) or multiplier <= 0 or shift <= 0:
        raise ValueError('INVALID_CANDLE_HEADER')
    rows = []; factor = 10**scale
    for i, delta in enumerate(payload['times']):
        if not isinstance(delta, (int, float)) or not math.isfinite(delta) or delta < 0 or (i and delta == 0):
            raise ValueError('INVALID_CANDLE_TIME_DELTA')
        current += delta*shift
        volume=payload['volumes'][i]
        if not isinstance(volume,(int,float))or not math.isfinite(volume)or volume<0:
            raise ValueError('INVALID_OBSERVED_VOLUME')
        if interval_minutes not in (1,60) or current % (interval_minutes*60_000) != 0:
            raise ValueError('NATIVE_CANDLE_NOT_UTC_ALIGNED')
        changes = [payload[k][i] for k in keys[1:5]]
        if not all(isinstance(v, (int, float)) and math.isfinite(v) for v in changes):
            raise ValueError('INVALID_PRICE_DELTA')
        prices = [math.floor((p+d*multiplier)*factor+.5)/factor for p, d in zip(prices, changes)]
        o,h,l,c = prices
        if not (0 < l <= min(o,c) <= max(o,c) <= h) or not math.isfinite(h):
            raise ValueError('INVALID_OBSERVED_OHLC')
        rows.append({'start': datetime.fromtimestamp(current/1000, UTC).isoformat(), 'o':o, 'h':h, 'l':l, 'c':c})
    return rows


def hourly_from_minutes(rows: list[dict]) -> list[dict]:
    buckets={}
    for row in sorted(rows,key=lambda r:r['start']):
        hour=stamp(row['start']).replace(minute=0,second=0,microsecond=0).isoformat()
        if hour not in buckets:buckets[hour]={**row,'start':hour}
        else:
            saved=buckets[hour];saved['h']=max(saved['h'],row['h']);saved['l']=min(saved['l'],row['l']);saved['c']=row['c']
    return list(buckets.values())


def months(start: datetime, end: datetime):
    current = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while current < end:
        yield current.year, current.month
        current = current.replace(year=current.year+1, month=1) if current.month == 12 else current.replace(month=current.month+1)


def download(symbol: str, start: date, end: date, output: Path):
    code = INSTRUMENTS[symbol][0]; output.mkdir(parents=True, exist_ok=True)
    metadata = get_json(BASE+'/instruments/'+code)
    if metadata.get('code') != code or not any(r.get('period') == 'HOUR' for r in metadata.get('histories', [])):
        raise ValueError('GENUINE_HOURLY_HISTORY_REQUIRED')
    first = datetime.combine(start-timedelta(days=90), datetime.min.time(), UTC)
    last = datetime.combine(end+timedelta(days=1), datetime.min.time(), UTC)+timedelta(hours=17)
    sides = {}; audit=[]
    for side in ('BID', 'ASK'):
        rows = {}
        for year, month in months(first, last):
            path = f'/candles/trade/hour/{code}/{side}/{year}/{month}'
            # Dukascopy's current-month H1 archive rejects "From time is too late".
            # Completed daily minute candles remain available. Aggregate that final
            # month into genuine UTC H1 bars without downloading annual tick files.
            now=datetime.now(UTC);current_month=(year,month)==(now.year,now.month)
            if current_month:
                decoded=[];day=datetime(year,month,1,tzinfo=UTC)
                while day<last and day.month==month:
                    if day+timedelta(days=1)>first:
                        minute_path=f'/candles/minute/{code}/{side}/{year}/{month}/{day.day}'
                        payload=get_json(BASE+minute_path)
                        minute_rows=decode(payload,metadata['priceScale'],1)
                        decoded.extend(hourly_from_minutes(minute_rows))
                        audit.append({'path':minute_path,'sha256':digest(payload),'native_interval':'M1','hourly_rows':len(hourly_from_minutes(minute_rows))})
                        time.sleep(1.25)
                    day+=timedelta(days=1)
            else:
                payload = get_json(BASE+path)
                decoded = decode(payload, metadata['priceScale'])
                audit.append({'path': path, 'sha256': digest(payload), 'native_interval':'H1', 'observed_hours': len(decoded)})
            for row in decoded:
                t = stamp(row['start'])
                if first <= t and t+timedelta(hours=1) <= last:
                    if t in rows and rows[t] != row:
                        raise ValueError('CONFLICTING_PROVIDER_CANDLE')
                    rows[t] = row
            # A bounded source matrix and paced requests avoid the tick feed bottleneck.
            time.sleep(1.25)
        sides[side] = rows
    shared = sorted(sides['BID'].keys() & sides['ASK'].keys())
    if not shared:
        raise ValueError('NO_PAIRED_BID_ASK_HOURS')
    rows=[]
    for t in shared:
        bid,ask = sides['BID'][t],sides['ASK'][t]
        if ask['o'] < bid['o'] or ask['c'] < bid['c']:
            raise ValueError('CROSSED_OBSERVED_QUOTE')
        rows.append({'start': t.isoformat(), 'end': (t+timedelta(hours=1)).isoformat(),
                     'bid': {k:bid[k] for k in ('o','h','l','c')}, 'ask': {k:ask[k] for k in ('o','h','l','c')}})
    with gzip.open(output/'quotes.jsonl.gz', 'wt') as stream:
        for row in rows:
            stream.write(json.dumps(row, separators=(',', ':'))+'\n')
    summary={'symbol':symbol,'dukascopy_code':code,'timeframe':'H1','metadata':metadata,
        'source':BASE,'downloaded_at':datetime.now(UTC).isoformat(),'timestamp_convention':'UTC bucket start; conservatively available at start + 1 hour',
        'warmup_start':first.isoformat(),'end':last.isoformat(),'paired_hours':len(rows),
        'bid_only_hours':len(sides['BID'].keys()-sides['ASK'].keys()),'ask_only_hours':len(sides['ASK'].keys()-sides['BID'].keys()),
        'rows_sha256':digest(rows),'files':audit,'orders_sent':0}
    (output/'source.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps({'symbol':symbol,'paired_hours':len(rows),'files':len(audit),'orders_sent':0}),flush=True)


def load_quotes(folder: Path) -> list[dict]:
    with gzip.open(folder/'quotes.jsonl.gz','rt') as stream:
        rows=[json.loads(line) for line in stream]
    source=json.loads((folder/'source.json').read_text())
    if digest(rows) != source['rows_sha256'] or [r['start'] for r in rows] != sorted({r['start'] for r in rows}):
        raise ValueError('FROZEN_QUOTES_INTEGRITY_FAILED')
    return rows


if __name__ == '__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--symbol',choices=INSTRUMENTS,required=True)
    parser.add_argument('--start',type=date.fromisoformat,required=True);parser.add_argument('--end',type=date.fromisoformat,required=True)
    parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    if args.start > args.end or (args.end-args.start).days>365:
        raise ValueError('BOUNDED_YEAR_REQUIRED')
    download(args.symbol,args.start,args.end,args.output)
