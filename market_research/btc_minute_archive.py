"""Independent, read-only BTC minute/settlement collector, including failed forecasts."""
from decimal import Decimal
import threading
import time

from .engine import digest, stamp

VERSION = 'btc_paired_minutes_v1'


def archive_minutes(store, provider, market, raw, received_at, historical=False):
    ticker = market['ticker']
    sides = provider.quotes(raw, market, received_at)
    yes = {q.timestamp: q for q in sides['yes']}
    no = {q.timestamp: q for q in sides['no']}
    candles = {r.get('end_period_ts'): r for r in raw}
    with store.lock, store.db:
        for timestamp in sorted(yes.keys() & no.keys()):
            y = yes[timestamp]
            candle = candles[timestamp]
            record = {'version': VERSION, 'market_id': ticker, 'event_id': market['event_ticker'],
                'timestamp': timestamp, 'received_at': received_at,
                'yes_ask': y.ask, 'yes_bid': y.bid,
                'no_ask': float(1-Decimal(str(y.bid))), 'no_bid': float(1-Decimal(str(y.ask))),
                'collection_mode': 'historical' if historical else 'live',
                'timely': not historical and 0 <= received_at-timestamp <= 30,
                'no_side_method': '1 - opposite YES bid/ask; binary-book complement',
                'source': 'kalshi_1min_candlestick_close',
                'raw_yes_bid': candle.get('yes_bid'), 'raw_yes_ask': candle.get('yes_ask'),
                'volume_fp': candle.get('volume_fp'), 'orderbook_depth_verified': False}
            key = digest([ticker, timestamp])
            previous = store._get('btc_minutes', key)
            if previous and any(previous[k] != record[k] for k in ('yes_ask','yes_bid','no_ask','no_bid')):
                # First-received values stay immutable; record later revisions separately.
                store._put('btc_minute_revisions', digest([key, record['yes_ask'], record['yes_bid']]), record, True)
            else:
                store._put('btc_minutes', key, record, True)
        expected = list(range(stamp(market['open_time'])+60,
                              min(received_at//60*60, stamp(market['close_time']))+1, 60))
        store._put('btc_minute_coverage', ticker, {'market_id':ticker, 'expected_minutes':len(expected),
            'returned_minutes':len(yes.keys() & no.keys()), 'missing_timestamps':[t for t in expected if t not in yes or t not in no],
            'checked_at':received_at})


def archive_settlement(store, market, received_at, historical=False):
    # Mutable provider responses never erase an already confirmed settlement.
    with store.lock, store.db:
        return _archive_settlement(store, market, received_at, historical)


def _archive_settlement(store, market, received_at, historical):
    ticker = market['ticker']
    key = ticker
    previous = store._get('btc_lifecycle', key) or {}
    result = market.get('result')
    resolved = market.get('status') in ('settled','finalized') and result in ('yes','no')
    if not resolved and previous.get('resolution_status') == 'resolved':
        return previous
    end = stamp(market['close_time'])
    row = {**previous, 'market_id':ticker, 'event_id':market['event_ticker'],
        'open_at':stamp(market['open_time']), 'close_at':end, 'checked_at':received_at,
        'status':market.get('status'), 'resolution_status':'resolved' if resolved else 'pending',
        'result':result if resolved else None, 'settlement_source':'kalshi_market_result',
        'official_settlement_ts':market.get('settlement_ts')}
    if resolved:
        same = previous.get('result') == result and previous.get('first_confirmed_at')
        row['first_confirmed_at'] = previous['first_confirmed_at'] if same else received_at
        row['confirmation_clock'] = 'recorded_receipt'
        if historical:
            try:
                value = market.get('settlement_ts')
                official = int(value) if isinstance(value,(int,float)) else stamp(value)
            except (ValueError,TypeError):
                official = 0
            row['first_confirmed_at'] = max(end+60, official)
            row['confirmation_clock'] = 'historical_official_or_assumed_60s_floor'
    row['market'] = market
    store._put('btc_lifecycle', key, row)
    if resolved:
        # Keep revisions as receipt-timed events, not retroactive truth changes.
        store._put('btc_settlements', digest([ticker, result, row['first_confirmed_at']]), row, True)
    return row


class MinuteCollector:
    """Network I/O independent of CPU inference; shares the existing encrypted DB."""
    def __init__(self, store, provider):
        self.store, self.provider = store, provider
        self.stop_event = threading.Event()
        self.thread = None
        self.markets = {}
        self.last_discovery = self.last_fee = 0
        self.discovery_cursor_present = False
        for row in store.values('btc_lifecycle'):
            if row.get('resolution_status') != 'resolved' and row.get('market'):
                self.markets[row['market_id']] = row['market']

    def tick(self, now):
        errors = []
        if now-self.last_discovery >= 60:
            # Covers recent settlements (including games without forecasts) plus current games.
            queries=[{'series_ticker':'KXBTC15M','min_close_ts':now-3600,
                      'max_close_ts':now+1800,'limit':100},
                     {'series_ticker':'KXBTC15M','status':'settled','limit':20}]
            self.discovery_cursor_present=False
            for params in queries:
                try:
                    response=self.provider.get('/markets',params)
                    if 'status' not in params:
                        self.discovery_cursor_present=bool(response.get('cursor'))
                    for market in response.get('markets',[]):
                        if self.provider.valid_market(market):
                            self.markets[market['ticker']]=market
                            archive_settlement(self.store,market,int(time.time()))
                except (RuntimeError,ValueError,OSError,KeyError) as error:
                    errors.append({'operation':'settlements' if 'status' in params else 'discovery',
                                   'error_type':type(error).__name__})
            self.last_discovery = now
        for ticker, market in list(self.markets.items()):
            if self.stop_event.is_set() or self.store.at_capacity:
                break
            start, end = stamp(market['open_time']), stamp(market['close_time'])
            try:
                if start+60 <= now <= end+120:
                    raw = self.provider.candles(market, now)
                    archive_minutes(self.store, self.provider, market, raw, int(time.time()))
                if now > end and (self.store._get('btc_lifecycle', ticker) or {}).get('resolution_status') != 'resolved':
                    latest = self.provider.market(ticker)
                    if latest.get('ticker') != ticker:
                        raise ValueError('KALSHI_RESOLUTION_IDENTITY_MISMATCH')
                    archive_settlement(self.store, latest, int(time.time()))
            except (RuntimeError, ValueError, OSError, KeyError) as error:
                errors.append({'market_id':ticker, 'error_type':type(error).__name__})
            if now > end+120 and (self.store._get('btc_lifecycle', ticker) or {}).get('resolution_status') == 'resolved':
                self.markets.pop(ticker)
        if now-self.last_fee >= 3600:
            series = self.provider.get('/series/KXBTC15M')['series']
            fee = {'observed_at':int(time.time()),'fee_type':series.get('fee_type'),
                   'multiplier':series.get('fee_multiplier'),'source':'kalshi_series_KXBTC15M'}
            self.store.checkpoint('btc_fee_policy', fee)
            self.last_fee = now
        self.store.checkpoint('btc_collector_health', {'at':int(time.time()),'status':'degraded' if errors else 'ok',
            'tracked_markets':len(self.markets),'discovery_cursor_present':self.discovery_cursor_present,
            'errors':errors})

    def start(self):
        def run():
            while not self.stop_event.is_set():
                try:
                    self.tick(int(time.time()))
                except (RuntimeError,ValueError,OSError,KeyError) as error:
                    self.store.checkpoint('btc_collector_health', {'at':int(time.time()),
                        'status':'error','error_type':type(error).__name__})
                self.stop_event.wait(15)
        self.thread = threading.Thread(target=run,name='btc-minute-archive',daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=30)
