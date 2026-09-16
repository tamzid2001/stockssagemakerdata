"""Discover actual crypto/commodity fifteen-minute contracts, not guessed tickers."""
import re
import threading
import time
from .engine import stamp
from .kalshi_btc import KalshiBTCProvider


class KalshiIntervalProvider(KalshiBTCProvider):
    _request_lock = threading.Lock()
    _last_request = 0.
    def __init__(self, series_ticker='KXBTC15M', **kwargs):
        if not re.fullmatch(r'KX[A-Z0-9]{1,20}15M', series_ticker):
            raise ValueError('INVALID_INTERVAL_SERIES')
        super().__init__(**kwargs)
        self.series_ticker = series_ticker

    def get(self, path, params=None):
        # A process-wide bound prevents 14 collectors bursting at one minute
        # boundary. Each worker still retries provider throttling via the base.
        with self._request_lock:
            delay = .2 - (time.monotonic() - KalshiIntervalProvider._last_request)
            if delay > 0:
                time.sleep(delay)
            KalshiIntervalProvider._last_request = time.monotonic()
        return super().get(path, params)

    def valid_market(self, market):
        try:
            return (bool(re.fullmatch(re.escape(self.series_ticker)+r'-[A-Z0-9-]+', market['ticker']))
                    and market['market_type']=='binary'
                    and stamp(market['close_time'])-stamp(market['open_time'])==900)
        except (KeyError, TypeError, ValueError):
            return False


def discover_series(provider=None, maximum=40):
    provider = provider or KalshiIntervalProvider(timeout=10, attempts=2)
    if not 1 <= maximum <= 40:
        raise ValueError('INVALID_SERIES_LIMIT')
    inventory = provider.get('/series').get('series', [])
    eligible = sorted((s for s in inventory if s.get('category') in ('Crypto', 'Commodities')
        and s.get('frequency')=='fifteen_min' and re.fullmatch(r'KX[A-Z0-9]{1,20}15M', s.get('ticker',''))),
        key=lambda s: (s['ticker']!='KXBTC15M', s['ticker']))
    active, unavailable = [], []
    for s in eligible:
        if s['ticker'] in ('KXCRYPTOLEAD15M', 'KXCRYPTOCOMP15M'):
            unavailable.append({'ticker':s['ticker'],'reason':'relative_coin_race_not_directional_price_up_down'})
            continue
        p = KalshiIntervalProvider(s['ticker'], timeout=10, attempts=2)
        try:
            response=provider.get('/markets', {'series_ticker':s['ticker'], 'status':'open', 'limit':5})
            markets=[m for m in response.get('markets',[]) if p.valid_market(m)]
            verification = 'open_contract'
            if not markets:
                # Around a 15-minute boundary the next open contract may not yet
                # be indexed. A verified recent contract keeps its collector on.
                cutoff = int(time.time())-3600
                response = provider.get('/markets', {'series_ticker':s['ticker'], 'min_close_ts':cutoff, 'limit':5})
                markets = [m for m in response.get('markets', []) if p.valid_market(m) and stamp(m['close_time']) >= cutoff]
                verification = 'recent_contract_within_hour'
            if markets:
                active.append({'ticker':s['ticker'],'title':s['title'],'category':s['category'],
                               'verified_contract':markets[0]['ticker'],'verification':verification})
            else:
                unavailable.append({'ticker':s['ticker'],'reason':'no_active_exact_15minute_binary_contract'})
        except (RuntimeError, ValueError, OSError) as error:
            unavailable.append({'ticker':s['ticker'],'reason':type(error).__name__})
    return {'series':active[:maximum], 'active_verified_count':len(active),
            'eligible_series_count':len(eligible),'not_selected':active[maximum:],
            'unavailable':unavailable,'selection':'BTC first, then stable ticker order among verified active contracts',
            'all_series_claimed':len(active)<=maximum}
