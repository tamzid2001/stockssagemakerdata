"""Sports catalog identities must survive discovery, forecasts and settlement."""
import time

import pytest

from market_research.engine import iso, normalize_quotes
from market_research.first_row_strategy import game_groups, VERSION
from market_research.kalshi_btc import KalshiBTCProvider
from market_research.local_store import LocalStore
from market_research.provider import KalshiProvider
from market_research.recovery_replay import process_game


EVENT = 'KXTESTGAME-26SEP160900AAABBB'


def contracts(books=2):
    return [dict(source='kalshi', eventId=EVENT, marketId=EVENT+'-'+team,
                 providerSymbol=EVENT+'-'+team, contractId=EVENT+'-'+team+':'+side,
                 side=side, eventStart=iso(3600), resolutionTime=iso(11000), sourceTier='historical')
            for team in ('AAA', 'BBB', 'TIE')[:books] for side in ('yes', 'no')]


@pytest.mark.parametrize('books', [1, 2, 3])
def test_complete_kalshi_binary_books_including_six_soccer_positions(books):
    rows = contracts(books)
    assert len(game_groups(rows)[EVENT]) == books*2
    assert game_groups(rows + rows) == game_groups(rows)
    assert game_groups(rows[:-1]) == {}


def test_grouping_rejects_conflicting_identity_and_preserves_polymarket():
    rows = contracts()
    assert game_groups(rows + [{**rows[0], 'side': 'no'}]) == {}
    assert game_groups([{**r, 'providerSymbol': 'FOREIGN'} for r in rows]) == {}
    assert game_groups([{**r, 'contractId': 'FOREIGN'} for r in rows]) == {}
    poly = [dict(source='polymarket_us', eventId='p', marketId='m', contractId=s, side=s)
            for s in ('long', 'short')]
    assert set(game_groups(rows + poly)) == {EVENT, 'p'}


def market():
    c = contracts()[0]
    return dict(ticker=c['marketId'], event_ticker=EVENT, market_type='binary',
                status='finalized', result='no', settlement_value_dollars='0.0000',
                settlement_ts=iso(11000))


def test_resolution_checks_historical_identity_and_complement(monkeypatch):
    calls = []
    def get(self, path, params=None):
        calls.append(path)
        return {'market': market()}
    monkeypatch.setattr(KalshiBTCProvider, 'get', get)
    p = KalshiProvider()
    yes, no = (p.resolution(c) for c in contracts(1))
    assert yes['selected_side_payout'] == 0 and no['selected_side_payout'] == 1
    assert yes['settled_at'] == 11000 and no['selected_side_won']
    assert all(path.startswith('/historical/markets/') for path in calls)


@pytest.mark.parametrize('patch,status', [
    ({'status': 'active'}, 'pending_or_unverified'),
    ({'is_provisional': True}, 'pending_or_unverified'),
    ({'result': 'scalar'}, 'pending_or_unverified'),
    ({'settlement_value_dollars': '.5'}, 'conflicting_or_partial_settlement'),
    ({'settlement_value_dollars': '1'}, 'conflicting_or_partial_settlement'),
    ({'settlement_value_dollars': 'NaN'}, 'conflicting_or_partial_settlement'),
    ({'settlement_ts': None}, 'settlement_time_unverified'),
])
def test_resolution_never_infers_win_from_quote_or_partial_outcome(monkeypatch, patch, status):
    monkeypatch.setattr(KalshiBTCProvider, 'get', lambda *a, **k: {'market': {**market(), **patch}})
    r = KalshiProvider().resolution(contracts()[0])
    assert r['resolution_status'] == status and r['selected_side_won'] is None


@pytest.mark.parametrize('field', ['ticker', 'event_ticker', 'market_type'])
def test_official_market_identity_mismatch_fails_closed(monkeypatch, field):
    monkeypatch.setattr(KalshiBTCProvider, 'get', lambda *a, **k: {'market': {**market(), field: 'FOREIGN'}})
    with pytest.raises(ValueError, match='IDENTITY_MISMATCH'):
        KalshiProvider().resolution(contracts()[0])


def test_not_found_falls_back_between_official_tiers_only(monkeypatch):
    calls = []
    def get(self, path, params=None):
        calls.append(path)
        if len(calls) == 1:
            raise RuntimeError('KALSHI_HTTP_404')
        return {'market': market()}
    monkeypatch.setattr(KalshiBTCProvider, 'get', get)
    c = {**contracts()[0], 'sourceTier': 'live'}
    assert KalshiProvider().resolution(c)['resolution_status'] == 'resolved'
    assert calls == ['/markets/'+c['marketId'], '/historical/markets/'+c['marketId']]
    calls.clear()
    def throttled(*a, **k): raise RuntimeError('KALSHI_HTTP_429')
    monkeypatch.setattr(KalshiBTCProvider, 'get', throttled)
    with pytest.raises(RuntimeError, match='429'):
        KalshiProvider().resolution(c)


def test_cached_yes_no_history_keeps_real_spread_and_minute_end():
    p = KalshiProvider()
    p.request = lambda *a: {'rows': [dict(source='kalshi', timestamp=iso(3660),
        yes_ask_close=.6, yes_bid_close=.55)]}
    yes, no = [normalize_quotes(p.history(c, 3600, 3700), 3700)[0] for c in contracts(1)]
    assert yes.timestamp == no.timestamp == 3660
    assert (yes.ask, yes.bid) == (.6, .55)
    assert no.ask == pytest.approx(.45) and no.bid == pytest.approx(.4)


def test_four_side_replay_publishes_atomic_forecasts_and_settlement(tmp_path):
    class Provider(KalshiProvider):
        def history(self, c, *args, **kwargs):
            return [dict(source='kalshi', timestamp=iso(t), yes_ask_close=.6,
                         yes_bid_close=.55, selected_position=c['side']) for t in range(1200, 11000, 60)]
        def resolution(self, c):
            return dict(contract_id=c['contractId'], resolution_status='resolved',
                        selected_side_payout=int(c['side']=='yes'), settled_at=11001)
    counter = []
    def forecast(window, horizon, models, quantiles, **kwargs):
        counter.append(1)
        origin = window[-1].timestamp
        return dict(forecast_id=str(len(counter)), origin=origin,
                    models=[dict(id=m, status='completed') for m in models],
                    rows=[dict(timestamp=origin+i*60, quantiles={str(q):q for q in quantiles}) for i in range(1,31)])
    store = LocalStore('kalshi-first', 'test', tmp_path); store.claim({'version': VERSION})
    pair = game_groups(contracts())[EVENT]
    result = process_game(store, Provider(), pair, 12000, 30, 100, time.monotonic()+60, forecast, first_row=True)
    assert result['complete'] and result['published_origins'] > 0
    assert len(store.values('forecasts')) == result['published_origins'] * 4
    assert all(f['expected_side_count']==4 for f in store.values('forecasts'))
    assert all(store._get('checkpoints', 'resolution:'+c['contractId'])['settled_at']==11001 for c in pair)
    store.db.close()
