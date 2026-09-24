from copy import deepcopy
from types import SimpleNamespace

import pytest

from market_research import kalshi_provisional_direction as provisional
from market_research.engine import iso
from market_research.local_store import LocalStore
from market_research.kalshi_live_state import LiveJournal

OPEN = 1800000000
CLOSE = OPEN + 900
TICKER = 'KXBTC15M-TEST-00'
CURRENT = 'KXBTC15M-TEST-15'


def market(**patch):
    return dict(ticker=TICKER, market_type='binary', status='active',
        open_time=iso(OPEN), close_time=iso(CLOSE), yes_bid_dollars='.99',
        yes_ask_dollars='1', no_bid_dollars='0', no_ask_dollars='.01',
        yes_bid_size_fp='1.00', yes_ask_size_fp='2.00', **patch)


def sample(**patch):
    return provisional.snapshot({**market(), **patch}, TICKER, CLOSE-2, CLOSE-1.8)


def official(result='no', received=CLOSE+10):
    return dict(market_id=TICKER, close_at=CLOSE, resolution_status='resolved',
                result=result, first_confirmed_at=received)


def direction(rows, outcomes=(), lifecycle=(), at=CLOSE+120, **kwargs):
    return provisional.direction_at(outcomes, rows, lifecycle, at, CURRENT, CLOSE, **kwargs)


def test_quote_is_provisional_direction_not_a_settlement():
    row = sample()
    assert row['inferred_winner'] == 'yes'
    assert 'result' not in row and row['settlement_confirmed'] is False
    assert row['exchange_quote_timestamp_verified'] is False
    selected = direction([row])
    assert selected['side'] == 'no' and selected['provisional'] is True
    assert 'confirmed_at' not in selected and 'prior_result' not in selected


def test_coin_direction_only_uses_same_series_predecessor():
    coin = 'KXETH15M'
    prior = coin + '-TEST-00'
    current = coin + '-TEST-15'
    row = provisional.snapshot({**market(), 'ticker': prior}, prior, CLOSE-2, CLOSE-1.8, coin)
    selected = provisional.direction_at([], [row], [], CLOSE+120, current, CLOSE,
                                        series_ticker=coin)
    assert selected['side'] == 'no'
    assert provisional.direction_at([], [row], [], CLOSE+120, CURRENT, CLOSE) is None


def test_no_side_is_selected_from_complementary_bid_not_ask_or_last_trade():
    row = sample(yes_bid_dollars='0', yes_ask_dollars='.01', no_bid_dollars='.99', no_ask_dollars='1')
    assert direction([row])['side'] == 'yes'
    neutral = sample(yes_bid_dollars='.50', yes_ask_dollars='.99', no_bid_dollars='.01',
                     no_ask_dollars='.50', last_price_dollars='.99')
    assert neutral['inferred_winner'] is None and direction([neutral]) is None
    assert direction([sample(yes_bid_size_fp='0')]) is None


@pytest.mark.parametrize('start,received', [(CLOSE-11, CLOSE-10), (CLOSE-2, CLOSE),
    (CLOSE-5, CLOSE-2), (CLOSE-2, CLOSE-3), (float('nan'), CLOSE-1)])
def test_no_postclose_backfill_slow_response_or_invalid_clock(start, received):
    with pytest.raises(ValueError): provisional.snapshot(market(), TICKER, start, received)


@pytest.mark.parametrize('patch', [{'ticker': CURRENT}, {'status':'settled'},
    {'market_type':'scalar'}, {'yes_bid_dollars':'NaN'}, {'no_bid_dollars':'.99'},
    {'yes_ask_dollars':'.98'}, {'yes_bid_size_fp':None}, {'yes_bid_dollars':'99'}])
def test_reject_invalid_or_mismatched_book(patch):
    with pytest.raises(ValueError): sample(**patch)


def test_latest_neutral_invalidates_earlier_99c_and_stale_last_observation_is_excluded():
    earlier = provisional.snapshot(market(), TICKER, CLOSE-4, CLOSE-3.8)
    assert direction([earlier]) is None
    neutral = sample(yes_bid_dollars='.98', yes_ask_dollars='.99', no_bid_dollars='.01', no_ask_dollars='.02')
    assert direction([earlier, neutral]) is None


def test_official_result_supersedes_without_rewriting_prior_decision():
    row = sample()
    before = direction([row], [official()], at=CLOSE+5)
    assert before['side'] == 'no' and before['provisional']
    after = direction([row], [official()], at=CLOSE+11)
    assert after['side'] == 'yes' and not after['provisional']
    assert before['side'] == 'no'
    audit = provisional.reconciliation(row, official())
    assert audit['matches'] is False and audit['accounting_source'] == 'official_settlement_only'


def test_confirmed_policy_and_missing_immediate_predecessor_do_not_guess():
    assert direction([sample()], policy='confirmed') is None
    assert direction([sample()], [official()], policy='confirmed')['side'] == 'yes'
    stale = {**official(), 'close_at': OPEN, 'first_confirmed_at': OPEN+1}
    assert direction([], [stale]) is None
    assert direction([sample()], at=CLOSE) is None


def test_collector_keeps_provisional_snapshots_out_of_minute_and_settlement_tables(tmp_path, monkeypatch):
    store = LocalStore('test', 'test', tmp_path)
    try:
        with store.lock, store.db:
            store._put('btc_lifecycle', TICKER, {'market_id':TICKER, 'close_at':CLOSE})
        clock = [CLOSE-2]
        monkeypatch.setattr(provisional.time, 'time', lambda: clock[0])
        calls = []
        def get(ticker):
            calls.append(ticker)
            return market()
        collector = provisional.NearCloseCollector(store, SimpleNamespace(market=get))
        collector.tick(CLOSE-20)
        assert not calls
        collector.tick(clock[0]); collector.tick(clock[0])
        assert calls == [TICKER]
        rows = store.values('btc_provisional_direction')
        assert len(rows) == 1 and direction(rows)['side'] == 'no'
        assert not store.values('btc_minutes') and not store.values('btc_settlements')
    finally:
        store.db.close()


def test_durable_handoff_is_bounded_immutable_and_does_not_change_accounting():
    root = {'state':{'size':5, 'cycle':'-2', 'active':{'ticker':'held'}}, 'direction_snapshots':[]}
    original = deepcopy(root['state'])
    ref = SimpleNamespace(get=lambda **_: SimpleNamespace(to_dict=lambda:deepcopy(root)))
    transaction = SimpleNamespace(update=lambda _, patch:root.update(deepcopy(patch)))
    journal = object.__new__(LiveJournal)
    journal.ref = ref
    journal.fs = SimpleNamespace(transactional=lambda fn:fn)
    checks = []
    journal.check = lambda _:checks.append(True)
    journal.transact = lambda fn:fn(transaction)
    for i in range(12):
        journal.remember_direction({**sample(), 'market_id':f'KXBTC15M-TEST-{i}', 'close_at':CLOSE+i*900})
    assert len(journal.direction_snapshots()) == 8 and checks
    assert root['state'] == original
    last = journal.direction_snapshots()[-1]
    journal.remember_direction(last)
    with pytest.raises(RuntimeError, match='IMMUTABLE'):
        journal.remember_direction({**last, 'inferred_winner':'no'})
    assert root['state'] == original


def test_collector_failure_is_visible_and_does_not_fabricate_signal(tmp_path, monkeypatch):
    store = LocalStore('test', 'test', tmp_path)
    try:
        with store.lock, store.db:
            store._put('btc_lifecycle', TICKER, {'market_id':TICKER, 'close_at':CLOSE})
        monkeypatch.setattr(provisional.time, 'time', lambda:CLOSE-1)
        def fail(_): raise RuntimeError('private upstream body must not be persisted')
        collector = provisional.NearCloseCollector(store, SimpleNamespace(market=fail))
        collector.tick(CLOSE-1)
        assert not store.values('btc_provisional_direction')
        health = store._get('checkpoints', 'btc_provisional_health')
        assert health['status'] == 'degraded' and 'private' not in str(health)
    finally:
        store.db.close()
