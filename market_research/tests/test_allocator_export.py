import csv
import io
import json
import hashlib
from dataclasses import dataclass
from types import SimpleNamespace
import sys

import pytest

from market_research.allocator_export import (
    canonical, content_cid, portfolio_csv, validate_bundle, validate_replay,
    write_replay, seal_bundle, stamp_file, filename_part,
)


def replay():
    return {'symbol': 'SPY', 'orders_sent': 0, 'trailing_rule': 'extreme_entry_distance',
            'initial_balance': 1000., 'ending_equity': 1010., 'net_pnl': 10., 'open_net_pnl': 0.,
            'closed_entries': 1, 'open_entries': 0, 'closed_baskets': 1, 'basket_wins': 1,
            'basket_losses': 0, 'basket_breakeven': 0,
            'entries': [{'at': '2026-01-01T13:00:00Z', 'entry': 100., 'initial_shares': 2.,
                         'shares': 0., 'exits': [{'at': '2026-01-02T13:00:00Z', 'price': 105.,
                                                'shares': 2., 'reason': 'trailing_stop'}]}],
            'open_basket': [], 'baskets': [{'net_pnl': 10.}],
            'hourly_equity': [{'at': '2026-01-01T14:00:00Z', 'equity': 1000., 'cash': 1000., 'shares': 2.},
                              {'at': '2026-01-02T14:00:00Z', 'equity': 1010., 'cash': 1010., 'shares': 0.}]}


def test_weight_units_cash_shorts_and_leverage_are_not_normalized():
    positions = [{'identifier': 'SPY', 'quantity': 2, 'price': 100, 'usd_per_quote': 1},
                 {'identifier': 'USDJPY', 'quantity': -100000, 'price': 150, 'usd_per_quote': 1 / 150}]
    rows = list(csv.DictReader(io.StringIO(portfolio_csv(positions, 100000).decode())))
    assert rows == [{'Ticker': 'SPY', 'Weight': '0.002000000000'},
                    {'Ticker': 'USDJPY', 'Weight': '-1.000000000000'}]
    assert b'CASH' not in portfolio_csv(positions, 100000)
    assert b'2.000000000000' in portfolio_csv([positions[0]], 100)


@pytest.mark.parametrize('symbol', ['=IMPORTXML(x)', 'CASH', 'SPY\nFAKE', '../SPY'])
def test_invalid_security_identifiers_are_rejected(symbol):
    with pytest.raises(ValueError):
        portfolio_csv([{'identifier': symbol, 'quantity': 1, 'price': 1, 'usd_per_quote': 1}], 1000)


@pytest.mark.parametrize('field,value', [('price', 0), ('usd_per_quote', -1), ('quantity', float('nan'))])
def test_invalid_marks_and_nonfinite_quantities_are_rejected(field, value):
    p = {'identifier': 'SPY', 'quantity': 1, 'price': 100, 'usd_per_quote': 1}
    p[field] = value
    with pytest.raises(ValueError):
        portfolio_csv([p], 1000)


def test_share_weight_history_reconciles_to_nav_and_stays_historical(tmp_path):
    root = tmp_path / 'variant'
    write_replay(root, replay(), 'shares')
    index = json.loads((root / 'weights-index.json').read_bytes())
    assert (root / index[0]['file']).read_text() == 'Ticker,Weight\nSPY,0.200000000000\n'
    assert (root / index[1]['file']).read_text() == 'Ticker,Weight\nSPY,0.000000000000\n'
    assert all(x['stamp_timestamp'] is None and x['evidence_kind'] == 'historical_simulation' for x in index)
    assert content_cid((root / index[0]['file']).read_bytes()) == index[0]['content_cid']
    positions = list(csv.DictReader((root / 'positions.csv').open()))
    assert [x['quantity'] for x in positions] == ['2.0', '0.0']


def test_replay_rejects_missing_open_legs_and_inconsistent_equity():
    r = replay()
    r['open_entries'] = 1
    with pytest.raises(ValueError, match='ENTRY_COUNT'):
        validate_replay(r, 'shares')
    r = replay()
    r['ending_equity'] = 1020
    with pytest.raises(ValueError, match='EQUITY_RECONCILIATION'):
        validate_replay(r, 'shares')


def test_bundle_commitment_detects_tampering_and_added_files(tmp_path):
    source = tmp_path / 'source.json'
    source.write_text('{}')
    root = tmp_path / 'bundle'
    root.mkdir()
    (root / 'evidence.csv').write_text('value\n1\n')
    seal_bundle(root, 'SPY', source)
    assert validate_bundle(root / 'manifest.json')['evidence_kind'] == 'historical_backtest'
    (root / 'extra.txt').write_text('undisclosed')
    with pytest.raises(ValueError, match='INVENTORY'):
        validate_bundle(root / 'manifest.json')
    (root / 'extra.txt').unlink()
    (root / 'evidence.csv').write_text('value\n2\n')
    with pytest.raises(ValueError, match='HASH_MISMATCH'):
        validate_bundle(root / 'manifest.json')


def test_sha3_uses_original_bytes_and_not_sha256():
    data = canonical({'b': 2, 'a': 1})
    assert data == b'{"a":1,"b":2}'
    assert content_cid(data) == '0x' + hashlib.sha3_256(data).hexdigest()
    assert content_cid(data) != '0x' + hashlib.sha256(data).hexdigest()
    assert content_cid(data) != content_cid(data + b'\n')


def test_stamp_sends_only_cid_and_recovers_existing_receipt(tmp_path, monkeypatch):
    @dataclass
    class Receipt:
        object_cid: str
        set_cid: str
        transaction_hash: str = '0x' + 'b' * 64
        timestamp: str = '2026-10-09T15:00:00Z'
        user_address: str = '0x' + 'c' * 40
        chain_id: int = 137
    calls, receipts = [], []
    class Client:
        def __init__(self, api_key):
            assert api_key == 'synthetic'
        def verify_stamps(self, **kwargs):
            assert kwargs['filter_by_user'] is True
            return SimpleNamespace(stamp_list=receipts)
        def create_stamp(self, **kwargs):
            assert set(kwargs) == {'data_cid', 'collection_cid', 'store_stamped_file', 'idempotent', 'idempotency_window'}
            assert kwargs['store_stamped_file'] is False
            calls.append(kwargs)
            value = Receipt(kwargs['data_cid'], kwargs['collection_cid'])
            receipts.append(value)
            return SimpleNamespace(commitment_receipt=value)
        def close(self):
            pass
    monkeypatch.setitem(sys.modules, 'vbase_api', SimpleNamespace(VBaseAPIClient=Client))
    monkeypatch.setenv('VBASE_API_KEY', 'synthetic')
    path = tmp_path / 'portfolio.csv'
    path.write_bytes(b'Ticker,Weight\nSPY,0.05\n')
    collection = '0x' + 'a' * 64
    first = stamp_file(path, collection)
    receipts[0].user_address = 'account-name'
    second = stamp_file(path, collection, verify=True)
    assert len(calls) == 1
    assert first['receipt']['transaction_hash'] == second['receipt']['transaction_hash']
    assert second['receipt']['user_address'] is None
    assert second['receipt']['user_label'] == 'account-name'


def test_same_timestamp_marks_are_disclosed_instead_of_guessed(tmp_path):
    r = replay()
    r['hourly_equity'][0]['at'] = r['entries'][0]['at']
    write_replay(tmp_path / 'variant', r, 'shares')
    skipped = json.loads((tmp_path / 'variant' / 'weights-skipped-marks.json').read_bytes())
    assert len(skipped) == 1
    assert len(json.loads((tmp_path / 'variant' / 'weights-index.json').read_bytes())) == 1


def test_path_components_and_fractional_spy_share_fills_are_rejected():
    with pytest.raises(ValueError):
        filename_part('../../outside')
    r = replay()
    r['entries'][0]['initial_shares'] = 1.5
    with pytest.raises(ValueError, match='INVALID_FILL'):
        validate_replay(r, 'shares')
