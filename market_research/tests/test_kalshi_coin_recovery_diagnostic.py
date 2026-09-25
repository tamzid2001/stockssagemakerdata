"""The coin audit must stay read-only and omit order credentials/identifiers."""
from pathlib import Path

import yaml

from market_research.kalshi_coin_recovery_diagnostic import safe_fill, safe_trade


def test_safe_trade_exposes_requested_versus_filled_without_order_ids():
    row = {'ticker': 'KXZEC15M-TEST', 'created_at': 123, 'status': 'settled',
        'intent': {'count': '5.00', 'client_order_id': 'private-id'},
        'filled': '2.00', 'attempt': 1, 'net_pnl': '-.7', 'secret': 'never-log'}
    assert safe_trade(row) == {'ticker': 'KXZEC15M-TEST', 'created_at': 123,
        'status': 'settled', 'requested': '5.00', 'latest_order_requested': '5.00',
        'filled': '2.00',
        'attempt': 1, 'net_pnl': '-.7'}


def test_safe_trade_reports_stop_timing_without_pending_order_identity():
    row = {'ticker': 'KXZEC15M-TEST', 'intent': {'count': '1.00'},
        'signal': {'market_end': 100}, 'stop': {'triggered_at': 78,
            'attempt': 0, 'sold': '0', 'pending': {'intent': {
                'client_order_id': 'private-id'}}}}
    result = safe_trade(row)
    assert {key: result[key] for key in ('market_end', 'stop_triggered_at',
        'stop_attempt', 'stop_sold')} == {'market_end': 100,
        'stop_triggered_at': 78, 'stop_attempt': 0, 'stop_sold': '0'}
    assert 'private-id' not in str(result)


def test_workflow_has_no_live_order_approval_or_write_capability():
    path = Path(__file__).resolve().parents[2] / '.github/workflows/kalshi-coin-recovery-diagnostic.yml'
    content = path.read_text()
    workflow = yaml.safe_load(content)
    assert workflow['permissions'] == {'contents': 'read'}
    assert 'workflow_dispatch' in workflow[True]
    assert 'QUANTURA_KALSHI_' not in content
    assert 'kalshi_live_worker' not in content


def test_safe_fill_exposes_count_but_not_exchange_order_identity():
    row = {'ticker': 'KXZEC15M-TEST', 'created_time': '2026-09-24T22:33:00Z',
        'action': 'buy', 'outcome_side': 'yes', 'book_side': 'bid',
        'count_fp': '2.00', 'yes_price_dollars': '.63', 'no_price_dollars': '.37',
        'order_id': 'private-order', 'fill_id': 'private-fill'}
    assert safe_fill(row) == {'ticker': 'KXZEC15M-TEST',
        'created_time': '2026-09-24T22:33:00Z', 'action': 'buy',
        'outcome_side': 'yes', 'book_side': 'bid', 'count': '2.00',
        'yes_price': '.63', 'no_price': '.37'}
