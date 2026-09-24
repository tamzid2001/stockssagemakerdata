"""The coin audit must stay read-only and omit order credentials/identifiers."""
from pathlib import Path

import yaml

from market_research.kalshi_coin_recovery_diagnostic import safe_trade


def test_safe_trade_exposes_requested_versus_filled_without_order_ids():
    row = {'ticker': 'KXZEC15M-TEST', 'created_at': 123, 'status': 'settled',
        'intent': {'count': '5.00', 'client_order_id': 'private-id'},
        'filled': '2.00', 'attempt': 1, 'net_pnl': '-.7', 'secret': 'never-log'}
    assert safe_trade(row) == {'ticker': 'KXZEC15M-TEST', 'created_at': 123,
        'status': 'settled', 'requested': '5.00', 'filled': '2.00',
        'attempt': 1, 'net_pnl': '-.7'}


def test_workflow_has_no_live_order_approval_or_write_capability():
    path = Path(__file__).resolve().parents[2] / '.github/workflows/kalshi-coin-recovery-diagnostic.yml'
    content = path.read_text()
    workflow = yaml.safe_load(content)
    assert workflow['permissions'] == {'contents': 'read'}
    assert 'workflow_dispatch' in workflow[True]
    assert 'QUANTURA_KALSHI_' not in content
    assert 'kalshi_live_worker' not in content
