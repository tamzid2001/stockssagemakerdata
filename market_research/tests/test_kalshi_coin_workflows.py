"""The five live templates remain independently approval-gated and off by default."""
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[2] / '.github/workflows'
COINS = ('BNB', 'DOGE', 'ETH', 'NEAR', 'ZEC')


def test_each_coin_has_its_own_disabled_dispatch_and_series_identity():
    shared = yaml.safe_load((ROOT / 'kalshi-coin-approved-worker.yml').read_text())
    assert 'workflow_call' in shared[True]
    assert shared['jobs']['worker']['environment'] == 'quantura-kalshi-live'
    assert 'KX${COIN}15M' in (ROOT / 'kalshi-coin-approved-worker.yml').read_text()
    assert shared['jobs']['worker']['timeout-minutes'] <= 355
    for coin in COINS:
        workflow = yaml.safe_load((ROOT / f'kalshi-{coin.lower()}-approved-trader.yml').read_text())
        inputs = workflow[True]['workflow_dispatch']['inputs']
        assert inputs['mode']['default'] == 'verify'
        assert inputs['continuous']['default'] is False
        assert inputs['subaccount']['default'] == 0
        assert inputs['history_minutes']['default'] == '1'
        assert workflow['concurrency']['cancel-in-progress'] is False
        called = workflow['jobs']['worker']
        assert called['uses'] == './.github/workflows/kalshi-coin-approved-worker.yml'
        assert called['with']['coin'] == coin
        assert called['with']['workflow_file'] == f'kalshi-{coin.lower()}-approved-trader.yml'
        assert f'QUANTURA_KALSHI_{coin}_LIVE_ENABLED' in str(called)
        assert f'QUANTURA_KALSHI_{coin}_APPROVED_CONFIG' in str(called)
        assert f'QUANTURA_KALSHI_{coin}_APPROVED_SHA' in str(called)
        assert f'QUANTURA_KALSHI_{coin}_CONTINUOUS' in str(called)


def test_watchdog_restarts_only_explicitly_enabled_coin_without_duplicates():
    path = ROOT / 'kalshi-coin-approved-watchdog.yml'
    workflow = yaml.safe_load(path.read_text())
    rows = workflow['jobs']['recover']['strategy']['matrix']['include']
    assert {row['coin'] for row in rows} == set(COINS)
    assert all('CONTINUOUS' in row['enabled'] for row in rows)
    text = path.read_text()
    assert "matrix.enabled == 'true'" in text
    assert "e.MODE === 'live' && e.LIVE_ENABLED !== 'true'" in text
    for status in ('queued', 'in_progress', 'requested', 'waiting', 'pending'):
        assert status in text
