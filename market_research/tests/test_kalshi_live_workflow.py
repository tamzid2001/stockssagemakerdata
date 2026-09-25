from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_live_workflow_defaults_off_pinned_code_and_no_public_state():
    path = ROOT / '.github/workflows/kalshi-btc-approved-trader.yml'
    text = path.read_text()
    workflow = yaml.safe_load(text)
    inputs = workflow[True]['workflow_dispatch']['inputs']
    assert inputs['mode']['default'] == 'verify'
    assert inputs['continuous']['default'] is False
    assert inputs['history_minutes']['options'] == [str(n) for n in range(1,13)]
    assert inputs['history_minutes']['default'] == '1'
    assert inputs['subaccount']['default'] == 0
    assert inputs['direction_policy']['default'] == 'provisional_near_close'
    assert inputs['starting_contracts']['default'] == 1
    assert inputs['recovery_multiplier']['default'] == 2.5
    assert 'max_contracts' not in inputs
    assert inputs['max_recovery_increases']['default'] == '3'
    assert inputs['max_recovery_increases']['options'] == [str(n) for n in range(7)]
    assert workflow['concurrency']['cancel-in-progress'] is False
    assert workflow['jobs']['worker']['timeout-minutes'] <= 355
    assert workflow['permissions']['contents'] == 'read'
    assert "inputs.mode == 'live' && inputs.continuous && vars.QUANTURA_KALSHI_APPROVED_SHA" in text
    assert "inputs.code_ref || (inputs.mode == 'live' && vars.QUANTURA_KALSHI_APPROVED_SHA) || github.sha" in text
    assert 'ref: ${{ env.QUANTURA_CODE_SHA }}' in text
    assert 'name: Validate live approval reference' in text
    assert 'Leave code_ref empty to use QUANTURA_KALSHI_APPROVED_SHA automatically' in text
    assert "code_ref: ''" in text  # Successor resolves the current approved SHA.
    assert 'upload-artifact' not in text and 'git push' not in text
    assert 'QUANTURA_KALSHI_APPROVED_CONFIG' in text and 'QUANTURA_KALSHI_APPROVED_SHA' in text
    assert 'QUANTURA_KALSHI_CONTINUOUS' in text
    assert 'cancel-in-progress: false' in text
    assert 'KALSHI_PRIVATE_KEY' in text and 'echo "$KALSHI_PRIVATE_KEY"' not in text
    assert 'direction_policy: process.env.DIRECTION_POLICY' in text
    assert 'starting_contracts: process.env.STARTING_CONTRACTS' in text
    assert 'recovery_multiplier: process.env.RECOVERY_MULTIPLIER' in text
    assert 'max_recovery_increases: process.env.MAX_RECOVERY_INCREASES' in text
    assert workflow['jobs']['worker']['steps'][0]['name'] == 'Record total job budget'
    assert workflow['jobs']['worker']['steps'][2]['name'] == 'Validate live approval reference'


def test_watchdog_disabled_unless_operator_enables_and_no_concurrent_horizons():
    text = (ROOT / '.github/workflows/kalshi-btc-approved-watchdog.yml').read_text()
    workflow = yaml.safe_load(text)
    assert workflow['jobs']['recover']['if'] == "vars.QUANTURA_KALSHI_CONTINUOUS == 'true'"
    assert "LIVE_ENABLED!=='true'" in text
    for status in ('queued', 'in_progress', 'requested', 'waiting', 'pending'):
        assert status in text
    assert 'direction_policy:direction' in text and 'account<0' in text


def test_execution_collections_are_explicitly_private():
    rules = (ROOT / 'quantura_site/firestore.rules').read_text()
    assert 'match /kalshi_execution_sessions/{document=**} { allow read, write: if false; }' in rules


def test_live_diagnostic_is_manual_read_only_and_cannot_submit():
    text = (ROOT / '.github/workflows/kalshi-btc-live-diagnostics.yml').read_text()
    workflow = yaml.safe_load(text)
    assert 'workflow_dispatch' in workflow[True]
    assert workflow['permissions'] == {'contents': 'read'}
    assert 'kalshi_live_diagnostics' in text
    assert 'QUANTURA_KALSHI_LIVE_ENABLED' not in text
    assert 'QUANTURA_KALSHI_APPROVED_CONFIG' not in text
    assert 'QUANTURA_KALSHI_APPROVED_SHA' not in text
    assert 'kalshi_live_worker' not in text


def test_stale_recovery_is_manual_proof_gated_and_shares_live_singleton():
    text = (ROOT / '.github/workflows/kalshi-btc-stale-intent-recovery.yml').read_text()
    workflow = yaml.safe_load(text)
    inputs = workflow[True]['workflow_dispatch']['inputs']
    assert inputs['confirmation']['required'] is True
    assert workflow['permissions'] == {'contents': 'read'}
    assert workflow['concurrency']['group'] == 'quantura-kalshi-btc-execution-singleton'
    assert 'kalshi_live_recovery' in text
    assert 'QUANTURA_KALSHI_LIVE_ENABLED' not in text
    assert 'QUANTURA_KALSHI_APPROVED_CONFIG' not in text
    assert 'RECOVERY_TICKER' in text and '${{ inputs.ticker }}' not in text.split('run: >-', 1)[1]


def test_existing_fill_settlement_is_manual_read_only_and_serialized():
    text = (ROOT / '.github/workflows/kalshi-btc-filled-settlement.yml').read_text()
    workflow = yaml.safe_load(text)
    assert workflow[True]['workflow_dispatch']['inputs']['confirmation']['required'] is True
    assert workflow['permissions'] == {'contents': 'read'}
    assert workflow['concurrency']['group'] == 'quantura-kalshi-btc-execution-singleton'
    assert 'kalshi_live_settlement' in text
    assert 'QUANTURA_KALSHI_LIVE_ENABLED' not in text
    assert 'QUANTURA_KALSHI_APPROVED_CONFIG' not in text
    assert 'RECOVERY_TICKER' not in text
