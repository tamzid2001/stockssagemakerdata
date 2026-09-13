import json
import os
import time
from pathlib import Path
import pytest
import yaml

from market_research.recovery_cloud import Campaign, add_summary, decode_catalog, encode_catalog, validate_campaign
from market_research.recovery_switch import VERSION, statistics
from market_research.recovery_watchdog import resume_inputs


def test_catalog_is_encrypted_authenticated_and_roundtrips(tmp_path, monkeypatch):
    monkeypatch.setenv("QUANTURA_RESEARCH_ARTIFACT_KEY", "ab"*32)
    value = {"contracts": [{"contractId": "sensitive-contract"}], "coverage": {"next_cursor": "1000"}}
    path = tmp_path/"catalog.enc"
    encode_catalog(value, path)
    assert b"sensitive-contract" not in path.read_bytes() and decode_catalog(path) == value
    changed = bytearray(path.read_bytes()); changed[-1] ^= 1; path.write_bytes(changed)
    with pytest.raises(Exception):
        decode_catalog(path)


def test_campaign_inputs_cannot_redirect_storage_or_change_pinned_code():
    for invalid in ("../escape", "p90-main", "new", "https://example.com"):
        with pytest.raises(ValueError): validate_campaign(invalid)
    record = {"id": "p90-"+"a"*24, "campaign_kind": VERSION, "status": "paused",
              "configuration": {"continuous": True, "horizon": 15, "code_sha": "b"*40}}
    assert resume_inputs(record)["code_ref"] == "b"*40
    assert resume_inputs({**record, "status": "completed"}) is None
    assert resume_inputs({**record, "lease": {"expires": time.time()+100}}) is None


def test_incremental_summary_matches_campaign_totals_and_no_fake_win_rate():
    from market_research.recovery_campaign import totals
    row = {**statistics([]), "recovery_signals": 0, "multiplier_increases": 0}
    record = {"summary": {"p90": row}, "forecast_count": 2}
    summary = add_summary({}, record)
    assert summary == totals([record]) and summary["win_rate"] is None
    assert add_summary(summary, record) == totals([record, record])


def test_large_forecasts_rejected_from_firestore_metadata():
    cloud = Campaign.__new__(Campaign)
    with pytest.raises(ValueError, match="SMALL_GAME_METADATA"):
        cloud.finish_game("game", {"forecast_rows": [1]}, {})


def test_durable_workflow_has_no_handoff_count_limit_or_model_cache_artifacts():
    root = Path(__file__).resolve().parents[2]
    text = (root/".github/workflows/polymarket-p90-ingame-backtest.yml").read_text()
    w = yaml.safe_load(text)
    assert w["jobs"]["replay"]["timeout-minutes"] == 360
    assert w["concurrency"]["cancel-in-progress"] is False
    assert "remaining_handoffs" not in text and "resume_required" in text
    assert "FIREBASE_SERVICE_ACCOUNT_JSON" in text and "POLYMARKET_SECRET_KEY" not in text
    assert "actions/cache" not in text and "upload-artifact" not in text
    assert "code_ref" in text
    watcher = yaml.safe_load((root/".github/workflows/polymarket-p90-ingame-watchdog.yml").read_text())
    assert watcher[True]["schedule"] and "POLYMARKET_RECOVERY_ENABLED" in watcher["jobs"]["recover"]["if"]
    rules = (root/"quantura_site/storage.rules").read_text()
    assert "match /{allPaths=**}" in rules and "private-research" not in rules


@pytest.mark.skipif(not os.environ.get("FIRESTORE_EMULATOR_HOST"), reason="Firestore emulator required")
def test_game_manifest_is_fenced_idempotent_and_contains_no_forecasts():
    import uuid
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import firestore
    from market_research.store import Store
    cloud = Campaign.__new__(Campaign)
    # Test-only, isolated emulator client: CI must never require or discover
    # production ADC credentials for an emulator integration test.
    cloud.lease = Store.__new__(Store)
    cloud.lease.db = firestore.Client(project='quantura-forecast-integration', credentials=AnonymousCredentials())
    cloud.lease.fs = firestore
    cloud.lease.session = 'recovery-test-'+uuid.uuid4().hex
    cloud.lease.holder = 'test'
    cloud.lease.fence = None
    cloud.lease.ref = cloud.lease.db.collection('market_research_sessions').document(cloud.lease.session)
    cloud.lease.claim({"version": VERSION})
    row = {**statistics([]), "recovery_signals": 0, "multiplier_increases": 0}
    record = {"game_id": "g", "archive": {"object": "encrypted", "sha256": "a"*64},
              "result": {"complete": True}, "forecast_count": 2, "summary": {"p90": row}}
    try:
        cloud.finish_game("g", record, {"page_index": 1})
        cloud.finish_game("g", record, {"page_index": 1})
        state = cloud.lease.ref.get().to_dict()
        assert state["summary"]["games_archived"] == 1 and state["page_index"] == 1
        with pytest.raises(RuntimeError, match="GAME_ALREADY_ARCHIVED"):
            cloud.finish_game("g", {**record, "archive": {"object": "different"}}, {})
    finally:
        cloud.lease.release()
        for snapshot in cloud.lease.ref.collection("games").stream(): snapshot.reference.delete()
        cloud.lease.ref.delete()
