from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_workflow_isolation_timeouts_privacy_and_safe_inputs():
    workflows = []
    for name in (
        "polymarket-live-paper.yml",
        "polymarket-historical-backtest.yml",
        "polymarket-paper-watchdog.yml",
        "polymarket-replay-archive.yml",
        "kalshi-replay-archive.yml",
    ):
        path = ROOT / ".github/workflows" / name
        text = path.read_text()
        value = yaml.safe_load(text)
        workflows.append(value)
        assert value["concurrency"]["cancel-in-progress"] is False
        assert "actions/cache" not in text
        if "historical-backtest" in name or "replay-archive" in name:
            assert "FIREBASE_SERVICE_ACCOUNT_JSON" not in text
            assert "uses: ./.github/actions/research-artifact" in text
            assert "QUANTURA_RESEARCH_ARTIFACT_KEY" in text
        else:
            assert "upload-artifact" not in text
        assert "place_order" not in text and "POLYMARKET_SECRET_KEY" not in text
        assert "ref: main" in text
        assert all(j["timeout-minutes"] <= 360 for j in value["jobs"].values())
    assert len({w["concurrency"]["group"] for w in workflows}) == 5
    live = (ROOT / ".github/workflows/polymarket-live-paper.yml").read_text()
    assert "--duration-minutes 345" in live and "handoff_ready == 'true'" in live
    action = (ROOT / ".github/actions/research-artifact/action.yml").read_text()
    assert "retention-days: 3" in action and "research.qra.enc" in action
    assert "overwrite: false" in action


def test_private_storage_rules():
    rules = (ROOT / "quantura_site/firestore.rules").read_text()
    for collection in ("sessions", "forecasts", "trades", "runs"):
        assert (
            f"match /market_research_{collection}/{{document=**}} {{ allow read, write: if false; }}"
            in rules
        )


def test_long_replay_has_pinned_handoff_complete_restore_and_private_online_checkpoints():
    text = (ROOT / ".github/workflows/historical-p10-replay.yml").read_text()
    workflow = yaml.safe_load(text)
    assert workflow["jobs"]["replay"]["timeout-minutes"] == 360
    assert workflow["concurrency"]["cancel-in-progress"] is False
    assert "restore-backtest" in text and "resume_artifact_id" in text
    assert "input.code_ref = process.env.QUANTURA_CODE_SHA" in text
    assert "FIREBASE_SERVICE_ACCOUNT_JSON" not in text
    assert "POLYMARKET_SECRET_KEY" not in text
    assert "actions/github-script@v8" in text
    script = (ROOT / "market_research/node/run.mjs").read_text()
    assert "snapshot" not in script or "checkpoint" in script
    assert "45 * 60 * 1000" in script and "retentionDays: 3" in script
    assert script.index("uploadArtifact") < script.index("deleteArtifact")
    assert "retained.length > 2" in script


def test_median_experiment_uses_shared_worker_with_isolated_bounded_configuration():
    text = (ROOT / ".github/workflows/historical-p10-replay.yml").read_text()
    workflow = yaml.safe_load(text)
    inputs = workflow[True]["workflow_dispatch"]["inputs"]
    assert inputs["strategy"]["options"] == ["p10", "median_cross", "quantiles"]
    assert inputs["loss_multiplier"]["default"] == 2.5
    assert inputs["max_shares"]["default"] == 100
    assert "inputs.strategy" in workflow["concurrency"]["group"]
    assert "supportsExperiments" in text and "Original checkpoint code" in text
    assert "spawn(process.execPath,args" in text
    assert "POLYMARKET_SECRET_KEY" not in text and "KALSHI_PRIVATE_KEY" not in text


def test_corpus_variants_and_live_shared_provider_dispatch():
    replay = yaml.safe_load((ROOT / ".github/workflows/historical-p10-replay.yml").read_text())
    inputs = replay[True]["workflow_dispatch"]["inputs"]
    assert inputs["lag_minutes"]["options"] == ["0", "15", "30"]
    assert inputs["horizon"]["options"] == ["30", "45", "60"]
    live = yaml.safe_load((ROOT / ".github/workflows/polymarket-live-paper.yml").read_text())
    inputs = live[True]["workflow_dispatch"]["inputs"]
    assert inputs["forecast_only"]["default"] is True
    assert inputs["provider"]["options"] == ["polymarket_us", "kalshi"]
    assert "inputs.provider" in live["concurrency"]["group"]
