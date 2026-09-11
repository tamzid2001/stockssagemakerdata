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
