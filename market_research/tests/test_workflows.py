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
    ):
        path = ROOT / ".github/workflows" / name
        text = path.read_text()
        value = yaml.safe_load(text)
        workflows.append(value)
        assert value["concurrency"]["cancel-in-progress"] is False
        assert "upload-artifact" not in text and "actions/cache" not in text
        assert "place_order" not in text and "POLYMARKET_SECRET_KEY" not in text
        assert "ref: main" in text
        assert all(j["timeout-minutes"] <= 360 for j in value["jobs"].values())
    assert len({w["concurrency"]["group"] for w in workflows}) == 4
    live = (ROOT / ".github/workflows/polymarket-live-paper.yml").read_text()
    assert "--duration-minutes 345" in live and "handoff_ready == 'true'" in live


def test_private_storage_rules():
    rules = (ROOT / "quantura_site/firestore.rules").read_text()
    for collection in ("sessions", "forecasts", "trades", "runs"):
        assert (
            f"match /market_research_{collection}/{{document=**}} {{ allow read, write: if false; }}"
            in rules
        )
