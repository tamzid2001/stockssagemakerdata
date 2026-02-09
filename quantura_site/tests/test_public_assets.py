import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PUBLIC = ROOT / "public"


def test_public_pages_exist():
    for name in ["index.html", "forecasting.html", "screener.html", "dashboard.html", "contact.html"]:
        path = PUBLIC / name
        assert path.exists(), f"Missing {path}"


def test_manifest_and_robots():
    manifest = PUBLIC / "manifest.json"
    robots = PUBLIC / "robots.txt"
    sitemap = PUBLIC / "sitemap.xml"
    assert manifest.exists()
    assert robots.exists()
    assert sitemap.exists()
    data = json.loads(manifest.read_text())
    assert data.get("name") == "Quantura"


def test_pages_include_analytics():
    for name in ["index.html", "forecasting.html", "screener.html", "dashboard.html", "contact.html"]:
        html = (PUBLIC / name).read_text()
        assert "firebase-analytics-compat" in html
        assert "app.js" in html
        assert "manifest.json" in html
