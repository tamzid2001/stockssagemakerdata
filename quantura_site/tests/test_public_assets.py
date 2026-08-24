import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PUBLIC = ROOT / "public"
PAGES = ROOT / "pages"


def test_pages_exist():
    for name in [
        "index.html",
        "forecasting.html",
        "screener.html",
        "dashboard.html",
        "pricing.html",
        "contact.html",
        "shop.html",
        "ticker.html",
    ]:
        path = PAGES / name
        assert path.exists(), f"Missing {path}"


def test_manifest_and_robots():
    manifest = PUBLIC / "manifest.json"
    robots = PUBLIC / "robots.txt"
    sitemap = PUBLIC / "sitemap.xml"
    messaging_sw = PUBLIC / "firebase-messaging-sw.js"
    assert manifest.exists()
    assert robots.exists()
    assert sitemap.exists()
    assert messaging_sw.exists()
    data = json.loads(manifest.read_text())
    assert data.get("name") == "Quantura"


def test_pages_include_analytics():
    for name in [
        "index.html",
        "forecasting.html",
        "screener.html",
        "dashboard.html",
        "pricing.html",
        "contact.html",
        "shop.html",
        "ticker.html",
    ]:
        html = (PAGES / name).read_text()
        assert "firebase-analytics-compat" in html
        assert "app.js" in html
        assert 'rel="manifest"' in html
        assert ("site.webmanifest" in html) or ("manifest.json" in html)
    dashboard_html = (PAGES / "dashboard.html").read_text()
    assert "firebase-messaging-compat" in dashboard_html


def test_vercel_observability_is_built_and_loaded_globally():
    package = json.loads((ROOT / "package.json").read_text())
    dependencies = package.get("dependencies") or {}
    assert "@vercel/analytics" in dependencies
    assert "@vercel/speed-insights" in dependencies

    bundle = PUBLIC / "vercel-observability.js"
    assert bundle.exists()
    bundle_text = bundle.read_text()
    assert "/_vercel/insights/script.js" in bundle_text
    assert "/_vercel/speed-insights/script.js" in bundle_text

    app_js = (PUBLIC / "app.js").read_text()
    assert "/vercel-observability.js" in app_js

    for page in PAGES.rglob("*.html"):
        html = page.read_text()
        assert "/app.js" in html or "/vercel-observability.js" in html, f"Observability missing from {page}"

    for direct_page in [PUBLIC / "shop/success.html"]:
        assert "/vercel-observability.js" in direct_page.read_text()


def test_removed_routes_and_assets_stay_removed():
    vercel = json.loads((ROOT / "vercel.json").read_text())
    redirect_sources = {item["source"]: item["destination"] for item in vercel.get("redirects", [])}
    assert redirect_sources["/explore"] == "/dashboard"
    assert redirect_sources["/profile"] == "/account"
    assert redirect_sources["/u/:path*"] == "/account"
    assert redirect_sources["/tools/fx"] == "/forecasting"
    assert not (PUBLIC / "explore.html").exists()
    assert not (PUBLIC / "profile.html").exists()
    assert not (PAGES / "tools/fx.html").exists()


def test_data_integration_surfaces_are_present():
    forecasting = (PAGES / "forecasting.html").read_text()
    dashboard = (PAGES / "dashboard.html").read_text()
    client = (PUBLIC / "data-integrations.js").read_text()
    for marker in [
        'id="alpaca-history-form"',
        'id="alpaca-options-form"',
        'id="alpaca-option-history-form"',
        'id="mlb-market-form"',
    ]:
        assert marker in forecasting
    assert 'id="aws-integration-form"' in dashboard
    assert "/api/market-data/stocks/history" in client
    assert "/api/market-data/options/history" in client
    assert "/api/sports/mlb/history" in client
    assert "/api/me/aws-integration" in client
