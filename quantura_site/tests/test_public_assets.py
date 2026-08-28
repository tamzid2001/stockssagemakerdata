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
    assert redirect_sources["/explore"] == "/forecasting"
    assert redirect_sources["/indicators"] == "/forecasting"
    assert redirect_sources["/profile"] == "/account"
    assert redirect_sources["/u/:path*"] == "/account"
    assert redirect_sources["/tools/fx"] == "/forecasting"
    assert not (PUBLIC / "explore.html").exists()
    assert not (PUBLIC / "profile.html").exists()
    assert not (PAGES / "tools/fx.html").exists()


def test_meta_prophet_is_the_only_forecast_indicator_workspace_and_ai_is_opt_in():
    forecasting = (PAGES / "forecasting.html").read_text()
    screener = (PAGES / "screener.html").read_text()
    client = (PUBLIC / "app.js").read_text()
    backend = (ROOT / "functions_explore" / "src" / "index.ts").read_text()
    analysis = (ROOT / "functions_explore" / "src" / "forecastAnalysis.ts").read_text()

    assert "Meta Prophet Forecast" in forecasting
    assert 'id="technical-indicators"' in forecasting
    assert 'id="forecast-ai-host"' in forecasting
    assert 'data-panel="indicators"' not in forecasting
    assert 'data-panel-target="indicators"' not in forecasting
    assert '/forecasting#technical-indicators' in screener

    assert "buildForecastWorkspaceContext" in client
    assert "((target - market.currentPrice) / market.currentPrice) * 100" in client
    assert 'fetch("/api/forecast-analysis"' in client
    assert 'data-action="forecast-ai-generate"' in client
    assert client.count("runForecastAiAnalysis({") == 1
    assert "runForecastAutoSummary" not in client
    assert "buildLocalIndicatorAnalysis" not in client
    assert 'fetch("/api/indicators/analyze"' in client

    assert 'ROUTES.post("/forecast-analysis"' in backend
    assert "FORECAST_ANALYSIS_SYSTEM_PROMPT" in analysis


def test_explore_surface_and_generated_content_are_retired():
    client = (PUBLIC / "app.js").read_text().lower()
    assert "explore feed" not in client
    assert not (PAGES / "explore.html").exists()
    assert not (PAGES / "blog" / "topics" / "explore-workflows.html").exists()
    manifest = (PAGES / "blog" / "posts.manifest.json").read_text().lower()
    assert '"explore-workflows"' not in manifest


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


def test_pricing_describes_platform_features_without_generic_model_pricing():
    pricing = (PAGES / "pricing.html").read_text().lower()
    for required in [
        "prediction csv",
        "quantile anomaly analysis",
        "historical equities",
        "options",
        "mlb polymarket",
        "aws/sagemaker",
        "billed separately by aws",
    ]:
        assert required in pricing
    for removed in ["gpt-5", "gpt token", "llm token", "ai model access"]:
        assert removed not in pricing


def test_prediction_csv_analysis_is_available_from_forecast_foundry():
    forecasting = (PAGES / "forecasting.html").read_text()
    client = (PUBLIC / "app.js").read_text()
    assert 'id="foundry-source-kind" name="sourceKind" type="hidden" value="prediction_csv"' in forecasting
    assert "Download business-day CSV" not in forecasting
    assert 'id="foundry-file-help"' in forecasting
    assert "P50 95% Statistical Anomaly Band" in client
    for marker in [
        "forecast-summary-card",
        "p50Anomalies",
        "p10Anomalies",
        "Extended P10 Buy Bias",
    ]:
        assert marker in client


def test_historical_data_supports_alpaca_yahoo_and_no_start_date():
    forecasting = (PAGES / "forecasting.html").read_text()
    client = (PUBLIC / "data-integrations.js").read_text()
    assert 'id="market-history-source"' in forecasting
    assert 'value="auto" selected>Automatic — Alpaca, then Yahoo Finance' in forecasting
    assert 'value="alpaca">Alpaca' in forecasting
    assert 'value="yahoo">Yahoo Finance' in forecasting
    assert 'id="options-market-source"' in forecasting
    assert 'id="alpaca-start"' not in forecasting
    assert 'source: byId("market-history-source")?.value || "auto"' in client


def test_notifications_page_has_functional_inbox_and_delivery_controls():
    dashboard = (PAGES / "dashboard.html").read_text()
    client = (PUBLIC / "app.js").read_text()
    for marker in [
        'id="notifications-items"',
        'id="notifications-unread-count"',
        'id="notifications-mark-all"',
        'data-notification-filter="unread"',
        'id="notifications-enable"',
        'id="notifications-send-test"',
        'id="notifications-privacy-host"',
    ]:
        assert marker in dashboard
    assert "loadNotificationFeed" in client
    assert "markAllNotificationsRead" in client
    assert "MODEL_COUNCIL_OUTPUT_DISCLAIMER" not in client


def test_blog_index_lists_every_current_generated_post():
    import json
    from datetime import date

    manifest = json.loads((PAGES / "blog" / "posts.manifest.json").read_text())
    index = (PAGES / "blog" / "index.html").read_text()
    assert manifest["count"] == len(manifest["posts"])
    assert manifest["count"] >= 77
    assert manifest["posts"][0]["dateIso"] <= date.today().isoformat()
    for post in manifest["posts"]:
        assert f'/blog/posts/{post["slug"]}' in index


def test_quantitative_screener_surface_replaces_manual_prophet_dispatch():
    screener = (PAGES / "screener.html").read_text()
    client = (PUBLIC / "screener.js").read_text()
    styles = (PUBLIC / "screener.css").read_text()
    for marker in [
        'id="qs-search"',
        'id="qs-universe"',
        'id="qs-market-cap"',
        'value="above-p50"',
        'value="below-p90"',
        'id="qs-special-p10"',
        'id="qs-table-body"',
        'id="qs-pagination"',
        "Download CSV",
    ]:
        assert marker in screener
    assert "/api/screener/data" in client
    assert "history.pushState" in client
    assert "@media (max-width: 760px)" in styles
    assert 'id="screener-generate-button"' not in screener
    assert "$100B" not in screener


def test_mobile_cookie_banner_resets_desktop_centering_transform():
    styles = (PUBLIC / "professional.css").read_text()
    mobile_start = styles.index("@media (max-width: 640px)")
    mobile_rules = styles[mobile_start:]
    cookie_start = mobile_rules.index(".cookie-banner")
    cookie_end = mobile_rules.index("}", cookie_start)
    cookie_rule = mobile_rules[cookie_start:cookie_end]
    assert "left: 10px !important" in cookie_rule
    assert "right: 10px !important" in cookie_rule
    assert "transform: none !important" in cookie_rule


def test_ssr_templates_mirror_every_public_html_page():
    templates = ROOT / "functions_ssr" / "templates"
    page_files = sorted(path.relative_to(PAGES) for path in PAGES.rglob("*.html"))
    template_files = sorted(path.relative_to(templates) for path in templates.rglob("*.html"))
    assert template_files == page_files
    for relative_path in page_files:
        assert (templates / relative_path).read_bytes() == (PAGES / relative_path).read_bytes()
