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


def test_q_forecast_has_no_standalone_indicators_panel_and_ai_is_opt_in():
    forecasting = (PAGES / "forecasting.html").read_text()
    screener = (PAGES / "screener.html").read_text()
    client = (PUBLIC / "app.js").read_text()
    backend = (ROOT / "functions_explore" / "src" / "index.ts").read_text()
    analysis = (ROOT / "functions_explore" / "src" / "forecastAnalysis.ts").read_text()

    assert "Quantura Forecast" in forecasting
    assert ">Q Forecast<" in forecasting
    assert "Meta Prophet Forecast" not in forecasting
    assert 'id="technical-indicators"' not in forecasting
    assert 'id="forecast-ai-host"' in forecasting
    assert 'data-panel="indicators"' not in forecasting
    assert 'data-panel-target="indicators"' not in forecasting
    assert '/forecasting#technical-indicators' not in screener

    assert "buildForecastWorkspaceContext" in client
    assert "((target - market.currentPrice) / market.currentPrice) * 100" in client
    assert "Object.freeze([0.01, 0.25, 0.5, 0.75, 0.99])" in client
    assert 'extremeBear: { label: "Extreme Bear", quantile: "P1"' in client
    assert 'bear: { label: "Bear", quantile: "P25"' in client
    assert 'base: { label: "Base", quantile: "P50"' in client
    assert 'bull: { label: "Bull", quantile: "P75"' in client
    assert 'extremeBull: { label: "Extreme Bull", quantile: "P99"' in client
    assert 'addBand(q01Key, q99Key, "P1\\u2013P99 extreme range"' in client
    assert 'addBand(q25Key, q75Key, "P25\\u2013P75"' in client
    assert 'fetch("/api/forecast-analysis"' in client
    assert 'data-action="forecast-ai-generate"' in client
    assert client.count("runForecastAiAnalysis({") == 1
    assert "runForecastAutoSummary" not in client
    assert "buildLocalIndicatorAnalysis" not in client
    assert 'fetch("/api/indicators/analyze"' in client

    assert 'ROUTES.post("/forecast-analysis"' in backend
    assert "FORECAST_ANALYSIS_SYSTEM_PROMPT" in analysis
    for quantile in ("P1", "P25", "P50", "P75", "P99"):
        assert quantile in analysis
    assert "P99 does not mean the stock will reach P99" in analysis
    assert "P1 does not mean the stock will fall to P1" in analysis


def test_meta_prophet_pipeline_uses_canonical_ordered_quantiles_in_both_backends():
    node_model = (ROOT / "functions_explore" / "src" / "forecastingScreener.ts").read_text()
    node_api = (ROOT / "functions_explore" / "src" / "index.ts").read_text()
    legacy_model = (ROOT / "functions_legacy_vercel" / "main.py").read_text()

    assert "META_PROPHET_FORECAST_QUANTILES = Object.freeze([0.01, 0.25, 0.5, 0.75, 0.99])" in node_model
    assert "quantiles: META_PROPHET_FORECAST_QUANTILES" in node_api
    assert "Forecast quantile ordering failed" in node_model
    assert "META_PROPHET_QUANTILES = [0.01, 0.25, 0.5, 0.75, 0.99]" in legacy_model
    assert 'quantiles = list(META_PROPHET_QUANTILES) if service == "prophet"' in legacy_model
    assert "Forecast quantile ordering failed" in legacy_model


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
        'id="prediction-market-form"',
        'value="polymarket_us" checked',
        'value="kalshi"',
        'id="pm-market-list"',
        'id="pm-canvas-options"',
        'id="pm-preview-table"',
        'id="pm-download-csv"',
    ]:
        assert marker in forecasting
    assert 'id="aws-integration-form"' in dashboard
    assert "/api/market-data/stocks/history" in client
    assert "/api/market-data/options/history" in client
    assert "/api/sports/prediction-markets/categories" in client
    assert "/api/sports/prediction-markets/markets" in client
    assert "/api/sports/prediction-markets/preview" in client
    assert "/api/sports/prediction-markets/export" in client
    assert "initMlb" not in client
    assert "/api/me/aws-integration" in client


def test_prediction_market_hub_is_capability_driven_and_canvas_ready():
    forecasting = (PAGES / "forecasting.html").read_text()
    backend = (ROOT / "functions_explore" / "src" / "predictionMarketData.ts").read_text()
    client = (PUBLIC / "data-integrations.js").read_text()

    for text in [
        "<title>Quantura Forecasting | Cross-Asset Forecast Intelligence</title>",
        "Prediction Market Historical Data",
        "Raw provider data",
        "Normalized data",
        "SageMaker Canvas ready",
        "Pregame only",
        "Missing intervals",
    ]:
        assert text in forecasting
    for route in [
        '"/sports/prediction-markets/status"',
        '"/sports/prediction-markets/categories"',
        '"/sports/prediction-markets/markets"',
        '"/sports/prediction-markets/preview"',
        '"/sports/prediction-markets/export"',
    ]:
        assert route in backend
    assert 'headers = ["item_id", "timestamp", "target", ...features]' in backend
    assert 'stableItemId(contract.source, contract.contractId)' in backend
    assert 'timestamp >= eventStart' in backend
    assert '"Canvas target values use decimal probability units from 0.00 to 1.00."' in backend
    assert 'input[name="pm-source"]:checked' in client
    assert 'input[name="pm-mode"]:checked' in client
    assert "function invalidatePreview()" in client
    assert 'validation.classList.add("hidden")' in client
    assert "Preview rows will appear here." in client


def test_pricing_describes_platform_features_without_generic_model_pricing():
    pricing = (PAGES / "pricing.html").read_text().lower()
    for required in [
        "prediction csv",
        "anomaly analysis",
        "historical equity",
        "options",
        "polymarket us/kalshi",
        "aws/sagemaker",
        "billed separately by aws",
        "$39/month",
        "$374/year",
        "$99/month",
        "$950/year",
        "$249/month",
        "$2,390/year",
        "enterprise data licensing",
    ]:
        assert required in pricing
    for removed in ["gpt token", "llm token", "ai model access", "quantura go", "quantura plus", "quantura business", "quantura desk"]:
        assert removed not in pricing


def test_platform_api_keys_collaboration_and_openapi_are_discoverable():
    dashboard = (PAGES / "dashboard.html").read_text()
    docs = (PAGES / "developers-api.html").read_text()
    client = (PUBLIC / "platform-api.js").read_text()
    backend = (ROOT / "functions_explore" / "src" / "apiAccess.ts").read_text()
    openapi = (ROOT / "functions_explore" / "src" / "openapi.ts").read_text()
    for marker in ['data-panel="developer"', 'id="api-key-form"', 'id="api-key-scopes"', 'id="api-key-list"']:
        assert marker in dashboard
    for marker in ["Workspaces &amp; collaborators", "List accessible workspaces", "Read shared forecasts", "Viewer policy", "Dataset catalog"]:
        assert marker in docs
    assert 'Authorization: `Bearer ${token}`' in client
    assert "Copy this key now. It will not be shown again." in client
    assert 'const KEY_PREFIX = "qnt_live_"' in backend
    assert "createHmac" in backend
    assert "api_key_revoked" in backend
    assert '"/workspaces/{workspace_id}/{resource}"' in openapi
    assert '"/datasets/forecast-trajectories"' in openapi


def test_shared_screener_is_lazy_and_market_headlines_panel_is_removed():
    forecasting = (PAGES / "forecasting.html").read_text()
    dedicated = (PAGES / "screener.html").read_text()
    lazy_loader = (PUBLIC / "screener-panel-lazy.js").read_text()
    workspace_loader = (PUBLIC / "screener-workspace-loader.js").read_text()
    screener_client = (PUBLIC / "screener.js").read_text()
    vercel = json.loads((ROOT / "vercel.json").read_text())
    assert 'data-panel="screener"' in forecasting
    assert 'id="forecasting-screener-workspace"' in forecasting
    assert '/screener.js' in dedicated
    assert 'script.src = "/screener-workspace-loader.js' in lazy_loader
    assert 'data-panel-target="screener"' in lazy_loader
    assert 'new URLSearchParams(window.location.search).get("panel") === "screener"' in lazy_loader
    assert "window.QuanturaScreenerWorkspace = Object.freeze({ mount })" in workspace_loader
    assert 'params.set("panel", "screener")' in screener_client
    assert "screener-plus.js" not in forecasting
    assert 'data-panel="market-headlines"' not in forecasting
    assert next(item for item in vercel["redirects"] if item["source"] == "/market-headlines")["destination"] == "/forecasting"


def test_five_model_ensemble_is_integrated_into_existing_forecast_workspace():
    forecasting = (PAGES / "forecasting.html").read_text()
    client = (PUBLIC / "app.js").read_text()
    backend = (ROOT / "functions_explore" / "src" / "ensembleForecastRoutes.ts").read_text()
    workflow = (ROOT.parent / ".github" / "workflows" / "ensemble-forecast.yml").read_text()
    docs = (PAGES / "developers-api.html").read_text()
    assert forecasting.count('id="forecast-form"') == 1
    for marker in [
        'id="ensemble-forecast-form"',
        'id="ensemble-model-list"',
        'id="ensemble-custom-quantiles"',
        'id="ensemble-prediction-length"',
        'id="ensemble-forecast-results"',
        'id="ensemble-download-csv"',
    ]:
        assert marker in forecasting
    for model in ["Prophet", "Toto 2.0", "Granite", "Chronos-2", "TimesFM 3.0"]:
        assert model in forecasting
    assert 'addEventListener("toggle"' in client
    assert 'apiRequestJson("/api/v1/ensemble-forecasts"' in client
    assert "pollEnsembleForecast" in client
    assert "Component prediction arrays remain private" in client
    assert 'router.post("/v1/ensemble-forecasts"' in backend
    assert 'router.get("/v1/ensemble-forecasts/:forecastId"' in backend
    assert "effective_weights_by_quantile" in backend
    assert "forecast_job_id" in workflow
    assert "Raw datasets were not passed through workflow inputs." in workflow
    assert "Five-model ensemble forecasting" in docs


def test_ensemble_timesfm_gating_and_server_only_persistence_are_explicit():
    backend = (ROOT / "functions_explore" / "src" / "ensembleForecastRoutes.ts").read_text()
    registry = json.loads((ROOT / "functions_explore" / "src" / "ensembleModelRegistry.json").read_text())
    rules = (ROOT / "firestore.rules").read_text()
    assert 'envTrue("TIMESFM_HF_ACCESS_APPROVED")' in backend
    assert 'envTrue("TIMESFM_COMMERCIAL_LICENSED")' in backend
    assert 'envTrue("ALLOW_NONCOMMERCIAL_TIMESFM")' in backend
    assert registry["models"]["timesfm"]["quantileSupport"]["extrapolate"] is False
    assert registry["models"]["toto"]["quantileSupport"]["extrapolate"] is False
    for collection in [
        "ensemble_forecast_jobs",
        "ensemble_forecast_results",
        "ensemble_forecast_presets",
        "ensemble_forecast_cache",
        "ensemble_forecast_idempotency",
    ]:
        assert f"match /{collection}/" in rules


def test_cross_asset_search_and_optional_search_grounded_analysis_are_explicit():
    forecasting = (PAGES / "forecasting.html").read_text()
    client = (PUBLIC / "market-search.js").read_text()
    analysis_client = (PUBLIC / "app.js").read_text()
    backend = (ROOT / "functions_explore" / "src" / "marketSearch.ts").read_text()
    prompt = (ROOT / "functions_explore" / "src" / "forecastAnalysis.ts").read_text()
    for marker in ['id="market-search-query"', 'id="market-search-source"', 'id="market-search-results"', 'id="forecast-source"']:
        assert marker in forecasting
    for source in ["alpaca", "yahoo", "polymarket_us", "kalshi"]:
        assert source in backend
    assert "/api/market-search" in client
    assert "data-forecast-market-context" in analysis_client
    assert 'analysisMode: "live"' in analysis_client
    assert "live_tool_forbidden_in_backtest" in (ROOT / "functions_explore" / "src" / "index.ts").read_text()
    assert "Hypothetical Headline Scenario" in prompt


def test_options_expirations_auto_load_and_contract_rows_are_accessible():
    client = (PUBLIC / "data-integrations.js").read_text()
    forecasting = (PAGES / "forecasting.html").read_text()
    assert "scheduleExpirations" in client
    assert "loadExpirations" in client
    assert 'setAttribute("aria-selected", "true")' in client
    assert 'event.key !== "Enter" && event.key !== " "' in client
    assert ">Select</th>" not in forecasting
    assert "Refresh expirations" in forecasting
    assert "Load expirations" not in forecasting


def test_static_pages_do_not_shadow_authoritative_forecasting_or_dashboard_ssr_routes():
    assert not (PUBLIC / "forecasting.html").exists()
    assert not (PUBLIC / "dashboard.html").exists()


def test_product_interactions_calendar_and_safe_csv_export_are_present():
    dashboard = (PAGES / "dashboard.html").read_text()
    client = (PUBLIC / "app.js").read_text()
    backend = (ROOT / "functions_explore" / "src" / "platformApiRoutes.ts").read_text()
    assert 'id="calendar-export"' in dashboard
    assert "calendar_interactions" in client
    assert "quantura-productivity-calendar" in client
    assert "if (/^[=+\\-@]/.test(clean))" in client
    assert 'router.post("/calendar/interactions"' in backend
    assert "foundry-notes" not in (PAGES / "forecasting.html").read_text()


def test_prediction_market_runners_and_watchdog_are_independent():
    polymarket = (ROOT.parent / ".github" / "workflows" / "polymarket-quantile-forecast.yml").read_text()
    kalshi = (ROOT.parent / ".github" / "workflows" / "kalshi-quantile-forecast.yml").read_text()
    runner = (ROOT / "scripts" / "prediction_market_quantile_runner.py").read_text()
    smoke = (ROOT.parent / ".github" / "workflows" / "quantura-live-smoke.yml").read_text()
    assert "group: polymarket-quantile-forecast" in polymarket
    assert "group: kalshi-quantile-forecast" in kalshi
    assert "runs-on: ubuntu-latest" in polymarket and "runs-on: ubuntu-latest" in kalshi
    assert "PROVIDER: polymarket_us" in polymarket and 'choices=("polymarket_us", "kalshi")' in runner
    assert "PROVIDER: kalshi" in kalshi and 'choices=("polymarket_us", "kalshi")' in runner
    for quantile in ["p1", "p25", "p50", "p75", "p99"]:
        assert quantile in runner
    assert "/api/health/watchdog" in smoke


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


def test_notifications_wait_for_an_active_service_worker_before_subscribing():
    client = (PUBLIC / "app.js").read_text()
    worker = (PUBLIC / "firebase-messaging-sw.js").read_text()
    ensure_start = client.index("const ensureMessagingServiceWorker")
    ensure_end = client.index("const loadVapidKey", ensure_start)
    ensure_worker = client[ensure_start:ensure_end]
    assert "navigator.serviceWorker.ready" in ensure_worker
    assert "activeRegistration?.active" in ensure_worker
    assert 'self.addEventListener("install"' in worker
    assert "self.skipWaiting()" in worker
    assert 'self.addEventListener("activate"' in worker
    assert "clients.claim()" in worker

    vercel_config = (ROOT / "vercel.json").read_text()
    firebase_config = (ROOT / "firebase.json").read_text()
    assert vercel_config.index('"source": "/(.*).js"') < vercel_config.index(
        '"source": "/firebase-messaging-sw.js"'
    )
    assert firebase_config.index('"source": "**/*.js"') < firebase_config.index(
        '"source": "/firebase-messaging-sw.js"'
    )


def test_shared_branding_uses_favicon_and_footer_has_no_personal_address():
    client = (PUBLIC / "app.js").read_text()
    ssr = (ROOT / "functions_ssr" / "index.js").read_text()
    assert 'const QUANTURA_ICON_URL = "/favicon.svg?v=20260903a"' in client
    assert 'const PUBLIC_SHELL_ASSET_VERSION = "20260903a"' in ssr
    assert ".replace(/\\/assets\\/quantura-icon\\.svg/g" in ssr
    assert "node.innerHTML = '<a href=\"mailto:hello@quantura.studio\">hello@quantura.studio</a>'" in client
    for marker in [
        ">Q Forecast<",
        ">Quantitative Screener<",
        ">Quantura Forecasts<",
        ">API reference<",
        ">Developer documentation<",
        ">Data licensing<",
    ]:
        assert marker in client


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
    # Finder/iCloud conflict copies are untracked local artifacts and must not
    # change the authoritative page inventory used by clean CI checkouts.
    page_files = sorted(path.relative_to(PAGES) for path in PAGES.rglob("*.html") if not path.stem.endswith(" 2"))
    template_files = sorted(path.relative_to(templates) for path in templates.rglob("*.html") if not path.stem.endswith(" 2"))
    assert template_files == page_files
    for relative_path in page_files:
        assert (templates / relative_path).read_bytes() == (PAGES / relative_path).read_bytes()
