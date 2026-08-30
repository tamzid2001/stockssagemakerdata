import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PAGES = ROOT / "pages"
PUBLIC = ROOT / "public"
FUNCTIONS = ROOT / "functions_explore" / "src"


def test_public_forecast_surfaces_are_explicitly_prospective():
    hub = (PAGES / "forecasts.html").read_text(encoding="utf-8")
    detail = (PAGES / "forecast-detail.html").read_text(encoding="utf-8")
    home = (PAGES / "index.html").read_text(encoding="utf-8")
    client = (PUBLIC / "forecasts.js").read_text(encoding="utf-8")

    for marker in ["Quantura Forecasts", "Tomorrow's Headlines", "A prediction record, not a news feed"]:
        assert marker in hub
    assert "THIS EVENT HAS NOT OCCURRED" in client
    assert "Forecasts are probabilistic scenarios" in detail
    assert 'id="home-forecast-preview"' in home
    assert "BREAKING NEWS" not in hub.upper()
    assert "BREAKING NEWS" not in detail.upper()


def test_versioned_forecast_api_and_admin_routes_are_server_backed():
    routes = (FUNCTIONS / "quanturaForecastRoutes.ts").read_text(encoding="utf-8")
    expected = [
        '"/v1/forecasts"',
        '"/v1/forecasts/:forecastId"',
        '"/v1/forecasts/:forecastId/history"',
        '"/v1/forecasts/:forecastId/resolution"',
        '"/v1/forecasts/resolved"',
        '"/v1/entities/:entity/forecasts"',
        '"/v1/calibration"',
        '"/v1/performance"',
        '"/v1/categories"',
        '"/v1/forecast-feed"',
        '"/v1/datasets/forecast-trajectories"',
        '"/forecasts/admin/:forecastId/publish"',
        '"/forecasts/admin/:forecastId/revisions"',
        '"/forecasts/admin/:forecastId/resolve"',
        '"/forecasts/admin/:forecastId/amendments"',
        '"/forecasts/admin/api-keys/:keyId/revoke"',
    ]
    for endpoint in expected:
        assert endpoint in routes
    assert "extractApiKey" in routes
    assert "QUANTURA_FORECAST_API_KEY_PEPPER" in routes
    assert "enforceRateLimit" in routes
    assert "writeAudit" in routes


def test_forecast_storage_and_http_access_are_separated():
    rules = (ROOT / "firestore.rules").read_text(encoding="utf-8")
    firebase = json.loads((ROOT / "firebase.json").read_text(encoding="utf-8"))
    rewrite_sources = {item["source"] for item in firebase["hosting"]["rewrites"]}

    assert "match /quantura_forecasts/{forecastId}" in rules
    assert "match /probability_history/{revisionId}" in rules
    assert "match /amendments/{amendmentId}" in rules
    assert "match /quantura_forecast_api_keys/{keyId}" in rules
    assert '{ "source": "/api/v1/**", "function": "quanturaExploreApi" }' in (ROOT / "firebase.json").read_text()
    assert "/forecasts/**" in rewrite_sources
    assert "/admin/forecasts" in rewrite_sources


def test_private_alpha_is_not_serialized_to_public_or_enterprise_consumers():
    domain = (FUNCTIONS / "quanturaForecasts.ts").read_text(encoding="utf-8")
    public_projection = domain[domain.index("export function publicForecastProjection"):domain.index("export function enterpriseForecastProjection")]
    enterprise_projection = domain[domain.index("export function enterpriseForecastProjection"):domain.index("export function buildCalibrationRows")]

    assert "private_strategy_json" not in public_projection
    assert "private_strategy_json" not in enterprise_projection
    assert "private_strategy_json" in domain
    assert "sanitizeDatasetRecord" in domain


def test_forecast_api_documentation_covers_auth_pagination_and_bulk_versions():
    api_docs = (ROOT.parent / "docs" / "quantura-forecasts-api.md").read_text(encoding="utf-8")
    operations = (ROOT.parent / "docs" / "quantura-forecasts-operations.md").read_text(encoding="utf-8")
    for marker in ["Authorization: Bearer", "X-API-Key", "next_cursor", "forecast-trajectories", "JSONL", "CSV"]:
        assert marker in api_docs
    for marker in ["QUANTURA_FORECAST_API_KEY_PEPPER", "Backup", "Recovery", "RPO", "RTO"]:
        assert marker in operations
