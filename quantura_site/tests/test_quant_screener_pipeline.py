import importlib.util
import json
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[2] / "scripts" / "quant_screener_pipeline.py"
SPEC = importlib.util.spec_from_file_location("quant_screener_pipeline", MODULE_PATH)
assert SPEC and SPEC.loader
pipeline = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pipeline)


NASDAQ_FIXTURE = """Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares
AAPL|Apple Inc. - Common Stock|Q|N|N|100|N|N
PLTR|Palantir Technologies Inc. - Class A Common Stock|Q|N|N|100|N|N
TESTZ|Nasdaq Test Stock|Q|Y|N|100|N|N
ACMEW|Acme Corp - Warrant|Q|N|N|100|N|N
QQQ|Invesco QQQ Trust|G|N|N|100|Y|N
File Creation Time: 0822202618:02|||||||
"""


def base_item(ticker, **overrides):
    item = {
        "ticker": ticker,
        "company_name": ticker,
        "exchange": "NASDAQ",
        "is_sp500": False,
        "is_nasdaq": True,
        "is_etf": False,
        "asset_type": "equity",
        "sector": None,
        "industry": None,
    }
    item.update(overrides)
    return item


def history(start=100.0, points=120):
    rows = []
    for index in range(points):
        day = 1 + index
        rows.append({"timestamp": f"2026-01-{min(day, 28):02d}T21:00:00Z", "close": start * (1.001 ** index)})
    # Forecast code only needs the last timestamp to be a valid date.
    rows[-1]["timestamp"] = "2026-06-30T21:00:00Z"
    return rows


def test_nasdaq_universe_filters_non_common_instruments():
    rows = pipeline.parse_nasdaq_listed(NASDAQ_FIXTURE)
    assert [row["ticker"] for row in rows] == ["AAPL", "PLTR"]


def test_sp500_universe_preserves_sector_and_industry():
    rows = pipeline.parse_sp500_table(
        [{"Symbol": "AAPL", "Security": "Apple", "GICS Sector": "Information Technology", "GICS Sub-Industry": "Technology Hardware"}]
    )
    assert rows[0]["is_sp500"] is True
    assert rows[0]["sector"] == "Information Technology"
    assert rows[0]["industry"] == "Technology Hardware"


def test_stock_metadata_snapshot_parses_market_cap_without_per_symbol_calls():
    rows = pipeline.parse_stock_metadata_rows(
        [
            {
                "symbol": "PLTR",
                "name": "Palantir Technologies Inc.",
                "marketCap": "417500000000.00",
                "sector": "Technology",
                "industry": "EDP Services",
            },
            {"symbol": "INVALID", "marketCap": "N/A"},
        ]
    )
    assert rows["PLTR"]["market_cap"] == 417_500_000_000
    assert rows["PLTR"]["sector"] == "Technology"
    assert rows["INVALID"]["market_cap"] is None


def test_universe_deduplicates_cross_membership_and_includes_spy():
    combined, duplicates = pipeline.merge_universes(
        [base_item("AAPL", is_sp500=True, is_nasdaq=False, sector="Information Technology")],
        [base_item("AAPL"), base_item("PLTR")],
    )
    by_ticker = {row["ticker"]: row for row in combined}
    assert duplicates == 1
    assert by_ticker["AAPL"]["is_sp500"] is True
    assert by_ticker["AAPL"]["is_nasdaq"] is True
    assert by_ticker["SPY"]["is_etf"] is True
    assert by_ticker["SPY"]["next_earnings_date"] == "N/A — ETF"


def test_deterministic_chunks_cover_every_symbol_once():
    symbols = [f"T{index}" for index in range(100)]
    buckets = [[symbol for symbol in symbols if pipeline.item_chunk(symbol, 8) == chunk] for chunk in range(8)]
    flattened = [symbol for bucket in buckets for symbol in bucket]
    assert sorted(flattened) == sorted(symbols)
    assert len(flattened) == len(set(flattened))


def test_development_subset_still_includes_spy():
    items = [base_item(f"T{index}") for index in range(20)] + [base_item("SPY", is_nasdaq=False, is_etf=True, asset_type="etf")]
    selected = pipeline.deterministic_subset(items, 5)
    assert len(selected) == 5
    assert any(row["ticker"] == "SPY" for row in selected)


def test_quantile_forecast_reuses_site_drift_methodology():
    forecast = pipeline.build_forecast(history())
    assert forecast is not None
    assert len(forecast["rows"]) == 10
    assert forecast["p10"] < forecast["p50"] < forecast["p90"]
    assert forecast["forecast_engine"] == "quantura_quantile_drift_v1"


def test_missing_history_is_reported_not_silently_dropped():
    row = pipeline.completed_row(base_item("PLTR"), [], None, "test")
    assert row["status"] == "missing_market_data"
    assert row["forecast_available"] is False


def test_quantile_position_and_distances_are_strict():
    assert pipeline.quantile_position(80, 90, 100, 110) == "below_p10"
    assert pipeline.quantile_position(95, 90, 100, 110) == "between_p10_p50"
    assert pipeline.quantile_position(105, 90, 100, 110) == "between_p50_p90"
    assert pipeline.quantile_position(120, 90, 100, 110) == "above_p90"
    absolute, percentage = pipeline.distance(110, 100)
    assert absolute == 10
    assert percentage == 10


def test_market_cap_convention_does_not_classify_spy():
    assert pipeline.cap_bucket(500_000_000_000, True) is None
    assert pipeline.cap_bucket(250_000_000_000, False) == "mega"
    assert pipeline.cap_bucket(20_000_000_000, False) == "large"
    assert pipeline.cap_bucket(5_000_000_000, False) == "mid"
    assert pipeline.cap_bucket(500_000_000, False) == "small"
    assert pipeline.cap_bucket(100_000_000, False) == "micro"


def test_coverage_manifest_reports_all_failure_categories():
    universe = {"scan_date": "2026-08-24", "universe_hash": "abc", "counts": {}, "earnings": {"source": "test"}}
    rows = [
        {"status": "success"},
        {"status": "success"},
        {"status": "missing_predictions"},
        {"status": "missing_market_data"},
        {"status": "failed"},
    ]
    manifest = pipeline.coverage_manifest(universe, rows, 0.9, pipeline.time.monotonic())
    assert manifest["successfully_processed"] == 2
    assert manifest["missing_predictions"] == 1
    assert manifest["missing_market_data"] == 1
    assert manifest["failed"] == 1
    assert manifest["coverage_percentage"] == 40
    assert manifest["status"] == "degraded"


def test_aggregate_fills_missing_chunk_symbols_and_fails_coverage(tmp_path):
    universe_path = tmp_path / "universe.json"
    chunks_dir = tmp_path / "chunks"
    output_dir = tmp_path / "output"
    chunks_dir.mkdir()
    universe = {
        "scan_date": "2026-08-24",
        "universe_hash": "fixture",
        "counts": {"selected": 2},
        "earnings": {"source": "test"},
        "items": [base_item("AAPL"), base_item("PLTR")],
    }
    universe_path.write_text(json.dumps(universe), encoding="utf-8")
    (chunks_dir / "chunk-0.json").write_text(
        json.dumps(
            {
                "schema_version": pipeline.SCHEMA_VERSION,
                "universe_hash": "fixture",
                "chunk": 0,
                "items": [{**base_item("AAPL"), "status": "success"}],
            }
        ),
        encoding="utf-8",
    )
    args = type(
        "Args",
        (),
        {
            "universe": str(universe_path),
            "chunks_dir": str(chunks_dir),
            "chunk_count": 2,
            "coverage_threshold": 0.9,
            "output_dir": str(output_dir),
        },
    )()
    assert pipeline.command_aggregate(args) == 0
    payload = json.loads((output_dir / "quantura-screener-latest.json").read_text(encoding="utf-8"))
    by_ticker = {row["ticker"]: row for row in payload["items"]}
    assert by_ticker["PLTR"]["error_code"] == "missing_chunk_result"
    assert payload["manifest"]["coverage_ok"] is False
