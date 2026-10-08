import importlib.util
import datetime as dt
import json
import subprocess
import sys
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[2] / "scripts" / "quant_screener_pipeline.py"
SPEC = importlib.util.spec_from_file_location("quant_screener_pipeline", MODULE_PATH)
assert SPEC and SPEC.loader
pipeline = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pipeline)


def test_publication_configuration_works_without_worker_dependencies():
    result = subprocess.run([sys.executable, "-S", "-c",
        "from scripts.weekly_screener import configuration_hash; print(configuration_hash())"],
        cwd=MODULE_PATH.parents[1], check=True, capture_output=True, text=True)
    assert result.stdout.strip() == pipeline.configuration_hash()


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


def test_chunk_zero_reuses_a_valid_checkpoint(tmp_path, monkeypatch, capsys):
    universe_path = tmp_path / "universe.json"
    output_path = tmp_path / "chunk-0.json"
    universe_path.write_text(
        json.dumps(
            {
                "schema_version": pipeline.SCHEMA_VERSION,
                "scan_date": "2026-08-24",
                "universe_hash": "checkpoint-fixture",
                "items": [base_item("SPY", is_nasdaq=False, is_etf=True)],
            }
        ),
        encoding="utf-8",
    )
    output_path.write_text(
        json.dumps(
            {
                "schema_version": pipeline.SCHEMA_VERSION,
                "universe_hash": "checkpoint-fixture",
                "forecast_config_hash": pipeline.configuration_hash(),
                "scan_date": "2026-08-24",
                "chunk": 0,
                "chunk_count": 1,
                "items": [{**base_item("SPY"), "status": "success"}],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(pipeline, "fetch_alpaca_histories", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("provider called")))
    args = type(
        "Args",
        (),
        {
            "universe": str(universe_path),
            "chunk": 0,
            "chunk_count": 1,
            "output": str(output_path),
            "resume": True,
        },
    )()
    assert pipeline.command_chunk(args) == 0
    assert '"event": "chunk_reused"' in capsys.readouterr().out


def test_quantile_forecast_delegates_to_shared_ensemble(monkeypatch):
    calls=[]
    expected={"forecast_engine":"quantura_weekly_ensemble_v1"}
    def run(rows):
        calls.append(rows)
        return expected
    monkeypatch.setattr(pipeline,"build_weekly_forecast",run)
    rows=history()
    assert pipeline.build_forecast(rows) is expected
    assert calls == [rows]


def test_missing_history_is_reported_not_silently_dropped():
    row = pipeline.completed_row(base_item("PLTR"), [], None, "test")
    assert row["status"] == "missing_market_data"
    assert row["forecast_available"] is False


def test_delayed_chunks_freeze_inputs_at_the_selected_close(monkeypatch):
    rows = [{"timestamp": f"2026-09-{day}T00:00:00Z", "close": price}
            for day, price in [(25, 100), (28, 101), (29, 102)]]
    inputs = []
    monkeypatch.setattr(pipeline, "build_forecast", lambda history: inputs.append(history))
    cutoff = dt.datetime.fromisoformat("2026-09-28T20:00:00+00:00")
    result = pipeline.completed_row(base_item("AAPL"), rows, None, "test", as_of=cutoff)
    assert [r["timestamp"][:10] for r in inputs[0]] == ["2026-09-25", "2026-09-28"]
    assert result["actual_price"] == 101
    assert result["actual_price_timestamp"].startswith("2026-09-28")


def test_missing_latest_session_is_not_published_as_a_fresh_forecast(monkeypatch):
    monkeypatch.setattr(pipeline, "build_forecast", lambda *_: (_ for _ in ()).throw(AssertionError("stale forecast")))
    result = pipeline.completed_row(base_item("AAPL"), [{"timestamp": "2026-09-25T00:00:00Z", "close": 100}],
                                    None, "test", as_of=dt.datetime.fromisoformat("2026-09-28T20:00:00+00:00"))
    assert result["status"] == "missing_market_data"
    assert result["error_code"] == "latest_session_bar_unavailable"
    assert result["forecast_available"] is False


def test_historical_sip_end_is_delayed_and_pagination_preserves_all_symbols(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "test-key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test-secret")
    monkeypatch.delenv("ALPACA_DATA_FEED", raising=False)
    now = dt.datetime.fromisoformat("2026-10-07T20:17:00+00:00")
    monkeypatch.setattr(pipeline, "utc_now", lambda: now)
    calls = []
    class Response:
        def __init__(self, payload): self.payload = payload
        def json(self): return self.payload
    def request(method, url, *, params, headers):
        calls.append(dict(params))
        return Response({"bars": {"AAPG": [{"t": "2026-10-07T04:00:00Z", "c": 15}]} , "next_page_token": "next"}) if len(calls) == 1 else Response({"bars": {"ACNB": [{"t": "2026-10-07T04:00:00Z", "c": 40}]}})
    monkeypatch.setattr(pipeline, "request_with_retry", request)
    result = pipeline.fetch_alpaca_histories(["AAPG", "ACNB"], "2025-01-01", "2026-10-07")
    assert all(c["feed"] == "sip" and c["end"] == "2026-10-07T20:01:00Z" for c in calls)
    assert calls[1]["page_token"] == "next"
    assert result["AAPG"][0]["close"] == 15 and result["ACNB"][0]["close"] == 40


def test_historical_sip_preserves_requested_past_end(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "test-key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test-secret")
    calls = []
    class Response:
        def json(self): return {"bars": {}}
    def request(*args, **kwargs):
        calls.append(kwargs["params"])
        return Response()
    monkeypatch.setattr(pipeline, "request_with_retry", request)
    pipeline.fetch_alpaca_histories(["AAPL"], "2025-01-01", "2026-01-01")
    assert calls[0]["end"] == "2026-01-01T23:59:59Z"


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
