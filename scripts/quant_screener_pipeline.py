#!/usr/bin/env python3
"""Build, scan, and aggregate the Quantura equity research universe.

The pipeline is intentionally split into three commands so GitHub Actions can
prepare the universe once, evaluate deterministic chunks in parallel, and only
publish an aggregate after coverage has been validated.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import io
import json
import math
import os
import random
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

try:
    import requests
except ModuleNotFoundError:  # Pure unit tests do not need provider dependencies installed.
    requests = None  # type: ignore[assignment]


SCHEMA_VERSION = "quantura-screener-v2"
DEFAULT_CHUNK_COUNT = 16
DEFAULT_COVERAGE_THRESHOLD = 0.90
DEFAULT_FORECAST_HORIZON = 10
Z10 = -1.2815515655446004
Z90 = 1.2815515655446004
NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
NASDAQ_EARNINGS_URL = "https://api.nasdaq.com/api/calendar/earnings"
NASDAQ_STOCK_METADATA_URL = "https://api.nasdaq.com/api/screener/stocks"
ALPACA_DATA_URL = "https://data.alpaca.markets"
USER_AGENT = "QuanturaScreener/2.0 (+https://quantura.studio/screener)"
NASDAQ_API_USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/126 Safari/537.36"


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_now() -> str:
    return utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_symbol(value: Any) -> str:
    text = str(value or "").strip().upper()
    return "".join(ch for ch in text if ch.isalnum() or ch in ".-")[:16]


def yahoo_symbol(symbol: str) -> str:
    return normalize_symbol(symbol).replace(".", "-")


def safe_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def round_optional(value: Any, digits: int = 6) -> float | None:
    parsed = safe_float(value)
    return round(parsed, digits) if parsed is not None else None


def security_is_ineligible(name: str, *, test_issue: bool, etf: bool) -> bool:
    if test_issue or etf:
        return True
    lowered = f" {name.lower()} "
    blocked = (
        " warrant",
        " warrants",
        " right",
        " rights",
        " unit",
        " units",
        " preferred",
        " depositary",
        " note due",
        " notes due",
    )
    return any(token in lowered for token in blocked)


def parse_nasdaq_listed(text: str) -> list[dict[str, Any]]:
    lines = [line.strip() for line in str(text or "").splitlines() if line.strip()]
    if not lines:
        return []
    headers = [cell.strip() for cell in lines[0].split("|")]
    rows: list[dict[str, Any]] = []
    for line in lines[1:]:
        if line.lower().startswith("file creation time"):
            continue
        cells = line.split("|")
        record = {header: str(cells[index] if index < len(cells) else "").strip() for index, header in enumerate(headers)}
        symbol = normalize_symbol(record.get("Symbol"))
        if not symbol or symbol == "FILE":
            continue
        name = str(record.get("Security Name") or "").strip()
        if security_is_ineligible(
            name,
            test_issue=str(record.get("Test Issue") or "").upper() == "Y",
            etf=str(record.get("ETF") or "").upper() == "Y",
        ):
            continue
        rows.append(
            {
                "ticker": symbol,
                "company_name": name or symbol,
                "exchange": "NASDAQ",
                "is_nasdaq": True,
                "is_sp500": False,
                "is_etf": False,
                "asset_type": "equity",
                "sector": None,
                "industry": None,
            }
        )
    return rows


def parse_sp500_table(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    parsed: list[dict[str, Any]] = []
    for record in rows:
        symbol = normalize_symbol(record.get("Symbol"))
        if not symbol:
            continue
        parsed.append(
            {
                "ticker": symbol,
                "company_name": str(record.get("Security") or symbol).strip(),
                "exchange": str(record.get("Exchange") or "").strip().upper() or None,
                "is_nasdaq": False,
                "is_sp500": True,
                "is_etf": False,
                "asset_type": "equity",
                "sector": str(record.get("GICS Sector") or "").strip() or None,
                "industry": str(record.get("GICS Sub-Industry") or "").strip() or None,
            }
        )
    return parsed


def merge_universes(sp500_rows: Sequence[Mapping[str, Any]], nasdaq_rows: Sequence[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    merged: dict[str, dict[str, Any]] = {}
    duplicates = 0
    for source in (sp500_rows, nasdaq_rows):
        for raw in source:
            ticker = normalize_symbol(raw.get("ticker"))
            if not ticker:
                continue
            if ticker in merged:
                duplicates += 1
                current = merged[ticker]
                current["is_sp500"] = bool(current.get("is_sp500") or raw.get("is_sp500"))
                current["is_nasdaq"] = bool(current.get("is_nasdaq") or raw.get("is_nasdaq"))
                for key in ("company_name", "exchange", "sector", "industry"):
                    if not current.get(key) and raw.get(key):
                        current[key] = raw.get(key)
            else:
                merged[ticker] = dict(raw)

    if "SPY" not in merged:
        merged["SPY"] = {
            "ticker": "SPY",
            "company_name": "SPDR S&P 500 ETF Trust",
            "exchange": "NYSE ARCA",
            "is_nasdaq": False,
            "is_sp500": False,
            "is_etf": True,
            "asset_type": "etf",
            "sector": None,
            "industry": None,
        }
    else:
        merged["SPY"].update({"is_etf": True, "asset_type": "etf", "is_sp500": False})

    output: list[dict[str, Any]] = []
    for ticker, row in merged.items():
        memberships: list[str] = []
        if row.get("is_sp500"):
            memberships.append("S&P 500")
        if row.get("is_nasdaq"):
            memberships.append("Nasdaq")
        if row.get("is_etf"):
            memberships.append("ETF")
        row.update(
            {
                "ticker": ticker,
                "universe_memberships": memberships,
                "forecast_available": False,
                "next_earnings_date": "N/A — ETF" if row.get("is_etf") else None,
            }
        )
        output.append(row)
    output.sort(key=lambda item: item["ticker"])
    return output, duplicates


def request_with_retry(
    method: str,
    url: str,
    *,
    attempts: int = 4,
    timeout: int = 25,
    retry_statuses: set[int] | None = None,
    **kwargs: Any,
) -> requests.Response:
    if requests is None:
        raise RuntimeError("The requests package is required for live provider calls.")
    statuses = retry_statuses or {408, 425, 429, 500, 502, 503, 504}
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            response = requests.request(method, url, timeout=timeout, **kwargs)
            if response.status_code not in statuses:
                response.raise_for_status()
                return response
            last_error = RuntimeError(f"provider returned HTTP {response.status_code}")
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as error:
            last_error = error
            status = getattr(getattr(error, "response", None), "status_code", None)
            if status is not None and status not in statuses:
                raise
        if attempt + 1 < attempts:
            time.sleep(min(8.0, 0.75 * (2**attempt)) + random.random() * 0.25)
    raise RuntimeError(str(last_error or "provider request failed"))


def fetch_sp500() -> list[dict[str, Any]]:
    import pandas as pd

    response = request_with_retry(
        "GET",
        SP500_URL,
        headers={"Accept": "text/html", "User-Agent": USER_AGENT},
    )
    tables = pd.read_html(io.StringIO(response.text))
    if not tables:
        raise RuntimeError("S&P 500 constituent table was unavailable")
    return parse_sp500_table(tables[0].to_dict(orient="records"))


def fetch_nasdaq() -> list[dict[str, Any]]:
    response = request_with_retry("GET", NASDAQ_LISTED_URL, headers={"User-Agent": USER_AGENT})
    return parse_nasdaq_listed(response.text)


def parse_stock_metadata_rows(rows: Iterable[Any]) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        ticker = normalize_symbol(row.get("symbol"))
        if not ticker:
            continue
        output[ticker] = {
            "company_name": str(row.get("name") or "").strip() or None,
            "sector": str(row.get("sector") or "").strip() or None,
            "industry": str(row.get("industry") or "").strip() or None,
            "market_cap": safe_float(row.get("marketCap")),
        }
    return output


def fetch_stock_metadata() -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Fetch one provider snapshot instead of issuing one metadata call per symbol."""
    response = request_with_retry(
        "GET",
        NASDAQ_STOCK_METADATA_URL,
        params={"tableonly": "true", "download": "true"},
        headers={
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.nasdaq.com/market-activity/stocks/screener",
            "User-Agent": NASDAQ_API_USER_AGENT,
            "X-Quantura-Client": "screener-v2",
        },
        timeout=30,
    )
    payload = response.json()
    rows = (((payload or {}).get("data") or {}).get("rows") or []) if isinstance(payload, dict) else []
    output = parse_stock_metadata_rows(rows)
    return output, {
        "source": "nasdaq_stock_screener_snapshot",
        "records": len(output),
        "fetched_at": iso_now(),
    }


def date_range(start: dt.date, days: int) -> Iterable[dt.date]:
    for offset in range(max(0, days) + 1):
        yield start + dt.timedelta(days=offset)


def fetch_earnings_for_date(day: dt.date) -> dict[str, str]:
    response = request_with_retry(
        "GET",
        NASDAQ_EARNINGS_URL,
        params={"date": day.isoformat()},
        headers={
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.nasdaq.com/",
            "User-Agent": NASDAQ_API_USER_AGENT,
            "X-Quantura-Client": "screener-v2",
        },
        attempts=1,
        timeout=7,
    )
    payload = response.json()
    rows = (((payload or {}).get("data") or {}).get("rows") or []) if isinstance(payload, dict) else []
    result: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = normalize_symbol(row.get("symbol"))
        if symbol:
            result[symbol] = day.isoformat()
    return result


def fetch_earnings_calendar(start: dt.date, days: int = 30) -> tuple[dict[str, str], dict[str, Any]]:
    weekdays = [day for day in date_range(start, days) if day.weekday() < 5]
    results: dict[str, str] = {}
    failed_days: list[str] = []
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(fetch_earnings_for_date, day): day for day in weekdays}
        for future in as_completed(futures):
            day = futures[future]
            try:
                for symbol, earnings_date in future.result().items():
                    if symbol not in results or earnings_date < results[symbol]:
                        results[symbol] = earnings_date
            except Exception:
                failed_days.append(day.isoformat())
    return results, {
        "source": "nasdaq_earnings_calendar",
        "queried_days": len(weekdays),
        "failed_days": sorted(failed_days),
        "fetched_at": iso_now(),
    }


def stable_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def universe_hash(items: Sequence[Mapping[str, Any]]) -> str:
    return stable_hash("\n".join(sorted(normalize_symbol(row.get("ticker")) for row in items)))[:20]


def deterministic_subset(items: Sequence[dict[str, Any]], limit: int | None) -> list[dict[str, Any]]:
    if not limit or limit <= 0 or limit >= len(items):
        return list(items)
    ranked = sorted(items, key=lambda row: (stable_hash(row["ticker"]), row["ticker"]))
    spy = next((row for row in ranked if row.get("ticker") == "SPY"), None)
    if not spy:
        return ranked[:limit]
    selected = [row for row in ranked if row.get("ticker") != "SPY"][: max(0, limit - 1)]
    return sorted([*selected, spy], key=lambda row: row["ticker"])


def build_matrix(chunk_count: int, item_count: int) -> dict[str, list[dict[str, int]]]:
    count = max(1, min(max(1, item_count), max(1, min(32, int(chunk_count)))))
    return {"include": [{"chunk": index, "chunk_count": count} for index in range(count)]}


def item_chunk(ticker: str, chunk_count: int) -> int:
    return int(stable_hash(normalize_symbol(ticker))[:12], 16) % max(1, chunk_count)


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")


def command_universe(args: argparse.Namespace) -> int:
    started = time.monotonic()
    sp500 = fetch_sp500()
    nasdaq = fetch_nasdaq()
    merged, duplicates = merge_universes(sp500, nasdaq)
    selected = deterministic_subset(merged, args.max_tickers)
    metadata: dict[str, dict[str, Any]] = {}
    metadata_meta: dict[str, Any] = {"source": "unavailable", "records": 0, "fetched_at": iso_now()}
    try:
        metadata, metadata_meta = fetch_stock_metadata()
    except Exception as error:
        metadata_meta["error"] = type(error).__name__
    for row in selected:
        enrichment = metadata.get(row["ticker"]) or {}
        for key in ("company_name", "sector", "industry"):
            if not row.get(key) and enrichment.get(key):
                row[key] = enrichment[key]
        row["market_cap"] = round_optional(enrichment.get("market_cap"), 2)
    earnings: dict[str, str] = {}
    earnings_meta: dict[str, Any] = {"source": "unavailable", "failed_days": [], "fetched_at": iso_now()}
    if not args.skip_earnings:
        try:
            earnings, earnings_meta = fetch_earnings_calendar(dt.date.today(), args.earnings_days)
        except Exception as error:
            earnings_meta["error"] = type(error).__name__
    for row in selected:
        if not row.get("is_etf"):
            row["next_earnings_date"] = earnings.get(row["ticker"])

    digest = universe_hash(selected)
    scan_date = str(args.scan_date or dt.date.today().isoformat())
    runtime_seconds = round(time.monotonic() - started, 2)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "scan_date": scan_date,
        "generated_at": iso_now(),
        "universe_hash": digest,
        "runtime_seconds": runtime_seconds,
        "counts": {
            "sp500": len(sp500),
            "nasdaq": len(nasdaq),
            "duplicates_removed": duplicates,
            "combined": len(merged),
            "selected": len(selected),
            "spy_included": any(row["ticker"] == "SPY" for row in selected),
        },
        "earnings": earnings_meta,
        "metadata": metadata_meta,
        "items": selected,
    }
    write_json(Path(args.output), payload)
    write_json(Path(args.matrix_output), build_matrix(args.chunk_count, len(selected)))
    print(
        json.dumps(
            {
                "event": "universe_ready",
                **payload["counts"],
                "universe_hash": digest,
                "runtime_seconds": runtime_seconds,
            },
            sort_keys=True,
        )
    )
    return 0


def next_business_days(last_day: dt.date, count: int) -> list[str]:
    output: list[str] = []
    cursor = last_day
    while len(output) < count:
        cursor += dt.timedelta(days=1)
        if cursor.weekday() < 5:
            output.append(cursor.isoformat())
    return output


def series_anomalies(values: Sequence[float]) -> dict[str, Any]:
    clean = [float(value) for value in values if safe_float(value) is not None]
    if not clean:
        return {"average": None, "standard_deviation": None, "lower": None, "upper": None, "unusual": []}
    average = statistics.fmean(clean)
    deviation = statistics.stdev(clean) if len(clean) > 1 else 0.0
    lower = average - 1.96 * deviation
    upper = average + 1.96 * deviation
    unusual = [index for index, value in enumerate(clean) if value < lower or value > upper]
    return {
        "average": average,
        "standard_deviation": deviation,
        "lower": lower,
        "upper": upper,
        "unusual": unusual,
    }


def build_forecast(history: Sequence[Mapping[str, Any]], horizon: int = DEFAULT_FORECAST_HORIZON) -> dict[str, Any] | None:
    usable = [row for row in history if safe_float(row.get("close")) not in (None, 0)]
    if len(usable) < 60:
        return None
    usable = usable[-504:]
    closes = [float(row["close"]) for row in usable]
    returns = [math.log(closes[index] / closes[index - 1]) for index in range(1, len(closes)) if closes[index - 1] > 0]
    if len(returns) < 30:
        return None
    drift = statistics.fmean(returns)
    volatility = statistics.pstdev(returns) if len(returns) > 1 else 0.0001
    volatility = max(volatility, 0.0001)
    last_date = dt.date.fromisoformat(str(usable[-1]["timestamp"])[:10])
    dates = next_business_days(last_date, horizon)
    rows: list[dict[str, Any]] = []
    for index, forecast_date in enumerate(dates, start=1):
        mean_log = math.log(closes[-1]) + drift * index
        sigma = volatility * math.sqrt(index)
        rows.append(
            {
                "date": forecast_date,
                "p10": math.exp(mean_log + Z10 * sigma),
                "p50": math.exp(mean_log),
                "p90": math.exp(mean_log + Z90 * sigma),
            }
        )
    p50_stats = series_anomalies([row["p50"] for row in rows])
    p10_stats = series_anomalies([row["p10"] for row in rows])
    unusual = p50_stats["unusual"]
    above = sum(1 for index in unusual if rows[index]["p50"] > p50_stats["average"])
    below = sum(1 for index in unusual if rows[index]["p50"] < p50_stats["average"])
    bias = "Neutral / Mixed"
    if unusual and above / len(unusual) > 0.5:
        bias = "Selling Bias"
    elif unusual and below / len(unusual) > 0.5:
        bias = "Buying Bias"
    p10_unusual_low = {index for index in p10_stats["unusual"] if rows[index]["p10"] < p10_stats["average"]}
    p10_signal = len(rows) >= 2 and {len(rows) - 2, len(rows) - 1}.issubset(p10_unusual_low)
    return {
        "rows": rows,
        "p10": rows[-1]["p10"],
        "p50": rows[-1]["p50"],
        "p90": rows[-1]["p90"],
        "forecast_date": rows[-1]["date"],
        "last_forecast_update": iso_now(),
        "general_bias": bias,
        "unusual_p50_count": len(unusual),
        "p10_signal_active": p10_signal,
        "forecast_engine": "quantura_quantile_drift_v1",
        "forecast_history_points": len(closes),
    }


def normalize_bar(timestamp: Any, close: Any) -> dict[str, Any] | None:
    parsed = safe_float(close)
    stamp = str(timestamp or "").strip()
    if parsed is None or parsed <= 0 or not stamp:
        return None
    return {"timestamp": stamp, "close": parsed}


def fetch_alpaca_histories(symbols: Sequence[str], start: str, end: str) -> dict[str, list[dict[str, Any]]]:
    key = str(os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID") or "").strip()
    secret = str(os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY") or "").strip()
    if not key or not secret:
        raise RuntimeError("alpaca_credentials_unavailable")
    base_url = str(os.getenv("ALPACA_DATA_URL") or os.getenv("ALPACA_DATA_BASE") or ALPACA_DATA_URL).strip().rstrip("/")
    output: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in symbols}
    headers = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret, "User-Agent": USER_AGENT}
    for batch_start in range(0, len(symbols), 100):
        batch = list(symbols[batch_start : batch_start + 100])
        page_token = ""
        while True:
            params: dict[str, Any] = {
                "symbols": ",".join(batch),
                "timeframe": "1Day",
                "start": f"{start}T00:00:00Z",
                "end": f"{end}T23:59:59Z",
                "adjustment": "all",
                "feed": str(os.getenv("ALPACA_DATA_FEED") or "iex").strip(),
                "limit": 10000,
                "sort": "asc",
            }
            if page_token:
                params["page_token"] = page_token
            response = request_with_retry("GET", f"{base_url}/v2/stocks/bars", params=params, headers=headers)
            payload = response.json()
            bars_by_symbol = payload.get("bars") or {}
            for symbol, bars in bars_by_symbol.items():
                canonical = normalize_symbol(symbol)
                for bar in bars or []:
                    normalized = normalize_bar(bar.get("t"), bar.get("c"))
                    if normalized:
                        output.setdefault(canonical, []).append(normalized)
            page_token = str(payload.get("next_page_token") or "")
            if not page_token:
                break
    return output


def fetch_yfinance_histories(symbols: Sequence[str]) -> dict[str, list[dict[str, Any]]]:
    import pandas as pd
    import yfinance as yf

    output: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in symbols}
    provider_map = {yahoo_symbol(symbol): symbol for symbol in symbols}
    provider_symbols = list(provider_map)
    for batch_start in range(0, len(provider_symbols), 80):
        batch = provider_symbols[batch_start : batch_start + 80]
        frame = yf.download(
            batch,
            period="2y",
            interval="1d",
            group_by="ticker",
            threads=True,
            progress=False,
            auto_adjust=True,
            timeout=30,
        )
        if frame is None or frame.empty:
            continue
        for provider_symbol in batch:
            canonical = provider_map[provider_symbol]
            try:
                if isinstance(frame.columns, pd.MultiIndex):
                    symbol_frame = frame[provider_symbol]
                else:
                    symbol_frame = frame
                close_column = "Close" if "Close" in symbol_frame.columns else "Adj Close"
                closes = symbol_frame[close_column].dropna()
                for timestamp, value in closes.items():
                    normalized = normalize_bar(timestamp.isoformat(), value)
                    if normalized:
                        output[canonical].append(normalized)
            except (KeyError, TypeError, AttributeError):
                continue
    return output


def cap_bucket(market_cap: float | None, is_etf: bool) -> str | None:
    if is_etf or market_cap is None:
        return None
    if market_cap >= 200_000_000_000:
        return "mega"
    if market_cap >= 10_000_000_000:
        return "large"
    if market_cap >= 2_000_000_000:
        return "mid"
    if market_cap >= 300_000_000:
        return "small"
    return "micro"


def quantile_position(price: float, p10: float, p50: float, p90: float) -> str:
    if price < p10:
        return "below_p10"
    if price < p50:
        return "between_p10_p50"
    if price <= p90:
        return "between_p50_p90"
    return "above_p90"


def distance(price: float, boundary: float) -> tuple[float, float]:
    absolute = price - boundary
    percentage = (price / boundary - 1.0) * 100 if boundary else 0.0
    return round(absolute, 6), round(percentage, 6)


def completed_row(item: Mapping[str, Any], history: Sequence[Mapping[str, Any]], market_cap: float | None, price_source: str) -> dict[str, Any]:
    row = dict(item)
    row.update(
        {
            "market_cap": round_optional(market_cap, 2),
            "market_cap_bucket": cap_bucket(market_cap, bool(item.get("is_etf"))),
            "actual_price": None,
            "actual_price_timestamp": None,
            "price_source": price_source,
            "forecast_available": False,
            "status": "missing_market_data",
            "error_code": None,
        }
    )
    if not history:
        return row
    latest = history[-1]
    price = safe_float(latest.get("close"))
    if price is None:
        return row
    row["actual_price"] = round(price, 6)
    row["actual_price_timestamp"] = str(latest.get("timestamp") or "")
    forecast = build_forecast(history)
    if not forecast:
        row["status"] = "missing_predictions"
        return row
    p10 = float(forecast["p10"])
    p50 = float(forecast["p50"])
    p90 = float(forecast["p90"])
    diff10, pct10 = distance(price, p10)
    diff50, pct50 = distance(price, p50)
    diff90, pct90 = distance(price, p90)
    row.update(
        {
            "forecast_available": True,
            "status": "success",
            "p10": round(p10, 6),
            "p50": round(p50, 6),
            "p90": round(p90, 6),
            "forecast_date": forecast["forecast_date"],
            "last_forecast_update": forecast["last_forecast_update"],
            "forecast_engine": forecast["forecast_engine"],
            "forecast_history_points": forecast["forecast_history_points"],
            "quantile_position": quantile_position(price, p10, p50, p90),
            "actual_minus_p10": diff10,
            "actual_minus_p50": diff50,
            "actual_minus_p90": diff90,
            "distance_p10_pct": pct10,
            "distance_p50_pct": pct50,
            "distance_p90_pct": pct90,
            "general_bias": forecast["general_bias"],
            "unusual_p50_count": forecast["unusual_p50_count"],
            "p10_signal_active": forecast["p10_signal_active"],
            "analysis_url": f"/forecasting?ticker={row['ticker']}",
        }
    )
    return row


def command_chunk(args: argparse.Namespace) -> int:
    started = time.monotonic()
    universe = read_json(Path(args.universe))
    items = list(universe.get("items") or [])
    expected_hash = str(universe.get("universe_hash") or "")
    chunk_items = [item for item in items if item_chunk(item["ticker"], args.chunk_count) == args.chunk]
    output_path = Path(args.output)
    if args.resume and output_path.exists():
        existing = read_json(output_path)
        if (
            existing.get("schema_version") == SCHEMA_VERSION
            and existing.get("universe_hash") == expected_hash
            and int(existing.get("chunk", -1)) == args.chunk
            and int(existing.get("chunk_count") or 0) == args.chunk_count
        ):
            print(json.dumps({"event": "chunk_reused", "chunk": args.chunk, "rows": len(existing.get("items") or [])}))
            return 0

    symbols = [normalize_symbol(item["ticker"]) for item in chunk_items]
    end = dt.date.today()
    start = end - dt.timedelta(days=760)
    price_source = "alpaca_daily_bar_close"
    provider_error = ""
    try:
        histories = fetch_alpaca_histories(symbols, start.isoformat(), end.isoformat())
    except Exception as error:
        provider_error = type(error).__name__
        histories = fetch_yfinance_histories(symbols)
        price_source = "yahoo_daily_bar_close_fallback"
    market_caps = {item["ticker"]: safe_float(item.get("market_cap")) for item in chunk_items}

    rows: list[dict[str, Any]] = []
    for item in chunk_items:
        symbol = item["ticker"]
        try:
            rows.append(completed_row(item, histories.get(symbol) or [], market_caps.get(symbol), price_source))
        except Exception as error:
            failed = dict(item)
            failed.update(
                {
                    "status": "failed",
                    "forecast_available": False,
                    "error_code": type(error).__name__,
                    "actual_price": None,
                    "market_cap": round_optional(market_caps.get(symbol), 2),
                    "market_cap_bucket": cap_bucket(market_caps.get(symbol), bool(item.get("is_etf"))),
                }
            )
            rows.append(failed)

    status_counts: dict[str, int] = {}
    for row in rows:
        status_counts[row["status"]] = status_counts.get(row["status"], 0) + 1
    payload = {
        "schema_version": SCHEMA_VERSION,
        "scan_date": universe.get("scan_date"),
        "generated_at": iso_now(),
        "universe_hash": expected_hash,
        "chunk": args.chunk,
        "chunk_count": args.chunk_count,
        "price_source": price_source,
        "provider_fallback_reason": provider_error,
        "runtime_seconds": round(time.monotonic() - started, 2),
        "status_counts": status_counts,
        "items": rows,
    }
    write_json(output_path, payload)
    print(json.dumps({"event": "chunk_complete", "chunk": args.chunk, "expected": len(chunk_items), **status_counts}, sort_keys=True))
    return 0


CSV_FIELDS = [
    "ticker",
    "company_name",
    "exchange",
    "universe_memberships",
    "is_sp500",
    "is_nasdaq",
    "is_etf",
    "sector",
    "industry",
    "market_cap",
    "market_cap_bucket",
    "actual_price",
    "actual_price_timestamp",
    "p10",
    "p50",
    "p90",
    "quantile_position",
    "distance_p10_pct",
    "distance_p50_pct",
    "distance_p90_pct",
    "next_earnings_date",
    "general_bias",
    "unusual_p50_count",
    "p10_signal_active",
    "forecast_date",
    "last_forecast_update",
    "status",
]


def write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for raw in rows:
            row = dict(raw)
            row["universe_memberships"] = ";".join(row.get("universe_memberships") or [])
            writer.writerow(row)


def coverage_manifest(universe: Mapping[str, Any], rows: Sequence[Mapping[str, Any]], threshold: float, started: float) -> dict[str, Any]:
    counts = {"success": 0, "failed": 0, "missing_predictions": 0, "missing_market_data": 0, "unsupported": 0}
    for row in rows:
        status = str(row.get("status") or "failed")
        counts[status if status in counts else "failed"] += 1
    eligible = max(0, len(rows) - counts["unsupported"])
    coverage = counts["success"] / eligible if eligible else 0.0
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "complete" if coverage >= threshold else "degraded",
        "scan_date": universe.get("scan_date"),
        "generated_at": iso_now(),
        "last_successful_complete_scan": iso_now() if coverage >= threshold else None,
        "universe_hash": universe.get("universe_hash"),
        "universe": universe.get("counts") or {},
        "expected": len(rows),
        "eligible": eligible,
        "successfully_processed": counts["success"],
        "failed": counts["failed"],
        "missing_predictions": counts["missing_predictions"],
        "missing_market_data": counts["missing_market_data"],
        "unsupported": counts["unsupported"],
        "coverage": round(coverage, 8),
        "coverage_percentage": round(coverage * 100, 4),
        "coverage_threshold": threshold,
        "coverage_ok": coverage >= threshold,
        "runtime_seconds": round(time.monotonic() - started, 2),
        "actual_price_definition": "Most recent adjusted daily bar close. Alpaca IEX is preferred; Yahoo is the credential-free fallback.",
        "forecast_methodology": "Quantura quantile drift v1: log-return drift with volatility-scaled P10/P50/P90 bands over 10 business days.",
        "market_cap_convention": "Mega ≥ $200B; Large $10B–$200B; Mid $2B–$10B; Small $300M–$2B; Micro < $300M. ETFs are unclassified.",
        "earnings_source": (universe.get("earnings") or {}).get("source") or "unavailable",
    }


def command_aggregate(args: argparse.Namespace) -> int:
    started = time.monotonic()
    universe = read_json(Path(args.universe))
    expected_items = {row["ticker"]: dict(row) for row in universe.get("items") or []}
    chunks: dict[int, Mapping[str, Any]] = {}
    for path in sorted(Path(args.chunks_dir).glob("**/chunk-*.json")):
        payload = read_json(path)
        if payload.get("schema_version") != SCHEMA_VERSION or payload.get("universe_hash") != universe.get("universe_hash"):
            continue
        chunks[int(payload.get("chunk") or 0)] = payload
    rows_by_symbol: dict[str, dict[str, Any]] = {}
    for chunk in chunks.values():
        for row in chunk.get("items") or []:
            ticker = normalize_symbol(row.get("ticker"))
            if ticker:
                rows_by_symbol[ticker] = dict(row)
    for ticker, source in expected_items.items():
        if ticker not in rows_by_symbol:
            source.update(
                {
                    "status": "failed",
                    "error_code": "missing_chunk_result",
                    "forecast_available": False,
                    "actual_price": None,
                    "market_cap": None,
                }
            )
            rows_by_symbol[ticker] = source
    rows = sorted(rows_by_symbol.values(), key=lambda row: row["ticker"])
    manifest = coverage_manifest(universe, rows, args.coverage_threshold, started)
    manifest["runtime_seconds"] = round(
        (safe_float(universe.get("runtime_seconds") or 0) or 0)
        + sum(safe_float(chunk.get("runtime_seconds") or 0) or 0 for chunk in chunks.values())
        + (time.monotonic() - started),
        2,
    )
    manifest["chunks_expected"] = args.chunk_count
    manifest["chunks_received"] = len(chunks)
    manifest["failed_symbols"] = [row["ticker"] for row in rows if row.get("status") == "failed"][:250]
    payload = {
        "schema_version": SCHEMA_VERSION,
        "scan_id": f"{universe.get('scan_date')}-{universe.get('universe_hash')}",
        "scan_date": universe.get("scan_date"),
        "generated_at": iso_now(),
        "manifest": manifest,
        "items": rows,
    }
    output_dir = Path(args.output_dir)
    write_json(output_dir / "quantura-screener-latest.json", payload)
    write_json(output_dir / "run-manifest.json", manifest)
    write_csv(output_dir / "quantura-screener-latest.csv", rows)
    print(json.dumps({"event": "aggregate_complete", **manifest}, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Quantura full-universe quantitative screener pipeline")
    subcommands = parser.add_subparsers(dest="command", required=True)

    universe = subcommands.add_parser("universe")
    universe.add_argument("--output", required=True)
    universe.add_argument("--matrix-output", required=True)
    universe.add_argument("--chunk-count", type=int, default=DEFAULT_CHUNK_COUNT)
    universe.add_argument("--max-tickers", type=int)
    universe.add_argument("--scan-date")
    universe.add_argument("--earnings-days", type=int, default=30)
    universe.add_argument("--skip-earnings", action="store_true")
    universe.set_defaults(handler=command_universe)

    chunk = subcommands.add_parser("chunk")
    chunk.add_argument("--universe", required=True)
    chunk.add_argument("--chunk", type=int, required=True)
    chunk.add_argument("--chunk-count", type=int, required=True)
    chunk.add_argument("--output", required=True)
    chunk.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True)
    chunk.set_defaults(handler=command_chunk)

    aggregate = subcommands.add_parser("aggregate")
    aggregate.add_argument("--universe", required=True)
    aggregate.add_argument("--chunks-dir", required=True)
    aggregate.add_argument("--chunk-count", type=int, required=True)
    aggregate.add_argument("--coverage-threshold", type=float, default=DEFAULT_COVERAGE_THRESHOLD)
    aggregate.add_argument("--output-dir", required=True)
    aggregate.set_defaults(handler=command_aggregate)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    return int(args.handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
