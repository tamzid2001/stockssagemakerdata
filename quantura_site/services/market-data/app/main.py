from __future__ import annotations

import math
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import yfinance as yf
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

SOURCE_NAME = "yfinance"


class TTLCache:
    def __init__(self, max_entries: int = 4096) -> None:
        self._max_entries = max(64, int(max_entries))
        self._lock = threading.Lock()
        self._store: dict[str, tuple[float, Any]] = {}
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Any | None:
        now = time.time()
        with self._lock:
            item = self._store.get(key)
            if not item:
                self._misses += 1
                return None
            expires_at, value = item
            if expires_at <= now:
                self._store.pop(key, None)
                self._misses += 1
                return None
            self._hits += 1
            return value

    def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        ttl = max(1, int(ttl_seconds))
        expires_at = time.time() + ttl
        with self._lock:
            self._store[key] = (expires_at, value)
            if len(self._store) > self._max_entries:
                # Drop oldest expiry first.
                for old_key, _ in sorted(self._store.items(), key=lambda item: item[1][0])[: max(1, len(self._store) // 12)]:
                    self._store.pop(old_key, None)

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "entries": len(self._store),
                "hits": self._hits,
                "misses": self._misses,
            }


def _clamp_env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.environ.get(name)
    try:
        value = int(raw) if raw is not None else int(default)
    except Exception:
        value = int(default)
    return max(minimum, min(maximum, value))


QUOTE_TTL_SECONDS = _clamp_env_int("MARKET_DATA_QUOTE_TTL_SECONDS", 90, 30, 120)
FAST_INFO_TTL_SECONDS = _clamp_env_int("MARKET_DATA_FAST_INFO_TTL_SECONDS", 300, 60, 1800)
FULL_INFO_TTL_SECONDS = _clamp_env_int("MARKET_DATA_FULL_INFO_TTL_SECONDS", 900, 300, 1800)
FX_TTL_SECONDS = _clamp_env_int("MARKET_DATA_FX_TTL_SECONDS", 90, 30, 120)
MAX_TICKERS_PER_REQUEST = _clamp_env_int("MARKET_DATA_MAX_TICKERS", 25, 1, 100)
MAX_INFO_WORKERS = _clamp_env_int("MARKET_DATA_INFO_WORKERS", 4, 1, 12)
YAHOO_CONCURRENCY = _clamp_env_int("MARKET_DATA_YAHOO_CONCURRENCY", 4, 1, 12)

CACHE = TTLCache(max_entries=_clamp_env_int("MARKET_DATA_CACHE_MAX_ENTRIES", 4096, 256, 20000))
YAHOO_SEMAPHORE = threading.BoundedSemaphore(YAHOO_CONCURRENCY)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except Exception:
        return None
    return number if math.isfinite(number) else None


def _safe_int(value: Any) -> int | None:
    num = _safe_float(value)
    if num is None:
        return None
    try:
        return int(num)
    except Exception:
        return None


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        if isinstance(value, float) and not math.isfinite(value):
            return None
        return value
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc).isoformat()
        return value.isoformat()
    if isinstance(value, pd.Timestamp):
        ts = value.to_pydatetime()
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(sub_value) for key, sub_value in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except Exception:
            return str(value)
    return str(value)


def _normalize_symbol(symbol: str) -> str:
    token = str(symbol or "").strip().upper()
    if not token:
        return ""
    return "".join(ch for ch in token if ch.isalnum() or ch in {".", "-", "=", "^"})


def _normalize_currency(code: str) -> str:
    text = str(code or "").strip().upper()
    return "".join(ch for ch in text if ch.isalpha())[:3]


def _parse_symbols(raw: str) -> list[str]:
    out: list[str] = []
    for part in str(raw or "").split(","):
        symbol = _normalize_symbol(part)
        if symbol and symbol not in out:
            out.append(symbol)
    return out[:MAX_TICKERS_PER_REQUEST]


def _extract_timestamp(value: Any) -> str:
    if value is None:
        return _utc_now_iso()
    if isinstance(value, pd.Timestamp):
        dt = value.to_pydatetime()
    elif isinstance(value, datetime):
        dt = value
    else:
        return _utc_now_iso()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def _empty_record(symbol: str) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "asOf": _utc_now_iso(),
        "source": SOURCE_NAME,
        "price": {
            "last": None,
            "prevClose": None,
            "open": None,
            "dayHigh": None,
            "dayLow": None,
            "volume": None,
            "currency": None,
        },
        "valuation": {
            "marketCap": None,
            "trailingPE": None,
            "forwardPE": None,
            "priceToBook": None,
            "enterpriseValue": None,
            "sharesOutstanding": None,
        },
        "fundamentals": {
            "revenueTTM": None,
            "grossMargins": None,
            "profitMargins": None,
            "operatingMargins": None,
            "ebitdaMargins": None,
            "returnOnAssets": None,
            "returnOnEquity": None,
        },
        "profile": {
            "longName": None,
            "sector": None,
            "industry": None,
            "country": None,
            "website": None,
            "longBusinessSummary": None,
        },
        "risk": {
            "beta": None,
            "shortRatio": None,
        },
        "dividends": {
            "dividendRate": None,
            "dividendYield": None,
            "exDividendDate": None,
            "payoutRatio": None,
            "fiveYearAvgDividendYield": None,
        },
    }


def _read_mapping_value(source: Any, *keys: str) -> Any:
    if source is None:
        return None
    for key in keys:
        if isinstance(source, dict) and key in source:
            value = source.get(key)
        else:
            value = getattr(source, key, None)
        if value not in (None, ""):
            return value
    return None


def _safe_yf_download(symbols: list[str]) -> pd.DataFrame | None:
    if not symbols:
        return None
    key = f"download:{','.join(symbols)}"
    cached = CACHE.get(key)
    if isinstance(cached, pd.DataFrame):
        return cached
    with YAHOO_SEMAPHORE:
        frame = yf.download(
            " ".join(symbols),
            period="5d",
            interval="1d",
            progress=False,
            auto_adjust=False,
            threads=False,
            group_by="ticker",
        )
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return None
    CACHE.set(key, frame, QUOTE_TTL_SECONDS)
    return frame


def _extract_price_rows(frame: pd.DataFrame | None, symbols: list[str]) -> dict[str, dict[str, Any]]:
    if frame is None or frame.empty:
        return {}

    def _from_series(series: pd.Series) -> dict[str, Any]:
        cleaned = series.dropna()
        if cleaned.empty:
            return {}
        last_row = cleaned.iloc[-1]
        prev_close = cleaned.iloc[-2].get("Close") if len(cleaned) > 1 else None
        return {
            "asOf": _extract_timestamp(cleaned.index[-1]),
            "last": _safe_float(last_row.get("Close")),
            "prevClose": _safe_float(prev_close if prev_close is not None else last_row.get("Close")),
            "open": _safe_float(last_row.get("Open")),
            "dayHigh": _safe_float(last_row.get("High")),
            "dayLow": _safe_float(last_row.get("Low")),
            "volume": _safe_int(last_row.get("Volume")),
        }

    output: dict[str, dict[str, Any]] = {}
    if isinstance(frame.columns, pd.MultiIndex):
        level0 = set(frame.columns.get_level_values(0))
        level1 = set(frame.columns.get_level_values(1))
        for symbol in symbols:
            sub = None
            try:
                if symbol in level0:
                    sub = frame[symbol]
                elif symbol in level1:
                    sub = frame.xs(symbol, axis=1, level=1)
            except Exception:
                sub = None
            if sub is None or not isinstance(sub, pd.DataFrame) or sub.empty:
                continue
            output[symbol] = _from_series(sub[[col for col in ["Open", "High", "Low", "Close", "Volume"] if col in sub.columns]].copy())
        return output

    # Single-symbol download shape.
    if len(symbols) == 1:
        single = frame[[col for col in ["Open", "High", "Low", "Close", "Volume"] if col in frame.columns]].copy()
        output[symbols[0]] = _from_series(single)
    return output


def _fetch_fast_info(symbol: str) -> dict[str, Any]:
    key = f"fast_info:{symbol}"
    cached = CACHE.get(key)
    if isinstance(cached, dict):
        return cached

    with YAHOO_SEMAPHORE:
        ticker_obj = yf.Ticker(symbol)
        raw_fast_info = getattr(ticker_obj, "fast_info", None)

    fast_payload: dict[str, Any]
    if isinstance(raw_fast_info, dict):
        fast_payload = dict(raw_fast_info)
    else:
        attrs = [
            "last_price",
            "previous_close",
            "open",
            "day_high",
            "day_low",
            "volume",
            "currency",
            "market_cap",
            "shares",
            "exchange",
        ]
        fast_payload = {name: getattr(raw_fast_info, name, None) for name in attrs}

    fast_payload = _json_safe(fast_payload) if isinstance(fast_payload, dict) else {}
    CACHE.set(key, fast_payload, FAST_INFO_TTL_SECONDS)
    return fast_payload


def _fetch_full_info(symbol: str) -> dict[str, Any]:
    key = f"full_info:{symbol}"
    cached = CACHE.get(key)
    if isinstance(cached, dict):
        return cached

    with YAHOO_SEMAPHORE:
        ticker_obj = yf.Ticker(symbol)
        raw_info = ticker_obj.info

    info_payload = _json_safe(raw_info) if isinstance(raw_info, dict) else {}
    CACHE.set(key, info_payload, FULL_INFO_TTL_SECONDS)
    return info_payload


def _apply_price_payload(record: dict[str, Any], payload: dict[str, Any]) -> None:
    if not payload:
        return
    price = record["price"]
    for key in ["last", "prevClose", "open", "dayHigh", "dayLow", "volume"]:
        if payload.get(key) is not None:
            price[key] = payload.get(key)
    as_of = payload.get("asOf")
    if as_of:
        record["asOf"] = as_of


def _apply_fast_info_payload(record: dict[str, Any], fast_info: dict[str, Any]) -> None:
    if not fast_info:
        return
    price = record["price"]
    valuation = record["valuation"]

    if price.get("last") is None:
        price["last"] = _safe_float(_read_mapping_value(fast_info, "last_price", "lastPrice"))
    if price.get("prevClose") is None:
        price["prevClose"] = _safe_float(_read_mapping_value(fast_info, "previous_close", "previousClose"))
    if price.get("open") is None:
        price["open"] = _safe_float(_read_mapping_value(fast_info, "open"))
    if price.get("dayHigh") is None:
        price["dayHigh"] = _safe_float(_read_mapping_value(fast_info, "day_high", "dayHigh"))
    if price.get("dayLow") is None:
        price["dayLow"] = _safe_float(_read_mapping_value(fast_info, "day_low", "dayLow"))
    if price.get("volume") is None:
        price["volume"] = _safe_int(_read_mapping_value(fast_info, "volume"))

    currency = _normalize_currency(str(_read_mapping_value(fast_info, "currency") or ""))
    if currency:
        price["currency"] = currency

    if valuation.get("marketCap") is None:
        valuation["marketCap"] = _safe_int(_read_mapping_value(fast_info, "market_cap", "marketCap"))
    if valuation.get("sharesOutstanding") is None:
        valuation["sharesOutstanding"] = _safe_int(_read_mapping_value(fast_info, "shares"))


def _apply_full_info_payload(record: dict[str, Any], info: dict[str, Any], include_raw: bool) -> None:
    if not info:
        if include_raw:
            record["raw"] = {}
        return

    price = record["price"]
    valuation = record["valuation"]
    fundamentals = record["fundamentals"]
    profile = record["profile"]
    risk = record["risk"]
    dividends = record["dividends"]

    if price.get("currency") is None:
        currency = _normalize_currency(str(info.get("currency") or ""))
        price["currency"] = currency or None

    valuation["marketCap"] = _safe_int(info.get("marketCap") if info.get("marketCap") is not None else valuation.get("marketCap"))
    valuation["trailingPE"] = _safe_float(info.get("trailingPE"))
    valuation["forwardPE"] = _safe_float(info.get("forwardPE"))
    valuation["priceToBook"] = _safe_float(info.get("priceToBook"))
    valuation["enterpriseValue"] = _safe_int(info.get("enterpriseValue"))
    valuation["sharesOutstanding"] = _safe_int(info.get("sharesOutstanding") if info.get("sharesOutstanding") is not None else valuation.get("sharesOutstanding"))

    fundamentals["revenueTTM"] = _safe_int(info.get("totalRevenue"))
    fundamentals["grossMargins"] = _safe_float(info.get("grossMargins"))
    fundamentals["profitMargins"] = _safe_float(info.get("profitMargins"))
    fundamentals["operatingMargins"] = _safe_float(info.get("operatingMargins"))
    fundamentals["ebitdaMargins"] = _safe_float(info.get("ebitdaMargins"))
    fundamentals["returnOnAssets"] = _safe_float(info.get("returnOnAssets"))
    fundamentals["returnOnEquity"] = _safe_float(info.get("returnOnEquity"))

    profile["longName"] = str(info.get("longName") or info.get("shortName") or "").strip() or None
    profile["sector"] = str(info.get("sector") or "").strip() or None
    profile["industry"] = str(info.get("industry") or "").strip() or None
    profile["country"] = str(info.get("country") or "").strip() or None
    profile["website"] = str(info.get("website") or "").strip() or None
    summary = str(info.get("longBusinessSummary") or "").strip()
    profile["longBusinessSummary"] = summary if summary else None

    risk["beta"] = _safe_float(info.get("beta"))
    risk["shortRatio"] = _safe_float(info.get("shortRatio"))

    dividends["dividendRate"] = _safe_float(info.get("dividendRate"))
    dividends["dividendYield"] = _safe_float(info.get("dividendYield"))
    dividends["exDividendDate"] = _json_safe(info.get("exDividendDate"))
    dividends["payoutRatio"] = _safe_float(info.get("payoutRatio"))
    dividends["fiveYearAvgDividendYield"] = _safe_float(info.get("fiveYearAvgDividendYield"))

    if include_raw:
        record["raw"] = _json_safe(info)


def _run_parallel(symbols: list[str], fetcher: Any) -> dict[str, Any]:
    if not symbols:
        return {}

    workers = min(MAX_INFO_WORKERS, len(symbols))
    output: dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {pool.submit(fetcher, symbol): symbol for symbol in symbols}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                output[symbol] = future.result()
            except Exception:
                output[symbol] = {}
    return output


def resolve_stock_quotes(symbols: list[str], mode: str) -> dict[str, Any]:
    if not symbols:
        raise HTTPException(status_code=400, detail="At least one ticker symbol is required.")

    if len(symbols) > MAX_TICKERS_PER_REQUEST:
        raise HTTPException(
            status_code=400,
            detail=f"Maximum {MAX_TICKERS_PER_REQUEST} tickers allowed per request.",
        )

    mode_value = "full" if str(mode).strip().lower() == "full" else "fast"

    download_frame = _safe_yf_download(symbols)
    batch_prices = _extract_price_rows(download_frame, symbols)
    fast_map = _run_parallel(symbols, _fetch_fast_info)
    info_map = _run_parallel(symbols, _fetch_full_info) if mode_value == "full" else {}

    records: list[dict[str, Any]] = []
    for symbol in symbols:
        record = _empty_record(symbol)
        _apply_price_payload(record, batch_prices.get(symbol) or {})
        _apply_fast_info_payload(record, fast_map.get(symbol) or {})
        if mode_value == "full":
            _apply_full_info_payload(record, info_map.get(symbol) or {}, include_raw=True)
        records.append(record)

    return {
        "source": SOURCE_NAME,
        "mode": mode_value,
        "count": len(records),
        "asOf": _utc_now_iso(),
        "items": records,
    }


def _fetch_fx_symbol(symbol: str) -> tuple[float | None, str]:
    cache_key = f"fx:{symbol}"
    cached = CACHE.get(cache_key)
    if isinstance(cached, tuple) and len(cached) == 2:
        return cached[0], str(cached[1])

    with YAHOO_SEMAPHORE:
        frame = yf.Ticker(symbol).history(period="5d", interval="1d", auto_adjust=False)

    if frame is None or frame.empty or "Close" not in frame.columns:
        CACHE.set(cache_key, (None, _utc_now_iso()), FX_TTL_SECONDS)
        return None, _utc_now_iso()

    close = frame["Close"].dropna()
    if close.empty:
        CACHE.set(cache_key, (None, _utc_now_iso()), FX_TTL_SECONDS)
        return None, _utc_now_iso()

    rate = _safe_float(close.iloc[-1])
    as_of = _extract_timestamp(close.index[-1])
    CACHE.set(cache_key, (rate, as_of), FX_TTL_SECONDS)
    return rate, as_of


def resolve_fx_conversion(amount: float, from_currency: str, to_currency: str) -> dict[str, Any]:
    from_code = _normalize_currency(from_currency)
    to_code = _normalize_currency(to_currency)
    if not from_code or not to_code:
        raise HTTPException(status_code=400, detail="Both from and to currencies must be 3-letter currency codes.")

    amount_in = float(amount)
    if amount_in < 0:
        raise HTTPException(status_code=400, detail="Amount must be non-negative.")

    if from_code == to_code:
        return {
            "amountIn": amount_in,
            "from": from_code,
            "to": to_code,
            "rate": 1.0,
            "amountOut": amount_in,
            "symbolUsed": f"{from_code}{to_code}=X",
            "asOf": _utc_now_iso(),
            "source": SOURCE_NAME,
        }

    direct_symbol = f"{from_code}{to_code}=X"
    direct_rate, direct_as_of = _fetch_fx_symbol(direct_symbol)
    if direct_rate is not None and direct_rate > 0:
        return {
            "amountIn": amount_in,
            "from": from_code,
            "to": to_code,
            "rate": direct_rate,
            "amountOut": amount_in * direct_rate,
            "symbolUsed": direct_symbol,
            "asOf": direct_as_of,
            "source": SOURCE_NAME,
        }

    inverse_symbol = f"{to_code}{from_code}=X"
    inverse_rate, inverse_as_of = _fetch_fx_symbol(inverse_symbol)
    if inverse_rate is not None and inverse_rate > 0:
        converted_rate = 1.0 / inverse_rate
        return {
            "amountIn": amount_in,
            "from": from_code,
            "to": to_code,
            "rate": converted_rate,
            "amountOut": amount_in * converted_rate,
            "symbolUsed": inverse_symbol,
            "asOf": inverse_as_of,
            "source": SOURCE_NAME,
        }

    raise HTTPException(status_code=404, detail=f"No FX quote found for {from_code}/{to_code}.")


def _build_equity_query(node: Any) -> Any:
    if not isinstance(node, dict):
        raise ValueError("Custom screener query must be an object with operator and operands.")

    operator = str(node.get("operator") or "").strip()
    operands = node.get("operands")
    if not operator or not isinstance(operands, list):
        raise ValueError("Query object requires 'operator' and list 'operands'.")

    parsed_operands: list[Any] = []
    for operand in operands:
        if isinstance(operand, dict) and "operator" in operand:
            parsed_operands.append(_build_equity_query(operand))
        else:
            parsed_operands.append(operand)

    return yf.EquityQuery(operator, parsed_operands)


def run_screener_query(payload: "ScreenerRequest") -> dict[str, Any]:
    size = max(1, min(int(payload.size or 50), 250))
    offset = max(0, int(payload.offset or 0))

    try:
        if payload.query:
            query_obj = _build_equity_query(payload.query)
            with YAHOO_SEMAPHORE:
                response = yf.screen(
                    query_obj,
                    size=size,
                    offset=offset,
                    sortField=payload.sortField or None,
                    sortAsc=payload.sortAsc,
                )
            screener_mode = "custom"
            preset = None
        else:
            preset_value = str(payload.preset or "most_actives").strip()
            with YAHOO_SEMAPHORE:
                response = yf.screen(preset_value, size=size, offset=offset)
            screener_mode = "preset"
            preset = preset_value
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Screener query failed: {exc}") from exc

    quotes = response.get("quotes") if isinstance(response, dict) else []
    rows: list[dict[str, Any]] = []
    for row in quotes if isinstance(quotes, list) else []:
        if not isinstance(row, dict):
            continue
        symbol = _normalize_symbol(str(row.get("symbol") or ""))
        if not symbol:
            continue
        rows.append(
            {
                "symbol": symbol,
                "shortName": str(row.get("shortName") or "").strip() or None,
                "longName": str(row.get("longName") or row.get("shortName") or "").strip() or None,
                "exchange": str(row.get("exchange") or "").strip() or None,
                "sector": str(row.get("sector") or "").strip() or None,
                "industry": str(row.get("industry") or "").strip() or None,
                "marketCap": _safe_int(row.get("marketCap") or row.get("intradaymarketcap")),
                "price": _safe_float(row.get("regularMarketPrice") or row.get("price") or row.get("intradayprice")),
                "changePercent": _safe_float(row.get("regularMarketChangePercent") or row.get("percentchange")),
                "volume": _safe_int(row.get("regularMarketVolume") or row.get("dayvolume") or row.get("volume")),
            }
        )

    return {
        "source": SOURCE_NAME,
        "mode": screener_mode,
        "preset": preset,
        "offset": _safe_int(response.get("offset") if isinstance(response, dict) else offset) or offset,
        "size": _safe_int(response.get("count") if isinstance(response, dict) else size) or size,
        "total": _safe_int(response.get("total") if isinstance(response, dict) else len(rows)) or len(rows),
        "count": len(rows),
        "items": rows,
    }


class ScreenerRequest(BaseModel):
    preset: str | None = Field(default="most_actives")
    query: dict[str, Any] | None = None
    size: int = Field(default=50, ge=1, le=250)
    offset: int = Field(default=0, ge=0)
    sortField: str | None = None
    sortAsc: bool | None = None


app = FastAPI(
    title="Quantura Market Data Service",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

cors_origins_raw = str(os.environ.get("MARKET_DATA_CORS_ORIGINS") or "*").strip()
if cors_origins_raw == "*":
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )
else:
    origins = [origin.strip() for origin in cors_origins_raw.split(",") if origin.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "ok": True,
        "service": "market-data",
        "source": SOURCE_NAME,
        "timestamp": _utc_now_iso(),
        "limits": {
            "maxTickersPerRequest": MAX_TICKERS_PER_REQUEST,
            "maxInfoWorkers": MAX_INFO_WORKERS,
            "yahooConcurrency": YAHOO_CONCURRENCY,
        },
        "cache": {
            **CACHE.stats(),
            "quoteTtlSeconds": QUOTE_TTL_SECONDS,
            "fastInfoTtlSeconds": FAST_INFO_TTL_SECONDS,
            "fullInfoTtlSeconds": FULL_INFO_TTL_SECONDS,
            "fxTtlSeconds": FX_TTL_SECONDS,
        },
    }


@app.get("/fx/convert")
def fx_convert(
    amount: float = Query(..., description="Amount in source currency."),
    from_currency: str = Query(..., alias="from", min_length=3, max_length=3),
    to_currency: str = Query(..., alias="to", min_length=3, max_length=3),
) -> dict[str, Any]:
    return resolve_fx_conversion(amount=amount, from_currency=from_currency, to_currency=to_currency)


@app.get("/stocks/quote")
def stocks_quote(
    tickers: str = Query(..., description="Comma-separated ticker symbols, e.g. AAPL,MSFT"),
    mode: str = Query("fast", pattern="^(fast|full)$"),
) -> dict[str, Any]:
    symbols = _parse_symbols(tickers)
    if not symbols:
        raise HTTPException(status_code=400, detail="No valid ticker symbols were provided.")
    return resolve_stock_quotes(symbols=symbols, mode=mode)


@app.post("/stocks/screener")
def stocks_screener(payload: ScreenerRequest) -> dict[str, Any]:
    if payload.query is None and not str(payload.preset or "").strip():
        raise HTTPException(status_code=400, detail="Provide preset or query.")
    return run_screener_query(payload)
