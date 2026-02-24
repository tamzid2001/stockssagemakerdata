from __future__ import annotations

import hashlib
import json
import threading
import time
from collections import OrderedDict, defaultdict
from datetime import datetime, timezone
from typing import Any

import requests

GAMMA_API_BASE = "https://gamma-api.polymarket.com"
CLOB_API_BASE = "https://clob.polymarket.com"
MAX_TOKEN_IDS_PER_REQUEST = 500
DEFAULT_TIMEOUT_SECONDS = 12


class TtlLruCache:
    """Simple in-memory TTL + LRU cache for per-instance request collapse."""

    def __init__(self, *, max_entries: int = 256) -> None:
        self.max_entries = max_entries
        self._lock = threading.Lock()
        self._items: OrderedDict[str, tuple[float, Any]] = OrderedDict()

    def get(self, key: str) -> Any | None:
        now = time.time()
        with self._lock:
            record = self._items.get(key)
            if not record:
                return None
            expires_at, value = record
            if expires_at <= now:
                self._items.pop(key, None)
                return None
            self._items.move_to_end(key)
            return value

    def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        expires_at = time.time() + max(1, int(ttl_seconds))
        with self._lock:
            self._items[key] = (expires_at, value)
            self._items.move_to_end(key)
            while len(self._items) > self.max_entries:
                self._items.popitem(last=False)


class InMemoryRateLimiter:
    """Best-effort per-instance limiter by identifier (IP)."""

    def __init__(self, *, max_keys: int = 4000) -> None:
        self.max_keys = max_keys
        self._lock = threading.Lock()
        self._events: dict[str, list[float]] = {}

    def check(self, *, key: str, limit: int, window_seconds: int) -> tuple[bool, int]:
        if limit <= 0:
            return True, 0
        now = time.time()
        window = max(1, int(window_seconds))
        with self._lock:
            events = [ts for ts in self._events.get(key, []) if now - ts < window]
            if len(events) >= limit:
                retry_after = int(max(1.0, window - (now - events[0])))
                self._events[key] = events
                return False, retry_after
            events.append(now)
            self._events[key] = events

            # Trim old keys opportunistically.
            if len(self._events) > self.max_keys:
                stale_cutoff = now - (window * 2)
                for item_key in list(self._events.keys())[: self.max_keys // 4]:
                    if not self._events[item_key] or self._events[item_key][-1] < stale_cutoff:
                        self._events.pop(item_key, None)
            return True, 0


_search_cache = TtlLruCache(max_entries=128)
_orderbook_cache = TtlLruCache(max_entries=256)
_history_cache = TtlLruCache(max_entries=512)
_rate_limiter = InMemoryRateLimiter(max_keys=5000)


def _request_headers() -> dict[str, str]:
    return {
        "Accept": "application/json",
        "User-Agent": "QuanturaPolymarketService/1.0",
    }


def _stable_hash(parts: list[str]) -> str:
    joined = "|".join(parts)
    return hashlib.sha1(joined.encode("utf-8")).hexdigest()  # nosec B324: cache key hash only.


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        num = float(value)
        return num if num == num and num not in {float("inf"), float("-inf")} else None
    text = str(value).strip()
    if not text:
        return None
    try:
        num = float(text)
    except Exception:
        return None
    return num if num == num and num not in {float("inf"), float("-inf")} else None


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(int(value))
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "on"}


def _parse_json_array(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass
        # Fall back to comma-splitting when upstream sends non-JSON array text.
        if "," in text:
            return [piece.strip() for piece in text.split(",") if piece.strip()]
    return []


def _normalize_outcomes(raw_outcomes: Any) -> list[str]:
    values = _parse_json_array(raw_outcomes)
    out: list[str] = []
    for value in values:
        clean = str(value).strip()
        if clean:
            out.append(clean)
    return out


def _normalize_outcome_prices(raw_prices: Any) -> list[float]:
    values = _parse_json_array(raw_prices)
    out: list[float] = []
    for value in values:
        parsed = _to_float(value)
        if parsed is None:
            continue
        out.append(max(0.0, min(1.0, parsed)))
    return out


def _normalize_token_ids(raw_token_ids: Any) -> list[str]:
    values = _parse_json_array(raw_token_ids)
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        token_id = str(value).strip()
        if not token_id or token_id in seen:
            continue
        seen.add(token_id)
        out.append(token_id)
    return out


def _event_url_from_slug(slug: str) -> str:
    clean = str(slug or "").strip().strip("/")
    if not clean:
        return ""
    return f"https://polymarket.com/event/{clean}"


def _market_url(raw_market: dict[str, Any], event_slug: str) -> str:
    direct = str(raw_market.get("url") or "").strip()
    if direct:
        return direct
    market_slug = str(raw_market.get("slug") or "").strip()
    if market_slug:
        return _event_url_from_slug(market_slug)
    if event_slug:
        return _event_url_from_slug(event_slug)
    return ""


def _normalize_market(raw_market: dict[str, Any], *, ticker: str, event: dict[str, Any]) -> dict[str, Any] | None:
    market_id = str(
        raw_market.get("id")
        or raw_market.get("marketId")
        or raw_market.get("market_id")
        or ""
    ).strip()
    if not market_id:
        return None

    event_id = str(event.get("id") or raw_market.get("eventId") or "").strip()
    event_slug = str(event.get("slug") or raw_market.get("eventSlug") or "").strip()
    event_title = str(event.get("title") or raw_market.get("eventTitle") or "").strip()

    normalized = {
        "marketId": market_id,
        "question": str(raw_market.get("question") or raw_market.get("title") or "").strip(),
        "conditionId": str(raw_market.get("conditionId") or raw_market.get("condition_id") or "").strip(),
        "outcomes": _normalize_outcomes(raw_market.get("outcomes")),
        "outcomePrices": _normalize_outcome_prices(raw_market.get("outcomePrices")),
        "volume": _to_float(raw_market.get("volume")) or 0.0,
        "volume24hr": _to_float(raw_market.get("volume24hr") or raw_market.get("volume24h")) or 0.0,
        "endDate": str(raw_market.get("endDate") or raw_market.get("end_date") or "").strip(),
        "enableOrderBook": _to_bool(raw_market.get("enableOrderBook")),
        "clobTokenIds": _normalize_token_ids(raw_market.get("clobTokenIds")),
        "slug": str(raw_market.get("slug") or "").strip(),
        "eventId": event_id,
        "eventSlug": event_slug,
        "eventTitle": event_title,
        "eventUrl": _event_url_from_slug(event_slug),
        "marketUrl": _market_url(raw_market, event_slug),
        "ticker": ticker,
        "tags": _normalize_outcomes(raw_market.get("tags")),
    }
    return normalized


def _as_event_record(raw_event: dict[str, Any], *, ticker: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    event_id = str(raw_event.get("id") or "").strip()
    event_title = str(raw_event.get("title") or "").strip()
    event_slug = str(raw_event.get("slug") or "").strip()
    event_url = _event_url_from_slug(event_slug)

    normalized_markets: list[dict[str, Any]] = []
    for raw_market in raw_event.get("markets") or []:
        if not isinstance(raw_market, dict):
            continue
        normalized = _normalize_market(raw_market, ticker=ticker, event=raw_event)
        if normalized:
            normalized_markets.append(normalized)

    event_record = {
        "eventId": event_id,
        "title": event_title,
        "slug": event_slug,
        "eventUrl": event_url,
        "ticker": ticker,
        "markets": normalized_markets,
    }
    return event_record, normalized_markets


def search_markets_for_ticker(ticker: str) -> dict[str, Any]:
    clean_ticker = str(ticker or "").strip().upper()
    if not clean_ticker:
        return {"ticker": "", "events": [], "markets": [], "source": GAMMA_API_BASE}

    cache_key = f"search:{clean_ticker}"
    cached = _search_cache.get(cache_key)
    if cached is not None:
        return cached

    response = requests.get(
        f"{GAMMA_API_BASE}/public-search",
        params={"q": clean_ticker, "limit_per_type": 20},
        headers=_request_headers(),
        timeout=DEFAULT_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json() if response.content else {}

    raw_events = payload.get("events") if isinstance(payload, dict) else []
    raw_events = raw_events if isinstance(raw_events, list) else []

    events: list[dict[str, Any]] = []
    deduped_markets: dict[str, dict[str, Any]] = {}

    for raw_event in raw_events:
        if not isinstance(raw_event, dict):
            continue
        event_record, event_markets = _as_event_record(raw_event, ticker=clean_ticker)
        if event_markets:
            events.append(event_record)
        for market in event_markets:
            market_id = str(market.get("marketId") or "").strip()
            if not market_id:
                continue
            if market_id in deduped_markets:
                continue
            deduped_markets[market_id] = market

    markets = list(deduped_markets.values())
    markets.sort(key=lambda item: (item.get("volume24hr") or 0.0, item.get("volume") or 0.0), reverse=True)

    # Keep event market ordering aligned with the global ranking for better UI relevance.
    volume_rank = {str(item.get("marketId") or ""): idx for idx, item in enumerate(markets)}
    for event in events:
        event_markets = event.get("markets") if isinstance(event.get("markets"), list) else []
        event_markets.sort(
            key=lambda item: (
                volume_rank.get(str(item.get("marketId") or ""), 10**6),
                -(item.get("volume24hr") or 0.0),
                -(item.get("volume") or 0.0),
            )
        )

    normalized = {
        "ticker": clean_ticker,
        "events": events,
        "markets": markets,
        "source": GAMMA_API_BASE,
        "fetchedAt": datetime.now(timezone.utc).isoformat(),
    }
    _search_cache.set(cache_key, normalized, ttl_seconds=180)
    return normalized


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[idx : idx + size] for idx in range(0, len(items), size)]


def _post_clob(path: str, body: Any) -> Any:
    response = requests.post(
        f"{CLOB_API_BASE}{path}",
        json=body,
        headers=_request_headers(),
        timeout=DEFAULT_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    if not response.content:
        return {}
    return response.json()


def _get_price_history(token_id: str, *, window_seconds: int, fidelity_seconds: int) -> list[dict[str, float]]:
    key = f"history:{token_id}:{window_seconds}:{fidelity_seconds}"
    cached = _history_cache.get(key)
    if cached is not None:
        return cached

    now_ts = int(time.time())
    start_ts = max(1, now_ts - max(60, int(window_seconds)))
    try:
        response = requests.get(
            f"{CLOB_API_BASE}/prices-history",
            params={
                "market": token_id,
                "startTs": start_ts,
                "endTs": now_ts,
                "fidelity": max(1, int(fidelity_seconds)),
            },
            headers=_request_headers(),
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json() if response.content else {}
    except Exception:
        payload = {}

    history_raw = payload.get("history") if isinstance(payload, dict) else []
    history_raw = history_raw if isinstance(history_raw, list) else []
    out: list[dict[str, float]] = []
    for point in history_raw:
        if not isinstance(point, dict):
            continue
        ts = _to_float(point.get("t"))
        price = _to_float(point.get("p"))
        if ts is None or price is None:
            continue
        out.append({"t": float(ts), "p": float(price)})

    _history_cache.set(key, out, ttl_seconds=60)
    return out


def _normalize_levels(levels: Any, max_levels: int) -> list[dict[str, float]]:
    out: list[dict[str, float]] = []
    if not isinstance(levels, list):
        return out
    for level in levels[: max(1, int(max_levels))]:
        if not isinstance(level, dict):
            continue
        price = _to_float(level.get("price"))
        size = _to_float(level.get("size"))
        if price is None or size is None:
            continue
        out.append({"price": float(price), "size": float(size)})
    return out


def _book_map(raw_books: Any) -> dict[str, dict[str, Any]]:
    mapping: dict[str, dict[str, Any]] = {}
    if not isinstance(raw_books, list):
        return mapping
    for raw in raw_books:
        if not isinstance(raw, dict):
            continue
        token_id = str(raw.get("asset_id") or raw.get("token_id") or "").strip()
        if not token_id:
            continue
        mapping[token_id] = raw
    return mapping


def _as_float_mapping(raw: Any) -> dict[str, float]:
    out: dict[str, float] = {}
    if not isinstance(raw, dict):
        return out
    for key, value in raw.items():
        token_id = str(key).strip()
        if not token_id:
            continue
        if isinstance(value, dict):
            # /prices response shape: {tokenId: {BUY: "0.12"}}
            for nested in value.values():
                parsed = _to_float(nested)
                if parsed is not None:
                    out[token_id] = parsed
                    break
            continue
        parsed = _to_float(value)
        if parsed is None:
            continue
        out[token_id] = parsed
    return out


def fetch_orderbook_bundle(token_ids: list[str], *, max_levels: int = 5) -> dict[str, dict[str, Any]]:
    unique_ids: list[str] = []
    seen: set[str] = set()
    for raw in token_ids:
        token_id = str(raw or "").strip()
        if not token_id or token_id in seen:
            continue
        seen.add(token_id)
        unique_ids.append(token_id)

    if not unique_ids:
        return {}

    # Respect documented cap per request body while still handling larger client requests by chunking.
    capped_ids = unique_ids[:2000]
    request_key = _stable_hash([str(max_levels)] + capped_ids)
    cache_key = f"orderbook:{request_key}"
    cached = _orderbook_cache.get(cache_key)
    if cached is not None:
        return cached

    output: dict[str, dict[str, Any]] = {}
    for chunk in _chunks(capped_ids, MAX_TOKEN_IDS_PER_REQUEST):
        books_payload = [{"token_id": token_id} for token_id in chunk]
        prices_buy_payload = [{"token_id": token_id, "side": "buy"} for token_id in chunk]
        prices_sell_payload = [{"token_id": token_id, "side": "sell"} for token_id in chunk]

        raw_books = _post_clob("/books", books_payload)
        raw_midpoints = _post_clob("/midpoints", books_payload)
        raw_spreads = _post_clob("/spreads", books_payload)
        raw_prices_buy = _post_clob("/prices", prices_buy_payload)
        raw_prices_sell = _post_clob("/prices", prices_sell_payload)

        books = _book_map(raw_books)
        midpoints = _as_float_mapping(raw_midpoints)
        spreads = _as_float_mapping(raw_spreads)
        prices_buy = _as_float_mapping(raw_prices_buy)
        prices_sell = _as_float_mapping(raw_prices_sell)

        for token_id in chunk:
            book = books.get(token_id) or {}
            bids = _normalize_levels(book.get("bids"), max_levels)
            asks = _normalize_levels(book.get("asks"), max_levels)

            best_bid = bids[0]["price"] if bids else prices_sell.get(token_id)
            best_ask = asks[0]["price"] if asks else prices_buy.get(token_id)
            midpoint = midpoints.get(token_id)
            if midpoint is None and best_bid is not None and best_ask is not None:
                midpoint = (best_bid + best_ask) / 2.0

            spread = spreads.get(token_id)
            if spread is None and best_bid is not None and best_ask is not None:
                spread = max(0.0, best_ask - best_bid)

            include_history = len(capped_ids) <= 80
            history_1d = _get_price_history(token_id, window_seconds=86_400, fidelity_seconds=300) if include_history else []
            history_1w = _get_price_history(token_id, window_seconds=604_800, fidelity_seconds=3_600) if include_history else []

            output[token_id] = {
                "bestBid": best_bid,
                "bestAsk": best_ask,
                "midpoint": midpoint,
                "spread": spread,
                "bids": bids,
                "asks": asks,
                "history": {
                    "1d": history_1d,
                    "1w": history_1w,
                },
            }

    _orderbook_cache.set(cache_key, output, ttl_seconds=25)
    return output


def check_rate_limit(
    *,
    route: str,
    client_ip: str,
    limit: int,
    window_seconds: int,
) -> tuple[bool, int]:
    key = f"{route}:{client_ip or 'unknown'}"
    return _rate_limiter.check(key=key, limit=limit, window_seconds=window_seconds)


def summarize_market_volumes(markets: list[dict[str, Any]]) -> dict[str, float]:
    totals = defaultdict(float)
    for market in markets:
        question = str(market.get("question") or "").lower()
        volume_24h = _to_float(market.get("volume24hr")) or 0.0
        volume_total = _to_float(market.get("volume")) or 0.0
        totals["volume24hr"] += volume_24h
        totals["volume"] += volume_total
        if "earnings" in question:
            totals["earnings24hr"] += volume_24h
    return dict(totals)
