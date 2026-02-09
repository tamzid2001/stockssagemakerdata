from __future__ import annotations

import os
from typing import Any

import requests
import yfinance as yf
import pandas as pd
import numpy as np
from finta import TA

import firebase_admin
from firebase_admin import credentials, firestore
from firebase_functions import https_fn
from firebase_functions.options import set_global_options

set_global_options(max_instances=10)

SERVICE_ACCOUNT_PATH = os.environ.get(
    "SERVICE_ACCOUNT_PATH",
    os.path.join(os.path.dirname(__file__), "serviceAccountKey.json"),
)
ADMIN_EMAIL = "tamzid257@gmail.com"
ALLOWED_STATUSES = {"pending", "in_progress", "fulfilled", "cancelled"}
CONTACT_REQUIRED_FIELDS = {"name", "email", "message"}
ALPACA_API_BASE = os.environ.get("ALPACA_API_BASE", "https://paper-api.alpaca.markets")
ALPACA_DATA_BASE = os.environ.get("ALPACA_DATA_BASE", "https://data.alpaca.markets")
ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY") or os.environ.get("ALPACAAPIKEY")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY") or os.environ.get("ALPACASECRETKEY")
TRENDING_URL = "https://query1.finance.yahoo.com/v1/finance/trending/US"
DEFAULT_FORECAST_PRICE = 349

if not firebase_admin._apps:
    if os.path.exists(SERVICE_ACCOUNT_PATH):
        cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
        firebase_admin.initialize_app(cred)
    else:
        firebase_admin.initialize_app()

db = firestore.client()


def _require_auth(req: https_fn.CallableRequest) -> dict[str, Any]:
    if req.auth is None:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.UNAUTHENTICATED,
            "Sign in is required to perform this action.",
        )
    return req.auth.token or {}


def _alpaca_headers() -> dict[str, str]:
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
            "Missing Alpaca API credentials.",
        )
    return {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
        "Content-Type": "application/json",
    }


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


@https_fn.on_call()
def create_order(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}

    product = str(data.get("product") or "Deep Forecast")
    currency = str(data.get("currency") or "USD")
    price = data.get("price")
    if not isinstance(price, (int, float)):
        price = DEFAULT_FORECAST_PRICE
    meta = data.get("meta") or {}
    if not isinstance(meta, dict):
        meta = {}

    order = {
        "userId": req.auth.uid,
        "userEmail": token.get("email"),
        "product": product,
        "price": price,
        "currency": currency,
        "status": "pending",
        "fulfillmentNotes": "",
        "meta": meta,
        "createdAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
    }

    doc_ref = db.collection("orders").document()
    doc_ref.set(order)

    return {"orderId": doc_ref.id}


@https_fn.on_call()
def update_order_status(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    email = token.get("email")

    if email != ADMIN_EMAIL:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.PERMISSION_DENIED,
            "Admin access required.",
        )

    data = req.data or {}
    order_id = data.get("orderId")
    status = data.get("status")
    notes = data.get("notes", "")

    if not order_id:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.INVALID_ARGUMENT,
            "Order ID is required.",
        )

    if status not in ALLOWED_STATUSES:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.INVALID_ARGUMENT,
            "Invalid order status.",
        )

    order_ref = db.collection("orders").document(order_id)
    snapshot = order_ref.get()
    if not snapshot.exists:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.NOT_FOUND,
            "Order not found.",
        )

    update_payload: dict[str, Any] = {
        "status": status,
        "fulfillmentNotes": notes,
        "updatedAt": firestore.SERVER_TIMESTAMP,
    }

    if status == "fulfilled":
        update_payload["fulfilledAt"] = firestore.SERVER_TIMESTAMP

    order_ref.update(update_payload)

    return {"orderId": order_id, "status": status}


@https_fn.on_call()
def submit_contact(req: https_fn.CallableRequest) -> dict[str, Any]:
    data = req.data or {}
    payload = {
        "name": str(data.get("name") or "").strip(),
        "email": str(data.get("email") or "").strip(),
        "company": str(data.get("company") or "").strip(),
        "role": str(data.get("role") or "").strip(),
        "message": str(data.get("message") or "").strip(),
        "sourcePage": str(data.get("sourcePage") or "").strip(),
        "utm": data.get("utm") or {},
        "meta": data.get("meta") or {},
        "createdAt": firestore.SERVER_TIMESTAMP,
    }

    missing = [field for field in CONTACT_REQUIRED_FIELDS if not payload[field]]
    if missing:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.INVALID_ARGUMENT,
            "Missing required contact fields.",
            {"missing": missing},
        )

    doc_ref = db.collection("contacts").document()
    doc_ref.set(payload)

    return {"contactId": doc_ref.id}


@https_fn.on_call()
def run_prophet_forecast(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}

    ticker = str(data.get("ticker") or "").upper()
    if not ticker:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.INVALID_ARGUMENT,
            "Ticker is required.",
        )

    horizon = int(data.get("horizon") or 90)
    interval = str(data.get("interval") or "1d")
    start = data.get("start")
    quantiles = data.get("quantiles") or [0.1, 0.25, 0.5, 0.75, 0.9]

    request_doc = {
        "userId": req.auth.uid,
        "userEmail": token.get("email"),
        "ticker": ticker,
        "interval": interval,
        "horizon": horizon,
        "start": start,
        "quantiles": quantiles,
        "status": "queued",
        "createdAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
        "meta": data.get("meta") or {},
        "utm": data.get("utm") or {},
    }
    doc_ref = db.collection("forecast_requests").document()
    doc_ref.set(request_doc)

    try:
        history = yf.download(ticker, start=start, interval=interval, progress=False)
        if history.empty or "Close" not in history.columns:
            return {"requestId": doc_ref.id, "status": "queued"}

        history = history.dropna()
        last_close = float(history["Close"].iloc[-1])
        recent = history["Close"].tail(min(len(history), 90))
        mae = float(np.mean(np.abs(recent - recent.shift(1)).dropna()))

        return {
            "requestId": doc_ref.id,
            "status": "queued",
            "lastClose": round(last_close, 2),
            "mae": round(mae, 2),
            "coverage10_90": "queued",
        }
    except Exception:
        return {"requestId": doc_ref.id, "status": "queued"}


@https_fn.on_call()
def get_ticker_history(req: https_fn.CallableRequest) -> dict[str, Any]:
    data = req.data or {}
    ticker = str(data.get("ticker") or "").upper()
    if not ticker:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.INVALID_ARGUMENT,
            "Ticker is required.",
        )
    interval = str(data.get("interval") or "1d")
    start = data.get("start")
    end = data.get("end")

    history = yf.download(ticker, start=start, end=end, interval=interval, progress=False)
    history = history.dropna().tail(500)
    history.reset_index(inplace=True)
    rows = history.to_dict(orient="records")
    return {"rows": rows}


@https_fn.on_call()
def get_trending_tickers(req: https_fn.CallableRequest) -> dict[str, Any]:
    try:
        response = requests.get(TRENDING_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        quotes = data.get("finance", {}).get("result", [{}])[0].get("quotes", [])
        tickers = [item.get("symbol") for item in quotes if item.get("symbol")]
        return {"tickers": tickers}
    except Exception:
        return {"tickers": []}


@https_fn.on_call()
def get_ticker_news(req: https_fn.CallableRequest) -> dict[str, Any]:
    data = req.data or {}
    ticker = str(data.get("ticker") or "").upper()
    if not ticker:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.INVALID_ARGUMENT,
            "Ticker is required.",
        )

    news_items = []
    try:
        ticker_obj = yf.Ticker(ticker)
        for item in ticker_obj.news[:10]:
            news_items.append(
                {
                    "title": item.get("title"),
                    "publisher": item.get("publisher"),
                    "link": item.get("link"),
                    "publishedAt": item.get("providerPublishTime"),
                }
            )
    except Exception:
        pass

    return {"news": news_items}


@https_fn.on_call()
def get_technicals(req: https_fn.CallableRequest) -> dict[str, Any]:
    data = req.data or {}
    ticker = str(data.get("ticker") or "").upper()
    if not ticker:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.INVALID_ARGUMENT,
            "Ticker is required.",
        )

    interval = str(data.get("interval") or "1d")
    lookback = int(data.get("lookback") or 120)
    indicators = data.get("indicators") or ["RSI", "MACD"]

    history = yf.download(ticker, period=f\"{lookback}d\", interval=interval, progress=False)
    history = history.dropna()
    if history.empty:
        return {"latest": []}

    history = history.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )

    latest_values = []
    indicator_map = {
        "RSI": lambda df: TA.RSI(df),
        "MACD": lambda df: TA.MACD(df)["MACD"],
        "SMA": lambda df: TA.SMA(df, 20),
        "EMA": lambda df: TA.EMA(df, 20),
        "BBANDS": lambda df: TA.BBANDS(df)["BB_UPPER"],
        "ATR": lambda df: TA.ATR(df, 14),
    }

    for indicator in indicators:
        func = indicator_map.get(indicator)
        if not func:
            continue
        series = func(history)
        value = series.dropna().iloc[-1] if not series.empty else None
        if value is not None:
            latest_values.append({"name": indicator, "value": round(float(value), 4)})

    return {"latest": latest_values}


@https_fn.on_call()
def queue_screener_run(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    payload = {
        "userId": req.auth.uid,
        "userEmail": token.get("email"),
        "universe": data.get("universe"),
        "market": data.get("market"),
        "minCap": data.get("minCap"),
        "maxNames": data.get("maxNames"),
        "notes": data.get("notes"),
        "status": "queued",
        "createdAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
        "meta": data.get("meta") or {},
    }

    doc_ref = db.collection("screener_runs").document()
    doc_ref.set(payload)
    return {"runId": doc_ref.id}


@https_fn.on_call()
def queue_autopilot_run(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    payload = {
        "userId": req.auth.uid,
        "userEmail": token.get("email"),
        "ticker": data.get("ticker"),
        "horizon": data.get("horizon"),
        "quantiles": data.get("quantiles"),
        "interval": data.get("interval"),
        "notes": data.get("notes"),
        "status": "queued",
        "createdAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
        "meta": data.get("meta") or {},
    }

    doc_ref = db.collection("autopilot_requests").document()
    doc_ref.set(payload)
    return {"requestId": doc_ref.id}


@https_fn.on_call()
def alpaca_get_options(req: https_fn.CallableRequest) -> dict[str, Any]:
    data = req.data or {}
    ticker = str(data.get("ticker") or "").upper()
    expiration = data.get("expiration")
    if not ticker:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.INVALID_ARGUMENT,
            "Ticker is required.",
        )

    options = []
    try:
        headers = _alpaca_headers()
        params = {"underlying_symbols": ticker, "limit": 20}
        if expiration:
            params["expiration_date"] = expiration
        response = requests.get(
            f\"{ALPACA_DATA_BASE}/v1beta1/options/contracts\",
            headers=headers,
            params=params,
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
        contracts = data.get("option_contracts", [])
        for contract in contracts[:20]:
            options.append(
                {
                    "symbol": contract.get("symbol"),
                    "type": contract.get("type"),
                    "strike": contract.get("strike_price"),
                    "expiration": contract.get("expiration_date"),
                    "lastPrice": contract.get("close_price"),
                    "delta": contract.get("delta"),
                    "impliedProbability": contract.get("delta"),
                }
            )
    except Exception:
        try:
            ticker_obj = yf.Ticker(ticker)
            expirations = ticker_obj.options
            if expirations:
                exp = expiration or expirations[0]
                chain = ticker_obj.option_chain(exp)
                for _, row in chain.calls.head(5).iterrows():
                    options.append(
                        {
                            "symbol": row.get("contractSymbol"),
                            "type": "call",
                            "strike": row.get("strike"),
                            "expiration": exp,
                            "lastPrice": row.get("lastPrice"),
                            "delta": None,
                            "impliedProbability": None,
                        }
                    )
                for _, row in chain.puts.head(5).iterrows():
                    options.append(
                        {
                            "symbol": row.get("contractSymbol"),
                            "type": "put",
                            "strike": row.get("strike"),
                            "expiration": exp,
                            "lastPrice": row.get("lastPrice"),
                            "delta": None,
                            "impliedProbability": None,
                        }
                    )
        except Exception:
            options = []

    return {"options": options}


@https_fn.on_call()
def alpaca_place_order(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    symbol = str(data.get("symbol") or "").upper()
    if not symbol:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.INVALID_ARGUMENT,
            "Symbol is required.",
        )

    order_payload: dict[str, Any] = {
        "symbol": symbol,
        "qty": data.get("qty") or 1,
        "side": data.get("side") or "buy",
        "type": data.get("type") or "market",
        "time_in_force": data.get("timeInForce") or "day",
    }

    limit_price = _safe_float(data.get("limitPrice"))
    if order_payload["type"] == "limit" and limit_price:
        order_payload["limit_price"] = limit_price

    headers = _alpaca_headers()
    response = requests.post(
        f\"{ALPACA_API_BASE}/v2/orders\",
        headers=headers,
        json=order_payload,
        timeout=10,
    )

    if response.status_code >= 400:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
            f\"Alpaca error: {response.text}\",
        )

    order_data = response.json()
    doc_ref = db.collection("alpaca_orders").document()
    doc_ref.set(
        {
            "userId": req.auth.uid,
            "userEmail": token.get("email"),
            "symbol": symbol,
            "side": order_payload["side"],
            "type": order_payload["type"],
            "qty": order_payload["qty"],
            "status": order_data.get("status"),
            "alpacaId": order_data.get("id"),
            "response": order_data,
            "createdAt": firestore.SERVER_TIMESTAMP,
            "meta": data.get("meta") or {},
        }
    )

    return {"orderId": doc_ref.id, "alpacaId": order_data.get("id")}
