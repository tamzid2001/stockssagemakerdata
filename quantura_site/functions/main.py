from __future__ import annotations

import os
from statistics import NormalDist
from typing import Any

import numpy as np
import pandas as pd
import requests
import yfinance as yf
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
FORECAST_SERVICES = {"prophet", "ibm_timemixer"}

ALPACA_API_BASE = os.environ.get("ALPACA_API_BASE", "https://paper-api.alpaca.markets")
ALPACA_DATA_BASE = os.environ.get("ALPACA_DATA_BASE", "https://data.alpaca.markets")
ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY") or os.environ.get("ALPACAAPIKEY")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY") or os.environ.get("ALPACASECRETKEY")

IBM_TIMEMIXER_MODEL_ID = os.environ.get("IBM_TIMEMIXER_MODEL_ID", "ibm-granite/granite-timeseries-ttm-r2")
IBM_TIMEMIXER_ENDPOINT = os.environ.get("IBM_TIMEMIXER_ENDPOINT", "").strip()
IBM_TIMEMIXER_API_KEY = os.environ.get("IBM_TIMEMIXER_API_KEY", "").strip()
HUGGINGFACEHUB_API_TOKEN = os.environ.get("HUGGINGFACEHUB_API_TOKEN", "").strip()

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "").strip()
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


def _require_admin(token: dict[str, Any]) -> None:
    if token.get("email") != ADMIN_EMAIL:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.PERMISSION_DENIED,
            "Admin access required.",
        )


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


def _audit_event(uid: str, email: str | None, event_type: str, payload: dict[str, Any]) -> None:
    db.collection("user_events").add(
        {
            "userId": uid,
            "userEmail": email,
            "eventType": event_type,
            "payload": payload,
            "createdAt": firestore.SERVER_TIMESTAMP,
        }
    )


def _serialize_for_firestore(value: Any) -> Any:
    if isinstance(value, (np.floating, np.integer)):
        return value.item()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.ndarray):
        return [_serialize_for_firestore(item) for item in value.tolist()]
    if isinstance(value, list):
        return [_serialize_for_firestore(item) for item in value]
    if isinstance(value, dict):
        return {key: _serialize_for_firestore(item) for key, item in value.items()}
    return value


def _load_history(ticker: str, start: str | None, interval: str) -> pd.DataFrame:
    period = "730d" if interval == "1h" else "10y"
    frame = yf.download(
        ticker,
        start=start or None,
        period=None if start else period,
        interval=interval,
        progress=False,
        auto_adjust=False,
    )
    if frame.empty:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "No market data found for ticker.")

    if isinstance(frame.columns, pd.MultiIndex):
        frame.columns = frame.columns.get_level_values(0)

    if "Close" not in frame.columns:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "Close prices unavailable for ticker.")

    frame = frame.dropna(subset=["Close"])
    if frame.empty:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "No valid close values for ticker.")

    return frame


def _generate_quantile_forecast(
    close_series: pd.Series,
    horizon: int,
    quantiles: list[float],
    interval: str,
) -> dict[str, Any]:
    quantiles = sorted({float(q) for q in quantiles if 0 < float(q) < 1})
    if 0.5 not in quantiles:
        quantiles.append(0.5)
        quantiles.sort()

    horizon = max(1, min(horizon, 365 if interval == "1d" else 240))

    values = close_series.astype(float).dropna().values
    if len(values) < 15:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
            "Not enough history to generate forecast.",
        )

    log_returns = np.diff(np.log(values))
    recent = log_returns[-min(len(log_returns), 252) :]
    drift = float(np.mean(recent[-min(len(recent), 40) :])) if len(recent) else 0.0
    vol = float(np.std(recent[-min(len(recent), 120) :])) if len(recent) else 0.01
    vol = max(vol, 0.0008)

    rng = np.random.default_rng(42)
    sims = rng.normal(loc=drift, scale=vol, size=(1500, horizon)).cumsum(axis=1)
    sim_prices = values[-1] * np.exp(sims)

    freq = "H" if interval == "1h" else "B"
    dates = pd.date_range(close_series.index[-1], periods=horizon + 1, freq=freq)[1:]

    forecast_rows: list[dict[str, Any]] = []
    for idx, ts in enumerate(dates):
        row = {"ds": ts.isoformat()}
        for quantile in quantiles:
            row[f"q{int(round(quantile * 100)):02d}"] = round(float(np.quantile(sim_prices[:, idx], quantile)), 4)
        forecast_rows.append(row)

    median_key = "q50"
    median_path = np.array([row.get(median_key, values[-1]) for row in forecast_rows], dtype=float)
    last_actual = float(values[-1])
    mae_recent = float(np.mean(np.abs(np.diff(values[-min(len(values), 90) :])))) if len(values) > 2 else 0.0

    return {
        "engine": "statistical_fallback",
        "status": "completed",
        "serviceMessage": "Fallback Monte Carlo quantile model used.",
        "forecastRows": forecast_rows,
        "metrics": {
            "lastClose": round(last_actual, 4),
            "mae": round(mae_recent, 4),
            "horizon": horizon,
            "medianEnd": round(float(median_path[-1]), 4),
        },
    }


def _run_prophet_engine(close_series: pd.Series, horizon: int, quantiles: list[float], interval: str) -> dict[str, Any]:
    try:
        from prophet import Prophet  # type: ignore
    except Exception:
        return _generate_quantile_forecast(close_series, horizon, quantiles, interval)

    forecast_core = _generate_quantile_forecast(close_series, horizon, quantiles, interval)

    df = pd.DataFrame({"ds": close_series.index.tz_localize(None), "y": close_series.values.astype(float)})
    is_hourly = interval == "1h"
    model = Prophet(
        daily_seasonality=is_hourly,
        weekly_seasonality=True,
        yearly_seasonality=not is_hourly,
        changepoint_prior_scale=0.08,
        seasonality_prior_scale=12.0,
        interval_width=0.8,
    )
    model.add_country_holidays(country_name="US")
    if not is_hourly:
        model.add_seasonality(name="monthly", period=30.5, fourier_order=5)

    model.fit(df)

    freq = "H" if is_hourly else "B"
    periods = max(1, min(horizon, 365 if not is_hourly else 240))
    future = model.make_future_dataframe(periods=periods, freq=freq, include_history=False)
    forecast = model.predict(future)

    normal = NormalDist()
    z_80 = normal.inv_cdf(0.9)
    if z_80 == 0:
        z_80 = 1.28155

    rows: list[dict[str, Any]] = []
    quantiles = sorted({float(q) for q in quantiles if 0 < float(q) < 1})
    if 0.5 not in quantiles:
        quantiles.append(0.5)

    for _, row in forecast.iterrows():
        yhat = float(row["yhat"])
        lower = float(row["yhat_lower"])
        upper = float(row["yhat_upper"])
        sigma = max((upper - lower) / (2 * z_80), 1e-6)
        out_row: dict[str, Any] = {"ds": row["ds"].isoformat()}
        for q in quantiles:
            z = normal.inv_cdf(q)
            out_row[f"q{int(round(q * 100)):02d}"] = round(yhat + sigma * z, 4)
        rows.append(out_row)

    forecast_core["engine"] = "prophet"
    forecast_core["serviceMessage"] = "Forecast generated with Prophet, quantiles derived from model uncertainty."
    forecast_core["forecastRows"] = rows
    forecast_core["metrics"]["medianEnd"] = rows[-1].get("q50") if rows else forecast_core["metrics"].get("medianEnd")

    return forecast_core


def _run_timemixer_engine(close_series: pd.Series, horizon: int, quantiles: list[float], interval: str) -> dict[str, Any]:
    payload = {
        "model_id": IBM_TIMEMIXER_MODEL_ID,
        "history": [round(float(value), 6) for value in close_series.tail(2048).tolist()],
        "horizon": int(horizon),
        "quantiles": quantiles,
        "interval": interval,
    }

    headers = {"Content-Type": "application/json"}
    if IBM_TIMEMIXER_API_KEY:
        headers["Authorization"] = f"Bearer {IBM_TIMEMIXER_API_KEY}"

    if IBM_TIMEMIXER_ENDPOINT:
        try:
            response = requests.post(IBM_TIMEMIXER_ENDPOINT, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            body = response.json()
            if isinstance(body, dict) and isinstance(body.get("forecastRows"), list):
                result = _generate_quantile_forecast(close_series, horizon, quantiles, interval)
                result["engine"] = "ibm_timemixer_endpoint"
                result["serviceMessage"] = "Forecast generated by configured IBM TimeMixer endpoint."
                result["forecastRows"] = body["forecastRows"]
                metrics = body.get("metrics") or {}
                result["metrics"].update({k: _serialize_for_firestore(v) for k, v in metrics.items()})
                return result
        except Exception:
            pass

    if HUGGINGFACEHUB_API_TOKEN:
        try:
            hf_headers = {
                "Authorization": f"Bearer {HUGGINGFACEHUB_API_TOKEN}",
                "Content-Type": "application/json",
            }
            hf_response = requests.post(
                f"https://api-inference.huggingface.co/models/{IBM_TIMEMIXER_MODEL_ID}",
                headers=hf_headers,
                json={"inputs": payload},
                timeout=45,
            )
            if hf_response.status_code < 400:
                body = hf_response.json()
                if isinstance(body, dict) and isinstance(body.get("forecastRows"), list):
                    result = _generate_quantile_forecast(close_series, horizon, quantiles, interval)
                    result["engine"] = "ibm_timemixer_hf"
                    result["serviceMessage"] = "Forecast generated via IBM TimeMixer Hugging Face endpoint."
                    result["forecastRows"] = body["forecastRows"]
                    return result
        except Exception:
            pass

    result = _generate_quantile_forecast(close_series, horizon, quantiles, interval)
    result["engine"] = "ibm_timemixer_proxy"
    result["serviceMessage"] = (
        "IBM TimeMixer service selected (model ibm-granite/granite-timeseries-ttm-r2). "
        "Proxy fallback model executed because no external TimeMixer endpoint is configured."
    )
    return result


def _run_forecast_service(
    service: str,
    close_series: pd.Series,
    horizon: int,
    quantiles: list[float],
    interval: str,
) -> dict[str, Any]:
    if service == "ibm_timemixer":
        return _run_timemixer_engine(close_series, horizon, quantiles, interval)
    return _run_prophet_engine(close_series, horizon, quantiles, interval)


def _forecast_preview_rows(rows: list[dict[str, Any]], max_rows: int = 12) -> list[dict[str, Any]]:
    return rows[:max_rows]


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
    _audit_event(req.auth.uid, token.get("email"), "order_created", {"orderId": doc_ref.id, "product": product})

    return {"orderId": doc_ref.id}


@https_fn.on_call()
def update_order_status(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    _require_admin(token)

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
    _audit_event(req.auth.uid, token.get("email"), "order_status_updated", {"orderId": order_id, "status": status})

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


def _handle_forecast_request(req: https_fn.CallableRequest, forced_service: str | None = None) -> dict[str, Any]:
    token = _require_auth(req)
    data = dict(req.data or {})
    if forced_service:
        data["service"] = forced_service

    ticker = str(data.get("ticker") or "").upper().strip()
    if not ticker:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Ticker is required.")

    service = str(data.get("service") or "prophet").strip().lower()
    if service not in FORECAST_SERVICES:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.INVALID_ARGUMENT,
            "Invalid forecast service.",
            {"allowed": sorted(FORECAST_SERVICES)},
        )

    interval = str(data.get("interval") or "1d")
    if interval not in {"1d", "1h"}:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Interval must be 1d or 1h.")

    horizon = int(data.get("horizon") or 90)
    quantiles_raw = data.get("quantiles") or [0.1, 0.25, 0.5, 0.75, 0.9]
    quantiles = [float(q) for q in quantiles_raw]
    start = data.get("start")

    history = _load_history(ticker=ticker, start=start, interval=interval)
    close_series = history["Close"].copy()

    result = _run_forecast_service(service, close_series, horizon, quantiles, interval)

    request_doc = {
        "userId": req.auth.uid,
        "userEmail": token.get("email"),
        "ticker": ticker,
        "interval": interval,
        "horizon": horizon,
        "start": start,
        "quantiles": quantiles,
        "service": service,
        "engine": result.get("engine"),
        "status": result.get("status", "completed"),
        "serviceMessage": result.get("serviceMessage"),
        "metrics": _serialize_for_firestore(result.get("metrics") or {}),
        "forecastPreview": _serialize_for_firestore(_forecast_preview_rows(result.get("forecastRows") or [])),
        "forecastRows": _serialize_for_firestore(result.get("forecastRows") or []),
        "meta": data.get("meta") or {},
        "utm": data.get("utm") or {},
        "createdAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
    }

    doc_ref = db.collection("forecast_requests").document()
    doc_ref.set(request_doc)

    _audit_event(
        req.auth.uid,
        token.get("email"),
        "forecast_requested",
        {
            "requestId": doc_ref.id,
            "ticker": ticker,
            "service": service,
            "engine": result.get("engine"),
        },
    )

    metrics = result.get("metrics") or {}
    return {
        "requestId": doc_ref.id,
        "status": request_doc["status"],
        "service": service,
        "engine": request_doc["engine"],
        "serviceMessage": request_doc["serviceMessage"],
        "lastClose": metrics.get("lastClose"),
        "mae": metrics.get("mae"),
        "coverage10_90": metrics.get("coverage10_90", "n/a"),
        "forecastPreview": request_doc["forecastPreview"],
    }


@https_fn.on_call()
def run_timeseries_forecast(req: https_fn.CallableRequest) -> dict[str, Any]:
    return _handle_forecast_request(req)


@https_fn.on_call()
def run_prophet_forecast(req: https_fn.CallableRequest) -> dict[str, Any]:
    return _handle_forecast_request(req, forced_service="prophet")


@https_fn.on_call()
def get_ticker_history(req: https_fn.CallableRequest) -> dict[str, Any]:
    data = req.data or {}
    ticker = str(data.get("ticker") or "").upper()
    if not ticker:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Ticker is required.")

    interval = str(data.get("interval") or "1d")
    start = data.get("start")
    end = data.get("end")

    history = yf.download(ticker, start=start, end=end, interval=interval, progress=False)
    if isinstance(history.columns, pd.MultiIndex):
        history.columns = history.columns.get_level_values(0)
    history = history.dropna().tail(500).reset_index()

    date_col = "Datetime" if "Datetime" in history.columns else "Date"
    if date_col in history.columns:
        history[date_col] = history[date_col].astype(str)

    rows = history.to_dict(orient="records")
    return {"rows": _serialize_for_firestore(rows)}


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
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Ticker is required.")

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
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Ticker is required.")

    interval = str(data.get("interval") or "1d")
    lookback = int(data.get("lookback") or 120)
    indicators = data.get("indicators") or ["RSI", "MACD"]

    history = yf.download(ticker, period=f"{lookback}d", interval=interval, progress=False)
    if isinstance(history.columns, pd.MultiIndex):
        history.columns = history.columns.get_level_values(0)
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
        "RSI": lambda frame: TA.RSI(frame),
        "MACD": lambda frame: TA.MACD(frame).get("MACD"),
        "SMA": lambda frame: TA.SMA(frame, 20),
        "EMA": lambda frame: TA.EMA(frame, 20),
        "BBANDS": lambda frame: TA.BBANDS(frame).get("BB_UPPER"),
        "ATR": lambda frame: TA.ATR(frame, 14),
    }

    for indicator in indicators:
        func = indicator_map.get(indicator)
        if not func:
            continue
        try:
            series = func(history)
            if series is None or len(series) == 0:
                continue
            value = series.dropna().iloc[-1]
            latest_values.append({"name": indicator, "value": round(float(value), 4)})
        except Exception:
            continue

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
    _audit_event(req.auth.uid, token.get("email"), "screener_queued", {"runId": doc_ref.id})
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
    _audit_event(req.auth.uid, token.get("email"), "autopilot_queued", {"requestId": doc_ref.id})
    return {"requestId": doc_ref.id}


@https_fn.on_call()
def alpaca_get_options(req: https_fn.CallableRequest) -> dict[str, Any]:
    _require_auth(req)
    data = req.data or {}
    ticker = str(data.get("ticker") or "").upper()
    expiration = data.get("expiration")
    if not ticker:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Ticker is required.")

    options = []
    try:
        headers = _alpaca_headers()
        params = {"underlying_symbols": ticker, "limit": 30}
        if expiration:
            params["expiration_date"] = expiration
        response = requests.get(
            f"{ALPACA_DATA_BASE}/v1beta1/options/contracts",
            headers=headers,
            params=params,
            timeout=12,
        )
        response.raise_for_status()
        contracts = response.json().get("option_contracts", [])
        for contract in contracts[:30]:
            delta = _safe_float(contract.get("delta"))
            implied_prob = None
            if delta is not None:
                implied_prob = max(0.0, min(abs(delta), 1.0))
            options.append(
                {
                    "symbol": contract.get("symbol"),
                    "type": contract.get("type"),
                    "strike": contract.get("strike_price"),
                    "expiration": contract.get("expiration_date"),
                    "lastPrice": contract.get("close_price"),
                    "delta": None if delta is None else round(delta, 4),
                    "impliedProbability": None if implied_prob is None else round(implied_prob * 100, 2),
                }
            )
    except Exception:
        try:
            ticker_obj = yf.Ticker(ticker)
            expirations = ticker_obj.options
            if expirations:
                exp = expiration or expirations[0]
                chain = ticker_obj.option_chain(exp)
                for _, row in chain.calls.head(10).iterrows():
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
                for _, row in chain.puts.head(10).iterrows():
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

    symbol = str(data.get("symbol") or "").upper().strip()
    if not symbol:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Symbol is required.")

    order_payload: dict[str, Any] = {
        "symbol": symbol,
        "qty": int(data.get("qty") or 1),
        "side": str(data.get("side") or "buy"),
        "type": str(data.get("type") or "market"),
        "time_in_force": str(data.get("timeInForce") or "day"),
    }

    limit_price = _safe_float(data.get("limitPrice"))
    if order_payload["type"] == "limit" and limit_price:
        order_payload["limit_price"] = limit_price

    response = requests.post(
        f"{ALPACA_API_BASE}/v2/orders",
        headers=_alpaca_headers(),
        json=order_payload,
        timeout=12,
    )

    if response.status_code >= 400:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
            f"Alpaca error: {response.text}",
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
            "response": _serialize_for_firestore(order_data),
            "createdAt": firestore.SERVER_TIMESTAMP,
            "meta": data.get("meta") or {},
        }
    )

    _audit_event(
        req.auth.uid,
        token.get("email"),
        "alpaca_order_placed",
        {"orderId": doc_ref.id, "alpacaId": order_data.get("id"), "symbol": symbol},
    )

    return {"orderId": doc_ref.id, "alpacaId": order_data.get("id")}


@https_fn.on_call()
def alpaca_get_account(req: https_fn.CallableRequest) -> dict[str, Any]:
    _require_auth(req)
    response = requests.get(f"{ALPACA_API_BASE}/v2/account", headers=_alpaca_headers(), timeout=12)
    if response.status_code >= 400:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.FAILED_PRECONDITION, response.text)
    account = response.json()
    return {
        "account": {
            "id": account.get("id"),
            "status": account.get("status"),
            "currency": account.get("currency"),
            "buying_power": account.get("buying_power"),
            "equity": account.get("equity"),
            "cash": account.get("cash"),
            "portfolio_value": account.get("portfolio_value"),
            "pattern_day_trader": account.get("pattern_day_trader"),
        }
    }


@https_fn.on_call()
def alpaca_get_positions(req: https_fn.CallableRequest) -> dict[str, Any]:
    _require_auth(req)
    response = requests.get(f"{ALPACA_API_BASE}/v2/positions", headers=_alpaca_headers(), timeout=12)
    if response.status_code >= 400:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.FAILED_PRECONDITION, response.text)
    positions = response.json()
    clean_positions = [
        {
            "symbol": item.get("symbol"),
            "qty": item.get("qty"),
            "market_value": item.get("market_value"),
            "cost_basis": item.get("cost_basis"),
            "unrealized_pl": item.get("unrealized_pl"),
            "unrealized_plpc": item.get("unrealized_plpc"),
            "side": item.get("side"),
        }
        for item in positions
    ]
    return {"positions": clean_positions}


@https_fn.on_call()
def alpaca_list_orders(req: https_fn.CallableRequest) -> dict[str, Any]:
    _require_auth(req)
    data = req.data or {}
    status = str(data.get("status") or "all")
    limit = int(data.get("limit") or 50)
    params = {"status": status, "limit": max(1, min(limit, 100)), "direction": "desc"}

    response = requests.get(f"{ALPACA_API_BASE}/v2/orders", headers=_alpaca_headers(), params=params, timeout=12)
    if response.status_code >= 400:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.FAILED_PRECONDITION, response.text)

    orders = response.json()
    clean_orders = [
        {
            "id": item.get("id"),
            "symbol": item.get("symbol"),
            "status": item.get("status"),
            "side": item.get("side"),
            "type": item.get("type"),
            "qty": item.get("qty"),
            "filled_qty": item.get("filled_qty"),
            "filled_avg_price": item.get("filled_avg_price"),
            "created_at": item.get("created_at"),
        }
        for item in orders
    ]
    return {"orders": clean_orders}


@https_fn.on_call()
def alpaca_cancel_order(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    order_id = str(data.get("orderId") or "").strip()
    if not order_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "orderId is required.")

    response = requests.delete(f"{ALPACA_API_BASE}/v2/orders/{order_id}", headers=_alpaca_headers(), timeout=12)
    if response.status_code >= 400:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.FAILED_PRECONDITION, response.text)

    _audit_event(req.auth.uid, token.get("email"), "alpaca_order_cancelled", {"alpacaId": order_id})
    return {"cancelled": True, "orderId": order_id}


@https_fn.on_call()
def send_slack_test_message(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    _require_admin(token)

    if not SLACK_WEBHOOK_URL:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
            "SLACK_WEBHOOK_URL is not configured.",
        )

    data = req.data or {}
    text = str(data.get("text") or "Quantura Slack webhook test from Firebase function.")
    payload = {"text": text}
    response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
    if response.status_code >= 400:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.INTERNAL,
            f"Slack webhook failed: {response.status_code} {response.text}",
        )

    _audit_event(req.auth.uid, token.get("email"), "slack_test_sent", {"message": text})
    return {"ok": True}
