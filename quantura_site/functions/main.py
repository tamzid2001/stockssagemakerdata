from __future__ import annotations

import hashlib
import math
import os
import time
from datetime import date, datetime, timezone
from statistics import NormalDist
from typing import Any

import requests

import firebase_admin
from firebase_admin import credentials, firestore, messaging as admin_messaging
from firebase_functions import https_fn
from firebase_functions.options import set_global_options

try:
    from firebase_admin import remote_config as admin_remote_config  # type: ignore
except Exception:  # pragma: no cover - optional dependency until firebase-admin>=7.x
    admin_remote_config = None

set_global_options(max_instances=10)

SERVICE_ACCOUNT_PATH = os.environ.get(
    "SERVICE_ACCOUNT_PATH",
    os.path.join(os.path.dirname(__file__), "serviceAccountKey.json"),
)
PUBLIC_ORIGIN = (os.environ.get("PUBLIC_ORIGIN") or "https://quantura-e2e3d.web.app").rstrip("/")
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
FCM_WEB_VAPID_KEY = os.environ.get("FCM_WEB_VAPID_KEY", "").strip()
RISK_FREE_RATE = float(os.environ.get("RISK_FREE_RATE", "0.045") or 0.045)
TRENDING_URL = "https://query1.finance.yahoo.com/v1/finance/trending/US"
YAHOO_SEARCH_URL = "https://query2.finance.yahoo.com/v1/finance/search"
DEFAULT_FORECAST_PRICE = 349

if not firebase_admin._apps:
    if os.path.exists(SERVICE_ACCOUNT_PATH):
        cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
        firebase_admin.initialize_app(cred)
    else:
        firebase_admin.initialize_app()

db = firestore.client()

_REMOTE_CONFIG_CACHE: dict[str, Any] = {"template": None, "loadedAt": 0.0}

DEFAULT_REMOTE_CONFIG: dict[str, str] = {
    "welcome_message": "Welcome to Quantura",
    "watchlist_enabled": "true",
    "forecast_prophet_enabled": "true",
    "forecast_timemixer_enabled": "true",
    "webpush_vapid_key": "",
}


def _get_remote_config_template(max_age_seconds: int = 300) -> Any | None:
    """Loads a Remote Config ServerTemplate and keeps it cached between invocations."""
    if admin_remote_config is None:
        return None
    now = time.time()
    cached = _REMOTE_CONFIG_CACHE.get("template")
    loaded_at = float(_REMOTE_CONFIG_CACHE.get("loadedAt") or 0.0)

    if cached is None:
        try:
            cached = admin_remote_config.init_server_template(default_config=DEFAULT_REMOTE_CONFIG)
            _REMOTE_CONFIG_CACHE["template"] = cached
        except Exception:
            return None

    if loaded_at and (now - loaded_at) < max_age_seconds:
        return cached

    try:
        cached.load()
        _REMOTE_CONFIG_CACHE["loadedAt"] = now
    except Exception:
        # Keep the old template cache (if any).
        pass
    return cached


def _remote_config_context(
    req: https_fn.CallableRequest | None,
    token: dict[str, Any] | None,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Builds a stable Remote Config evaluation context.

    Note: Python ServerTemplate percent conditions expect `randomization_id`.
    """
    meta = meta or {}
    token = token or {}
    context: dict[str, Any] = {}

    randomization_id = ""
    if req and req.auth:
        randomization_id = req.auth.uid
    if not randomization_id and isinstance(meta.get("sessionId"), str) and meta.get("sessionId"):
        # For unauthenticated calls, fall back to a stable per-device session id.
        randomization_id = meta["sessionId"]
    if randomization_id:
        context["randomization_id"] = randomization_id

    email = token.get("email")
    if isinstance(email, str) and email:
        context["email"] = email.lower()
    if isinstance(meta.get("pagePath"), str) and meta.get("pagePath"):
        context["page_path"] = meta["pagePath"]
    if isinstance(meta.get("timezone"), str) and meta.get("timezone"):
        context["timezone"] = meta["timezone"]
    return context


def _remote_config_param(key: str, default: str = "", context: dict[str, Any] | None = None) -> str:
    template = _get_remote_config_template()
    if template is None:
        return default
    try:
        config = template.evaluate(context or {})
        value = config.get_string(key)
        if value is None:
            return default
        value_str = str(value)
        return value_str if value_str != "" else default
    except Exception:
        return default


def _remote_config_bool(key: str, default: bool = False, context: dict[str, Any] | None = None) -> bool:
    template = _get_remote_config_template()
    if template is None:
        return default
    try:
        config = template.evaluate(context or {})
        return bool(config.get_boolean(key))
    except Exception:
        raw = _remote_config_param(key, "", context=context)
        if not raw:
            return default
        normalized = raw.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
        return default


def _yahoo_headers() -> dict[str, str]:
    # Yahoo endpoints frequently rate-limit requests without a browser-like UA.
    return {
        "User-Agent": "Mozilla/5.0 (Quantura; +https://quantura-e2e3d.web.app)",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }


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


def _normalize_email(value: Any) -> str:
    return str(value or "").strip().lower()


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


def _token_doc_id(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _active_notification_tokens_for_user(user_id: str) -> list[str]:
    docs = (
        db.collection("notification_tokens")
        .where("userId", "==", user_id)
        .where("active", "==", True)
        .limit(500)
        .stream()
    )
    tokens: list[str] = []
    for doc in docs:
        token_value = (doc.to_dict() or {}).get("token")
        if isinstance(token_value, str) and token_value:
            tokens.append(token_value)
    return list(dict.fromkeys(tokens))


def _send_push_tokens(tokens: list[str], title: str, body: str, data: dict[str, Any] | None = None) -> dict[str, Any]:
    tokens = [str(token).strip() for token in tokens if str(token).strip()]
    if not tokens:
        return {"successCount": 0, "failureCount": 0, "failed": [], "staleTokenHashes": []}

    payload_data = {str(key): str(value) for key, value in (data or {}).items() if value is not None}
    target = payload_data.get("url") or "/dashboard"
    link = target if target.startswith("http") else f"{PUBLIC_ORIGIN}{target}"
    icon = f"{PUBLIC_ORIGIN}/assets/quantura-icon.svg"
    message = admin_messaging.MulticastMessage(
        tokens=tokens[:500],
        notification=admin_messaging.Notification(title=title, body=body),
        data=payload_data,
        webpush=admin_messaging.WebpushConfig(
            notification=admin_messaging.WebpushNotification(
                title=title,
                body=body,
                icon=icon,
                badge=icon,
            ),
            fcm_options=admin_messaging.WebpushFCMOptions(link=link),
            headers={"Urgency": "high"},
        ),
    )

    try:
        response = admin_messaging.send_each_for_multicast(message)
    except Exception as exc:
        # Avoid surfacing an opaque "internal error" to the caller. We still audit via _notify_user().
        return {
            "successCount": 0,
            "failureCount": len(tokens[:500]),
            "failed": [{"tokenHash": _token_doc_id(t), "error": str(exc)} for t in tokens[:5]],
            "staleTokenHashes": [],
            "error": str(exc),
        }
    failed: list[dict[str, str]] = []
    stale_token_hashes: list[str] = []

    for idx, resp in enumerate(response.responses):
        if resp.success:
            continue
        token = tokens[idx]
        error_text = str(resp.exception or "unknown_error")
        failed.append({"tokenHash": _token_doc_id(token), "error": error_text})
        normalized = error_text.lower()
        if (
            "unregistered" in normalized
            or "registration-token-not-registered" in normalized
            or "invalid registration token" in normalized
            or "requested entity was not found" in normalized
        ):
            stale_token_hashes.append(_token_doc_id(token))

    return {
        "successCount": response.success_count,
        "failureCount": response.failure_count,
        "failed": failed,
        "staleTokenHashes": stale_token_hashes,
    }


def _notify_user(
    user_id: str,
    user_email: str | None,
    title: str,
    body: str,
    data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    tokens = _active_notification_tokens_for_user(user_id)
    result = _send_push_tokens(tokens, title=title, body=body, data=data)
    for stale_hash in result.get("staleTokenHashes", []):
        db.collection("notification_tokens").document(str(stale_hash)).set(
            {
                "active": False,
                "updatedAt": firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )
    _audit_event(
        user_id,
        user_email,
        "push_notification_attempted",
        {
            "title": title,
            "successCount": result.get("successCount", 0),
            "failureCount": result.get("failureCount", 0),
        },
    )
    return result


def _serialize_for_firestore(value: Any) -> Any:
    try:
        import numpy as np  # type: ignore

        if isinstance(value, (np.floating, np.integer)):
            return value.item()
        if isinstance(value, np.ndarray):
            return [_serialize_for_firestore(item) for item in value.tolist()]
    except Exception:
        pass

    try:
        import pandas as pd  # type: ignore

        if isinstance(value, pd.Timestamp):
            return value.isoformat()
    except Exception:
        pass

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, list):
        return [_serialize_for_firestore(item) for item in value]
    if isinstance(value, dict):
        return {key: _serialize_for_firestore(item) for key, item in value.items()}
    return value


def _trending_snapshots(tickers: list[str], max_rows: int = 18) -> dict[str, dict[str, Any]]:
    import pandas as pd  # type: ignore
    import yfinance as yf  # type: ignore

    tickers = [str(t).upper().strip() for t in (tickers or []) if str(t).strip()]
    tickers = list(dict.fromkeys([t for t in tickers if t]))[: max(1, int(max_rows or 18))]
    if not tickers:
        return {}

    symbols = " ".join(tickers)
    try:
        frame = yf.download(
            symbols,
            period="14d",
            interval="1d",
            group_by="ticker",
            progress=False,
            threads=True,
            auto_adjust=False,
        )
    except Exception:
        frame = pd.DataFrame()

    if frame.empty:
        return {}

    snapshots: dict[str, dict[str, Any]] = {}

    def _snap_from_close(series: pd.Series) -> dict[str, Any] | None:
        close = pd.to_numeric(series, errors="coerce").dropna()
        if close.empty:
            return None
        last = float(close.iloc[-1])
        prev = float(close.iloc[-2]) if len(close) >= 2 else None
        change = round(last - prev, 4) if prev else None
        change_pct = round((last - prev) / prev * 100.0, 4) if prev else None
        return {"lastClose": round(last, 4), "prevClose": round(prev, 4) if prev else None, "change": change, "changePct": change_pct}

    if isinstance(frame.columns, pd.MultiIndex):
        # shape: (field, ticker) or (ticker, field)
        level0 = list(frame.columns.get_level_values(0))
        is_ticker_level0 = any(t in set(level0) for t in tickers)
        for ticker in tickers:
            try:
                sub = frame[ticker] if is_ticker_level0 else frame.xs(ticker, axis=1, level=1)
            except Exception:
                continue
            if isinstance(sub.columns, pd.MultiIndex):
                sub.columns = sub.columns.get_level_values(-1)
            if "Close" not in sub.columns:
                continue
            snap = _snap_from_close(sub["Close"])
            if snap:
                snapshots[ticker] = snap
        return snapshots

    if "Close" in frame.columns and len(tickers) == 1:
        snap = _snap_from_close(frame["Close"])
        if snap:
            snapshots[tickers[0]] = snap
    return snapshots


def _parse_quantiles(raw: Any) -> list[float]:
    if raw is None:
        return [0.1, 0.5, 0.9]

    values: list[float] = []
    if isinstance(raw, str):
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        if not parts:
            raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Quantiles must be comma-delimited values between 0 and 1.")
        for part in parts:
            try:
                values.append(float(part))
            except Exception:
                raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, f"Invalid quantile value: {part}")
    elif isinstance(raw, (list, tuple)):
        if not raw:
            raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "At least one quantile is required.")
        for item in raw:
            try:
                values.append(float(item))
            except Exception:
                raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, f"Invalid quantile value: {item}")
    else:
        try:
            values = [float(raw)]
        except Exception:
            raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Invalid quantile value.")

    cleaned: list[float] = []
    seen: set[int] = set()
    for q in values:
        if not (0.0 < float(q) < 1.0):
            raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Quantiles must be between 0 and 1 (exclusive).")
        key = int(round(float(q) * 10000))
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(float(q))

    if len(cleaned) > 12:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Too many quantiles requested (max 12).")

    return cleaned


def _load_history(ticker: str, start: str | None, interval: str) -> pd.DataFrame:
    import pandas as pd  # type: ignore
    import yfinance as yf  # type: ignore

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


def _latest_close_prices(tickers: list[str]) -> dict[str, float]:
    import pandas as pd  # type: ignore
    import yfinance as yf  # type: ignore

    tickers = [str(t).upper().strip() for t in (tickers or []) if str(t).strip()]
    tickers = list(dict.fromkeys([t for t in tickers if t]))
    if not tickers:
        return {}

    symbols = " ".join(tickers)
    try:
        frame = yf.download(
            symbols,
            period="7d",
            interval="1d",
            group_by="ticker",
            progress=False,
            threads=True,
            auto_adjust=False,
        )
    except Exception:
        frame = pd.DataFrame()

    prices: dict[str, float] = {}
    if frame.empty:
        return prices

    if isinstance(frame.columns, pd.MultiIndex):
        # shape: (field, ticker) or (ticker, field) depending on yfinance version
        level0 = list(frame.columns.get_level_values(0))
        level1 = list(frame.columns.get_level_values(1))
        is_ticker_level0 = any(t in set(level0) for t in tickers)
        for ticker in tickers:
            try:
                sub = frame[ticker] if is_ticker_level0 else frame.xs(ticker, axis=1, level=1)
            except Exception:
                continue
            if isinstance(sub.columns, pd.MultiIndex):
                sub.columns = sub.columns.get_level_values(-1)
            if "Close" not in sub.columns:
                continue
            close = pd.to_numeric(sub["Close"], errors="coerce").dropna()
            if close.empty:
                continue
            prices[ticker] = float(close.iloc[-1])
        return prices

    if "Close" in frame.columns and len(tickers) == 1:
        close = pd.to_numeric(frame["Close"], errors="coerce").dropna()
        if not close.empty:
            prices[tickers[0]] = float(close.iloc[-1])
    return prices


def _generate_quantile_forecast(
    close_series: pd.Series,
    horizon: int,
    quantiles: list[float],
    interval: str,
) -> dict[str, Any]:
    import numpy as np  # type: ignore
    import pandas as pd  # type: ignore

    quantiles = sorted({float(q) for q in (quantiles or []) if 0 < float(q) < 1})
    if not quantiles:
        quantiles = [0.5]

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

    ref_q = min(quantiles, key=lambda q: abs(q - 0.5)) if quantiles else 0.5
    median_key = f"q{int(round(ref_q * 100)):02d}"
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

    import pandas as pd  # type: ignore

    forecast_core = _generate_quantile_forecast(close_series, horizon, quantiles, interval)
    try:
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
        quantiles = sorted({float(q) for q in (quantiles or []) if 0 < float(q) < 1})
        if not quantiles:
            quantiles = [0.5]

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
        forecast_core["serviceMessage"] = "Forecast generated with Meta Prophet, quantiles derived from model uncertainty."
        forecast_core["forecastRows"] = rows
        ref_q = min(quantiles, key=lambda q: abs(q - 0.5)) if quantiles else 0.5
        ref_key = f"q{int(round(ref_q * 100)):02d}"
        forecast_core["metrics"]["medianEnd"] = rows[-1].get(ref_key) if rows else forecast_core["metrics"].get("medianEnd")

        return forecast_core
    except Exception:
        # Prophet is best-effort: keep the product usable by returning the fallback model output.
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
    order_data = snapshot.to_dict() or {}

    update_payload: dict[str, Any] = {
        "status": status,
        "fulfillmentNotes": notes,
        "updatedAt": firestore.SERVER_TIMESTAMP,
    }

    if status == "fulfilled":
        update_payload["fulfilledAt"] = firestore.SERVER_TIMESTAMP

    order_ref.update(update_payload)
    _audit_event(req.auth.uid, token.get("email"), "order_status_updated", {"orderId": order_id, "status": status})

    order_user_id = order_data.get("userId")
    order_user_email = order_data.get("userEmail")
    if isinstance(order_user_id, str) and order_user_id:
        product = str(order_data.get("product") or "Deep Forecast")
        human_status = status.replace("_", " ").title()
        _notify_user(
            order_user_id,
            order_user_email if isinstance(order_user_email, str) else None,
            title="Quantura order update",
            body=f"{product} order {order_id} is now {human_status}.",
            data={
                "type": "order_status",
                "orderId": order_id,
                "status": status,
                "url": "/dashboard",
            },
        )

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
def create_collab_invite(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    to_email_raw = str(data.get("email") or "").strip()
    to_email_norm = _normalize_email(to_email_raw)
    if not to_email_norm or "@" not in to_email_norm:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Valid email is required.")

    role = str(data.get("role") or "viewer").strip().lower()
    if role not in {"viewer", "editor"}:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Role must be viewer or editor.")

    from_email = _normalize_email(token.get("email"))
    invite_doc = {
        "fromUserId": req.auth.uid,
        "fromEmail": from_email,
        "workspaceUserId": req.auth.uid,
        "workspaceEmail": from_email,
        "toEmail": to_email_raw,
        "toEmailNorm": to_email_norm,
        "toUserId": "",
        "role": role,
        "status": "pending",
        "createdAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
    }

    doc_ref = db.collection("collab_invites").document()
    doc_ref.set(invite_doc)
    _audit_event(
        req.auth.uid,
        token.get("email"),
        "collab_invite_created",
        {"inviteId": doc_ref.id, "toEmail": to_email_norm, "role": role},
    )
    return {"inviteId": doc_ref.id, "status": "pending"}


@https_fn.on_call()
def list_collab_invites(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    email_norm = _normalize_email(token.get("email"))
    if not email_norm:
        return {"invites": []}

    docs = (
        db.collection("collab_invites")
        .where("toEmailNorm", "==", email_norm)
        .limit(100)
        .stream()
    )

    invites: list[dict[str, Any]] = []
    for doc in docs:
        data = doc.to_dict() or {}
        if data.get("status") != "pending":
            continue
        invites.append(
            {
                "inviteId": doc.id,
                "fromEmail": data.get("fromEmail"),
                "workspaceUserId": data.get("workspaceUserId"),
                "workspaceEmail": data.get("workspaceEmail"),
                "role": data.get("role", "viewer"),
                "status": data.get("status", "pending"),
                "createdAt": data.get("createdAt"),
            }
        )

    return {"invites": _serialize_for_firestore(invites)}


@https_fn.on_call()
def accept_collab_invite(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    invite_id = str(data.get("inviteId") or "").strip()
    if not invite_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Invite ID is required.")

    email_norm = _normalize_email(token.get("email"))
    invite_ref = db.collection("collab_invites").document(invite_id)
    snapshot = invite_ref.get()
    if not snapshot.exists:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "Invite not found.")

    invite = snapshot.to_dict() or {}
    if invite.get("status") != "pending":
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.FAILED_PRECONDITION, "Invite is not pending.")
    if _normalize_email(invite.get("toEmailNorm")) != email_norm:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.PERMISSION_DENIED, "Invite is not addressed to you.")

    workspace_user_id = str(invite.get("workspaceUserId") or invite.get("fromUserId") or "").strip()
    workspace_email = _normalize_email(invite.get("workspaceEmail") or invite.get("fromEmail"))
    if not workspace_user_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INTERNAL, "Invite is missing workspace info.")

    role = str(invite.get("role") or "viewer").strip().lower()
    if role not in {"viewer", "editor"}:
        role = "viewer"

    collab_uid = req.auth.uid
    collab_email = _normalize_email(token.get("email"))

    batch = db.batch()
    batch.set(
        db.collection("users").document(workspace_user_id).collection("collaborators").document(collab_uid),
        {
            "userId": collab_uid,
            "email": collab_email,
            "role": role,
            "addedAt": firestore.SERVER_TIMESTAMP,
            "addedBy": workspace_user_id,
        },
        merge=True,
    )
    batch.set(
        db.collection("users").document(collab_uid).collection("shared_workspaces").document(workspace_user_id),
        {
            "workspaceUserId": workspace_user_id,
            "workspaceEmail": workspace_email,
            "role": role,
            "addedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )
    batch.update(
        invite_ref,
        {
            "status": "accepted",
            "toUserId": collab_uid,
            "acceptedAt": firestore.SERVER_TIMESTAMP,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
    )
    batch.commit()

    _audit_event(
        collab_uid,
        token.get("email"),
        "collab_invite_accepted",
        {"inviteId": invite_id, "workspaceUserId": workspace_user_id, "role": role},
    )
    return {"status": "accepted", "workspaceUserId": workspace_user_id, "workspaceEmail": workspace_email, "role": role}


@https_fn.on_call()
def revoke_collab_invite(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    invite_id = str(data.get("inviteId") or "").strip()
    if not invite_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Invite ID is required.")

    invite_ref = db.collection("collab_invites").document(invite_id)
    snapshot = invite_ref.get()
    if not snapshot.exists:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.NOT_FOUND, "Invite not found.")
    invite = snapshot.to_dict() or {}
    if invite.get("fromUserId") != req.auth.uid:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.PERMISSION_DENIED, "Only the inviter can revoke.")
    if invite.get("status") != "pending":
        return {"inviteId": invite_id, "status": invite.get("status")}

    invite_ref.update({"status": "revoked", "updatedAt": firestore.SERVER_TIMESTAMP})
    _audit_event(req.auth.uid, token.get("email"), "collab_invite_revoked", {"inviteId": invite_id})
    return {"inviteId": invite_id, "status": "revoked"}


@https_fn.on_call()
def list_collaborators(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    docs = db.collection("users").document(req.auth.uid).collection("collaborators").limit(200).stream()
    collaborators: list[dict[str, Any]] = []
    for doc in docs:
        data = doc.to_dict() or {}
        collaborators.append(
            {
                "userId": doc.id,
                "email": data.get("email"),
                "role": data.get("role", "viewer"),
                "addedAt": data.get("addedAt"),
            }
        )
    return {"collaborators": _serialize_for_firestore(collaborators)}


@https_fn.on_call()
def remove_collaborator(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    collaborator_user_id = str(data.get("collaboratorUserId") or "").strip()
    if not collaborator_user_id:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Collaborator user ID is required.")

    owner_id = req.auth.uid
    batch = db.batch()
    batch.delete(db.collection("users").document(owner_id).collection("collaborators").document(collaborator_user_id))
    batch.delete(db.collection("users").document(collaborator_user_id).collection("shared_workspaces").document(owner_id))
    batch.commit()
    _audit_event(owner_id, token.get("email"), "collaborator_removed", {"collaboratorUserId": collaborator_user_id})
    return {"status": "removed", "collaboratorUserId": collaborator_user_id}


@https_fn.on_call()
def get_web_push_config(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    meta = req.data.get("meta") if isinstance(req.data, dict) else {}
    context = _remote_config_context(req, token, meta if isinstance(meta, dict) else None)
    vapid_key = FCM_WEB_VAPID_KEY or _remote_config_param("webpush_vapid_key", "", context=context)
    if not vapid_key:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
            "FCM_WEB_VAPID_KEY is not configured.",
        )
    return {"vapidKey": vapid_key}


@https_fn.on_call()
def get_feature_flags(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    meta = req.data.get("meta") if isinstance(req.data, dict) else {}
    context = _remote_config_context(req, token, meta if isinstance(meta, dict) else None)
    return {
        "forecastProphetEnabled": _remote_config_bool("forecast_prophet_enabled", True, context=context),
        "forecastTimeMixerEnabled": _remote_config_bool("forecast_timemixer_enabled", True, context=context),
        "watchlistEnabled": _remote_config_bool("watchlist_enabled", True, context=context),
        "webPushVapidKey": _remote_config_param("webpush_vapid_key", "", context=context),
    }


@https_fn.on_call()
def register_notification_token(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    registration_token = str(data.get("token") or "").strip()
    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
    if len(registration_token) < 20:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.INVALID_ARGUMENT,
            "Valid notification token is required.",
        )

    doc_id = _token_doc_id(registration_token)
    doc_ref = db.collection("notification_tokens").document(doc_id)
    doc_ref.set(
        {
            "token": registration_token,
            "tokenHash": doc_id,
            "active": True,
            "userId": req.auth.uid,
            "userEmail": token.get("email"),
            "userAgent": str(meta.get("userAgent") or ""),
            "meta": meta,
            "forceRefresh": bool(data.get("forceRefresh")),
            "updatedAt": firestore.SERVER_TIMESTAMP,
            "lastSeenAt": firestore.SERVER_TIMESTAMP,
            "createdAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )

    _audit_event(req.auth.uid, token.get("email"), "notification_token_registered", {"tokenHash": doc_id})
    return {"tokenHash": doc_id, "active": True}


@https_fn.on_call()
def unregister_notification_token(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    registration_token = str(data.get("token") or "").strip()
    if len(registration_token) < 20:
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.INVALID_ARGUMENT,
            "Valid notification token is required.",
        )

    doc_id = _token_doc_id(registration_token)
    db.collection("notification_tokens").document(doc_id).set(
        {
            "active": False,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )
    _audit_event(req.auth.uid, token.get("email"), "notification_token_unregistered", {"tokenHash": doc_id})
    return {"tokenHash": doc_id, "active": False}


@https_fn.on_call()
def send_test_notification(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    title = str(data.get("title") or "Quantura notification test")
    body = str(data.get("body") or "Your web push notifications are active.")
    payload_data = data.get("data") if isinstance(data.get("data"), dict) else {}

    result = _notify_user(
        req.auth.uid,
        token.get("email"),
        title=title,
        body=body,
        data={
            **payload_data,
            "type": "test",
            "url": "/dashboard",
        },
    )
    return result


@https_fn.on_call()
def check_price_alerts(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    data = req.data or {}
    workspace_id = str(data.get("workspaceId") or req.auth.uid or "").strip()
    if not workspace_id:
        workspace_id = req.auth.uid

    # Allow scanning your own workspace, or a shared workspace you have access to.
    if workspace_id != req.auth.uid and token.get("email") != ADMIN_EMAIL:
        shared = (
            db.collection("users")
            .document(workspace_id)
            .collection("collaborators")
            .document(req.auth.uid)
            .get()
        )
        if not shared.exists:
            raise https_fn.HttpsError(
                https_fn.FunctionsErrorCode.PERMISSION_DENIED,
                "Workspace access required to scan alerts.",
            )

    alerts_ref = db.collection("users").document(workspace_id).collection("price_alerts")
    docs = (
        alerts_ref.where("createdByUid", "==", req.auth.uid)
        .where("active", "==", True)
        .limit(200)
        .stream()
    )
    alert_docs = [(doc, doc.to_dict() or {}) for doc in docs]
    if not alert_docs:
        return {"workspaceId": workspace_id, "checked": 0, "triggered": 0, "triggeredAlerts": []}

    alerts: list[dict[str, Any]] = []
    tickers: list[str] = []
    for doc, payload in alert_docs:
        ticker = str(payload.get("ticker") or "").upper().strip()
        if ticker:
            tickers.append(ticker)
        alerts.append({"ref": doc.reference, "id": doc.id, "data": payload, "ticker": ticker})

    price_map = _latest_close_prices(tickers)
    batch = db.batch()
    triggered: list[dict[str, Any]] = []

    for alert in alerts:
        ref = alert["ref"]
        payload = alert["data"] or {}
        ticker = alert["ticker"]
        condition = str(payload.get("condition") or "above").strip().lower()
        target = _safe_float(payload.get("targetPrice") or payload.get("target") or payload.get("price"))
        current = price_map.get(ticker)

        update_payload: dict[str, Any] = {
            "lastCheckedAt": firestore.SERVER_TIMESTAMP,
            "lastPrice": current,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        }

        fired = False
        if current is not None and target is not None:
            if condition == "below" and current <= target:
                fired = True
            if condition != "below" and current >= target:
                fired = True

        if fired:
            update_payload.update(
                {
                    "active": False,
                    "status": "triggered",
                    "triggeredAt": firestore.SERVER_TIMESTAMP,
                    "triggeredPrice": current,
                }
            )
            triggered.append(
                {
                    "alertId": alert["id"],
                    "workspaceId": workspace_id,
                    "ticker": ticker,
                    "condition": condition,
                    "targetPrice": target,
                    "currentPrice": current,
                }
            )
            event_ref = db.collection("price_alert_events").document()
            batch.set(
                event_ref,
                {
                    "alertId": alert["id"],
                    "workspaceId": workspace_id,
                    "userId": req.auth.uid,
                    "userEmail": token.get("email"),
                    "ticker": ticker,
                    "condition": condition,
                    "targetPrice": target,
                    "currentPrice": current,
                    "createdAt": firestore.SERVER_TIMESTAMP,
                    "meta": data.get("meta") or {},
                },
            )

        batch.set(ref, update_payload, merge=True)

    batch.commit()

    if triggered:
        lines = []
        for item in triggered[:6]:
            t = item.get("ticker") or "?"
            cond = ">= " if item.get("condition") != "below" else "<= "
            lines.append(f"{t} {cond}{item.get('targetPrice')} (now {item.get('currentPrice')})")
        body = "Triggered: " + "; ".join(lines)
        _notify_user(
            req.auth.uid,
            token.get("email"),
            title="Quantura price alert",
            body=body,
            data={"type": "price_alert", "count": len(triggered), "url": "/dashboard#watchlist"},
        )

    _audit_event(
        req.auth.uid,
        token.get("email"),
        "price_alerts_checked",
        {"workspaceId": workspace_id, "checked": len(alerts), "triggered": len(triggered)},
    )

    return {
        "workspaceId": workspace_id,
        "checked": len(alerts),
        "triggered": len(triggered),
        "triggeredAlerts": _serialize_for_firestore(triggered),
        "prices": _serialize_for_firestore(price_map),
    }


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
    context = _remote_config_context(req, token, data.get("meta") if isinstance(data.get("meta"), dict) else None)
    if service == "prophet" and not _remote_config_bool("forecast_prophet_enabled", True, context=context):
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
            "Prophet forecasting is currently disabled.",
        )
    if service == "ibm_timemixer" and not _remote_config_bool("forecast_timemixer_enabled", True, context=context):
        raise https_fn.HttpsError(
            https_fn.FunctionsErrorCode.FAILED_PRECONDITION,
            "IBM TimeMixer forecasting is currently disabled.",
        )

    interval = str(data.get("interval") or "1d")
    if interval not in {"1d", "1h"}:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Interval must be 1d or 1h.")

    horizon = int(data.get("horizon") or 90)
    quantiles = _parse_quantiles(data.get("quantiles"))
    start = data.get("start")

    history = _load_history(ticker=ticker, start=start, interval=interval)
    close_series = history["Close"].copy()

    try:
        result = _run_forecast_service(service, close_series, horizon, quantiles, interval)
    except Exception:
        result = _generate_quantile_forecast(close_series, horizon, quantiles, interval)
        result["serviceMessage"] = "Forecast service failed; fallback model executed."

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
    import pandas as pd  # type: ignore
    import yfinance as yf  # type: ignore

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
    tickers: list[str] = []
    try:
        response = requests.get(TRENDING_URL, headers=_yahoo_headers(), timeout=10)
        response.raise_for_status()
        data = response.json()
        quotes = data.get("finance", {}).get("result", [{}])[0].get("quotes", [])
        tickers = [item.get("symbol") for item in quotes if item.get("symbol")]
    except Exception:
        # Keep the UI functional even when Yahoo throttles.
        tickers = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "TSLA", "META"]

    tickers = [str(t).upper().strip() for t in (tickers or []) if str(t).strip()]
    tickers = list(dict.fromkeys(tickers))

    snapshots = _trending_snapshots(tickers, max_rows=18)
    items: list[dict[str, Any]] = []
    for symbol in tickers[:18]:
        snap = snapshots.get(symbol) or {}
        items.append({"symbol": symbol, **snap})

    return {"tickers": tickers, "items": _serialize_for_firestore(items)}


@https_fn.on_call()
def get_ticker_intel(req: https_fn.CallableRequest) -> dict[str, Any]:
    import yfinance as yf  # type: ignore

    data = req.data or {}
    ticker = str(data.get("ticker") or "").upper().strip()
    if not ticker:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Ticker is required.")

    ticker_obj = yf.Ticker(ticker)

    info: dict[str, Any] = {}
    try:
        info = ticker_obj.info or {}
        if not isinstance(info, dict):
            info = {}
    except Exception:
        info = {}

    summary = str(info.get("longBusinessSummary") or "").strip()
    if len(summary) > 900:
        summary = summary[:897].rstrip() + "..."

    profile = {
        "name": info.get("longName") or info.get("shortName") or "",
        "sector": info.get("sector") or "",
        "industry": info.get("industry") or "",
        "website": info.get("website") or "",
        "country": info.get("country") or "",
        "exchange": info.get("fullExchangeName") or info.get("exchange") or "",
        "currency": info.get("currency") or "",
        "summary": summary,
        "marketCap": info.get("marketCap"),
        "beta": info.get("beta"),
        "trailingPE": info.get("trailingPE"),
        "forwardPE": info.get("forwardPE"),
        "dividendYield": info.get("dividendYield"),
        "fiftyTwoWeekLow": info.get("fiftyTwoWeekLow"),
        "fiftyTwoWeekHigh": info.get("fiftyTwoWeekHigh"),
    }

    calendar_raw: dict[str, Any] = {}
    try:
        cal = ticker_obj.calendar or {}
        if isinstance(cal, dict):
            calendar_raw = cal
    except Exception:
        calendar_raw = {}

    events: list[dict[str, Any]] = []
    for key, value in (calendar_raw or {}).items():
        if value is None:
            continue
        events.append({"label": str(key), "value": _serialize_for_firestore(value)})

    analyst = {
        "recommendationKey": info.get("recommendationKey"),
        "recommendationMean": info.get("recommendationMean"),
        "analystOpinions": info.get("numberOfAnalystOpinions"),
        "targetLowPrice": info.get("targetLowPrice"),
        "targetMeanPrice": info.get("targetMeanPrice"),
        "targetHighPrice": info.get("targetHighPrice"),
    }

    recommendation_trend: list[dict[str, Any]] = []
    try:
        rec = ticker_obj.recommendations
        if rec is not None and getattr(rec, "empty", True) is False:
            rec_rows = rec.reset_index(drop=True).head(6)
            for _, row in rec_rows.iterrows():
                recommendation_trend.append(
                    {
                        "period": str(row.get("period") or ""),
                        "strongBuy": int(row.get("strongBuy") or 0),
                        "buy": int(row.get("buy") or 0),
                        "hold": int(row.get("hold") or 0),
                        "sell": int(row.get("sell") or 0),
                        "strongSell": int(row.get("strongSell") or 0),
                    }
                )
    except Exception:
        recommendation_trend = []

    return {
        "ticker": ticker,
        "profile": _serialize_for_firestore(profile),
        "events": _serialize_for_firestore(events),
        "analyst": _serialize_for_firestore(analyst),
        "recommendationTrend": _serialize_for_firestore(recommendation_trend),
    }


@https_fn.on_call()
def get_ticker_news(req: https_fn.CallableRequest) -> dict[str, Any]:
    data = req.data or {}
    ticker = str(data.get("ticker") or "").upper()
    if not ticker:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Ticker is required.")

    seen: set[str] = set()

    def _extract_thumbnail(item: dict[str, Any]) -> str:
        thumb = item.get("thumbnail")
        if isinstance(thumb, dict):
            resolutions = thumb.get("resolutions")
            if isinstance(resolutions, list) and resolutions:
                # Prefer the largest image available.
                best = None
                for res in resolutions:
                    if not isinstance(res, dict):
                        continue
                    url = res.get("url")
                    width = res.get("width") or 0
                    if not url:
                        continue
                    if best is None or int(width or 0) > int(best.get("width") or 0):
                        best = {"url": str(url), "width": int(width or 0)}
                if best and best.get("url"):
                    return str(best["url"])
        return ""

    def _append_item(item: dict[str, Any]) -> None:
        title = str(item.get("title") or "").strip()
        link = str(item.get("link") or item.get("url") or "").strip()
        if not title:
            return
        key = (link or title).strip().lower()
        if key in seen:
            return
        seen.add(key)
        publisher = str(item.get("publisher") or item.get("source") or "").strip()
        published = item.get("publishedAt") or item.get("providerPublishTime")
        summary = str(item.get("summary") or item.get("description") or "").strip()
        news_items.append(
            {
                "title": title,
                "publisher": publisher,
                "link": link,
                "publishedAt": published,
                "summary": summary,
                "thumbnailUrl": _extract_thumbnail(item),
            }
        )

    news_items: list[dict[str, Any]] = []
    # 1) Prefer Yahoo search news (more reliable than yfinance .news)
    try:
        resp = requests.get(
            YAHOO_SEARCH_URL,
            headers=_yahoo_headers(),
            params={"q": ticker, "newsCount": 12, "quotesCount": 0, "listsCount": 0},
            timeout=10,
        )
        if resp.status_code < 400:
            payload = resp.json() if resp.text else {}
            for item in (payload.get("news") or [])[:12]:
                if isinstance(item, dict):
                    _append_item(item)
    except Exception:
        pass

    # 2) Fallback: yfinance news
    if not news_items:
        try:
            import yfinance as yf  # type: ignore

            ticker_obj = yf.Ticker(ticker)
            for item in (ticker_obj.news or [])[:10]:
                if not isinstance(item, dict):
                    continue
                _append_item(item)
        except Exception:
            pass

    # 3) Optional: RSS fallback (lightweight parse)
    if not news_items:
        try:
            import xml.etree.ElementTree as ET

            rss_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
            rss = requests.get(rss_url, headers=_yahoo_headers(), timeout=10)
            if rss.status_code < 400 and rss.text:
                root = ET.fromstring(rss.text)
                for item in root.findall(".//item")[:10]:
                    title = (item.findtext("title") or "").strip()
                    link = (item.findtext("link") or "").strip()
                    pub_date = (item.findtext("pubDate") or "").strip()
                    if title:
                        key = (link or title).strip().lower()
                        if key in seen:
                            continue
                        seen.add(key)
                        news_items.append(
                            {
                                "title": title,
                                "publisher": "Yahoo Finance",
                                "link": link,
                                "publishedAt": pub_date,
                                "summary": "",
                                "thumbnailUrl": "",
                            }
                        )
        except Exception:
            pass

    return {"news": news_items}


@https_fn.on_call()
def get_options_chain(req: https_fn.CallableRequest) -> dict[str, Any]:
    import pandas as pd  # type: ignore
    import yfinance as yf  # type: ignore

    _require_auth(req)
    data = req.data or {}
    ticker = str(data.get("ticker") or "").upper().strip()
    expiration = str(data.get("expiration") or "").strip()
    limit = int(data.get("limit") or 24)
    limit = max(10, min(limit, 200))
    if not ticker:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Ticker is required.")

    ticker_obj = yf.Ticker(ticker)
    expirations = []
    try:
        expirations = list(ticker_obj.options or [])
    except Exception:
        expirations = []

    if not expirations:
        return {"ticker": ticker, "underlyingPrice": None, "expirations": [], "selectedExpiration": "", "calls": [], "puts": []}

    selected = expiration if expiration in expirations else expirations[0]

    underlying_price = None
    try:
        fast_info = getattr(ticker_obj, "fast_info", None) or {}
        underlying_price = _safe_float(fast_info.get("last_price") or fast_info.get("lastPrice"))
    except Exception:
        underlying_price = None
    if underlying_price is None:
        try:
            hist = ticker_obj.history(period="5d", interval="1d")
            if not hist.empty and "Close" in hist.columns:
                underlying_price = float(hist["Close"].dropna().iloc[-1])
        except Exception:
            underlying_price = None

    # Time to expiry in years (approx). If we cannot parse, probabilities will be null.
    T = None
    try:
        exp_dt = datetime.strptime(selected, "%Y-%m-%d").date()
        today = datetime.now(tz=timezone.utc).date()
        days = max((exp_dt - today).days, 0)
        T = days / 365.0
    except Exception:
        T = None

    normal = NormalDist()

    def _option_metrics(
        strike: float | None, iv: float | None, is_call: bool
    ) -> tuple[float, float, float, float] | None:
        if underlying_price is None or T is None or T <= 0:
            return None
        if strike is None or strike <= 0:
            return None
        if iv is None or iv <= 0:
            return None
        S = float(underlying_price)
        K = float(strike)
        sigma = float(iv)
        # Protect the probability math from extreme / bad IV values.
        if sigma > 5:
            return None
        denom = sigma * math.sqrt(T)
        if denom <= 0:
            return None
        d1 = (math.log(S / K) + (RISK_FREE_RATE + 0.5 * sigma * sigma) * T) / denom
        d2 = d1 - sigma * math.sqrt(T)
        p_call = normal.cdf(d2)
        prob_itm = p_call if is_call else normal.cdf(-d2)
        delta = normal.cdf(d1) if is_call else normal.cdf(d1) - 1.0
        return (prob_itm, delta, d1, d2)

    def _format_chain(df: pd.DataFrame, opt_type: str) -> list[dict[str, Any]]:
        if df is None or df.empty:
            return []
        frame = df.copy()
        if "strike" in frame.columns:
            frame["strike"] = pd.to_numeric(frame["strike"], errors="coerce")
        for col in ["openInterest", "volume"]:
            if col in frame.columns:
                frame[col] = pd.to_numeric(frame[col], errors="coerce").fillna(0)
        if "strike" in frame.columns:
            frame = frame.dropna(subset=["strike"])

        # Select a compact window around the underlying, then return results ordered by strike.
        if underlying_price is not None and "strike" in frame.columns:
            frame["distance"] = (frame["strike"] - float(underlying_price)).abs()
            sort_cols: list[str] = ["distance"]
            ascending: list[bool] = [True]
            if "openInterest" in frame.columns:
                sort_cols.append("openInterest")
                ascending.append(False)
            elif "volume" in frame.columns:
                sort_cols.append("volume")
                ascending.append(False)
            frame = frame.sort_values(by=sort_cols, ascending=ascending)
        elif "strike" in frame.columns:
            frame = frame.sort_values(by="strike", ascending=True)
        else:
            sort_col = "openInterest" if "openInterest" in frame.columns else ("volume" if "volume" in frame.columns else None)
            if sort_col:
                frame = frame.sort_values(by=sort_col, ascending=False)

        frame = frame.head(limit)
        if "strike" in frame.columns:
            frame = frame.sort_values(by="strike", ascending=True)

        items: list[dict[str, Any]] = []
        for _, row in frame.iterrows():
            strike = _safe_float(row.get("strike"))
            iv = _safe_float(row.get("impliedVolatility"))
            metrics = _option_metrics(strike, iv, is_call=(opt_type == "call"))
            p_itm = metrics[0] if metrics else None
            delta = metrics[1] if metrics else None
            bid = _safe_float(row.get("bid"))
            ask = _safe_float(row.get("ask"))
            mid = None
            if bid is not None and ask is not None and bid >= 0 and ask >= 0:
                mid = (bid + ask) / 2.0
            items.append(
                {
                    "contractSymbol": row.get("contractSymbol"),
                    "type": opt_type,
                    "strike": strike,
                    "lastPrice": _safe_float(row.get("lastPrice")),
                    "bid": bid,
                    "ask": ask,
                    "mid": None if mid is None else round(mid, 4),
                    "impliedVolatility": None if iv is None else round(iv, 4),
                    "delta": None if delta is None else round(delta, 4),
                    "volume": int(row.get("volume") or 0),
                    "openInterest": int(row.get("openInterest") or 0),
                    "inTheMoney": bool(row.get("inTheMoney")) if "inTheMoney" in row else None,
                    "probabilityITM": None if p_itm is None else round(p_itm * 100.0, 2),
                }
            )
        return items

    try:
        chain = ticker_obj.option_chain(selected)
        calls = _format_chain(chain.calls, "call")
        puts = _format_chain(chain.puts, "put")
    except Exception:
        calls = []
        puts = []

    return {
        "ticker": ticker,
        "underlyingPrice": underlying_price,
        "timeToExpiryYears": None if T is None else round(float(T), 6),
        "riskFreeRate": RISK_FREE_RATE,
        "expirations": expirations,
        "selectedExpiration": selected,
        "calls": _serialize_for_firestore(calls),
        "puts": _serialize_for_firestore(puts),
    }


@https_fn.on_call()
def get_technicals(req: https_fn.CallableRequest) -> dict[str, Any]:
    import pandas as pd  # type: ignore
    import yfinance as yf  # type: ignore
    from finta import TA  # type: ignore

    data = req.data or {}
    ticker = str(data.get("ticker") or "").upper()
    if not ticker:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Ticker is required.")

    interval = str(data.get("interval") or "1d")
    lookback = int(data.get("lookback") or 120)
    indicators = data.get("indicators") or ["RSI", "MACD"]
    include_series = bool(data.get("includeSeries"))
    max_points = int(data.get("maxPoints") or (240 if interval == "1h" else 260))
    max_points = max(30, min(max_points, 900))

    history = yf.download(ticker, period=f"{lookback}d", interval=interval, progress=False)
    if isinstance(history.columns, pd.MultiIndex):
        history.columns = history.columns.get_level_values(0)
    history = history.dropna()
    if history.empty:
        return {"latest": [], "series": {"dates": [], "items": []} if include_series else None}

    history = history.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )

    def _ta(name: str, frame: pd.DataFrame):
        func = getattr(TA, name, None)
        if not callable(func):
            return None
        return func(frame)

    latest_values = []
    series_items: list[dict[str, Any]] = []
    dates_index = history.index[-max_points:]
    dates_out = [pd.Timestamp(ts).isoformat() for ts in dates_index]
    indicator_map = {
        "RSI": lambda frame: _ta("RSI", frame),
        "MACD": lambda frame: TA.MACD(frame).get("MACD"),
        "SMA": lambda frame: TA.SMA(frame, 20),
        "EMA": lambda frame: TA.EMA(frame, 20),
        "BBANDS": lambda frame: _ta("BBANDS", frame),
        "ATR": lambda frame: TA.ATR(frame, 14),
        "ADX": lambda frame: _ta("ADX", frame),
        "CCI": lambda frame: _ta("CCI", frame),
        "MFI": lambda frame: _ta("MFI", frame),
        "OBV": lambda frame: _ta("OBV", frame),
        "ROC": lambda frame: _ta("ROC", frame),
        "STOCH": lambda frame: _ta("STOCH", frame),
        "WILLR": lambda frame: _ta("WILLIAMS", frame),
    }

    for indicator in indicators:
        func = indicator_map.get(indicator)
        if not func:
            continue
        try:
            series = func(history)
            if series is None or len(series) == 0:
                continue
            if indicator == "BBANDS" and isinstance(series, pd.DataFrame):
                for key, label in [("BB_UPPER", "BBANDS_UPPER"), ("BB_MIDDLE", "BBANDS_MIDDLE"), ("BB_LOWER", "BBANDS_LOWER")]:
                    band = series.get(key)
                    if band is None or len(band) == 0:
                        continue
                    value = band.dropna().iloc[-1]
                    latest_values.append({"name": label, "value": round(float(value), 4)})
                    if include_series:
                        aligned = band.reindex(dates_index)
                        series_items.append(
                            {
                                "name": label,
                                "values": [None if pd.isna(v) else round(float(v), 6) for v in aligned.tolist()],
                            }
                        )
                continue

            if isinstance(series, pd.DataFrame):
                series = series.iloc[:, 0]
            value = series.dropna().iloc[-1]
            latest_values.append({"name": indicator, "value": round(float(value), 4)})
            if include_series:
                aligned = series.reindex(dates_index)
                series_items.append(
                    {
                        "name": indicator,
                        "values": [None if pd.isna(v) else round(float(v), 6) for v in aligned.tolist()],
                    }
                )
        except Exception:
            continue

    out: dict[str, Any] = {"latest": latest_values}
    if include_series:
        out["series"] = {"dates": dates_out, "items": series_items}
    return out


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
def run_quick_screener(req: https_fn.CallableRequest) -> dict[str, Any]:
    import numpy as np  # type: ignore
    import pandas as pd  # type: ignore
    import yfinance as yf  # type: ignore
    from finta import TA  # type: ignore

    token = _require_auth(req)
    data = req.data or {}

    market = str(data.get("market") or "us").lower()
    max_names = int(data.get("maxNames") or 25)
    max_names = max(5, min(max_names, 50))

    trending: list[str] = []
    try:
        response = requests.get(TRENDING_URL, headers=_yahoo_headers(), timeout=10)
        response.raise_for_status()
        payload = response.json()
        quotes = payload.get("finance", {}).get("result", [{}])[0].get("quotes", [])
        trending = [str(item.get("symbol") or "").upper() for item in quotes if item.get("symbol")]
    except Exception:
        trending = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "TSLA", "META"]

    tickers = [t for t in trending if t][: max(10, min(len(trending), max_names * 2))]
    results: list[dict[str, Any]] = []

    if tickers:
        try:
            history = yf.download(
                " ".join(tickers),
                period="6mo",
                interval="1d",
                group_by="ticker",
                progress=False,
                threads=True,
                auto_adjust=False,
            )
        except Exception:
            history = pd.DataFrame()

        def _extract(history_frame: pd.DataFrame, symbol: str) -> pd.DataFrame:
            if history_frame.empty:
                return pd.DataFrame()
            if not isinstance(history_frame.columns, pd.MultiIndex):
                return history_frame.copy()
            if symbol in history_frame.columns.get_level_values(0):
                frame = history_frame[symbol].copy()
                if isinstance(frame.columns, pd.MultiIndex):
                    frame.columns = frame.columns.get_level_values(-1)
                return frame
            if symbol in history_frame.columns.get_level_values(1):
                frame = history_frame.xs(symbol, level=1, axis=1).copy()
                if isinstance(frame.columns, pd.MultiIndex):
                    frame.columns = frame.columns.get_level_values(-1)
                return frame
            return pd.DataFrame()

        for symbol in tickers:
            frame = _extract(history, symbol)
            if frame.empty or "Close" not in frame.columns:
                continue
            frame = frame.dropna()
            if frame.empty:
                continue

            close = frame["Close"].astype(float).dropna()
            if len(close) < 30:
                continue

            last_close = float(close.iloc[-1])
            ret_1m = (last_close / float(close.iloc[-21]) - 1.0) if len(close) >= 22 else None
            ret_3m = (last_close / float(close.iloc[-63]) - 1.0) if len(close) >= 64 else None

            returns = close.pct_change().dropna()
            vol_ann = float(returns.std() * np.sqrt(252)) if len(returns) else None

            rsi_val = None
            try:
                ta_frame = frame.rename(
                    columns={
                        "Open": "open",
                        "High": "high",
                        "Low": "low",
                        "Close": "close",
                        "Volume": "volume",
                    }
                )
                rsi_series = TA.RSI(ta_frame)
                if rsi_series is not None and len(rsi_series.dropna()) > 0:
                    rsi_val = float(rsi_series.dropna().iloc[-1])
            except Exception:
                rsi_val = None

            score = 0.0
            if isinstance(ret_3m, float):
                score += 0.65 * ret_3m
            if isinstance(ret_1m, float):
                score += 0.35 * ret_1m
            if isinstance(rsi_val, float):
                score += 0.05 * ((rsi_val - 50.0) / 50.0)

            results.append(
                {
                    "symbol": symbol,
                    "lastClose": round(last_close, 4),
                    "return1m": None if ret_1m is None else round(ret_1m * 100.0, 2),
                    "return3m": None if ret_3m is None else round(ret_3m * 100.0, 2),
                    "rsi14": None if rsi_val is None else round(rsi_val, 2),
                    "volatility": None if vol_ann is None else round(vol_ann, 4),
                    "score": round(score, 6),
                }
            )

    results.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
    results = results[:max_names]

    doc_ref = db.collection("screener_runs").document()
    run_doc = {
        "userId": req.auth.uid,
        "userEmail": token.get("email"),
        "market": market,
        "universe": str(data.get("universe") or "trending"),
        "maxNames": max_names,
        "status": "completed",
        "results": _serialize_for_firestore(results),
        "notes": str(data.get("notes") or ""),
        "createdAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
        "meta": data.get("meta") or {},
    }
    doc_ref.set(run_doc)
    _audit_event(req.auth.uid, token.get("email"), "screener_completed", {"runId": doc_ref.id, "count": len(results)})

    return {"runId": doc_ref.id, "status": "completed", "results": results}


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
def submit_feedback(req: https_fn.CallableRequest) -> dict[str, Any]:
    data = req.data or {}
    message = str(data.get("message") or "").strip()
    if not message:
        raise https_fn.HttpsError(https_fn.FunctionsErrorCode.INVALID_ARGUMENT, "Feedback message is required.")

    rating = data.get("rating")
    page_path = str(data.get("pagePath") or "").strip()
    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}

    user_id = req.auth.uid if req.auth else ""
    user_email = ""
    if req.auth and req.auth.token:
        user_email = str(req.auth.token.get("email") or "")
    if not user_email:
        user_email = str(data.get("email") or "").strip()

    doc_ref = db.collection("feedback").document()
    doc_ref.set(
        {
            "userId": user_id,
            "userEmail": user_email,
            "rating": rating,
            "message": message,
            "pagePath": page_path or str(meta.get("pagePath") or ""),
            "meta": meta,
            "createdAt": firestore.SERVER_TIMESTAMP,
        }
    )

    if user_id:
        _audit_event(user_id, user_email, "feedback_submitted", {"feedbackId": doc_ref.id})

    return {"feedbackId": doc_ref.id}


@https_fn.on_call()
def alpaca_get_options(req: https_fn.CallableRequest) -> dict[str, Any]:
    token = _require_auth(req)
    _require_admin(token)
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
    _require_admin(token)
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
    token = _require_auth(req)
    _require_admin(token)
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
    token = _require_auth(req)
    _require_admin(token)
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
    token = _require_auth(req)
    _require_admin(token)
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
    _require_admin(token)
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
