from __future__ import annotations

import hmac
import os
from typing import Any, Callable

from flask import Flask, request

import main

app = Flask(__name__)

CALLABLE_FUNCTIONS = {
    "accept_collab_invite", "alpaca_cancel_order", "alpaca_get_account", "alpaca_get_options",
    "alpaca_get_positions", "alpaca_list_orders", "alpaca_place_order", "check_price_alerts",
    "confirm_native_iap_purchase", "confirm_stripe_checkout", "create_collab_invite",
    "create_creator_support_checkout", "create_order", "create_share_link",
    "create_stripe_billing_portal_session", "create_stripe_checkout_session",
    "create_stripe_connect_onboarding_link", "delete_autopilot_request", "delete_backtest",
    "delete_forecast_request", "delete_prediction_upload", "delete_screener_run", "download_price_csv",
    "generate_forecast_report_assets", "generate_social_campaign_drafts", "get_corporate_events_calendar",
    "get_feature_flags", "get_feature_vote_summary", "get_market_headlines_feed", "get_options_chain",
    "get_prediction_upload_csv", "get_technicals", "get_ticker_full_info", "get_ticker_history",
    "get_ticker_info_snapshot", "get_ticker_intel", "get_ticker_news", "get_ticker_x_trends",
    "get_trending_tickers", "get_unsplash_gallery", "get_web_push_config", "import_shared_item",
    "list_alerts_v2", "list_collab_invites", "list_collaborators", "list_social_campaigns",
    "list_social_queue", "publish_social_queue_now", "query_ticker_insight", "queue_autopilot_run",
    "queue_screener_run", "queue_social_campaign_posts", "register_device_v2",
    "register_notification_token", "remove_collaborator", "rename_backtest", "rename_prediction_upload",
    "rename_screener_run", "revoke_collab_invite", "run_backtest", "run_prediction_upload_agent",
    "run_prophet_forecast", "run_quick_screener", "run_timeseries_forecast",
    "schedule_social_autopilot_now", "send_slack_test_message", "send_test_notification",
    "set_screener_public_visibility", "submit_contact", "submit_feature_vote", "submit_feedback",
    "track_meta_conversion_event", "unregister_notification_token", "update_order_status",
    "upsert_ai_agent_social_action", "upsert_alert_v2", "upsert_watchlist_v2",
}

HTTP_FUNCTIONS = {
    "auth/exchange": "exchange_native_auth",
    "auth/exchange-native-id-token": "exchange_native_id_token",
    "predictions/search": "predictions_search_http",
    "predictions/orderbook": "predictions_orderbook_http",
    "openai/models": "api_openai_models",
    "model-council/models": "api_model_council_models",
    "model-council/improve-prompt": "api_model_council_improve_prompt",
    "model-council/query": "api_model_council_query",
    "model-council/feedback": "api_model_council_feedback",
    "model-council/share": "api_model_council_share",
    "ticker/modules": "api_ticker_modules",
    "massive/capabilities": "api_massive_capabilities",
    "massive/economy/treasury-yields": "api_massive_economy_treasury_yields",
    "massive/economy/inflation": "api_massive_economy_inflation",
    "massive/economy/inflation-expectations": "api_massive_economy_inflation_expectations",
    "massive/economy/labor-market": "api_massive_economy_labor_market",
    "massive/stocks/ipos": "api_massive_stocks_ipos",
    "massive/options/contracts": "api_massive_options_contracts",
    "chat": "api_chat",
    "stripe/webhook": "stripe_webhook",
}

SCHEDULED_FUNCTIONS = {
    "forecast-report": "forecast_report_agent_scheduler",
    "newsletter-daily": "newsletter_daily_scheduler",
    "social-dispatch": "social_dispatch_scheduler",
    "social-planner": "social_daily_planner_scheduler",
    "alert-tick": "alert_tick_scheduler",
}


def _dispatch(handler: Callable[[Any], Any]) -> Any:
    return app.make_response(handler(request))


def _cron_authorized() -> bool:
    expected = str(os.environ.get("CRON_SECRET") or "").strip()
    supplied = str(request.headers.get("Authorization") or "").strip()
    if supplied.lower().startswith("bearer "):
        supplied = supplied[7:].strip()
    if not supplied:
        supplied = str(request.headers.get("X-Cron-Secret") or "").strip()
    return bool(expected and supplied and hmac.compare_digest(expected, supplied))


@app.get("/api/health")
def health() -> dict[str, object]:
    return {"ok": True, "service": "quantura-legacy-compat"}


@app.route("/api/callable/<function_name>", methods=["POST", "OPTIONS"])
def callable_function(function_name: str) -> Any:
    if function_name not in CALLABLE_FUNCTIONS:
        return {"error": {"status": "NOT_FOUND", "message": "Callable function not found."}}, 404
    return _dispatch(getattr(main, function_name))


@app.route("/api/model-council/share/<path:share_path>", methods=["GET", "POST", "OPTIONS"])
def model_council_share(share_path: str) -> Any:
    del share_path
    return _dispatch(main.api_model_council_share)


@app.route("/api/<path:api_path>", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
def http_function(api_path: str) -> Any:
    handler_name = HTTP_FUNCTIONS.get(api_path)
    if not handler_name:
        return {"error": "not_found"}, 404
    return _dispatch(getattr(main, handler_name))


@app.route("/api/internal/cron/<job_name>", methods=["GET", "POST"])
def scheduled_function(job_name: str) -> Any:
    if not _cron_authorized():
        return {"ok": False, "error": "unauthorized"}, 401
    handler_name = SCHEDULED_FUNCTIONS.get(job_name)
    if not handler_name:
        return {"ok": False, "error": "job_not_found"}, 404
    # Firebase's scheduler decorator wraps each handler as a Flask endpoint and
    # expects request headers, even when invoked outside Cloud Scheduler.
    # Passing the active request preserves that contract on Vercel.
    return app.make_response(getattr(main, handler_name)(request))
