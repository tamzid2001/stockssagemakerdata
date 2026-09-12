"""P50 mean-reversion PAPER replay with crossing-driven ensemble refreshes.

The current saved curve, never the curve refitted using the crossing quote,
determines a signal. Entries/exits require subsequent observed ask/bid quotes.
Each crossing requests a new forecast; serial inference latency is carried into
simulated publication times. No simultaneous five-model loading or venue orders.
"""
from dataclasses import asdict
import math
import time

from .engine import crosses, history_window, normalize_quotes, validate_forecast
from .provider import historical_range

MEDIAN_VERSION = "median_cross_v1"


def initial_median_state(config):
    return {"last_timestamp": 0, "previous": None, "position": None,
            "pending_entry": None, "pending_exit": None, "size": config.base_shares,
            "recovery": 0.0, "count": 0, "forecast_attempts": 0,
            "active_forecast_id": None, "publications": [], "busy_until": 0,
            "crossings": 0, "expired_before_publication": 0}


def advance_median(state, quote, forecast, config):
    """One idempotent observation transition. Returns events and refresh reason.

    P01 is a standing protective stop frozen when the entry was signaled.
    P50 entry/exit limits also freeze at signal time; newer curves cannot rewrite
    an order. At the entry forecast deadline, mark an open position at the next
    observed bid. At end-of-data keep it censored/open, not a fabricated close.
    """
    if not quote.observed:
        raise ValueError("IMPUTED_EXECUTION_QUOTE")
    if quote.timestamp <= state["last_timestamp"]:
        return [], None
    rows = {r["timestamp"]: r["quantiles"] for r in (forecast or {}).get("rows", [])}
    current = rows.get(quote.timestamp)
    previous = state["previous"]
    before = rows.get(previous["timestamp"]) if previous else None
    if forecast and previous and previous["timestamp"] == forecast["origin"]:
        before = next(iter(rows.values()), None)
    available = bool(current and quote.timestamp >= forecast.get("available_at", forecast["origin"] + 60))
    crossing = bool(available and previous and before
                    and quote.timestamp - previous["timestamp"] == 60
                    and crosses(previous["ask"], quote.ask, before["0.5"], current["0.5"], "cross_either"))
    events = []
    if crossing:
        state["crossings"] += 1
        events.append({"kind": "median_crossing", "level": "0.5", "timestamp": quote.timestamp,
                       "direction": "cross_above" if previous["ask"] < before["0.5"] else "cross_below",
                       "forecast_id": forecast["forecast_id"], "paper_only": True})

    closed = False
    position = state["position"]
    if position and quote.timestamp > position["entered_at"]:
        exit_order = state["pending_exit"]
        # A gap cannot be credited as reaching a target before expiry. Stop
        # precedes a resting median sell when both might be actionable.
        reason = ("horizon" if quote.timestamp > position["expires_at"] else
                  "stop" if quote.bid <= position["stop"] else
                  "target" if exit_order and quote.timestamp > exit_order["signal_at"]
                  and quote.bid - config.slippage > exit_order["limit"] else
                  "horizon" if quote.timestamp == position["expires_at"] else None)
        if reason:
            exit_price = max(0, quote.bid - config.slippage)
            gross = (exit_price - position["entry"]) * position["size"]
            fees = 2 * config.fee_per_contract * position["size"]
            net = gross - fees
            state["recovery"] = min(0, state["recovery"] + net)
            if net < 0:
                state["size"] = min(config.max_shares, position["size"] * config.loss_multiplier)
            elif (config.reset == "profitable_trade" and net > 0) or (config.reset == "cumulative_recovery" and state["recovery"] >= 0):
                state["size"] = config.base_shares
            events.append({**position, "kind": "trade", "level": "0.5", "exited_at": quote.timestamp,
                           "exit": exit_price, "exit_signal_at": exit_order["signal_at"] if reason == "target" else None,
                           "exit_forecast_id": exit_order["forecast_id"] if reason == "target" else None,
                           "reason": reason, "gross_pnl": gross, "fees": fees, "net_pnl": net,
                           "slippage_cost": 2 * config.slippage * position["size"],
                           "multiplier_increased": state["size"] > position["size"],
                           "gap_seconds": quote.timestamp - state["last_timestamp"], "paper_only": True})
            state.update(position=None, pending_exit=None, count=state["count"] + 1)
            closed = True
        elif available and quote.bid > current["0.5"] and not exit_order:
            state["pending_exit"] = {"signal_at": quote.timestamp, "limit": current["0.5"],
                                     "forecast_id": forecast["forecast_id"]}

    pending = state["pending_entry"]
    if pending and quote.timestamp > pending["signal_at"]:
        if quote.timestamp >= pending["expires_at"] or quote.bid <= pending["stop"]:
            state["pending_entry"] = None
        elif quote.ask + config.slippage < pending["limit"]:
            state["position"] = {**pending, "entry": quote.ask + config.slippage,
                                 "entered_at": quote.timestamp, "size": min(config.max_shares, state["size"])}
            state["pending_entry"] = None
    if (not closed and not state["position"] and not state["pending_entry"]
            and state["count"] < config.max_trades and available
            and current["0.01"] < quote.bid <= quote.ask < current["0.5"]):
        state["pending_entry"] = {"signal_at": quote.timestamp, "limit": current["0.5"], "stop": current["0.01"],
                                  "expires_at": forecast["rows"][-1]["timestamp"], "forecast_origin": forecast["origin"],
                                  "forecast_id": forecast["forecast_id"], "trigger_direction": "at_or_below"}
    state["previous"] = asdict(quote)
    state["last_timestamp"] = quote.timestamp
    return events, "median_crossing" if crossing else None


def process_median_historical(store, provider, contract, now, horizon, strategy,
                              forecaster, max_origins, events, heartbeat, deadline):
    start, end = historical_range(contract, now)
    raw = store.archived_history(contract, start, end)
    cache_hit = raw is not None
    if raw is None:
        raw = provider.history(contract, start, end)
        store.archive_history(contract, start, end, raw)
    quotes = normalize_quotes(raw, end)
    if len(quotes) < 2:
        raise ValueError("TWO_GENUINE_OBSERVATIONS_REQUIRED")
    saved = store.load(contract["contractId"])
    state = saved.get("state") or initial_median_state(strategy)
    active = store._get("forecasts", state["active_forecast_id"]) if state["active_forecast_id"] else None
    successes = failed = 0
    for index, quote in enumerate(quotes):
        if quote.timestamp <= state["last_timestamp"]:
            continue
        if time.monotonic() >= deadline or store.at_capacity or state["forecast_attempts"] >= max_origins:
            break
        heartbeat.check()
        waiting = []
        for publication in state["publications"]:
            if publication["available_at"] > quote.timestamp:
                waiting.append(publication)
                continue
            published = store._get("forecasts", publication["forecast_id"])
            if published["rows"][-1]["timestamp"] < quote.timestamp:
                state["expired_before_publication"] += 1
                continue
            active = published
            state["active_forecast_id"] = active["forecast_id"]
        state["publications"] = waiting
        if active and active["rows"][-1]["timestamp"] < quote.timestamp:
            active = None
            state["active_forecast_id"] = None
        changes, refresh = advance_median(state, quote, active, strategy)
        if not active and not waiting:
            refresh = "initial_or_expired"
        new_forecast = None
        if refresh and index >= 1 and index < len(quotes) - 1:
            state["forecast_attempts"] += 1
            try:
                window = history_window(quotes[:index + 1], quote.timestamp)
                new_forecast = forecaster(window, horizon)
                validate_forecast(new_forecast, quote.timestamp, horizon)
                duration = float(new_forecast.get("duration_seconds", 0))
                if not math.isfinite(duration) or duration < 0:
                    raise ValueError("INVALID_INFERENCE_DURATION")
                # Simulated serial execution. Every detected crossing requests
                # inference; queued results are not magically available at T.
                available_at = max(quote.timestamp, state["busy_until"]) + max(60, math.ceil(duration / 60) * 60)
                new_forecast.update(mode="median_cross_historical_latency_simulation", available_at=available_at,
                                    refresh_reason=refresh, strategy_version=MEDIAN_VERSION)
                state["busy_until"] = available_at
                state["publications"].append({"forecast_id": new_forecast["forecast_id"], "available_at": available_at})
                successes += 1
            except (ValueError, RuntimeError) as error:
                new_forecast = None
                failed += 1
                changes.append({"kind": "forecast_failure", "level": "0.5", "timestamp": quote.timestamp,
                                "code": "ENSEMBLE_UNAVAILABLE", "type": type(error).__name__, "paper_only": True})
        tagged = [{**change, "contract_id": contract["contractId"], "market_id": contract["marketId"]} for change in changes]
        # One transaction commits the quote cursor, signals, trades and newly
        # generated immutable forecast before the next quote is revealed.
        store.save(contract["contractId"], state, new_forecast, tagged, contract)
        events.extend(tagged)
    bounded = state["forecast_attempts"] >= max_origins
    return {"complete": bounded or state["last_timestamp"] >= quotes[-1].timestamp,
            "completion_reason": "forecast_budget" if bounded else "history_end" if state["last_timestamp"] >= quotes[-1].timestamp else "checkpoint",
            "forecasts": successes, "failed_origins": failed, "observations": len(quotes),
            "archive_cache_hit": cache_hit, "open_positions": int(bool(state["position"])),
            "crossings": state["crossings"], "forecast_attempts": state["forecast_attempts"],
            "expired_before_publication": state["expired_before_publication"]}
