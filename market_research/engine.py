"""Shared quote-based simulation for historical replay and prospective monitoring.

Display quotes are not trades or proof of liquidity. These results are paper
estimates only: entry at a later observed ask, exit at a later observed bid.
No interpolation is used to manufacture executions between observations.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
from typing import Callable

QUANTILES = (0.01, *tuple(i / 10 for i in range(1, 10)), 0.99)
VERSION = "quantura_quote_research_v6"
EXIT_LEVELS = (*tuple(str(i / 10) for i in range(2, 10)), "0.99")


def stamp(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp_timezone_required")
    return int(parsed.timestamp())


def iso(seconds: int) -> str:
    return (
        datetime.fromtimestamp(seconds, timezone.utc).isoformat().replace("+00:00", "Z")
    )


def digest(value) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
    ).hexdigest()


@dataclass(frozen=True)
class Quote:
    timestamp: int
    ask: float
    bid: float
    observed: bool = True

    def __post_init__(self):
        if (
            not all(math.isfinite(x) and 0 <= x <= 1 for x in (self.ask, self.bid))
            or self.bid > self.ask + 1e-6
        ):
            raise ValueError("invalid_quote")


@dataclass(frozen=True)
class Strategy:
    base_shares: float = 1
    loss_multiplier: float = 2.5
    max_shares: float = 100
    fee_per_contract: float = 0.01
    slippage: float = 0.005
    reset: str = "profitable_trade"
    entry: str = "cross_either"
    max_trades: int = 1000

    def __post_init__(self):
        for value in (
            self.base_shares,
            self.loss_multiplier,
            self.max_shares,
            self.fee_per_contract,
            self.slippage,
        ):
            if not math.isfinite(value) or value < 0:
                raise ValueError("invalid_strategy_number")
        if (
            not 0 < self.base_shares <= self.max_shares <= 10000
            or not 1 <= self.loss_multiplier <= 10
        ):
            raise ValueError("invalid_position_limits")
        if (
            self.slippage > 0.1
            or self.fee_per_contract > 0.1
            or not 1 <= self.max_trades <= 10000
        ):
            raise ValueError("invalid_cost_or_trade_limit")
        if self.reset not in {"profitable_trade", "cumulative_recovery"}:
            raise ValueError("invalid_reset")
        if self.entry not in {
            "cross_either",
            "cross_above",
            "cross_below",
            "touch",
            "at_or_above",
            "at_or_below",
        }:
            raise ValueError("invalid_trigger")


def crosses(
    previous: float, current: float, before: float, after: float, rule: str
) -> bool:
    up, down = (
        previous < before and current >= after,
        previous > before and current <= after,
    )
    return {
        "cross_either": up or down,
        "cross_above": up,
        "cross_below": down,
        "touch": current == after,
        "at_or_above": current >= after,
        "at_or_below": current <= after,
    }[rule]


def normalize_quotes(rows: list[dict], as_of: int) -> list[Quote]:
    """Last genuine quote in each COMPLETED minute, timestamped at minute end.

    NO ask is not 1-YES ask: public prices preserve spread. We derive each
    side's bid from the opposite side's actual ask, never from its own ask.
    """
    buckets = {}
    for row in sorted(rows, key=lambda r: stamp(r["timestamp"])):
        t = stamp(row["timestamp"])
        # Kalshi candles are already timestamped at the completed interval end.
        end = t if row.get("source") == "kalshi" else (t // 60 + 1) * 60
        if end > as_of or row.get("is_forward_filled"):
            continue
        raw = row.get("raw") or {}
        if row.get("source") == "kalshi":
            yes_ask = row.get("yes_ask_close", raw.get("yes_ask_close"))
            yes_bid = row.get("yes_bid_close", raw.get("yes_bid_close"))
            side = row.get("selected_position", raw.get("selected_position", "yes"))
            ask, bid = (None, None)
            if yes_ask is not None and yes_bid is not None:
                ask, bid = (1 - float(yes_bid), 1 - float(yes_ask)) if side == "no" else (yes_ask, yes_bid)
            if ask is not None and bid is not None:
                try:
                    buckets[end] = Quote(end, float(ask), float(bid))
                except (ValueError, TypeError):
                    pass
            continue
        # Normalized exports flatten provider fields; raw exports retain raw.
        long = row.get("long_price", raw.get("long_price"))
        short = row.get("short_price", raw.get("short_price"))
        side = row.get("selected_position", raw.get("selected_position", "long"))
        if long is None or short is None:
            continue
        ask, bid = (
            (float(short), 1 - float(long))
            if side == "short"
            else (float(long), 1 - float(short))
        )
        try:
            buckets[end] = Quote(end, ask, bid)
        except ValueError:
            continue
    return [buckets[t] for t in sorted(buckets)]


def history_window(
    quotes: list[Quote], origin: int, minutes: int = 500, minimum: int = 2
) -> list[Quote]:
    """Last 500 genuine observations through origin, preserving original times.

    Gaps are retained and disclosed by inference, never filled. Foundation
    model observation-step semantics are a limitation for irregular histories.
    """
    eligible = [
        q
        for q in quotes
        if q.observed and q.timestamp <= origin
    ]
    if not eligible or eligible[-1].timestamp != origin:
        raise ValueError("missing_origin_quote")
    window = eligible[-minutes:]
    if len(window) < minimum:
        raise ValueError("insufficient_observed_minute_history")
    return window


def history_quality(window: list[Quote]) -> dict:
    """Retain real repeated quotes; do not infer trades or create variation."""
    changes, run_start, longest = 0, 0, 0
    for i in range(1, len(window)):
        if abs(window[i].ask - window[i - 1].ask) > 1e-9:
            changes += 1
            run_start = i
        longest = max(longest, window[i].timestamp - window[run_start].timestamp)
    tail = (window[-1].timestamp - window[run_start].timestamp) / 60 if window else 0
    return {"methodology_version": "event_history_v2", "observations": len(window),
            "price_changes": changes, "longest_unchanged_minutes": longest / 60,
            "trailing_unchanged_minutes": tail, "repeated_prices_preserved": True,
            "forecast_blocked": len(window) >= 2 and (changes == 0 or (len(window) - run_start >= 30 and tail >= 120))}


def rolling_origins(quotes: list[Quote], horizon: int):
    """Start after two observed minutes; resume after gaps without invented bars."""
    if horizon not in {30, 60}:
        raise ValueError("horizon_must_be_30_or_60")
    due = 0
    for previous, current in zip(quotes, quotes[1:-1]):
        if (
            previous.observed
            and current.observed
            and current.timestamp >= due
        ):
            yield current.timestamp
            due = current.timestamp + horizon * 60


def validate_forecast(forecast: dict, origin: int, horizon: int) -> None:
    if forecast.get("origin") != origin or len(forecast.get("rows", [])) != horizon:
        raise ValueError("misaligned_forecast")
    for step, row in enumerate(forecast["rows"], 1):
        if row["timestamp"] != origin + step * 60:
            raise ValueError("misaligned_forecast_timestamp")
        values = [row["quantiles"][str(q)] for q in QUANTILES]
        if any(
            not math.isfinite(v) or not 0 <= v <= 1 for v in values
        ) or values != sorted(values):
            raise ValueError("invalid_or_crossing_quantiles")


def initial_state() -> dict:
    return {
        "levels": {
            level: {
                "size": 1.0,
                "recovery": 0.0,
                "position": None,
                "pending": None,
                "count": 0,
            }
            for level in EXIT_LEVELS
        },
        "last_timestamp": 0,
        "previous": None,
        "observations": [],
    }


def advance(state: dict, quote: Quote, forecast: dict, config: Strategy) -> list[dict]:
    """Idempotent one-minute state transition; nine independent TP experiments.

    Thresholds are frozen at signal time. Positions and pending limit orders
    expire at the issuing forecast's horizon, not reset on each new forecast.
    Different TP-level experiments must never be summed as one portfolio.
    """
    if not quote.observed:
        raise ValueError("IMPUTED_EXECUTION_QUOTE")
    if quote.timestamp <= state["last_timestamp"]:
        return []
    events = []
    previous = state["previous"]
    frows = {row["timestamp"]: row["quantiles"] for row in forecast.get("rows", [])}
    current_q = frows.get(quote.timestamp)
    previous_q = (
        (
            frows.get(previous["timestamp"])
            or (
                forecast["rows"][0]["quantiles"]
                if previous["timestamp"] == forecast.get("origin")
                and forecast.get("rows")
                else None
            )
        )
        if previous
        else None
    )
    # A live forecast is not available before inference + durable publication.
    signal_allowed = quote.timestamp > forecast.get(
        "available_at", forecast.get("origin", 0)
    )
    trigger = bool(
        signal_allowed
        and current_q
        and previous_q
        and previous
        and previous.get("observed", True)
        and quote.timestamp - previous["timestamp"] == 60
        and crosses(
            previous["ask"],
            quote.ask,
            previous_q["0.1"],
            current_q["0.1"],
            config.entry,
        )
    )
    trigger_direction = (
        "cross_above" if trigger and previous["ask"] < previous_q["0.1"] and quote.ask >= current_q["0.1"]
        else "cross_below" if trigger and previous["ask"] > previous_q["0.1"] and quote.ask <= current_q["0.1"]
        else config.entry
    )
    active = []
    for observation in state.setdefault("observations", []):
        reason = (
            "horizon"
            if quote.timestamp > observation["expires_at"]
            else "stop"
            if quote.bid <= observation["stop"]
            else "target"
            if quote.bid >= observation["target"]
            else "horizon"
            if quote.timestamp >= observation["expires_at"]
            else None
        )
        if reason:
            events.append(
                {
                    **observation,
                    "kind": "trigger_outcome",
                    "reason": reason,
                    "exited_at": quote.timestamp,
                    "paper_only": True,
                }
            )
        else:
            active.append(observation)
    state["observations"] = active
    if trigger:
        for level in state["levels"]:
            if current_q["0.01"] < current_q["0.1"] < current_q[level]:
                active.append(
                    {
                        "level": level,
                        "signal_at": quote.timestamp,
                        "trigger_direction": trigger_direction,
                        "stop": current_q["0.01"],
                        "target": current_q[level],
                        "forecast_id": forecast["forecast_id"],
                        "expires_at": forecast["rows"][-1]["timestamp"],
                    }
                )
    for level, lane in state["levels"].items():
        lane.setdefault("size", config.base_shares)
        if lane["count"] == 0 and not lane["position"] and not lane["pending"]:
            lane["size"] = config.base_shares
        position = lane["position"]
        if position and quote.timestamp > position["entered_at"]:
            # Stop first. A gap is marked at the observed bid, not the stop price.
            reason = (
                "horizon"
                if quote.timestamp > position["expires_at"]
                else "stop"
                if quote.bid <= position["stop"]
                else "target"
                if quote.bid >= position["target"]
                else "horizon"
                if quote.timestamp >= position["expires_at"]
                else None
            )
            if reason:
                exit_price = max(
                    0,
                    min(
                        quote.bid,
                        position["target"] if reason == "target" else quote.bid,
                    )
                    - config.slippage,
                )
                gross = (exit_price - position["entry"]) * position["size"]
                fees = 2 * config.fee_per_contract * position["size"]
                net = gross - fees
                lane["recovery"] = min(0, lane["recovery"] + net)
                increase = net < 0
                if increase:
                    lane["size"] = min(
                        config.max_shares, position["size"] * config.loss_multiplier
                    )
                elif (config.reset == "profitable_trade" and net > 0) or (
                    config.reset == "cumulative_recovery" and lane["recovery"] >= 0
                ):
                    lane["size"] = config.base_shares
                events.append(
                    {
                        **position,
                        "kind": "trade",
                        "level": level,
                        "exited_at": quote.timestamp,
                        "exit": exit_price,
                        "reason": reason,
                        "gross_pnl": gross,
                        "fees": fees,
                        "net_pnl": net,
                        "slippage_cost": 2 * config.slippage * position["size"],
                        "multiplier_increased": increase
                        and lane["size"] > position["size"],
                        "gap_seconds": quote.timestamp - state["last_timestamp"],
                        "paper_only": True,
                    }
                )
                lane["position"] = None
                lane["count"] += 1
                continue  # no re-entry on the closing observation
        pending = lane["pending"]
        if pending and quote.timestamp > pending["signal_at"]:
            if quote.timestamp >= pending["expires_at"]:
                lane["pending"] = None
            elif (
                quote.ask + config.slippage <= pending["limit"]
                and quote.bid > pending["stop"]
            ):
                lane["position"] = {
                    **pending,
                    "entry": quote.ask + config.slippage,
                    "entered_at": quote.timestamp,
                    "size": min(config.max_shares, lane["size"]),
                }
                lane["pending"] = None
        if lane["position"] or lane["pending"] or lane["count"] >= config.max_trades:
            continue
        if (
            not current_q
            or not previous_q
            or not previous
            or quote.timestamp - previous["timestamp"] != 60
        ):
            continue  # do not infer a crossing across missing quotes
        if trigger:
            stop, limit, target = current_q["0.01"], current_q["0.1"], current_q[level]
            if not stop < limit < target:
                continue
            lane["pending"] = {
                "signal_at": quote.timestamp,
                "trigger_direction": trigger_direction,
                "limit": limit,
                "stop": stop,
                "target": target,
                "forecast_id": forecast["forecast_id"],
                "forecast_origin": forecast["origin"],
                "expires_at": forecast["rows"][-1]["timestamp"],
            }
    state["previous"] = asdict(quote)
    state["last_timestamp"] = quote.timestamp
    return events


def direction_statistics(rows: list[dict], observations: list[dict]) -> dict:
    output = {}
    for direction in ("cross_above", "cross_below", "at_or_above", "at_or_below", "touch", "unrecorded"):
        group = [t for t in rows if t.get("trigger_direction", "unrecorded") == direction]
        triggers = [t for t in observations if t.get("trigger_direction", "unrecorded") == direction]
        targets = sum(t["reason"] == "target" for t in triggers)
        stops = sum(t["reason"] == "stop" for t in triggers)
        wins = sum(t["net_pnl"] > 0 for t in group)
        output[direction] = {
            "trades": len(group), "wins": wins,
            "win_rate": wins / len(group) if group else None,
            "net_pnl": sum(t["net_pnl"] for t in group),
            "target_hits": sum(t["reason"] == "target" for t in group),
            "stops": sum(t["reason"] == "stop" for t in group),
            "trigger_targets": targets, "trigger_stops": stops,
            "trigger_censored": len(triggers) - targets - stops,
            "target_before_stop_rate": targets / (targets + stops) if targets + stops else None,
        }
    return output


def summarize(trades: list[dict], exit_levels=EXIT_LEVELS) -> dict:
    result = {}
    for level in exit_levels:
        rows = [
            t
            for t in trades
            if t["level"] == level and t.get("kind", "trade") == "trade"
        ]
        observations = [
            t
            for t in trades
            if t["level"] == level and t.get("kind") == "trigger_outcome"
        ]
        otargets = sum(t["reason"] == "target" for t in observations)
        ostops = sum(t["reason"] == "stop" for t in observations)
        wins = [t["net_pnl"] for t in rows if t["net_pnl"] > 0]
        losses = [t["net_pnl"] for t in rows if t["net_pnl"] < 0]
        targets = sum(t["reason"] == "target" for t in rows)
        stops = sum(t["reason"] == "stop" for t in rows)
        # Aggregate chronologically, explicitly realized-only (not a portfolio NAV).
        equity = peak = drawdown = 0.0
        curve = []
        for trade in sorted(
            rows, key=lambda t: (t["exited_at"], t.get("contract_id", ""))
        ):
            equity += trade["net_pnl"]
            peak = max(peak, equity)
            drawdown = max(drawdown, peak - equity)
            curve.append({"timestamp": iso(trade["exited_at"]), "realized_pnl": equity})
        result[level] = {
            "trades": len(rows),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": len(wins) / len(rows) if rows else None,
            "target_hits": targets,
            "stops": stops,
            "timed_out": len(rows) - targets - stops,
            "target_before_stop_rate": targets / (targets + stops)
            if targets + stops
            else None,
            "gross_pnl": sum(t["gross_pnl"] for t in rows),
            "net_pnl": equity,
            "fees": sum(t["fees"] for t in rows),
            "slippage_cost": sum(t["slippage_cost"] for t in rows),
            "average_win": sum(wins) / len(wins) if wins else None,
            "average_loss": sum(losses) / len(losses) if losses else None,
            "profit_factor": sum(wins) / -sum(losses) if losses else None,
            "max_realized_drawdown": drawdown,
            "equity_curve": curve,
            "max_shares": max((t["size"] for t in rows), default=0),
            "average_shares": sum(t["size"] for t in rows) / len(rows)
            if rows
            else None,
            "average_entry": sum(t["entry"] for t in rows) / len(rows)
            if rows
            else None,
            "average_exit": sum(t["exit"] for t in rows) / len(rows) if rows else None,
            "trigger_outcomes": {
                "observed": len(observations),
                "targets": otargets,
                "stops": ostops,
                "censored_at_horizon": len(observations) - otargets - ostops,
                "target_before_stop_rate": otargets / (otargets + ostops)
                if otargets + ostops
                else None,
            },
            "multiplier_increases": sum(t["multiplier_increased"] for t in rows),
            "entry_direction": direction_statistics(rows, observations),
        }
    return {
        "paper_only": True,
        "method_version": VERSION,
        "levels_are_independent": True,
        "levels": result,
    }


def rolling_backtest(
    quotes: list[Quote],
    forecaster: Callable,
    horizon: int,
    config: Strategy,
    max_origins: int = 100,
) -> dict:
    if horizon not in {30, 60}:
        raise ValueError("horizon_must_be_30_or_60")
    state = initial_state()
    trades = []
    forecasts = []
    failures = []
    for origin in rolling_origins(quotes, horizon):
        if len(forecasts) + len(failures) >= max_origins:
            break
        try:
            window = history_window(quotes, origin)
            forecast = forecaster(window, horizon)
            validate_forecast(forecast, origin, horizon)
            forecasts.append(forecast)
            if state["last_timestamp"] == 0:
                state["previous"] = asdict(window[-1])
                state["last_timestamp"] = origin
        except (ValueError, RuntimeError):
            failures.append(
                {"origin": origin, "code": "HISTORY_OR_FORECAST_UNAVAILABLE"}
            )
            forecast = {"rows": []}
        for quote in quotes:
            if origin < quote.timestamp <= origin + horizon * 60:
                trades.extend(advance(state, quote, forecast, config))
    return {
        "forecasts": forecasts,
        "trades": trades,
        "state": state,
        "failures": failures,
        "summary": summarize(trades),
    }
