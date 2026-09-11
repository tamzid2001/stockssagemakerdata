import pytest

from market_research.engine import (
    QUANTILES,
    EXIT_LEVELS,
    Quote,
    Strategy,
    advance,
    crosses,
    history_window,
    initial_state,
    iso,
    normalize_quotes,
    rolling_backtest,
    rolling_origins,
    summarize,
    validate_forecast,
)
from market_research.store import claim_transition
from market_research.worker import paired_contracts
from market_research.provider import QuanturaProvider


def curve(origin=600, horizon=30):
    values = {str(q): 0.1 + q * 0.8 for q in QUANTILES}
    return {
        "origin": origin,
        "forecast_id": "fixture-forecast",
        "rows": [
            {"timestamp": origin + i * 60, "quantiles": values.copy()}
            for i in range(1, horizon + 1)
        ],
    }


def seed():
    state = initial_state()
    state["previous"] = {"timestamp": 600, "ask": 0.3, "bid": 0.28}
    state["last_timestamp"] = 600
    return state


@pytest.mark.parametrize("count", [2, 3, 10, 40, 50, 120, 499, 500])
def test_shorter_pregame_and_ingame_history_is_accepted(count):
    quotes = [Quote(i * 60, 0.4, 0.38) for i in range(1, count + 1)]
    assert len(history_window(quotes, count * 60)) == count


def test_trailing_500_and_no_future():
    quotes = [Quote(i * 60, 0.4, 0.38) for i in range(1, 1001)]
    selected = history_window(quotes, 800 * 60)
    assert (
        len(selected) == 500
        and selected[0].timestamp == 301 * 60
        and selected[-1].timestamp == 800 * 60
    )


def test_missing_minutes_are_skipped_without_filling_or_compressing_time():
    quotes = [Quote(i * 60, 0.4, 0.38) for i in range(1, 100) if i != 70]
    window = history_window(quotes, 99 * 60)
    assert len(window) == 29 and all(q.observed for q in window)
    assert window[0].timestamp == 71 * 60
    with pytest.raises(ValueError, match="IMPUTED_EXECUTION"):
        advance(
            initial_state(), Quote(660, 0.4, 0.38, observed=False), curve(), Strategy()
        )


def test_long_gaps_restart_context_instead_of_unbounded_carry_forward():
    quotes = [Quote(i * 60, 0.4, 0.38) for i in range(1, 100) if not 60 <= i <= 70]
    assert len(history_window(quotes, 99 * 60)) == 29


def test_one_value_rejected_and_two_value_origin_does_not_wait_half_hour():
    with pytest.raises(ValueError, match="insufficient"):
        history_window([Quote(60, 0.4, 0.38)], 60)
    quotes = [Quote(i * 60, 0.4, 0.38) for i in range(1, 70)]
    assert list(rolling_origins(quotes, 30)) == [120, 1920, 3720]


def test_spread_and_completed_minute_integrity():
    rows = [
        {
            "timestamp": iso(65),
            "raw": {
                "long_price": 0.6,
                "short_price": 0.43,
                "selected_position": "short",
            },
        },
        {
            "timestamp": iso(125),
            "raw": {
                "long_price": 0.7,
                "short_price": 0.33,
                "selected_position": "short",
            },
        },
    ]
    quotes = normalize_quotes(rows, 120)
    assert quotes == [Quote(120, 0.43, 0.4)]


@pytest.mark.parametrize(
    "rule,previous,current,expected",
    [
        ("cross_either", 0.2, 0.1, True),
        ("cross_either", 0.1, 0.2, True),
        ("cross_above", 0.2, 0.1, False),
        ("cross_below", 0.1, 0.2, False),
        ("touch", 0.1, 0.15, True),
        ("at_or_below", 0.2, 0.1, True),
    ],
)
def test_crossing_rules(rule, previous, current, expected):
    assert crosses(previous, current, 0.15, 0.15, rule) == expected


def test_no_same_observation_fill_and_targets_are_independent():
    state = seed()
    forecast = curve()
    strategy = Strategy(slippage=0, fee_per_contract=0)
    assert advance(state, Quote(660, 0.17, 0.16), forecast, strategy) == []
    assert all(l["pending"] and not l["position"] for l in state["levels"].values())
    assert advance(state, Quote(720, 0.16, 0.15), forecast, strategy) == []
    events = advance(state, Quote(780, 0.35, 0.34), forecast, strategy)
    trades = [e for e in events if e["kind"] == "trade"]
    assert {t["level"] for t in trades} == {"0.2", "0.3"}
    assert all(t["entered_at"] > t["signal_at"] for t in trades)
    assert advance(state, Quote(780, 0.35, 0.34), forecast, strategy) == []
    assert summarize(events)["levels"]["0.2"]["wins"] == 1


def test_loss_multiplier_caps_and_stop_uses_observed_bid():
    state = seed()
    f = curve()
    s = Strategy(
        base_shares=2,
        loss_multiplier=2.5,
        max_shares=4,
        slippage=0,
        fee_per_contract=0.01,
    )
    advance(state, Quote(660, 0.17, 0.16), f, s)
    advance(state, Quote(720, 0.16, 0.15), f, s)
    events = advance(state, Quote(780, 0.09, 0.08), f, s)
    trades = [t for t in events if t["kind"] == "trade"]
    assert len(trades) == len(EXIT_LEVELS) == 9 and all(
        t["exit"] == 0.08 and t["size"] == 2 for t in trades
    )
    assert all(l["size"] == 4 for l in state["levels"].values())
    assert all(t["fees"] == 0.04 and t["net_pnl"] < t["gross_pnl"] for t in trades)


def test_publication_delay_prevents_retrospective_live_signals():
    state = seed()
    f = curve()
    f["available_at"] = 720
    advance(state, Quote(660, 0.17, 0.16), f, Strategy())
    assert not state["observations"] and all(
        not l["pending"] for l in state["levels"].values()
    )


@pytest.mark.parametrize("direction,previous,signal", [("cross_below", .3, .17), ("cross_above", .16, .19)])
def test_p99_and_crossing_direction_are_preserved_through_fills_and_scoring(direction, previous, signal):
    state = seed()
    state["previous"]["ask"] = previous
    f = curve()
    s = Strategy(slippage=0, fee_per_contract=0)
    advance(state, Quote(660, signal, signal-.01), f, s)
    assert state["levels"]["0.99"]["pending"]["trigger_direction"] == direction
    advance(state, Quote(720, .16, .15), f, s)
    events = advance(state, Quote(780, .95, .94), f, s)
    level = summarize(events)["levels"]["0.99"]
    assert level["trades"] == 1 and level["target_hits"] == 1
    split = level["entry_direction"][direction]
    assert split["trades"] == 1 and split["win_rate"] == 1
    assert split["target_before_stop_rate"] == 1
    assert level["entry_direction"]["unrecorded"]["trades"] == 0


def test_trigger_outcomes_do_not_claim_unfilled_orders_traded():
    state = seed()
    f = curve()
    s = Strategy(slippage=0)
    advance(state, Quote(660, 0.17, 0.16), f, s)
    events = advance(state, Quote(720, 0.9, 0.89), f, s)
    assert all(e["kind"] == "trigger_outcome" for e in events)
    result = summarize(events)["levels"]["0.2"]
    assert result["trades"] == 0 and result["trigger_outcomes"]["targets"] == 1


def test_horizon_censor_is_not_a_win_or_stop():
    state = seed()
    f = curve(horizon=2)
    s = Strategy(slippage=0)
    advance(state, Quote(660, 0.17, 0.16), f, s)
    events = advance(state, Quote(720, 0.2, 0.19), f, s)
    result = summarize(events)["levels"]["0.9"]
    assert result["trigger_outcomes"]["censored_at_horizon"] == 1
    assert result["trigger_outcomes"]["target_before_stop_rate"] is None


def test_gap_past_horizon_cannot_be_counted_as_target_hit():
    state = seed()
    f = curve(horizon=2)
    advance(state, Quote(660, 0.17, 0.16), f, Strategy(slippage=0))
    events = advance(state, Quote(780, 0.95, 0.94), f, Strategy(slippage=0))
    assert all(e["reason"] == "horizon" for e in events)


def test_backtest_forecaster_never_sees_revealed_future():
    quotes = [Quote(i * 60, 0.4 + i * 0.0001, 0.38 + i * 0.0001) for i in range(1, 140)]
    cutoffs = []

    def forecast(window, horizon):
        origin = window[-1].timestamp
        cutoffs.append(origin)
        assert all(q.timestamp <= origin for q in window)
        return curve(origin, horizon)

    result = rolling_backtest(quotes, forecast, 30, Strategy())
    assert cutoffs and all(f["origin"] in cutoffs for f in result["forecasts"])


def test_fenced_lease_rejects_duplicate_and_allows_expired_handoff():
    a = claim_transition({}, "a", 100)
    with pytest.raises(RuntimeError, match="LEASE_HELD"):
        claim_transition(a, "b", 150)
    b = claim_transition(a, "b", 221)
    assert b["fence"] > a["fence"] and b["holder"] == "b"


def test_only_paired_moneyline_sides():
    rows = [
        {"marketId": "a", "side": "long"},
        {"marketId": "a", "side": "short"},
        {"marketId": "b", "side": "long"},
    ]
    assert len(paired_contracts(rows)) == 2


def test_provider_cannot_address_orders_or_arbitrary_origins():
    with pytest.raises(ValueError):
        QuanturaProvider("http://127.0.0.1")
    with pytest.raises(ValueError):
        QuanturaProvider().request("/api/orders", {})


def test_forecast_crossing_validation():
    f = curve()
    f["rows"][0]["quantiles"]["0.01"] = 0.99
    with pytest.raises(ValueError, match="crossing"):
        validate_forecast(f, 600, 30)


@pytest.mark.parametrize(
    "update",
    [
        {"max_shares": 0},
        {"loss_multiplier": float("nan")},
        {"slippage": 1},
        {"reset": "random"},
    ],
)
def test_invalid_strategy(update):
    with pytest.raises(ValueError):
        Strategy(**update)
