import pytest
from market_research.recovery_switch import VERSION, simulate, statistics


def forecasts(game="g", origin=120, published=150, end=1500):
    return [{"forecast_id": f"{game}-{s}-{origin}", "strategy": VERSION, "origin": origin,
             "available_at": published, "game_start": 0, "expected_side_count": 2,
             "market_context": {"event_id": game, "market_id": game+"-binary", "contract_id": f"{game}-{s}"},
             "rows": [{"timestamp": t, "quantiles": {"0.01": .2, "0.1": .3, "0.9": .8, "0.99": .95}}
                      for t in range(origin+60, end+1, 60)]} for s in ("yes", "no")]


def quote(t, ask, bid=None, side="yes", game="g"):
    return {"timestamp": t, "contract_id": f"{game}-{side}", "ask": ask,
            "bid": ask-.01 if bid is None else bid, "observed": True}


def test_first_p90_cross_requires_no_low_quantile_and_fills_next_quote():
    rows = [quote(180, .5), quote(240, .9), quote(300, .91), quote(360, .5)]
    r = simulate(forecasts(), rows, {}, as_of=360)
    t = r["trades"][0]
    assert (t["signal_at"], t["entry_at"], t["entry_price"]) == (240, 300, .91)
    assert t["quantity"] == 1 and t["status"] == "open"  # No 51c stop.
    assert not simulate(forecasts(), rows, {}, as_of=240)["trades"]


def test_p10_exit_buys_opposite_without_its_p90_signal_and_caps_escalation():
    rows = [quote(180, .5), quote(240, .9), quote(300, .91),
            quote(360, .2), quote(420, .11), quote(420, .9, side="no"),
            quote(480, .2, side="no"), quote(540, .11, side="no"), quote(540, .9)]
    r = simulate(forecasts(), rows, {}, as_of=540, max_shares=3)
    assert [t["quantity"] for t in r["trades"]] == [1, 2.5, 3]
    assert [t["contract_id"] for t in r["trades"]] == ["g-yes", "g-no", "g-yes"]
    assert all(t["exit_reason"] == "p10_exit_and_opposite" for t in r["trades"][:2])
    assert r["trades"][1]["signal_p90"] is None
    s = next(iter(r["summary"].values()))
    assert s["losses"] == 2 and s["longest_losing_streak"] == 2 and s["multiplier_increases"] == 2
    assert r["trades"][0]["exit_at"] == r["trades"][1]["entry_at"]


def test_other_p90_signal_does_not_exit_held_position_above_p10():
    rows = [quote(180, .5), quote(240, .9), quote(300, .91),
            quote(360, .5, side="no"), quote(420, .9, side="no"), quote(480, .5), quote(480, .91, side="no")]
    r = simulate(forecasts(), rows, {}, as_of=480)
    assert len(r["trades"]) == 1 and r["trades"][0]["status"] == "open"


def test_no_forced_horizon_exit_future_period_p10_controls_exit():
    rows = [quote(180, .5), quote(240, .9), quote(300, .91), quote(360, .2),
            quote(420, .5), quote(480, .2), quote(540, .11), quote(540, .9, side="no")]
    fs = forecasts(end=300)
    assert simulate(fs, rows, {}, as_of=600)["trades"][0]["status"] == "open"
    r = simulate(fs + forecasts(origin=360, published=390), rows, {}, as_of=600)
    assert r["trades"][0]["exit_at"] == 540 and r["trades"][1]["quantity"] == 2.5


def test_settlement_is_terminal_payout_not_entry_or_forecast_information():
    rows = [quote(180, .5), quote(240, .9), quote(300, .91)]
    for payout in (0, 1):
        result = simulate(forecasts(), rows, {"g-yes": {"resolution_status": "resolved", "settled_at": 600, "selected_side_payout": payout}}, as_of=600)
        t = result["trades"][0]
        assert t["entry_at"] == 300 and t["exit_reason"] == "authoritative_settlement" and t["exit_price"] == payout
    assert not simulate(forecasts(), rows[:1], {}, as_of=600)["trades"]


def test_no_ambiguous_side_cherry_pick_or_stale_fill_or_revision_cross():
    rows = [quote(180, .5, side=s) for s in ("yes", "no")] + [quote(240, .9, side=s) for s in ("yes", "no")]
    assert not simulate(forecasts(), rows+[quote(300, .91)], {}, as_of=300)["trades"]
    assert not simulate(forecasts(), [quote(180, .5), quote(240, .9), quote(420, .9)], {}, as_of=420)["trades"]
    assert not simulate(forecasts()+forecasts(origin=180, published=210), [quote(180, .5), quote(240, .9), quote(300, .91)], {}, as_of=300)["trades"]


def test_sizing_is_per_game_and_strict_cross_not_touch():
    rows = [quote(180, .5), quote(240, .81, bid=.8), quote(300, .9), quote(360, .91)]
    rows += [quote(180, .5, game="h"), quote(240, .9, game="h"), quote(300, .91, game="h")]
    r = simulate(forecasts()+forecasts("h"), rows, {}, as_of=360)
    assert len(r["trades"]) == 2 and all(t["quantity"] == 1 for t in r["trades"])
    assert next(t for t in r["trades"] if t["game_id"] == "g")["signal_at"] == 300


def test_tick_events_and_missing_minutes_cannot_create_crossings():
    rows = [quote(180, .5), quote(241, .9), quote(300, .91), quote(360, .92)]
    assert not simulate(forecasts(), rows, {}, as_of=360)['trades']


def test_partial_profit_does_not_reset_until_cumulative_game_pnl_recovers():
    fs = forecasts()
    for f in fs:
        for r in f['rows']:
            if f['market_context']['contract_id'] == 'g-no' and r['timestamp'] >= 480:
                r['quantiles']['0.1'] = .5
            if f['market_context']['contract_id'] == 'g-yes' and r['timestamp'] >= 600:
                r['quantiles']['0.1'] = .6
    rows = [quote(180,.5),quote(240,.9),quote(300,.91),quote(360,.21,bid=.2),
            quote(420,.11,bid=.1),quote(420,.2,side='no'),quote(480,.41,bid=.4,side='no'),
            quote(540,.41,bid=.4,side='no'),quote(540,.2),quote(600,.51,bid=.5),
            quote(660,.51,bid=.5),quote(660,.2,side='no')]
    r = simulate(fs,rows,{},as_of=660)
    assert [t['quantity'] for t in r['trades']] == [1,2.5,6.25,1]
    partial = r['trades'][1]
    assert partial['net_pnl'] > 0 and partial['game_realized_net_pnl'] < 0
    assert partial['next_quantity'] == 6.25
    assert r['trades'][2]['game_realized_net_pnl'] > 0


def test_partial_pair_and_future_forecast_cannot_trade():
    rows = [quote(180, .5), quote(240, .9), quote(300, .91)]
    assert not simulate(forecasts()[:1], rows, {}, as_of=300)["trades"]
    assert not simulate(forecasts(published=301), rows, {}, as_of=300)["trades"]


def test_soccer_reversal_uses_paired_binary_side_not_unrelated_outcome():
    fs = []
    for outcome in ("home", "draw", "away"):
        for f in forecasts(outcome):
            f["market_context"]["event_id"] = "soccer"
            f["expected_side_count"] = 6
            fs.append(f)
    rows = [quote(180, .5, game="home"), quote(240, .9, game="home"), quote(300, .91, game="home"),
            quote(360, .2, game="home"), quote(420, .11, game="home"),
            quote(420, .9, side="no", game="home"), quote(420, .99, game="away")]
    trades = simulate(fs, rows, {}, as_of=420)["trades"]
    assert [t["contract_id"] for t in trades] == ["home-yes", "home-no"]


def test_streaks_returns_and_breakeven():
    trades = [{"trade_id": str(i), "status": "closed", "exit_at": i, "game_id": "g", "net_pnl": value,
               "gross_pnl": value+.01, "fees": .01, "entry_price": .5, "quantity": 1}
              for i, value in enumerate([1, 1, -1, -1, -1, 0, 1])]
    s = statistics(trades)
    assert (s["wins"], s["losses"], s["breakeven"]) == (3, 3, 1)
    assert (s["longest_winning_streak"], s["longest_losing_streak"]) == (2, 3)
    assert s["realized_equity_max_drawdown"] == 3 and s["net_pnl"] == 0


@pytest.mark.parametrize("settings", [{"multiplier": float("nan")}, {"multiplier": 0}, {"max_shares": 0}, {"fee_rate": -.1}])
def test_configuration_validation(settings):
    with pytest.raises(ValueError):
        simulate([], [], {}, as_of=0, **settings)
