from copy import deepcopy
from decimal import Decimal

import pytest

from market_research.opposite_statistics import bootstrap, performance, record_settlement, log_lines


def entry(ticker, *, won, quantity="5", cost="1", fee=".05", started=100, settled=600, side="no"):
    net = (Decimal(quantity) if won else Decimal(0)) - Decimal(cost) - Decimal(fee)
    return dict(ticker=ticker, side=side, filled=quantity, cost=cost, fees=fee,
                net_pnl=str(net), status="settled", official_result=side if won else ("yes" if side == "no" else "no"),
                created_at=started, entry_at=started, settled_at=settled)


def state(groups):
    result = dict(entries=dict(groups), totals={})
    for group in result["entries"].values():
        for mode, e in group.items():
            t = result["totals"].setdefault(mode, dict(trades=0, wins=0, losses=0, net="0", fees="0"))
            t["trades"] += 1
            t["wins"] += int(Decimal(e["net_pnl"]) > 0)
            t["losses"] += int(Decimal(e["net_pnl"]) < 0)
            for key, field in (("net", "net_pnl"), ("fees", "fees")):
                t[key] = str(Decimal(t[key]) + Decimal(e[field]))
    return result


def test_net_after_fees_streaks_and_realized_drawdown_are_separate_for_live_and_paper():
    # -1.05, -1.05, +3.95, -1.05, +3.95 = +4.75; deepest loss from zero = 2.10.
    groups = [(str(i), {"live": entry(str(i), won=won, settled=600+i)})
              for i, won in enumerate([False, False, True, False, True])]
    groups[0][1]["paper"] = entry("0", won=True)
    s = state(groups)
    bootstrap(s, [], 1000)
    report = performance(s, [], 1000)
    live = report["live"]
    assert (live["closed_trades"], live["wins"], live["losses"], live["win_rate_pct"]) == (5, 2, 3, 40)
    assert live["net_pnl_usd"] == 4.75 and live["fees_usd"] == .25
    assert live["max_realized_drawdown_usd"] == 2.10
    assert (live["current_win_streak"], live["current_loss_streak"], live["max_win_streak"], live["max_loss_streak"]) == (1, 0, 1, 2)
    assert report["paper"]["net_pnl_usd"] == 3.95
    assert live["return_on_closed_entry_spend_pct"] == round(4.75 / 5.25 * 100, 6)
    assert "W/L=2/3" in log_lines("KXSOL15M", report)[1]


def test_archive_recovery_deduplicates_current_journal_and_orders_confirmed_settlements():
    first, second = entry("first", won=False, settled=600), entry("second", won=True, settled=700)
    s = state([("first", {"live": first}), ("second", {"live": second})])
    del s["entries"]["first"]
    bootstrap(s, [("second", {"live": deepcopy(second)}), ("first", {"live": first}), ("first", {"live": first})], 900)
    r = performance(s, [], 900)["live"]
    assert r["history_complete"] and r["closed_trades"] == r["reconstructed_closed_trades"] == 2
    assert r["current_win_streak"] == 1 and r["max_realized_drawdown_usd"] == 1.05
    before = deepcopy(s["statistics"]["modes"])
    bootstrap(s, [], 950)
    record_settlement(s, second, "live", 950)
    assert s["statistics"]["modes"] == before


def test_conflicting_immutable_settlement_evidence_is_never_silently_accepted():
    a, b = entry("one", won=False), entry("one", won=True)
    with pytest.raises(ValueError, match="ARCHIVE_CONFLICT"):
        bootstrap(state([]), [("one", {"live": a}), ("one", {"live": b})], 900)


def test_incomplete_history_preserves_known_lifetime_totals_but_does_not_invent_streaks_or_drawdown():
    s = state([("one", {"live": entry("one", won=True)})])
    s["entries"] = {}
    r = performance(s, [], 900)["live"]
    assert r["closed_trades"] == 1 and r["net_pnl_usd"] == 3.95 and r["win_rate_pct"] == 100
    assert not r["history_complete"] and r["reconstructed_closed_trades"] == 0
    assert r["max_realized_drawdown_usd"] is None and r["avg_entry_cents"] is None
    assert r["max_win_streak"] is None and r["return_on_closed_entry_spend_pct"] is None


def test_entry_price_uses_filled_cost_over_quantity_and_reports_both_average_methods():
    a = entry("a", won=True, quantity="4", cost="1", settled=600)
    a.pop("entry_at")
    a["fills"] = [dict(filled="0", reconciled_at=110), dict(filled="3", reconciled_at=120), dict(filled="1", reconciled_at=140)]
    b = entry("b", won=False, quantity="2", cost="1", settled=700)
    s = state([("a", {"live": a}), ("b", {"live": b})])
    r = performance(s, [], 900)["live"]
    assert r["avg_entry_cents"] == 37.5
    assert r["volume_weighted_entry_cents"] == round(100 / 3, 6)
    assert r["avg_contracts_per_trade"] == 3 and r["min_entry_cents"] == 25 and r["max_entry_cents"] == 50
    assert r["avg_trade_duration_seconds"] == 540
    assert r["duration_sources"] == {"live_first_fill_confirmation": 1, "paper_ask_receipt": 1}


def test_atomic_increment_and_repeated_settlement_cannot_double_count_after_restart():
    s = state([])
    bootstrap(s, [], 50)
    e = entry("new", won=True)
    incoming = state([("new", {"live": e})])
    s["entries"], s["totals"] = incoming["entries"], incoming["totals"]
    record_settlement(s, e, "live", 600)
    recovered = deepcopy(s)
    record_settlement(recovered, recovered["entries"]["new"]["live"], "live", 650)
    r = performance(recovered, [], 900)["live"]
    assert r["closed_trades"] == r["reconstructed_closed_trades"] == 1 and r["net_pnl_usd"] == 3.95


def test_unfilled_orders_are_not_trades_and_breakeven_is_not_a_win():
    e = entry("flat", won=True, quantity="1", cost=".95", fee=".05")
    s = state([("flat", {"paper": e})])
    s["entries"]["empty"] = {"live": dict(status="entering", filled="0", cost="0", fees="0", pending={"intent": {}}, attempt=3)}
    r = performance(s, [], 900)
    assert r["paper"]["breakevens"] == 1 and r["paper"]["wins"] == 0 and r["paper"]["losses"] == 0
    assert r["live"]["closed_trades"] == 0 and r["live"]["win_rate_pct"] is None
    assert r["live"]["pending_intents"] == 1 and r["live"]["unfilled_entries"] == 1


def test_open_bid_marks_and_sampled_equity_include_fees_and_do_not_change_realized_returns():
    s = state([])
    s["entries"]["open"] = {"live": dict(ticker="open", side="no", status="held", filled="5", cost="1", fees=".05", pending=None)}
    quote = dict(market_id="open", timestamp=600, received_at=605, collection_mode="live", no_bid=.18)
    r = performance(s, [quote], 610)["live"]
    assert r["net_pnl_usd"] == 0 and r["closed_trades"] == 0
    assert r["open_average_entry_cents"] == 20 and r["unrealized_bid_pnl_usd"] == -.15
    assert r["strategy_cash_pnl_usd"] == -1.05 and r["sampled_bid_equity_pnl_usd"] == -.15
    quote.update(timestamp=660, received_at=665, no_bid=.10)
    r = performance(s, [quote], 670)["live"]
    assert r["max_sampled_bid_equity_drawdown_usd"] == .4
    assert r["max_realized_drawdown_usd"] == 0


@pytest.mark.parametrize("kind", ["missing", "stale", "future_receipt", "historical"])
def test_missing_or_unusable_open_bid_does_not_become_a_zero_mark_or_new_equity_sample(kind):
    s = state([])
    s["entries"]["open"] = {"live": dict(ticker="open", side="no", status="held", filled="5", cost="1", fees=".05")}
    q = dict(market_id="open", timestamp=600, received_at=605, collection_mode="live", no_bid=.20)
    if kind == "stale": q["timestamp"] = 300
    if kind == "future_receipt": q["received_at"] = 800
    if kind == "historical": q["collection_mode"] = "historical"
    r = performance(s, [] if kind == "missing" else [q], 610)["live"]
    assert r["unmarked_positions"] == 1 and r["unrealized_bid_pnl_usd"] is None
    assert r["sampled_bid_equity_pnl_usd"] is None and r["bid_equity_samples"] == 0
