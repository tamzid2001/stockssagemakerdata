import pytest

from market_research.btc_bucket_report import aggregate, bankroll, capped_recovery_scenarios, price_buckets, recovery_cycles, streaks
from market_research.btc_sticky_tracking import taker_fee


def trade(price, pnl, at, quantity=1):
    return {
        "status": "closed",
        "entry_price": price,
        "quantity": quantity,
        "net_pnl": pnl,
        "entry_at": at,
        "outcome_confirmed_at": at + 10,
        "market_id": f"m-{at}",
        "exit_price": 1 if pnl > 0 else 0,
        "fees": (quantity if pnl > 0 else 0) - price * quantity - pnl,
    }


def test_one_cent_buckets_split_winners_and_losers():
    rows = [trade(.501, .4, 1), trade(.509, -.5, 2), trade(.51, .2, 3, 2)]
    buckets = price_buckets(rows)
    assert [(row["label"], row["wins"], row["losses"]) for row in buckets] == [
        ("50c-<51c", 1, 1),
        ("51c-<52c", 1, 0),
    ]
    assert buckets[0]["win_rate"] == .5
    assert buckets[1]["entry_notional"] == 1.02


def test_cent_boundary_float_noise_does_not_move_trade_to_lower_bucket():
    rows = [trade(.57999999999999996, .4, 1), trade(.5799, -.58, 2)]
    buckets = price_buckets(rows)
    assert [(r["entry_bucket_cents"], r["wins"], r["losses"]) for r in buckets] == [
        (57, 0, 1), (58, 1, 0)
    ]


def test_streak_and_recovery_cycle_statistics():
    rows = [
        trade(.5, -.5, 1),
        trade(.5, .2, 2),
        trade(.5, .4, 3),
        trade(.5, .1, 4),
        trade(.5, -.8, 5),
        trade(.5, -.2, 6),
        trade(.5, 1.1, 7),
    ]
    assert streaks(rows) == {"longest_winning_streak": 3, "longest_losing_streak": 2}
    recovery = recovery_cycles(rows)
    assert recovery["completed_cycles"] == 2
    assert recovery["wins_to_recover_mean"] == 1.5
    assert recovery["wins_to_recover_median"] == 1.5
    assert recovery["distribution"] == {1: 1, 2: 1}
    assert not recovery["open_unrecovered_cycle"]
    assert recovery["final_consecutive_wins_distribution"] == {1: 1, 2: 1}


def test_bankroll_accounts_for_locked_capital_and_settlement_receipts():
    first = trade(.6, .39, 1)
    second = trade(.7, -1.42, 2, quantity=2)
    result = bankroll([first, second])
    assert result["historical_minimum_initial_cash"] == 2.03
    assert result["maximum_single_entry_cash_with_fees"] == 1.42
    assert result["ending_cash_change"] == -1.03
    # With the second entry after the first settlement, proceeds are reusable.
    second["entry_at"] = 12
    second["outcome_confirmed_at"] = 20
    assert bankroll([first, second])["historical_minimum_initial_cash"] == 1.03
    # An entry sharing the settlement timestamp must fund itself before credit.
    second["entry_at"] = 11
    assert bankroll([first, second])["historical_minimum_initial_cash"] == 2.03


def test_bankroll_rejects_inconsistent_pnl():
    row = trade(.5, .49, 1)
    row["net_pnl"] = .9
    with pytest.raises(ValueError, match="TRADE_CASH_FLOW_MISMATCH"):
        bankroll([row])


def test_aggregate_warns_that_origins_overlap():
    scenarios = {
        1: {"trades": [trade(.9, .09, 1)]},
        2: {"trades": [trade(.9, -.91, 2)]},
    }
    result = aggregate(scenarios, "p90-" + "a" * 24, "report-" + "b" * 32)
    assert result["origins"][0]["forecast_minutes"] == 14
    assert result["combined_price_buckets"][0]["wins"] == 1
    assert result["combined_price_buckets"][0]["losses"] == 1
    assert "not independent" in result["combined_bucket_warning"]


def test_capped_recovery_uses_confirmed_outcomes_and_replays_baseline():
    prices = [0.6, 0.6, 0.6, 0.6, 0.6, 0.6]
    payouts = [0, 0, 0, 0, 1, 1]
    original_sizes = [1, 2, 5, 12, 30, 30]
    rows = []
    for index, (price, payout, size) in enumerate(zip(prices, payouts, original_sizes)):
        fee = taker_fee(size, price, "0.0001", 1)
        rows.append({"status": "closed", "market_id": f"btc-{index}",
                     "entry_at": 100 + index * 20, "exit_at": 110 + index * 20,
                     "outcome_confirmed_at": 110 + index * 20,
                     "entry_price": price, "exit_price": payout, "quantity": size,
                     "fees": fee, "gross_pnl": size * (payout - price),
                     "net_pnl": size * (payout - price) - fee,
                     "game_id": f"btc-{index}", "trade_id": f"trade-{index}"})
    scenarios = capped_recovery_scenarios(rows, {"fee_type": "quadratic", "multiplier": 1})
    assert [s["maximum_contracts_used"] for s in scenarios] == [2, 5, 12, 30]
    assert all(s["closed_trades"] == 6 for s in scenarios)
    assert [s["maximum_loss_escalations_per_recovery_cycle"] for s in scenarios] == [1, 2, 3, None]
    assert scenarios[3]["net_pnl"] == round(sum(r["net_pnl"] for r in rows), 6)


def test_capped_recovery_fails_if_original_ledger_does_not_replay():
    row = trade(.6, -.6, 1)
    row.update(game_id="m-1", trade_id="trade-1", exit_at=11, outcome_confirmed_at=11)
    with pytest.raises(ValueError, match="BASELINE_REPLAY_MISMATCH"):
        capped_recovery_scenarios([row], {"fee_type": "quadratic", "multiplier": 1})
