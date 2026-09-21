from market_research.btc_bucket_report import aggregate, price_buckets, recovery_cycles, streaks


def trade(price, pnl, at, quantity=1):
    return {
        "status": "closed",
        "entry_price": price,
        "quantity": quantity,
        "net_pnl": pnl,
        "entry_at": at,
        "outcome_confirmed_at": at + 10,
        "market_id": f"m-{at}",
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
