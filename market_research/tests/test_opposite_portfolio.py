import pytest
from market_research.opposite_portfolio_report import portfolio
from market_research.btc_fixed_report import opposite_replay
from market_research.opposite_strategies import DollarConfig, STRATEGIES

def trade(key,entry,settle,payout):
    return {"trade_id":key,"market_id":key,"contract_id":key+":yes","status":"closed",
            "entry_at":entry,"outcome_confirmed_at":settle,"entry_price":.5,"quantity":2,
            "exit_price":payout,"fees":.05,"net_pnl":2*payout-1.05}

def test_shared_cash_captures_overlapping_positions_and_settlement_delay():
    rows=[trade("a",100,300,0),trade("b",110,200,1)]
    games={t["market_id"]:{"tape":[{"contract_id":t["contract_id"],"timestamp":t["entry_at"],
             "received_at":t["entry_at"],"bid":.45}]} for t in rows}
    r=portfolio(rows,games)
    assert r["historical_minimum_initial_cash"]==2.1
    assert r["maximum_simultaneous_positions"]==2
    assert r["net_pnl_usd"]==pytest.approx(-.1)
    assert r["realized_max_drawdown_usd"]==pytest.approx(1.05)
    assert r["observed_bid_equity_max_drawdown_usd"]==pytest.approx(.9)

def test_funding_debits_before_simultaneous_settlement_credit():
    rows=[trade("a",100,200,1),trade("b",200,300,1)]
    games={t["market_id"]:{"tape":[{"contract_id":t["contract_id"],"timestamp":t["entry_at"],
             "received_at":t["entry_at"],"bid":.45}]} for t in rows}
    assert portfolio(rows,games)["historical_minimum_initial_cash"]==2.1

def test_selected_configuration_does_not_permit_btc_or_other_timings():
    assert {s:DollarConfig(s).history_minutes for s in STRATEGIES}==STRATEGIES
    with pytest.raises(ValueError):DollarConfig("KXBTC15M")
    with pytest.raises(ValueError):DollarConfig("KXSOL15M",entry_budget="2.00")

def test_duplicate_ledger_rows_are_rejected():
    row=trade("a",100,200,1)
    with pytest.raises(ValueError,match="DUPLICATE"):portfolio([row,row],{})
