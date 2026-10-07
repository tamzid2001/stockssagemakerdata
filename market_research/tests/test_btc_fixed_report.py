import pytest
from market_research.btc_fixed_report import budget_quantity, opposite_replay

def fixture():
    market = "KXBTC15M-26SEP191600"
    trade = {"status":"closed","quantity":1,"fills":[{"timestamp":120}],"market_id":market,
             "game_id":"e","contract_id":market+":yes","entry_price":.82,"entry_at":125,
             "signal_received_at":65,"market_end":900,"trade_id":"original","exit_price":0,
             "exit_at":900,"outcome_confirmed_at":960}
    game = {"tape":[{"contract_id":market+":yes","timestamp":120,"received_at":125,"ask":.82,"bid":.80},
                    {"contract_id":market+":no","timestamp":120,"received_at":125,"ask":.20,"bid":.18}]}
    return {"trades":[trade]}, {market:game}

def replay(original, games, **kwargs):
    return opposite_replay(original,games,price_policy="recorded_opposite_ask",include_fees=False,
                           precision="0.0001",multiplier=1,**kwargs)

def test_opposite_ask_uses_bid_complement_and_inverts_outcome():
    original,games=fixture()
    r=replay(original,games)
    assert r["average_entry_cents"] == pytest.approx(20)
    assert r["average_contracts"] == 5
    assert r["wins"] == 1 and r["losses"] == 0
    assert r["gross_pnl_usd"] == 4
    assert r["net_pnl_usd"] == pytest.approx(3.944)
    assert r["historical_minimum_initial_cash"] == 1.06

def test_ideal_18_cent_price_is_separate_from_recorded_ask():
    original,games=fixture()
    r=opposite_replay(original,games,price_policy="ideal_one_minus_original_ask",include_fees=False,
                      precision="0.0001",multiplier=1)
    assert r["average_contracts"] == 5.55
    assert r["average_entry_cents"] == pytest.approx(18)
    assert r["entry_notional_usd"] == pytest.approx(.999)
    original["trades"][0]["exit_price"]=1
    losing=replay(original,games)
    assert losing["wins"] == 0 and losing["losses"] == 1
    assert losing["net_pnl_usd"] == pytest.approx(-1.056)

def test_one_dollar_all_in_budget_floors_without_overspend():
    original,games=fixture()
    r=opposite_replay(original,games,price_policy="recorded_opposite_ask",include_fees=True,
                      precision="0.0001",multiplier=1)
    assert r["average_contracts"] == 4.73
    assert r["total_cash_debited_usd"] <= 1
    assert budget_quantity(.18,"0.0001",1,False)==5.55

def test_modified_original_tape_fails_instead_of_replaying():
    original,games=fixture()
    games[original["trades"][0]["market_id"]]["tape"][0]["ask"] = .83
    with pytest.raises(ValueError,match="ORIGINAL_ENTRY_TAPE_MISMATCH"):
        replay(original,games)

def test_late_opposite_quote_is_not_an_entry():
    original,games=fixture()
    games[original["trades"][0]["market_id"]]["tape"][1]["received_at"] = 151
    r=replay(original,games)
    assert r["trades"] == 0 and r["missed_entries"] == 1

def test_binary_quote_integrity_is_checked():
    original,games=fixture()
    games[original["trades"][0]["market_id"]]["tape"][1]["ask"] = .18
    with pytest.raises(ValueError,match="BINARY_OPPOSITE_ASK_MISMATCH"):
        replay(original,games)
