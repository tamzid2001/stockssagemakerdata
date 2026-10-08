from market_research.opposite_entry_audit import entry_rows

def value(source_bid=.20,p90=.18,initial=.80,fill=.80,side="no"):
    ticker="KXSOL15M-26OCT081100"
    return {"entries":{ticker:{"live":{"filled":"1.25","cost":str(fill*1.25),"side":side,
        "source_side":"yes","created_at":125,"status":"held",
        "signal":{"signal_at":60,"signal_bid":source_bid,"signal_p90":p90,"sticky":{"side":"yes"}}}}},
        "rows":[{"kind":"btc_minutes","value":{"market_id":ticker,"timestamp":120,"no_ask":initial}}]}

def test_high_opposite_entry_can_correctly_follow_a_low_p90():
    row=entry_rows(value())[0]
    assert row["opposite_side_verified"] and row["signal_crossed_p90"]
    assert (row["signal_bid_cents"],row["signal_p90_cents"],row["average_fill_cents"])==(20,18,80)
    assert row["classification"]=="signal_side_bid_at_or_below_50c"

def test_signal_to_next_minute_price_reversal_is_visible():
    row=entry_rows(value(.82,.80,.70,.75))[0]
    assert row["opposite_side_verified"] and row["signal_crossed_p90"]
    assert round(row["signal_complement_cents"],6)==18
    assert row["next_minute_opposite_ask_cents"]==70
    assert row["difference_from_signal_complement_cents"]==57
    assert row["classification"]=="opposite_ask_at_or_above_50c_next_minute"

def test_later_retry_fill_is_distinguished_from_the_initial_quote():
    row=entry_rows(value(.82,.80,.18,.80))[0]
    assert row["next_minute_opposite_ask_cents"]==18 and row["average_fill_cents"]==80
    assert row["classification"]=="fill_average_above_50c_after_next_minute_quote"

def test_wrong_side_cannot_be_reported_as_verified():
    row=entry_rows(value(side="yes"))[0]
    assert not row["opposite_side_verified"] and row["classification"]=="side_agreement_not_verified"

def test_immutable_archive_shape_keeps_the_same_signal_and_fill_evidence():
    current=value();ticker=next(iter(current["entries"]))
    archive={"ticker":ticker,"entries":current["entries"][ticker],"rows":current["rows"]}
    assert entry_rows(current)==entry_rows(archive)
