from market_research.btc_hold import compare
from market_research.tests.test_recovery_switch import forecasts, quote


def test_holding_never_switches_on_p10_or_uses_future_outcome_for_entry():
    rows=[quote(180,.9,bid=.89),quote(240,.91,bid=.90),quote(300,.2,bid=.19),
          quote(360,.2,bid=.19),quote(360,.9,side='no')]
    outcomes={'g-yes':{'resolution_status':'resolved','settled_at':600,'selected_side_payout':1}}
    r=compare(forecasts(end=600),rows,outcomes,as_of=600)
    assert len(r['trades'])==1 and r['trades'][0]['exit_at']==600
    assert r['trades'][0]['quantity']==1 and r['trades'][0]['fees']==.0091
    before=compare(forecasts(end=600),rows,outcomes,as_of=360)
    assert before['trades'][0]['status']=='open' and before['trades'][0]['entry_at']==240
