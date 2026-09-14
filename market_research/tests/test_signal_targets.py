from copy import deepcopy

from market_research.signal_targets import study
from market_research.tests.test_recovery_switch import forecasts, quote


def test_buy_signal_freezes_original_terminal_p99_even_after_revision():
    fs=forecasts(end=600)
    revision=forecasts(origin=300,published=330,end=900)
    for f in revision:
        for row in f['rows']: row['quantiles']['0.99']=.99
    rows=[quote(180,.6,bid=.59),quote(240,.86,bid=.85),quote(300,.91,bid=.90),quote(360,.97,bid=.96)]
    r=study(fs+revision,rows,as_of=600)
    e=r['episodes'][0]
    assert e['target_price']==.95 and e['target_timestamp']==600
    assert e['signal_at']==240 and e['hit_at']==360 and e['minutes_to_hit']==2
    assert r['summary']['p90_buy_to_terminal_p99']['hit']==1
    assert r['summary']['p90_buy_to_terminal_p99']['hit_rate_completed_paths']==1


def test_sell_signal_tracks_frozen_terminal_p1_not_short_trade_profit():
    rows=[quote(180,.41,bid=.4),quote(240,.26,bid=.25),quote(300,.20,bid=.19)]
    r=study(forecasts(end=600),rows,as_of=600)
    e=r['episodes'][0]
    assert e['study']=='p10_sell_to_terminal_p1' and e['target_price']==.2
    assert e['status']=='hit' and e['hit_at']==300


def test_already_met_pending_censored_and_miss_are_separate():
    fs=forecasts(end=420)
    base=[quote(180,.6,bid=.59),quote(240,.86,bid=.85)]
    key='p90_buy_to_terminal_p99'
    assert study(fs,base,as_of=300)['summary'][key]['pending']==1
    censored=study(fs,base,as_of=420)['summary'][key]
    assert censored['censored_missing_minutes']==1 and censored['miss']==0
    assert censored['hit_rate_completed_paths'] is None
    assert censored['hit_rate_all_eligible_lower_bound']==0 and censored['hit_rate_all_eligible_upper_bound']==1
    complete=base+[quote(t,.91,bid=.9) for t in (300,360,420)]
    assert study(fs,complete,as_of=420)['summary'][key]['miss']==1
    beyond=study(fs,[base[0],quote(240,.97,bid=.96)],as_of=420)['summary'][key]
    assert beyond['already_met_at_signal']==1 and beyond['eligible_new_target_paths']==0


def test_stream_ticks_gaps_prepublication_or_partial_side_forecasts_do_not_signal():
    fs=forecasts(end=600)
    assert not study(fs,[quote(180,.6),quote(241,.9),quote(300,.97)],as_of=600)['episodes']
    assert not study(fs,[quote(180,.6),quote(300,.97)],as_of=600)['episodes']
    assert not study(fs[:1],[quote(180,.6),quote(240,.9)],as_of=600)['episodes']
    later=deepcopy(fs)
    for f in later:f['available_at']=240
    assert not study(later,[quote(180,.6),quote(240,.9)],as_of=600)['episodes']
