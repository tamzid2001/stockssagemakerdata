import pytest
from market_research.quantile_paths import simulate


def fixture():
    forecasts=[]
    for side in ('yes','no'):
        forecasts.append({'forecast_id':side,'origin':120,'available_at':150,
            'market_context':{'event_id':'game','market_id':'market','contract_id':side,'side':side},
            'rows':[{'timestamp':t,'quantiles':{'0.01':.2,'0.1':.3,'0.9':.8,'0.99':.95}} for t in (180,240,300)]})
    return forecasts


def quote(side,t,ask,bid):
    return {'contract_id':side,'timestamp':t,'ask':ask,'bid':bid,'observed':True}


def test_prospective_entries_count_failures_and_final_outcomes_separately():
    data=[quote('yes',120,.01,.01),quote('yes',180,.1,.09),quote('yes',240,.9,.85),quote('yes',300,.4,.39)]
    result=simulate(fixture(),data,{'yes':{'resolution_status':'resolved','selected_side_won':False}},300)
    p90=result['summary']['3m:below_p1->0.9:no_stop']
    assert p90['target_hits']==1 and p90['eventual_win_rate_after_hit']==0
    assert p90['net_win_rate']==1
    p99=result['summary']['3m:below_p1->0.99:no_stop']
    assert p99['target_hits']==0 and p99['closed_trades']==1
    assert all(e['entry_at']==180 for e in result['episodes'])


def test_exclusive_middle_stratum_and_inclusive_below_p10():
    data=[quote('yes',180,.25,.24),quote('yes',240,.97,.96)]
    result=simulate(fixture(),data,{},300)
    assert result['summary']['3m:below_p1->0.99:no_stop']['entries']==0
    assert result['summary']['3m:p1_to_p10->0.99:no_stop']['target_hits']==1
    assert result['summary']['3m:below_p10->0.99:no_stop']['target_hits']==1


def test_no_same_bar_target_or_opposing_side_cherry_picking():
    data=[quote('yes',180,.1,.09),quote('no',180,.1,.09),quote('yes',240,.99,.99)]
    r=simulate(fixture(),data,{},300)['summary']['3m:below_p1->0.9:no_stop']
    assert r['ambiguous_origins_excluded']==1 and r['entries']==0
    r=simulate(fixture(),[quote('yes',300,.1,.1)],{},300)['summary']['3m:below_p1->0.9:no_stop']
    assert r['closed_trades']==0 and r['open_or_censored']==1


def test_missing_end_censored_not_zero_return_and_retry_stable():
    data=[quote('yes',180,.1,.09),quote('yes',240,.08,.07)]
    r=simulate(fixture(),data,{},300)
    assert r==simulate(fixture(),data,{},300)
    s=r['summary']['3m:below_p1->0.9:no_stop']
    assert s['net_win_rate'] is None and s['open_or_censored']==1
    assert s['open_or_censored_mark_pnl']<0


def test_absolute_51_cent_stop_cannot_be_above_entry_and_gaps_not_filled_at_stop():
    data=[quote('yes',180,.1,.09),quote('yes',240,.99,.98)]
    r=simulate(fixture(),data,{},300,stop_price=.51)
    assert all(s['entries']==0 for s in r['summary'].values())
    forecasts=fixture()
    for f in forecasts:
        for row in f['rows']:
            row['quantiles'].update({'0.01':.7,'0.1':.75})
    data=[quote('yes',180,.6,.59),quote('yes',240,.5,.49),quote('yes',300,1.,.99)]
    result=simulate(forecasts,data,{},300,stop_price=.51)
    assert all(e['status']=='stop_loss' and e['exit_price']==.49 and e['net_pnl']<0 for e in result['episodes'])


def test_future_quote_never_used_and_missing_pair_excluded():
    data=[quote('yes',180,.1,.09),quote('yes',240,.99,.98)]
    r=simulate(fixture(),data,{},200)['summary']['3m:below_p1->0.9:no_stop']
    assert r['target_hits']==0 and r['closed_trades']==0
    assert not simulate(fixture()[:1],data,{},300)['episodes']


@pytest.mark.parametrize('value',[float('nan'),-1,1])
def test_bad_stop_rejected(value):
    with pytest.raises(ValueError):simulate([],[],{},0,stop_price=value)


def test_failed_model_is_visible_even_when_the_game_forecast_succeeds():
    from market_research.quantile_paths import model_participation
    forecasts=[{'models':[{'id':'prophet','status':'failed'},{'id':'chronos','status':'completed'},{'id':'granite','status':'completed'}]},
               {'models':[{'id':'prophet','status':'completed'},{'id':'chronos','status':'completed'}]}]
    result=model_participation(forecasts)
    assert result['component_runs']['prophet']=={'completed':1,'failed_or_unavailable':1}
    assert result['forecast_count_by_actual_participants']=={'chronos+granite':1,'chronos+prophet':1}
