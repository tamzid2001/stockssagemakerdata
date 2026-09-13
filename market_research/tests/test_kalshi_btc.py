from dataclasses import asdict
from pathlib import Path
import pytest
import yaml
from market_research import kalshi_btc as btc
from market_research.engine import Quote, iso
from market_research.local_store import LocalStore

OPEN=120000
MARKET={'ticker':'KXBTC15M-TEST-00','event_ticker':'KXBTC15M-TEST','market_type':'binary',
        'open_time':iso(OPEN),'close_time':iso(OPEN+900)}


def candles():
    return [{'end_period_ts':OPEN+i*60,'yes_ask':{'close_dollars':str(.4+i*.01)},
             'yes_bid':{'close_dollars':str(.39+i*.01)}} for i in range(1,16)]


def test_exact_first_two_completed_bars_no_synthetic_or_later_replacement():
    rows=btc.KalshiBTCProvider.quotes(candles(),MARKET,OPEN+121)
    assert [q.timestamp for q in rows['yes']]==[OPEN+60,OPEN+120]
    assert rows['no'][0].ask==pytest.approx(1-rows['yes'][0].bid)
    assert rows['no'][0].bid==pytest.approx(1-rows['yes'][0].ask)
    assert len(btc.two_minute_window(rows['yes'],OPEN))==2
    with pytest.raises(ValueError):btc.two_minute_window([rows['yes'][0],Quote(OPEN+180,.5,.4)],OPEN)
    assert 'toto' not in btc.short_context_models()
    missing=candles();missing[0]['yes_bid']['close_dollars']=None
    assert len(btc.KalshiBTCProvider.quotes(missing,MARKET,OPEN+121)['yes'])==1


def test_direct_adapter_allowlist_and_historical_cutoff():
    provider=btc.KalshiBTCProvider()
    with pytest.raises(ValueError):provider.get('/portfolio/orders')
    with pytest.raises(ValueError):provider.get('/markets/../../secrets')
    calls=[]
    def get(path,params=None):
        calls.append((path,params))
        return {'market_settled_ts':iso(OPEN+1000)} if path=='/historical/cutoff' else {'ticker':MARKET['ticker'],'candlesticks':[]}
    provider.get=get
    provider.candles(MARKET,OPEN+900)
    assert calls[-1][0].startswith('/historical/markets/')
    assert calls[-1][1]['start_ts']==OPEN+60


def test_publication_latency_and_resume_no_duplicate_forecasts(tmp_path):
    class Provider(btc.KalshiBTCProvider):
        def candles(self,*args):return candles()
        def resolution(self,m,side):return {'contract_id':m['ticker']+':'+side,'resolution_status':'resolved','selected_side_won':side=='yes'}
    calls=[]
    def forecast(window,horizon,models,quantiles):
        calls.append((window,horizon,models))
        assert len(window)==2 and window[-1].timestamp==OPEN+120 and horizon==13
        return {'forecast_id':str(len(calls)),'origin':OPEN+120,'models':[{'id':m,'status':'completed'} for m in models],
                'rows':[{'timestamp':OPEN+(i+2)*60,'quantiles':{str(q):q for q in quantiles}} for i in range(1,14)]}
    store=LocalStore('test','holder',tmp_path);store.claim({'version':btc.VERSION})
    state=btc.process_market(store,Provider(),MARKET,OPEN+900,True,forecast)
    assert state['status']=='complete' and state['available_at']>OPEN+120
    btc.process_market(store,Provider(),MARKET,OPEN+900,True,forecast)
    assert len(calls)==2 and len(store.values('forecasts'))==2
    assert all(f['history_count']==2 for f in store.values('forecasts'))
    assert len(store.values('observations'))==26
    from market_research.p1_exports import export
    export(tmp_path)
    assert (tmp_path/'btc_forecast_quantiles.csv.gz').exists()
    assert (tmp_path/'quantile_path_report.json').exists()
    assert (tmp_path/'btc_signal_report.json').exists()
    store.db.close()


def test_late_live_start_skipped_and_interrupted_inference_not_backdated(tmp_path):
    store=LocalStore('test','holder',tmp_path);store.claim({})
    result=btc.process_market(store,btc.KalshiBTCProvider(),MARKET,OPEN+181)
    assert result['status']=='missed_start' and not store.values('forecasts')
    store.db.close()


def test_resolved_result_not_inferred_from_price():
    p=btc.KalshiBTCProvider()
    p.market=lambda t:{'ticker':t,'status':'active','result':'','yes_bid_dollars':'1.0000'}
    assert p.resolution(MARKET,'yes')['selected_side_won'] is None
    p.market=lambda t:{'ticker':t,'status':'finalized','result':'no'}
    assert p.resolution(MARKET,'no')['selected_side_won'] is True
    assert p.resolution(MARKET,'yes')['selected_side_won'] is False


def test_workflow_cpu_safe_ids_checkpoint_timeout_and_no_exchange_credentials():
    path=Path('.github/workflows/kalshi-btc-paper.yml')
    raw=path.read_text();data=yaml.safe_load(raw)
    assert data['jobs']['paper']['runs-on']=='ubuntu-latest'
    assert data['jobs']['paper']['timeout-minutes']==360
    assert 'QUANTURA_RESEARCH_DIR=$RUNNER_TEMP/btc-paper-output' in raw
    assert 'FIREBASE_SERVICE_ACCOUNT' not in raw and 'KALSHI_PRIVATE_KEY' not in raw
    assert 'restore-backtest' in raw and 'persist-credentials: false' in raw
    assert '--paper-btc' in raw and 'resume_artifact_id' in raw


def test_btc_p90_touch_buy_then_other_p90_or_held_p10_reverses_one_position():
    from market_research.tests.test_recovery_switch import forecasts, quote
    from market_research.recovery_switch import simulate
    rows = [quote(180,.81,bid=.8),quote(240,.9),quote(300,.6),quote(300,.85,side='no'),
            quote(360,.6),quote(360,.9,side='no'),quote(420,.2,side='no'),
            quote(480,.1,side='no'),quote(480,.91)]
    r = simulate(forecasts(),rows,{},as_of=480,switch_on_other_p90=True,p90_touch=True)
    assert [t['contract_id'] for t in r['trades']] == ['g-yes','g-no','g-yes']
    assert r['trades'][0]['signal_at'] == 180
    assert r['trades'][0]['exit_reason'] == 'opposite_p90_switch'
    assert r['trades'][1]['exit_reason'] == 'p10_exit_and_opposite'
    assert r['trades'][0]['exit_at'] == r['trades'][1]['entry_at']
    assert r['trades'][1]['exit_at'] == r['trades'][2]['entry_at']
    assert sum(t['status']=='open' for t in r['trades']) == 1
