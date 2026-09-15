import pytest
from market_research import kalshi_btc as btc
from market_research.tests.test_kalshi_btc import OPEN, MARKET, candles
from market_research.local_store import LocalStore
from ensemble_forecasting.worker import execute_job


def test_first_minute_exact_quote_no_repetition_and_exactly_two_forecasts(tmp_path):
    class Provider(btc.KalshiBTCProvider):
        def candles(self,*args):return candles()
        def resolution(self,m,s):return {'contract_id':m['ticker']+':'+s,'resolution_status':'pending_or_unverified'}
    calls=[]
    def forecast(window,horizon,models,quantiles,**kwargs):
        assert len(window)==1 and window[0].timestamp==OPEN+60
        assert horizon==14 and models==('granite','chronos','timesfm')
        assert kwargs=={'single_point_research':True}
        calls.append(window)
        return {'forecast_id':str(len(calls)),'origin':OPEN+60,'models':[{'id':m,'status':'completed'} for m in models],
                'rows':[{'timestamp':OPEN+(i+1)*60,'quantiles':{str(q):q for q in quantiles}} for i in range(1,15)]}
    store=LocalStore('test','test',tmp_path);store.claim({'version':btc.ONE_MINUTE_VERSION})
    result=btc.process_market(store,Provider(),MARKET,OPEN+900,True,forecast,history_minutes=1)
    btc.process_market(store,Provider(),MARKET,OPEN+900,True,forecast,history_minutes=1)
    assert len(calls)==2 and result['available_at']>OPEN+60
    assert all(f['history_count']==1 and f['strategy']==btc.ONE_MINUTE_VERSION for f in store.values('forecasts'))
    assert all(f['rows'][-1]['timestamp']==OPEN+900 for f in store.values('forecasts'))
    from market_research.p1_exports import export
    export(tmp_path)
    assert (tmp_path/'btc_signal_report.json').exists()
    store.db.close()


def test_missing_first_minute_not_replaced_with_second():
    quotes=btc.KalshiBTCProvider.quotes(candles()[1:],MARKET,OPEN+900)['yes']
    with pytest.raises(ValueError,match='FIRST_COMPLETED_MINUTE'):
        btc.first_minute_window(quotes,OPEN)


def test_one_point_research_does_not_relax_public_worker_defaults(monkeypatch):
    monkeypatch.setenv('TIMESFM_HF_ACCESS_APPROVED','true')
    monkeypatch.setenv('TIMESFM_COMMERCIAL_LICENSED','true')
    job={'request':{'models':{m:{'enabled':True,'weight':1} for m in ('granite','chronos','timesfm')},
         'quantiles':[.1,.5,.9],'prediction_length':14,'frequency':'1min','calendar':'NONE',
         'horizon_mode':'frequency_periods','failure_policy':'fail','transform':'none'},
         'input':{'rows':[{'timestamp':'2026-09-14T12:01:00Z','target':.4}],'frequency':'1min'}}
    with pytest.raises(ValueError):execute_job(job,mock=True,minimum_history_rows=1)
    result=execute_job(job,mock=True,minimum_history_rows=1,single_point_research=True)
    assert len(result['predictions'])==14 and len(result['models'])==3
    job['request']['models']['prophet']={'enabled':True,'weight':1}
    with pytest.raises(ValueError,match='SINGLE_POINT_RESEARCH'):execute_job(job,mock=True,minimum_history_rows=1,single_point_research=True)
