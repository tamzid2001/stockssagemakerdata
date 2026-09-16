import pytest
from market_research import btc_minute_archive as archive
from market_research import kalshi_btc as btc
from market_research.local_store import LocalStore
from market_research.tests.test_kalshi_btc import MARKET, OPEN, candles


@pytest.fixture
def store(tmp_path):
    value=LocalStore('test','test',tmp_path);value.claim({})
    yield value
    value.db.close()


def test_all_fifteen_paired_minutes_including_before_forecast(store):
    archive.archive_minutes(store,btc.KalshiBTCProvider(),MARKET,candles(),OPEN+905)
    rows=store.values('btc_minutes')
    assert len(rows)==15 and min(r['timestamp'] for r in rows)==OPEN+60
    assert all(r['no_ask']==pytest.approx(1-r['yes_bid']) for r in rows)
    assert sum(r['timely'] for r in rows)==1
    assert store.values('btc_minute_coverage')[0]['missing_timestamps']==[]


def test_missing_minutes_not_filled_and_revisions_preserve_first_receipt(store):
    raw=candles()[1:]
    archive.archive_minutes(store,btc.KalshiBTCProvider(),MARKET,raw,OPEN+905)
    original=sorted(store.values('btc_minutes'),key=lambda r:r['timestamp'])[0]
    raw[0]['yes_ask']['close_dollars']='0.8'
    archive.archive_minutes(store,btc.KalshiBTCProvider(),MARKET,raw,OPEN+965)
    assert original in store.values('btc_minutes')
    assert len(store.values('btc_minute_revisions'))==1
    assert store.values('btc_minute_coverage')[0]['missing_timestamps']==[OPEN+60]


def test_settlement_does_not_require_quotes_and_preserves_confirmation(store):
    settled={**MARKET,'status':'settled','result':'yes'}
    archive.archive_settlement(store,settled,OPEN+910)
    archive.archive_settlement(store,settled,OPEN+980)
    archive.archive_settlement(store,{**MARKET,'status':'closed'},OPEN+999)
    assert store.values('btc_lifecycle')[0]['first_confirmed_at']==OPEN+910
    assert store.values('btc_lifecycle')[0]['result']=='yes'
    archive.archive_settlement(store,{**settled,'result':'no'},OPEN+1000)
    assert len(store.values('btc_settlements'))==2


def test_collector_collects_despite_failed_forecast_and_resumes_pending(store,monkeypatch):
    class Provider(btc.KalshiBTCProvider):
        def get(self,path,params=None):
            if path.startswith('/series'):return {'series':{'fee_type':'quadratic','fee_multiplier':1}}
            return {'markets':[MARKET]}
        def candles(self,*args):return candles()
    monkeypatch.setattr(archive.time,'time',lambda:OPEN+125)
    store.checkpoint('btc:'+MARKET['ticker'],{'market':MARKET,'status':'failed'})
    collector=archive.MinuteCollector(store,Provider());collector.tick(OPEN+125)
    collector.tick(OPEN+140)
    assert len(store.values('btc_minutes'))==2
    assert store._get('checkpoints','btc_collector_health')['status']=='ok'
    assert MARKET['ticker'] in archive.MinuteCollector(store,Provider()).markets


def test_live_forecast_retry_keeps_input_and_never_backdates(store,monkeypatch):
    class Provider(btc.KalshiBTCProvider):
        def candles(self,*args):return candles()
    clock=[OPEN+125];monkeypatch.setattr(btc.time,'time',lambda:clock[0])
    calls=[]
    def forecast(window,horizon,models,quantiles,**options):
        assert options=={'btc_two_point_research':True}
        calls.append(window)
        if len(calls)==1:raise RuntimeError('transient')
        return {'forecast_id':str(len(calls)),'origin':OPEN+120,
            'models':[{'id':m,'status':'completed'} for m in models],
            'rows':[{'timestamp':OPEN+(i+2)*60,'quantiles':{str(q):q for q in quantiles}} for i in range(1,14)]}
    with pytest.raises(RuntimeError):btc.process_market(store,Provider(),MARKET,clock[0],False,forecast)
    assert store._get('checkpoints','btc:'+MARKET['ticker'])['status']=='retry_wait'
    clock[0]=OPEN+150
    result=btc.process_market(store,Provider(),MARKET,clock[0],False,forecast)
    assert result['attempts']==2 and result['available_at']==OPEN+150
    assert calls[0]==calls[1] and len(store.values('forecasts'))==2
    btc.process_market(store,Provider(),MARKET,OPEN+160,False,forecast)
    assert len(calls)==3


def test_permanent_validation_error_not_retried(store,monkeypatch):
    class Provider(btc.KalshiBTCProvider):
        def candles(self,*args):return candles()
    monkeypatch.setattr(btc.time,'time',lambda:OPEN+125)
    def fail(*args,**kwargs):raise ValueError('LICENSE_REQUIRED')
    with pytest.raises(ValueError):btc.process_market(store,Provider(),MARKET,OPEN+125,False,fail)
    assert store._get('checkpoints','btc:'+MARKET['ticker'])['status']=='failed'
    assert not store.values('forecasts')
