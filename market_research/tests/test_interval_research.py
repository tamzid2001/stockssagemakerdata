from copy import deepcopy
from pathlib import Path
import pytest
import yaml

from market_research import interval_studies as study, interval_markets as markets
from market_research import interval_collector as collector
from market_research.btc_minute_archive import archive_minutes, archive_settlement, MinuteCollector
from market_research.engine import Quote, iso
from market_research.local_store import LocalStore
from market_research.tests.test_btc_horizons import Archive

OPEN = 1_788_000_000 // 900 * 900
MARKET = {'ticker':'KXETH15M-TEST','event_ticker':'KXETH15M-TEST','market_type':'binary',
          'open_time':iso(OPEN),'close_time':iso(OPEN+900),'status':'settled','result':'yes'}


class Provider(markets.KalshiIntervalProvider):
    def __init__(self): super().__init__('KXETH15M'); self.missing=None
    def candles(self, market, end):
        return [{'end_period_ts':OPEN+i*60,'yes_bid':{'close_dollars':'0.49'},
                 'yes_ask':{'close_dollars':'0.51'}} for i in range(1,16) if i != self.missing]
    def market(self, ticker): return MARKET


def model(window, horizon, models, quantiles, **kwargs):
    assert 'toto' not in models
    assert kwargs == ({'single_point_research':True,'failure_policy':'fail'} if len(window)==1 else {'failure_policy':'fail'})
    return {'origin':window[-1].timestamp,'forecast_id':'only-test','duration_seconds':.1,
        'models':[{'id':m,'status':'completed'} for m in models],
        'weights':{str(q):{m:1/len(models) for m in models} for q in quantiles},'failures':[],
        'rows':[{'timestamp':window[-1].timestamp+i*60, 'quantiles':{str(q):q for q in quantiles}}
                for i in range(1,horizon+1)]}


@pytest.mark.parametrize('n',range(1,13))
def test_every_origin_exact_prefix_flat_context_strict_models_and_resume(n):
    config=study.configuration('KXETH15M', OPEN+1800, 2, 'a'*40)
    p=Provider();archive=Archive();source=study.source_for(MARKET, archive, p);calls=[]
    def fit(window,*args,**kw):calls.append(window);return model(window,*args,**kw)
    r=study.forecast_origin(MARKET,n,source,config,archive,p,fit)
    assert len(calls)==2 and all(len(w)==n for w in calls)
    assert all(w[0].timestamp==OPEN+60 and w[-1].timestamp==OPEN+n*60 for w in calls)
    assert all(f['rows'][-1]['timestamp']==OPEN+900 and len(f['rows'])==15-n for f in r['forecasts'])
    assert len(r['forecasts'][0]['models'])==(3 if n==1 else 4)
    assert r['forecasts'][0]['available_at']>OPEN+n*60
    assert study.forecast_origin(MARKET,n,source,config,archive,p,lambda *a,**k:pytest.fail('duplicate'))==r


def test_missing_minute_does_not_shift_window():
    with pytest.raises(ValueError,match='MISSING_FIRST'):
        study.window_at([Quote(OPEN+120,.5,.49)],OPEN,1)


def test_short_or_failed_ensemble_is_rejected():
    f=model([Quote(OPEN+60,.5,.49)],14,study.models_for(1),study.QUANTILES,
            single_point_research=True,failure_policy='fail')
    f['models'].pop()
    with pytest.raises(ValueError,match='STRICT_INTERVAL'):
        study.validate_pair_member(f,OPEN+60,14,study.models_for(1))


def pair():
    config=study.configuration('KXETH15M',OPEN+1800,2,'a'*40);a=Archive();p=Provider()
    return study.forecast_origin(MARKET,3,study.source_for(MARKET,a,p),config,a,p,model)['forecasts']


def tape():
    return [{'contract_id':MARKET['ticker']+':'+s,'timestamp':OPEN+i*60,
             'received_at':OPEN+i*60+5,'ask':.95 if s=='yes' else .15,
             'bid':.94 if s=='yes' else .14,'observed':True}
            for s in ('yes','no') for i in range(4,16)]


def test_three_strategies_next_minute_entry_and_asset_isolated_direction():
    fs=pair();q=tape();seed={'market_id':'KXETH15M-PRIOR','close_at':OPEN,
        'first_confirmed_at':OPEN+5,'result':'no','resolution_status':'resolved'}
    current={'market_id':MARKET['ticker'],'close_at':OPEN+900,'first_confirmed_at':OPEN+960,
             'result':'yes','resolution_status':'resolved'}
    other={**seed,'market_id':'KXBTC15M-PRIOR','result':'yes','first_confirmed_at':OPEN+120}
    r=study.compare(fs,q,[seed,current,other],{'fee_type':'quadratic','multiplier':1},OPEN+1800)
    for label in ('first_p90','p90_sticky'):
        outcome=r['strategies'][label]
        assert outcome['wins']==1 and outcome['losses']==0
        t=outcome['scenarios']['fixed_one']['trades'][0]
        assert t['entry_at']==OPEN+305 and t['exit_at']==OPEN+900
        assert t['exit_reason']=='authoritative_settlement'
    assert r['strategies']['first_quote_below_p10']['wins']==0
    fs[0]['market_context']['series']='KXBTC15M'
    with pytest.raises(ValueError,match='SEPARATE_SERIES'):study.compare(fs,q,[],{},OPEN+1800)


def test_first_p10_strict_boundary_first_row_only_missing_not_replaced():
    fs=pair();q=tape()
    next(r for r in q if r['contract_id'].endswith(':yes') and r['timestamp']==OPEN+240)['ask']=.09
    signals,_=study.first_p10_signals(fs,q,OPEN+1800)
    assert len(signals)==1 and signals[0]['first_row_p10']==.1
    q=[r for r in q if not (r['contract_id'].endswith(':no') and r['timestamp']==OPEN+240)]
    assert study.first_p10_signals(fs,q,OPEN+1800)[0]==[]
    q=tape();next(r for r in q if r['contract_id'].endswith(':yes') and r['timestamp']==OPEN+240)['ask']=.1
    assert study.first_p10_signals(fs,q,OPEN+1800)[0]==[]


def test_unknown_fee_does_not_fabricate_returns_or_zero_win_rate():
    r=study.compare(pair(),[],[],{'fee_type':'unknown'},OPEN+1800)
    assert r['fee_status']=='unknown_no_net_return'
    for v in r['strategies'].values():
        assert v['settlement_win_rate'] is None and not v['scenarios']


def test_missing_official_settlement_time_uses_disclosed_floor():
    r=study.settlement(MARKET, OPEN+1800)
    assert r['first_confirmed_at']==OPEN+960
    assert r['confirmation_clock']=='historical_official_or_assumed_60s_floor'
    assert study.settlement({**MARKET,'result':None},OPEN+1800) is None


def test_discovery_survives_gap_between_successive_markets(monkeypatch):
    monkeypatch.setattr(markets.time,'time',lambda:OPEN+901)
    class P:
        def get(self,path,params=None):
            if path=='/series':return {'series':[{'ticker':'KXETH15M','title':'ETH','category':'Crypto','frequency':'fifteen_min'}]}
            return {'markets':[] if params.get('status')=='open' else [MARKET]}
    found=markets.discover_series(P())
    assert found['series'][0]['verification']=='recent_contract_within_hour'


def test_dynamic_discovery_all_directional_series_not_coin_races():
    class P:
        def get(self,path,params=None):
            if path=='/series':return {'series':[{'ticker':s,'title':s,'category':'Crypto','frequency':'fifteen_min'}
                for s in ['KXBTC15M','KXETH15M','KXZEC15M','KXCRYPTOLEAD15M']]}
            s=params['series_ticker'];return {'markets':[{**MARKET,'ticker':s+'-TEST'}]}
    found=markets.discover_series(P())
    assert [s['ticker'] for s in found['series']]==['KXBTC15M','KXETH15M','KXZEC15M']
    assert found['all_series_claimed'] and found['unavailable'][0]['ticker']=='KXCRYPTOLEAD15M'


def test_collector_restores_exact_receipts_and_only_prunes_after_remote_success(tmp_path):
    s=LocalStore('x','x',tmp_path/'a');p=Provider();a=Archive()
    archive_minutes(s,p,MARKET,p.candles(MARKET,OPEN+900),OPEN+905)
    archive_settlement(s,MARKET,OPEN+960)
    saved=collector.snapshot_rows(s);copy=LocalStore('y','y',tmp_path/'b')
    collector.restore_rows(copy,saved)
    assert collector.snapshot_rows(copy)==saved
    class Broken(Archive):
        def put(self,*a):raise RuntimeError('UPLOAD_FAILED')
    with pytest.raises(RuntimeError):collector.finalize_markets(s,Broken(),OPEN+1300)
    assert len(s.values('btc_minutes'))==15
    assert collector.finalize_markets(s,a,OPEN+1300)==1
    assert len(a.get('source',MARKET['ticker'])['records']['btc_minutes'])==15
    assert not s.values('btc_minutes') and s._get('checkpoints','finalized:'+MARKET['ticker'])
    s.db.close();copy.db.close()


def test_provider_hard_read_boundary_and_duration():
    p=Provider()
    assert p.valid_market(MARKET)
    assert not p.valid_market({**MARKET,'close_time':iso(OPEN+960)})
    assert not p.valid_market({**MARKET,'ticker':'KXBTC15M-TEST'})
    with pytest.raises(ValueError,match='UNAPPROVED'):
        p.get('/portfolio/orders')
    with pytest.raises(ValueError):markets.KalshiIntervalProvider('../bad')


def test_late_revision_during_upload_is_not_pruned(tmp_path):
    s=LocalStore('x','x',tmp_path);p=Provider()
    archive_minutes(s,p,MARKET,p.candles(MARKET,OPEN+900),OPEN+905)
    archive_settlement(s,MARKET,OPEN+960)
    class Concurrent(Archive):
        def put(self,kind,key,value):
            super().put(kind,key,value)
            with s.lock,s.db:
                s._put('btc_minute_revisions','late',{'market_id':MARKET['ticker'],'timestamp':OPEN+60})
    assert collector.finalize_markets(s,Concurrent(),OPEN+1300)==0
    assert s.values('btc_minute_revisions') and len(s.values('btc_minutes'))==15
    s.db.close()


def test_workflows_have_bounded_concurrency_no_orders_no_heavy_github_artifacts():
    root=Path(__file__).resolve().parents[2]
    for name in ('kalshi-interval-minutes.yml','kalshi-interval-studies.yml'):
        raw=(root/'.github/workflows'/name).read_text();workflow=yaml.safe_load(raw)
        assert workflow['permissions']['contents']=='read'
        assert 'upload-artifact' not in raw and 'cancel-in-progress: false' in raw
        assert 'QUANTURA_RESEARCH_ARTIFACT_KEY' in raw and 'HF_TOKEN' not in raw if name.endswith('minutes.yml') else 'max-parallel: 2' in raw
