"""No network, credentials or model weights needed for normal CI."""
from copy import deepcopy
import pytest

from market_research import btc_horizons as study, forecast
from market_research.engine import Quote, iso

OPEN = 1_788_000_000 // 900 * 900
MARKET = dict(ticker='KXBTC15M-TEST', event_ticker='KXBTC15M-TEST', market_type='binary',
              open_time=iso(OPEN), close_time=iso(OPEN+900), status='settled', result='yes')


class Archive:
    def __init__(self): self.rows={}
    def get(self,kind,key): return deepcopy(self.rows.get((kind,key)))
    def put(self,kind,key,value):
        if (kind,key) in self.rows and self.rows[kind,key]!=value:
            raise ValueError('IMMUTABLE_BTC_RECORD_CONFLICT')
        self.rows[kind,key]=deepcopy(value)


class Provider(study.KalshiBTCProvider):
    def __init__(self): self.requests=[]; self.missing=None
    def get(self,path,params=None):
        self.requests.append((path,params))
        return {'markets':[MARKET], 'cursor':'more'}
    def candles(self,market,end): return []
    def market(self,ticker): return MARKET
    def quotes(self,raw,market,end):
        # Oscillating fixture allows late and early arms to see an entry and
        # a subsequent strict ask trade-through. These are test data only.
        return {s:[Quote(OPEN+i*60, (.95 if i%2 else .85) if s=='yes' else .1,
                               (.94 if i%2 else .84) if s=='yes' else .09)
                   for i in range(1,16) if i!=self.missing] for s in ('yes','no')}


def config(n=3): return study.configuration(n,OPEN+1800,10,'a'*40)


def model(window,horizon,models,quantiles,**kwargs):
    assert models==study.MODELS and kwargs=={'failure_policy':'fail'}
    origin=window[-1].timestamp
    return {'origin':origin,'forecast_id':'test-only','duration_seconds':.1,
        'rows':[{'timestamp':origin+i*60,'quantiles':{str(q):q for q in quantiles}}
                for i in range(1,horizon+1)],
        'models':[{'id':m,'status':'completed'} for m in models],
        'weights':{'0.9':{m:.25 for m in models}},'failures':[]}


@pytest.mark.parametrize('n',range(3,13))
def test_every_horizon_has_exact_prefix_four_models_and_hold_only(n):
    archive=Archive();provider=Provider();seen=[]
    def fit(window,*args,**kw):
        seen.append(deepcopy(window));return model(window,*args,**kw)
    result=study.process_market(MARKET,config(n),archive,provider,fit)
    assert len(seen)==2
    assert all(len(w)==n and w[0].timestamp==OPEN+60 and w[-1].timestamp==OPEN+n*60 for w in seen)
    assert len(result['forecasts'])==2
    assert all(len(f['rows'])==15-n and f['rows'][-1]['timestamp']==OPEN+900 for f in result['forecasts'])
    assert len(result['trades'])<=1
    assert all(t['exit_at']==OPEN+900 and t['exit_reason']=='authoritative_settlement' for t in result['trades'])
    assert all(t['entry_at']>OPEN+n*60+result['publication_latency_seconds'] for t in result['trades'])
    before=deepcopy(archive.rows)
    assert study.process_market(MARKET,config(n),archive,provider,lambda *a,**kw:pytest.fail('rerun'))==result
    assert archive.rows==before


@pytest.mark.parametrize('n',[0,1,2,13,15,True,3.0])
def test_invalid_horizons(n):
    with pytest.raises(ValueError,match='3_TO_12'):study.configuration(n,OPEN,10,'a'*40)


def test_missing_opening_minute_is_not_replaced_with_later_history():
    provider=Provider();provider.missing=2
    with pytest.raises(ValueError,match='MISSING_FIRST'):
        study.process_market(MARKET,config(),Archive(),provider,model)


def test_conflicting_duplicate_minute_fails():
    with pytest.raises(ValueError,match='CONFLICTING'):
        study.first_window([Quote(OPEN+60,.5,.4),Quote(OPEN+60,.6,.4)],OPEN,3)


def test_strict_member_failure_never_becomes_three_model_result():
    def broken(*args,**kw):
        f=model(*args,**kw);f['models'].pop();return f
    with pytest.raises(ValueError,match='FOUR_SUCCESSFUL'):
        study.process_market(MARKET,config(),Archive(),Provider(),broken)


@pytest.mark.parametrize('weights',[{}, {'chronos':1.}, {m:float('nan') for m in study.MODELS}])
def test_invalid_central_weights_rejected(weights):
    def broken(*args,**kw):
        f=model(*args,**kw);f['weights']={'0.9':weights};return f
    with pytest.raises(ValueError,match='EQUAL_FOUR'):
        study.process_market(MARKET,config(),Archive(),Provider(),broken)


def test_resume_reuses_first_side_after_interruption_without_overwriting():
    archive=Archive();calls=[]
    def fail_second(*args,**kw):
        calls.append(1)
        if len(calls)==2:raise RuntimeError('MODEL_INFERENCE_FAILED')
        return model(*args,**kw)
    with pytest.raises(RuntimeError):study.process_market(MARKET,config(),archive,Provider(),fail_second)
    first=archive.get('forecast',MARKET['ticker']+'-yes')
    calls=[]
    def resume(*args,**kw):calls.append(1);return model(*args,**kw)
    study.process_market(MARKET,config(),archive,Provider(),resume)
    assert len(calls)==1 and archive.get('forecast',MARKET['ticker']+'-yes')==first


def test_late_inference_cannot_use_earlier_quotes():
    def slow(*args,**kw):
        f=model(*args,**kw);f['duration_seconds']=500;return f
    r=study.process_market(MARKET,config(),Archive(),Provider(),slow)
    assert r['status']=='missed_deadline' and not r['signals'] and not r['trades']


def test_catalog_selection_does_not_filter_by_settlement_winner_or_bad_history():
    p=Provider();catalog=study.discover(p,OPEN+1000,10)
    assert catalog['markets']==[MARKET] and catalog['coverage']['upstream_has_more']
    assert p.requests[0][1]=={'series_ticker':'KXBTC15M','max_close_ts':OPEN+1000,'limit':1000}
    assert catalog['coverage']['all_history_claimed'] is False


def test_report_separates_recovery_and_unit_sizing_and_failures():
    p=Provider();catalog=study.discover(p,OPEN+1000,10)
    record=study.process_market(MARKET,config(),Archive(),p,model)
    report=study.summarize([record],catalog,config())
    assert report['forecast_count']==2
    assert set(report['scenarios'])=={'fixed_one','recover_cycle'}
    assert report['scenarios']['recover_cycle']['configuration']['cap']==100
    assert report['scenarios']['recover_cycle']['configuration']['multiplier']==2.5
    failed=study.summarize([{'market':MARKET['ticker'],'status':'failed'}],catalog,config())
    assert failed['complete'] and failed['usable_fraction']==0
    assert failed['ranking_valid_for_full_cohort'] is False
    assert failed['scenarios']['fixed_one']['summary']['win_rate'] is None


def test_strict_policy_passes_through_shared_worker_with_mock_adapters(monkeypatch):
    from ensemble_forecasting.worker import execute_job
    monkeypatch.setenv('TIMESFM_HF_ACCESS_APPROVED','true')
    monkeypatch.setenv('TIMESFM_COMMERCIAL_LICENSED','true')
    monkeypatch.setattr(forecast,'execute_job',lambda job,**kw:execute_job(job,mock=True,**kw))
    window=[Quote(OPEN+i*60,.5+i*.01,.4) for i in range(1,4)]
    result=forecast.forecast_window(window,12,study.MODELS,study.QUANTILES,failure_policy='fail')
    study.validate_four_model_forecast(result,OPEN+180,12)
    assert result['configuration']['failure_policy']=='fail'
    with pytest.raises(ValueError,match='INVALID_RESEARCH_FAILURE_POLICY'):
        forecast.forecast_window(window,12,failure_policy='ignored')
