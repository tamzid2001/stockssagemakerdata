"""Routine CI uses a fake library; opt-in regression executes pinned Prophet."""
import os
import sys
from types import SimpleNamespace

import numpy as np
import pytest

from ensemble_forecasting.adapters.prophet import ProphetAdapter
from ensemble_forecasting.adapters.base import ModelExecutionError
from ensemble_forecasting.preprocessing import prepare_series
from ensemble_forecasting.schemas import ForecastRequest
from ensemble_forecasting.calendars import build_future_timestamps


def inputs(values):
    rows = [{'timestamp':f'2026-09-16T12:{i:02d}:00Z', 'target':value} for i,value in enumerate(values)]
    series = prepare_series(rows, minimum_rows=2, transform='logit', frequency='1min')
    request = ForecastRequest.from_dict({'prediction_length':13, 'horizon_mode':'frequency_periods',
        'frequency':'1min','calendar':'NONE','transform':'none',
        'quantiles':[.01,.1,.25,.5,.75,.9,.99],'models':{'prophet':{'enabled':True,'weight':1}}})
    dates = build_future_timestamps(series.timestamps[-1],prediction_length=13,
                                   horizon_mode='frequency_periods',frequency='1min',calendar='NONE')
    return series, dates, request


@pytest.mark.parametrize('length',[2,3,40])
def test_two_point_optimizer_is_bounded_deterministic_and_uses_original_rows(monkeypatch,length):
    captured={}
    class FakeProphet:
        def __init__(self,**kwargs):
            captured['settings']=kwargs
            self.stan_backend=SimpleNamespace(set_options=lambda **kw:captured.update(backend=kw))
        def fit(self,train,**kwargs):captured.update(train=train.copy(),fit=kwargs)
        def predictive_samples(self,future):
            captured['samples']=captured.get('samples',0)+1
            return {'yhat':np.tile(np.linspace(-1,1,500),(len(future),1))}
    monkeypatch.setitem(sys.modules,'prophet',SimpleNamespace(Prophet=FakeProphet))
    series,dates,request=inputs([.5]*length)
    result=ProphetAdapter().forecast(series,dates,request);result.validate()
    assert len(captured['train'])==length and captured['samples']==1
    np.testing.assert_array_equal(captured['train'].y,series.transformed_values)
    if length==2:
        assert captured['fit']=={'algorithm':'BFGS','seed':int(series.dataset_hash[:8],16),
                                  'timeout':20,'require_converged':True}
        assert captured['backend']=={'newton_fallback':False}
        assert any('TWO_POINT_BFGS' in w for w in result.warnings)
    else:
        assert captured['fit']=={} and 'backend' not in captured


def test_optimizer_timeout_never_returns_fake_or_partial_forecast(monkeypatch):
    class FakeProphet:
        def __init__(self,**kw):self.stan_backend=SimpleNamespace(set_options=lambda **kw:None)
        def fit(self,*a,**kw):raise TimeoutError('private local process details')
        def predictive_samples(self,*a):pytest.fail('must not sample a failed fit')
    monkeypatch.setitem(sys.modules,'prophet',SimpleNamespace(Prophet=FakeProphet))
    with pytest.raises(ModelExecutionError) as error:ProphetAdapter().forecast(*inputs([.42,.54]))
    assert str(error.value)=='prophet:MODEL_INFERENCE_FAILED'


def test_predictive_samples_are_reproducible_without_changing_caller_rng(monkeypatch):
    class FakeProphet:
        def __init__(self,**kw):pass
        def fit(self,*a,**kw):pass
        def predictive_samples(self,future):
            return {'yhat':np.random.normal(size=(len(future),500))}
    monkeypatch.setitem(sys.modules,'prophet',SimpleNamespace(Prophet=FakeProphet))
    args=inputs([.42,.44,.46])
    np.random.seed(1234)
    first=ProphetAdapter().forecast(*args)
    next_draw=np.random.random()
    np.random.seed(1234)
    second=ProphetAdapter().forecast(*args)
    assert np.random.random()==next_draw
    np.testing.assert_array_equal(first.quantile_matrix,second.quantile_matrix)


# Anonymized numeric cases from the five failing two-minute markets, both sides,
# plus flat/boundary windows. No credentials or private archive records included.
REGRESSION_PAIRS = ((.59,.47),(.42,.54),(.53,.5),(.48,.51),(.58,.47),(.43,.54),
                    (.47,.66),(.54,.35),(.49,.57),(.52,.44),(.5,.5),(.01,.01),
                    (.99,.99),(0.,0.),(1.,1.),(0.,1.),(1.,0.))


@pytest.mark.skipif(os.getenv('QUANTURA_REAL_PROPHET_SMOKE')!='1',reason='Real Prophet opt-in')
@pytest.mark.parametrize('values',REGRESSION_PAIRS)
def test_real_prophet_two_point_regression(values):
    np.random.seed(42)
    result=ProphetAdapter().forecast(*inputs(values));result.validate()
    assert result.quantile_matrix.shape==(7,13)
    assert np.isfinite(result.quantile_matrix).all()
    assert np.all(np.diff(result.quantile_matrix,axis=0)>=0)
    assert result.package_versions['prophet']=='1.4.0'
