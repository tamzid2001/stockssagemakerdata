from dataclasses import replace

import pytest

from ensemble_forecasting.adapters.toto import toto_checkpoint
from ensemble_forecasting.adapters.mock import MockAdapter
from ensemble_forecasting.capabilities import public_capabilities
from ensemble_forecasting.tests.test_engine import request, series_rows
from ensemble_forecasting import worker

REVISION = "8306a9801cf98c0f5ffe4b2dcc8f496e616d84d9"


def test_new_default_is_pinned_but_saved_checkpoints_remain_unchanged():
    assert toto_checkpoint(request()) == ("Datadog/Toto-2.0-4m", REVISION)
    assert toto_checkpoint(request(model_checkpoints={"toto":"Datadog/Toto-2.0-4m"})) == ("Datadog/Toto-2.0-4m", None)
    old_revision=REVISION
    assert toto_checkpoint(request(model_checkpoints={"toto":"Datadog/Toto-2.0-4m"},
                                   model_revisions={"toto":old_revision})) == ("Datadog/Toto-2.0-4m", old_revision)
    model=next(m for m in public_capabilities()['models'] if m['id']=='toto')
    assert model['name']=='Toto 2.0 4M' and model['checkpoint_revision']==REVISION
    assert model['minimum_observed_context']==32
    previous_revision='a7bab288f5e95f8606f8306f86659357e1c001ef'
    assert toto_checkpoint(request(model_checkpoints={'toto':'Datadog/Toto-2.0-313m'},
                                   model_revisions={'toto':previous_revision})) == ('Datadog/Toto-2.0-313m', previous_revision)
    largest_revision='51a2812bbe449437c01b79c0e425ed578f335f5b'
    assert toto_checkpoint(request(model_checkpoints={'toto':'Datadog/Toto-2.0-2.5B'},
                                   model_revisions={'toto':largest_revision})) == ('Datadog/Toto-2.0-2.5B', largest_revision)


@pytest.mark.parametrize('value',[{'toto':'main'},{'toto':'../secret'}, {'other':REVISION}, ['invalid']])
def test_revisions_are_immutable_server_metadata(value):
    with pytest.raises(ValueError,match='model_revisions'):
        request(model_revisions=value)


def test_worker_passes_saved_identity_and_persists_revision(monkeypatch):
    old='Datadog/Toto-2.0-4m';revision='8306a9801cf98c0f5ffe4b2dcc8f496e616d84d9'
    class Adapter:
        def forecast(self,series,timestamps,req):
            assert toto_checkpoint(req)==(old,revision)
            return replace(MockAdapter('toto').forecast(series,timestamps,req),
                           checkpoint=old,checkpoint_revision=revision)
    monkeypatch.setattr(worker,'adapter_factory',lambda *args,**kwargs:Adapter())
    result=worker.execute_job({'request':{'models':{'toto':{'enabled':True,'weight':1}},
        'prediction_length':3,'quantiles':[.1,.5,.9],'horizon_mode':'frequency_periods',
        'frequency':'1D'}, 'input':{'rows':series_rows()},
        'model_checkpoints':{'toto':old},'model_revisions':{'toto':revision}})
    assert result['model_runs'][0]['checkpoint_revision']==revision
    assert result['models'][0]['checkpoint']==old
    assert result['models'][0]['checkpoint_revision']==revision


def test_btc_short_minute_studies_still_exclude_toto():
    from market_research.kalshi_btc import short_context_models
    from market_research.btc_horizons import MODELS
    assert 'toto' not in short_context_models()
    assert 'toto' not in MODELS
