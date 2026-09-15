from copy import deepcopy
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest
import yaml

from market_research import spy_year
from market_research.spy_weekly import digest,iso
from market_research.spy_option_targets import summarize


def test_52_windows_are_contiguous_and_use_real_holiday_closes():
    windows=spy_year.windows('2026-09-11')
    assert len(windows)==52
    assert windows[0]['origin']=='2025-09-12T20:00:00Z'
    assert windows[-1]['week_end']=='2026-09-11T20:00:00Z'
    assert all(a['week_end']==b['origin'] for a,b in zip(windows,windows[1:]))
    shifted={w['nominal_end_friday']:w['week_end'] for w in windows if w['expiry_holiday_shifted']}
    assert shifted=={'2026-04-03':'2026-04-02T20:00:00Z','2026-06-19':'2026-06-18T20:00:00Z','2026-07-03':'2026-07-02T20:00:00Z'}
    early=next(w for w in windows if w['nominal_end_friday']=='2025-11-28')
    assert early['week_end']=='2025-11-28T18:00:00Z'
    assert any(w['week_end'].endswith('21:00:00Z') for w in windows)  # EST, not fixed UTC hours.


@pytest.mark.parametrize('day,weeks',[('2026-09-10',52),('2026-09-11',0),('2026-09-11',53),('2026-09-11',True)])
def test_invalid_year_windows(day,weeks):
    with pytest.raises(ValueError):spy_year.windows(day,weeks)


def test_source_chunks_cover_every_day_once_and_remain_bounded():
    chunks=list(spy_year.date_chunks('2025-04-15','2026-09-11'))
    days=[]
    for start,end in chunks:
        a,b=date.fromisoformat(start),date.fromisoformat(end)
        assert (b-a).days<28
        days.extend(a+timedelta(days=i) for i in range((b-a).days+1))
    assert len(days)==len(set(days))
    assert days[0]==date(2025,4,15) and days[-1]==date(2026,9,11)


def test_original_seed_rows_override_refetched_history():
    r={'timestamp':'2026-08-01T14:00:00Z','close':10}
    assert spy_year.merge_sources([{'rows':[r]}],[{**r,'close':11}])[0]['close']==11
    with pytest.raises(ValueError,match='CONFLICTING_YEAR_SOURCE'):
        spy_year.merge_sources([{'rows':[r,{**r,'close':12}]}],[])


class Memory:
    def __init__(self):self.data={}
    def get(self,kind,key):return deepcopy(self.data.get((kind,key)))
    def put(self,kind,key,value):
        previous=self.data.get((kind,key))
        if previous is not None and previous!=value:raise ValueError('IMMUTABLE_YEAR_RECORD_CONFLICT')
        self.data[kind,key]=deepcopy(value)


def week_case():
    origin='2026-09-04T20:00:00Z';end='2026-09-11T20:00:00Z'
    history=[{'timestamp':iso(pd.Timestamp(origin)-pd.Timedelta(hours=499-i)),'close':100.,'open':100.,'high':101.,'low':99.} for i in range(500)]
    grid=[{'start':'2026-09-08T13:30:00Z','timestamp':'2026-09-08T14:30:00Z','minutes':60}]
    f={'origin':origin,'week_end':end,'training_sha256':digest(history),'future_grid':grid,
       'predictions':[{'timestamp':grid[0]['timestamp'],'quantiles':{'0.01':80.,'0.1':90.,'0.5':100.,'0.9':110.,'0.99':120.}}],
       'model_runs':[{'model_id':m} for m in ('prophet','toto','granite','chronos','timesfm')]}
    w={'index':0,'origin':origin,'week_end':end}
    d={'weeks':[{'end':end,'average_quantiles':{'0.1':90.,'0.9':110.}}]}
    return w,history,grid,f,d


def test_completed_week_resume_never_refetches_or_reforecasts():
    store=Memory();saved={'result':{'origin':'saved'}};store.put('week','week-00',saved)
    assert spy_year.process_week({'index':0},[],[],[],{},store,None)==saved


def test_seed_forecast_reused_and_permanent_option_gaps_are_explicit(monkeypatch):
    w,bars,grid,f,d=week_case();store=Memory()
    monkeypatch.setattr(spy_year,'diagnose',lambda *_:d)
    monkeypatch.setattr(spy_year,'forecast_week',lambda *_:pytest.fail('Must reuse archived forecast'))
    monkeypatch.setattr(spy_year,'fetch',lambda *_args,**_kwargs:None)
    result=spy_year.process_week(w,bars,[],grid,{w['origin']:f},store,None)
    assert result['result']['seed_forecast_reused']
    assert len(result['result']['errors'])==2
    assert result['result']['scenarios']=={}
    assert store.get('stage','week-00-forecast')==f


def test_seed_inputs_cannot_be_rewritten(monkeypatch):
    w,bars,grid,f,_=week_case();bars[-1]['close']=102
    with pytest.raises(ValueError,match='PRIOR_STUDY_FORECAST_INPUT_CHANGED'):
        spy_year.process_week(w,bars,[],grid,{w['origin']:f},Memory(),None)


def test_model_checkpoint_survives_option_transport_failure(monkeypatch):
    w,bars,grid,f,d=week_case();store=Memory();inferences=[]
    monkeypatch.setattr(spy_year,'forecast_week',lambda *_:(inferences.append(True) or f))
    monkeypatch.setattr(spy_year,'diagnose',lambda *_:d)
    monkeypatch.setattr(spy_year,'fetch',lambda *_args,**_kwargs:(_ for _ in ()).throw(RuntimeError('network')))
    with pytest.raises(RuntimeError,match='network'):spy_year.process_week(w,bars,[],grid,{},store,None)
    assert store.get('stage','week-00-forecast')==f
    monkeypatch.setattr(spy_year,'fetch',lambda *_args,**_kwargs:None)
    spy_year.process_week(w,bars,[],grid,{},store,None)
    assert len(inferences)==1


def test_52_week_summary_does_not_hide_missing_results_or_tuning_cohort():
    def result(pnl,seed=False):
        s={'status':'priced_trade_bar_proxy','net_pnl_proxy_before_costs':pnl,'entry_premiums_usd':100,
           'target_exit_legs':2,'timed_exit_legs':0,'legs':[{'net_pnl_proxy_before_costs':pnl}]}
        return {'origin':str(pnl),'expiry':'date','seed_forecast_reused':seed,'scenarios':{'next_session_morning/both_legs':s}}
    r=spy_year.yearly_summary([result(10),result(-20),result(5,True)],52)
    assert not r['complete']
    assert r['all_weeks']['next_session_morning/both_legs']['complete_weeks']==3
    assert r['additional_weeks']['next_session_morning/both_legs']['complete_weeks']==2
    assert r['previously_tested_weeks']['next_session_morning/both_legs']['complete_weeks']==1
    assert summarize([])['next_session_morning/both_legs']['ranking_valid_for_full_sample'] is False


def test_workflow_is_private_pinned_and_carries_no_raw_dataset_inputs():
    workflow=yaml.safe_load(Path('.github/workflows/spy-year-study.yml').read_text())
    assert workflow['jobs']['research']['runs-on']=='ubuntu-latest'
    assert workflow['jobs']['research']['timeout-minutes']==330
    assert workflow['concurrency']['cancel-in-progress'] is False
    steps=workflow['jobs']['research']['steps']
    upload=next(s for s in steps if s.get('uses','').startswith('actions/upload-artifact'))
    assert upload['with']['path'].endswith('*.qrc.enc')
    assert upload['with']['retention-days']==3
    assert upload['continue-on-error'] is True  # Durable private store is primary.
    handoff=next(s for s in steps if s.get('name')=='Continue only a checkpointed unfinished study')
    assert "steps.study.outputs.complete == 'false'" in handoff['if']
    assert 'ref:process.env.QUANTURA_CODE_SHA' in handoff['with']['script']
