from datetime import datetime, timedelta
import sys
import types

import pytest

from market_research.ftmo_dukas_data import UTC, digest, stamp
from market_research.ftmo_dukas_engine import Conversion, RULES, replay
from market_research.ftmo_dukas_study import MODELS
from market_research.ftmo_trigger_reforecast import aggregate, candidate_days, detect_triggers, forecast_trigger, write_json
from market_research.tests.test_ftmo_dukas_study import quote, spec


def primary(origin,available=None):
    return [{'status':'completed','day':origin.date().isoformat(),'origin':origin.isoformat(),
       'forecast_end':(origin+timedelta(hours=23)).isoformat(),
       'earliest_actionable_at':(available or origin+timedelta(seconds=5)).isoformat(),
       'history_input_sha256':'original',
       'predictions':[{'timestamp':(origin+timedelta(hours=i)).isoformat(),'p01':100,'p50':110,'p99':120}for i in range(1,24)]}]


def minute(at,o,h,l,c,spread=.5):
    r=quote(at,o,h,l,c,spread);r['end']=(at+timedelta(minutes=1)).isoformat();return r


def test_first_breach_uses_only_available_complete_minutes_and_is_once_per_side():
    origin=datetime(2026,9,24,18,tzinfo=UTC);p=primary(origin)
    rows=[minute(origin,110,130,90,110),  # primary unavailable at minute open
       minute(origin+timedelta(minutes=1),110,115,98,110),
       minute(origin+timedelta(minutes=2),99,100,90,99),
       minute(origin+timedelta(minutes=3),119,122,118,119)]
    e=detect_triggers('US500.sim',p,rows,spec())
    assert [(v['side'],stamp(v['detected_at']))for v in e]==[
       ('long',origin+timedelta(minutes=2)),('short',origin+timedelta(minutes=4))]
    assert all(v['detection_kind']=='completed_minute_breach'for v in e)


def test_known_outside_open_can_trigger_at_its_timestamp_and_touch_is_not_breach():
    origin=datetime(2026,9,24,18,tzinfo=UTC);p=primary(origin)
    rows=[minute(origin+timedelta(minutes=1),99.5,100,99.5,99.5),
       minute(origin+timedelta(minutes=2),99,100,99,99),
       minute(origin+timedelta(minutes=3),120,120,120,120),
       minute(origin+timedelta(minutes=4),121,121,121,121)]
    events=detect_triggers('US500.sim',p,rows,spec())
    assert [stamp(e['detected_at'])for e in events]==[origin+timedelta(minutes=2),origin+timedelta(minutes=4)]
    assert all(e['detection_kind']=='known_minute_open'for e in events)


def test_prefilter_includes_first_partial_primary_hour_but_does_not_timestamp_signal():
    origin=datetime(2026,9,24,18,tzinfo=UTC)
    rows=[quote(origin,110,115,90,110)]
    assert candidate_days(rows,primary(origin),spec())==[origin.date()]
    # Its full-hour low cannot produce a trigger without real minute data.
    assert detect_triggers('US500.sim',primary(origin),[],spec())==[]


def test_forecast_has_no_future_crossing_hour_or_post_trigger_data(monkeypatch):
    origin=datetime(2026,9,24,18,tzinfo=UTC);detected=origin+timedelta(hours=7,minutes=23);end=origin+timedelta(hours=23)
    observed=detected.replace(minute=0)
    rows=[quote(observed-timedelta(hours=500-i),100,102,98,101)for i in range(500)]
    rows.append(quote(observed,99999,100000,99998,99999))
    events=detect_triggers('US500.sim',primary(origin),[minute(detected-timedelta(minutes=1),110,115,98,110)],spec())
    event=events[0];assert stamp(event['detected_at'])==detected
    module=types.ModuleType('ensemble_forecasting.worker');captured=[]
    def execute(job,**kwargs):
        captured.append(job);source=job['input']['rows']
        assert len(source)==500 and max(stamp(r['timestamp'])for r in source)==observed
        assert all(r['target']<99999 for r in source)
        assert job['request']['prediction_length']==16
        assert job['request']['failure_policy']=='fail'
        return {'model_runs':[{'model':m,'status':'completed'}for m in MODELS],
          'predictions':[{'timestamp':(observed+timedelta(hours=i)).isoformat(),
             **{f'p{q:02d}':100+q/100 for q in (1,10,25,50,75,90,99)}}for i in range(1,17)]}
    module.execute_job=execute;monkeypatch.setitem(sys.modules,'ensemble_forecasting.worker',module)
    r=forecast_trigger(rows,event)
    assert stamp(r['predictions'][-1]['timestamp'])==end
    assert stamp(r['earliest_actionable_at'])>detected
    assert r['new_completed_hours_since_primary']==7


def test_incomplete_second_ensemble_cannot_be_called_real_forecast(monkeypatch):
    origin=datetime(2026,9,24,18,tzinfo=UTC)
    rows=[quote(origin-timedelta(hours=500-i),100,102,98,101)for i in range(500)]
    event=detect_triggers('US500.sim',primary(origin),[minute(origin+timedelta(minutes=1),99,100,98,99)],spec())[0]
    module=types.ModuleType('ensemble_forecasting.worker')
    module.execute_job=lambda *a,**k:{'model_runs':[{'model':'prophet','status':'completed'}],'predictions':[]}
    monkeypatch.setitem(sys.modules,'ensemble_forecasting.worker',module)
    with pytest.raises(ValueError,match='FIVE_REAL_REFRESHED'):forecast_trigger(rows,event)


def test_new_quantile_gate_and_measured_latency_both_required_before_entry():
    origin=datetime(2026,9,24,18,tzinfo=UTC);trigger=origin+timedelta(hours=1,minutes=20)
    refresh=[{'status':'completed','origin':trigger.isoformat(),'earliest_actionable_at':(trigger+timedelta(seconds=10)).isoformat(),
       'predictions':[{'timestamp':(origin+timedelta(hours=i)).isoformat(),'p01':90,'p50':110,'p99':130}for i in range(2,5)]}]
    rows=[quote(origin-timedelta(hours=1),110,112,108,110),
       quote(origin+timedelta(hours=1),99,110,80,99),  # partial refresh hour must not fill
       quote(origin+timedelta(hours=2),99,110,94,99),  # below OLD P01, above NEW P01
       quote(origin+timedelta(hours=3),99,99,89,90)]
    r=replay('US500.sim','long',rows,refresh,spec(),Conversion('USD',[]),origin,origin+timedelta(hours=4),origin+timedelta(hours=4),RULES[0])
    assert r['open_entries']==1 and r['open_basket'][0]['entry']==pytest.approx(89.99)
    assert stamp(r['open_basket'][0]['entry_at'])==origin+timedelta(hours=4)


def test_horizon_end_prevents_new_trigger_and_closed_market_cannot_trigger():
    origin=datetime(2026,9,24,18,tzinfo=UTC)
    p=primary(origin);s=spec();s['weekly_minutes']=[]
    assert detect_triggers('US500.sim',p,[minute(origin+timedelta(minutes=1),99,100,98,99)],s)==[]
    assert detect_triggers('US500.sim',p,[minute(origin+timedelta(hours=23),99,100,98,99)],spec())==[]


def test_incomplete_asset_report_produces_no_profit_claim(tmp_path):
    with pytest.raises(ValueError,match='INCOMPLETE_TRIGGERED_YEAR'):
        aggregate(['EURUSD.sim'],{}, {},tmp_path,tmp_path/'report')
    assert not (tmp_path/'report'/'report.md').exists()
    assert (tmp_path/'report'/'incomplete.json').exists()
