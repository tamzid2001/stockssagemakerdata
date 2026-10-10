from __future__ import annotations

import copy
import json
import socket
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from metaculus_bot.api import ApiError, Metaculus, RateLimited, eligible_post, retry_seconds
from metaculus_bot.checkpoint import Checkpoint
from metaculus_bot.llm import FreeLLM, FreeQuota, answer_schema, select_model, zero_price
from metaculus_bot.questions import context, payload, unpack
from metaculus_bot.research import public_get, public_url
from metaculus_bot.runner import question_hash, record_text, run, saved_record, submit
from metaculus_bot.time_series import definition, history


@pytest.fixture
def posts():
    data = json.loads(Path(__file__).with_name("questions.json").read_text())
    # Fixtures are unscored testing-area questions. Keep chronology stable even
    # after those questions eventually close on the real platform.
    for post in data:
        for q in ([post["question"]] if post.get("question") else (post.get("group_of_questions") or {}).get("questions", [])):
            q["status"] = "open"
            q["open_time"] = "2020-01-01T00:00:00Z"
            q["scheduled_close_time"] = "2040-01-01T00:00:00Z"
    return data


def one(posts, kind):
    return next(q for post in posts for q in unpack(post) if q.question_type == kind)


def test_formats_and_groups(posts):
    questions = [q for p in posts for q in unpack(p)]
    assert {q.question_type for q in questions} == {"binary", "numeric", "discrete", "date", "multiple_choice"}
    assert len(questions) == 12
    assert len({q.id_of_question for q in questions}) == 12


@pytest.mark.parametrize("field,value", [("user_permission", "viewer"), ("status", "closed")])
def test_non_forecastable_post_is_excluded(posts, field, value):
    post = copy.deepcopy(posts[1]);post[field] = value
    assert unpack(post) == []


def test_closed_group_child_excluded(posts):
    group = next(copy.deepcopy(p) for p in posts if p.get("group_of_questions"))
    child_id = group["group_of_questions"]["questions"][0]["id"]
    group["group_of_questions"]["questions"][0]["status"] = "closed"
    assert child_id not in {q.id_of_question for q in unpack(group)}


def test_binary_and_mc_validation(posts):
    binary = one(posts, "binary")
    for invalid in [float("nan"), float("inf"), -0.1, 1.0]:
        with pytest.raises(ValueError):payload(binary, {"probability_yes": invalid})
    assert payload(binary, {"probability_yes": 0.2})["probability_yes"] == 0.2
    mc = one(posts, "multiple_choice")
    with pytest.raises(ValueError):payload(mc, {"probabilities": {"fake option": 1}})
    probs = {o: 1 / len(mc.options) for o in mc.options}
    assert sum(payload(mc, {"probabilities": probs})["probability_yes_per_category"].values()) == pytest.approx(1)


@pytest.mark.parametrize("kind", ["numeric", "discrete", "date"])
def test_sdk_cdf_dimensions_bounds_and_monotonicity(posts, kind):
    q = one(posts, kind)
    lo = q.lower_bound.timestamp() if kind == "date" else q.lower_bound
    hi = q.upper_bound.timestamp() if kind == "date" else q.upper_bound
    points = [{"percentile": p, "value": lo + p * (hi - lo)} for p in [0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99]]
    if kind == "date":
        for point in points:point["value"] = datetime.fromtimestamp(point["value"], timezone.utc).isoformat()
    cdf = payload(q, {"percentiles": points})["continuous_cdf"]
    assert len(cdf) == q.cdf_size
    assert all(0 <= a <= b <= 1 for a, b in zip(cdf, cdf[1:]))
    if not q.open_lower_bound:assert cdf[0] == 0
    if not q.open_upper_bound:assert cdf[-1] == 1
    points.reverse()
    with pytest.raises(ValueError):payload(q, {"percentiles": points})


def test_log_scale_supported(posts):
    q = one(posts, "numeric");q.lower_bound=1;q.upper_bound=10000;q.zero_point=0
    points=[{"percentile":p,"value":10**(p*4)} for p in [0.01,0.1,0.25,0.5,0.75,0.9,0.99]]
    cdf=payload(q,{"percentiles":points})["continuous_cdf"]
    assert len(cdf)==201 and cdf[100]==pytest.approx(.5,abs=.04)


def test_paid_models_and_fake_free_names_rejected():
    assert not zero_price({"pricing": {"prompt": "0", "completion": "0", "web_search": "0.01"}})
    catalog = [{"id": "google/gemma-4-31b-it:free", "pricing": {"prompt": "0", "completion": "0"}},
               {"id": "openai/gpt-4o:free", "pricing": {"prompt": "0.01", "completion": "0.01"}}]
    assert select_model(catalog)["id"] == "google/gemma-4-31b-it:free"
    with pytest.raises(FreeQuota):select_model(catalog[1:])


def test_free_pool_preferred_to_single_busy_provider():
    catalog=[{'id':m,'pricing':{'prompt':'0','completion':'0'}} for m in ['google/gemma-4-31b-it:free','openrouter/free']]
    assert select_model(catalog)['id']=='openrouter/free'


def test_structured_output_preserves_exact_options(posts):
    q=one(posts,'multiple_choice');schema=answer_schema(q,[])
    assert set(schema['properties']['probabilities']['properties'])==set(q.options)
    assert set(schema['required'])=={'abstain','reasoning','source_ids','probabilities'}
    assert not schema['additionalProperties']


class Response:
    def __init__(self, data=None, status=200, headers=None):
        self.data=data;self.status_code=status;self.headers=headers or {};self.content=b'{}'
    def json(self):return self.data


class RouterSession:
    def __init__(self, remaining=0, status=200):self.headers={};self.posts=[];self.remaining=remaining;self.status=status
    def get(self,url,**kwargs):
        if url.endswith('/models'):return Response({"data":[{"id":"openrouter/free","pricing":{"prompt":"0","completion":"0"}}]})
        return Response({"data":{"free_model_daily_requests":{"remaining":self.remaining}}})
    def post(self,url,**kwargs):self.posts.append(kwargs);return Response({},self.status)


def test_quota_exhausted_never_calls_model(posts):
    session=RouterSession();llm=FreeLLM('test',session)
    with pytest.raises(FreeQuota):llm.forecast(one(posts,'binary'),[])
    assert session.posts==[]


def test_429_defers_without_paid_fallback(posts):
    session=RouterSession(remaining=1,status=429);llm=FreeLLM('test',session)
    with pytest.raises(FreeQuota):llm.forecast(one(posts,'binary'),[])
    body=session.posts[0]['json']
    assert body['model']=='openrouter/free' and not body['provider']['allow_fallbacks']
    assert 'tools' not in body and 'plugins' not in body


def test_no_community_predictions_in_prompt(posts):
    q=one(posts,'binary');q.api_json['question']['aggregations']={'probability_yes':0.9}
    assert 'aggregations' not in json.dumps(context(q))


def test_recover_nested_prediction_without_regeneration(posts):
    q=one(posts,'binary')
    assert payload(q,{'prediction':{'probability_yes':.17}})['probability_yes']==.17
    assert payload(q,{'prediction':{'binary':.17}})['probability_yes']==.17
    q=one(posts,'multiple_choice');probs={o:1/len(q.options) for o in q.options}
    assert payload(q,{'prediction':probs})['probability_yes_per_category']==probs


def test_all_competitions_permission_and_deadline_filter():
    api=Metaculus('test');now=datetime(2026,10,10,tzinfo=timezone.utc)
    base={'id':1,'is_ongoing':True,'bot_leaderboard_status':'bots_only','user_permission':'forecaster',
          'start_date':'2026-09-01T00:00:00Z','forecasting_end_date':'2027-01-01T00:00:00Z'}
    rows=[base,{**base,'id':2,'bot_leaderboard_status':'exclude_and_show'},
          {**base,'id':3,'user_permission':'viewer'},{**base,'id':4,'forecasting_end_date':'2026-09-01T00:00:00Z'}]
    api.get=lambda path:rows if path=='projects/tournaments/' else [base]
    assert [p['id'] for p in api.competitions(now)]==[1]


def test_posts_are_paginated():
    api=Metaculus('test');calls=[]
    def get(path,**params):
        calls.append(params['offset']);return {'results':[{'id':params['offset']+i} for i in range(100 if params['offset']==0 else 2)],'next':'next'}
    api.get=get
    assert len(api.posts(123))==102 and calls==[0,100]


def test_records_are_owned_and_recover_exact_answer(posts):
    q=one(posts,'binary');answer={'probability_yes':.15,'reasoning':'Reasoning','engine':'free_llm','model':'openrouter/free'}
    record={'question_id':q.id_of_question,'generated_at':'2026-10-10T00:00:00Z','answer':answer,'question_sha256':question_hash(q)}
    text=record_text(record)
    assert saved_record([{'author':{'id':3},'text':text}],3,q.id_of_question)==record
    assert saved_record([{'author':{'id':2},'text':text}],3,q.id_of_question) is None


def test_lost_forecast_response_reconciles_without_repost(posts):
    q=one(posts,'binary');record={'answer':{'probability_yes':.2},'question_sha256':question_hash(q)}
    post=next(p for p in posts if p.get('question',{}).get('id')==q.id_of_question)
    class Api:
        writes=0
        def get(self,path):
            p=copy.deepcopy(post)
            if self.writes:p['question']['my_forecasts']={'history':[{'start_time':1780000000}],'latest':{'start_time':1780000000}}
            return p
        def post(self,path,body):self.writes+=1;raise ApiError('METACULUS_WRITE_UNCERTAIN')
        def own_forecasts(self,ids):return set(ids) if self.writes else set()
    api=Api();assert submit(api,q,record)=='submitted_reconciled';assert api.writes==1


def test_changed_criteria_not_submitted(posts):
    q=one(posts,'binary');record={'answer':{'probability_yes':.2},'question_sha256':question_hash(q)}
    post=next(copy.deepcopy(p) for p in posts if p.get('question',{}).get('id')==q.id_of_question)
    post['question']['resolution_criteria']='Changed meaning'
    api=SimpleNamespace(get=lambda path:post,post=lambda *args:pytest.fail('Must not write'))
    assert submit(api,q,record)=='criteria_changed_deferred'


def test_private_or_metadata_source_urls_blocked(monkeypatch):
    monkeypatch.setattr(socket,'getaddrinfo',lambda *args,**kwargs:[(2,1,6,'',('169.254.169.254',443))])
    assert not public_url('https://metadata.google.internal/latest')
    assert not public_url('https://user:pass@example.com/')
    assert not public_url('http://example.com/')
    assert not public_url('https://example.com:8080/')


def test_resolution_source_mapping_uses_stated_date_not_resolve_date(posts):
    q=one(posts,'numeric');q.question_text='What will Donald Trump net approval be?'
    q.resolution_criteria='This resolves to net approval on December 31, 2026, according to https://www.natesilver.net/p/trump-approval-ratings-nate-silver-bulletin.'
    spec=definition(q,[])
    assert spec['target_date']=='2026-12-31' and spec['value_column']=='net'
    q.resolution_criteria='Approval will resolve sometime according to another provider.'
    assert definition(q,[]) is None


def test_time_series_preserves_real_rows_and_rejects_future_data(monkeypatch):
    from metaculus_bot import time_series
    now=datetime(2026,10,10,tzinfo=timezone.utc);monkeypatch.setattr(time_series,'utcnow',lambda:now)
    lines=['modeldate,approve,disapprove']+[f"{(now-timedelta(days=45-i)).strftime('%m/%d/%Y')},40,50" for i in range(47)]
    raw=('\n'.join(lines)).encode()
    monkeypatch.setattr(time_series,'public_get',lambda url:(b'https://datawrapper.dwcdn.net/kSCt4/123/' if url.endswith('kSCt4/') else raw,url))
    rows,source=history({'kind':'silver_bulletin','value_column':'net','unit':'percentage points'})
    assert len(rows)==46 and rows[-1]['timestamp']==now.isoformat() and rows[-1]['target']==-10
    assert source['observed_rows']==46


class Timer:
    now=0
    def clock(self):return self.now
    def sleep(self,seconds):self.now+=seconds


class MetaculusSession:
    def __init__(self, timer, responses):self.headers={};self.timer=timer;self.responses=iter(responses);self.calls=[]
    def get(self,url,**kwargs):self.calls.append(self.timer.now);return next(self.responses)
    def post(self,url,**kwargs):return self.get(url,**kwargs)


def test_metaculus_throttle_and_retry_after():
    timer=Timer();session=MetaculusSession(timer,[Response(status=429,headers={'Retry-After':'45'}),Response({'ok':True}),Response({'ok':True})])
    api=Metaculus('test',session,clock=timer.clock,sleep=timer.sleep)
    assert api.get('posts/')=={'ok':True}
    api.get('posts/')
    assert session.calls==[0,45,50]
    now=datetime(2026,10,10,12,0,tzinfo=timezone.utc)
    assert retry_seconds('Sat, 10 Oct 2026 12:01:30 GMT',now)==90
    assert retry_seconds('not a date',now) is None


def test_429_write_not_reposted_and_long_cooldown_not_ignored():
    timer=Timer();session=MetaculusSession(timer,[Response(status=429,headers={'Retry-After':'600'})])
    api=Metaculus('test',session,clock=timer.clock,sleep=timer.sleep)
    with pytest.raises(RateLimited) as error:api.post('questions/forecast/',[])
    assert error.value.retry_after==600
    with pytest.raises(RateLimited):api.get('posts/')
    assert session.calls==[0]


def test_api_forecasting_disabled_enum_is_rejected():
    api=Metaculus('test');api.get=lambda path:{'id':1,'username':'bot','is_bot':True,'is_active':True,'api_forecasting_access':'disabled'}
    with pytest.raises(RuntimeError,match='BOT_API_FORECASTING_ACCESS_REQUIRED'):api.identity()


def test_authoritative_bulk_own_forecasts():
    api=Metaculus('test');calls=[]
    def get(path,**params):
        calls.append(params['question_ids'])
        return {'results':[{'question_id':i,'forecasts':[{}] if i%2 else []} for i in params['question_ids']]}
    api.get=get
    assert api.own_forecasts(list(range(1,103)))==set(range(1,103,2))
    assert len(calls)==2 and len(calls[0])==100


def test_confirmed_receipt_required(posts):
    q=one(posts,'binary');record={'answer':{'probability_yes':.2},'question_sha256':question_hash(q)}
    post=next(p for p in posts if p.get('question',{}).get('id')==q.id_of_question)
    api=SimpleNamespace(get=lambda path:post,post=lambda *args:{},own_forecasts=lambda ids:set())
    assert submit(api,q,record)=='submission_unconfirmed'
    api.own_forecasts=lambda ids:set(ids)
    assert submit(api,q,record)=='submitted'


def test_saved_abstentions_do_not_starve_new_questions(posts,monkeypatch,tmp_path):
    sample=[copy.deepcopy(p) for p in posts if p.get('question',{}).get('type')=='binary']
    first=unpack(sample[0])[0]
    # One persisted abstention precedes a new question, with a one-generation budget.
    sample.append(copy.deepcopy(sample[0]));sample[1]['id']+=1000;sample[1]['question']['id']+=1000
    saved={'question_id':first.id_of_question,'answer':{'abstain':True},'question_sha256':question_hash(first),'generated_at':'2026-10-10T00:00:00Z'}
    class Api:
        comment_calls=0
        def identity(self):return {'id':1,'username':'test'}
        def posts(self,project):return sample
        def own_forecasts(self,ids):return set()
        def comments(self,post,author):
            self.comment_calls+=1
            assert post is None
            return [{'author':{'id':1},'text':record_text(saved)}]
    class Llm:
        calls=0
        def __init__(self,key):pass
        def forecast(self,q,sources):self.calls+=1;return {'probability_yes':.2}
    from metaculus_bot import runner
    monkeypatch.setattr(runner,'evidence',lambda q:[])
    monkeypatch.setenv('OPENROUTER_API_KEY','test')
    args=SimpleNamespace(scope='test',engine='llm',submit=False,max_questions=1,question_ids=[],report=str(tmp_path/'summary.json'))
    api=Api();result=run(args,api,Llm)
    assert result['counts']=={'abstained':1,'validated_not_submitted':1} and api.comment_calls==1


def test_selected_posts_recheck_eligibility_without_discovery(posts,monkeypatch,tmp_path):
    sample=next(copy.deepcopy(p) for p in posts if p.get('question',{}).get('type')=='binary')
    sample['projects']={'default_project':{'id':32977,'slug':'bot-testing-area'}}
    q=unpack(sample)[0]
    api=SimpleNamespace(identity=lambda:{'id':1,'username':'test'},get=lambda path:sample,
                        own_forecasts=lambda ids:set(ids),competitions=lambda:pytest.fail('No second discovery scan'))
    args=SimpleNamespace(scope='test',engine='time-series',submit=False,max_questions=1,question_ids=[q.id_of_question],
                         post_ids=[sample['id']],report=str(tmp_path/'summary.json'))
    assert run(args,api)['counts']=={'already_submitted':1}
    sample['projects']={'default_project':{'slug':'human-cup','is_ongoing':True,'bot_leaderboard_status':'exclude_and_show'}}
    assert not eligible_post(sample,'competitions')


def test_checkpoint_encrypted_and_exact_generation_recovered(tmp_path):
    from cryptography.fernet import InvalidToken
    path=tmp_path/'checkpoint.enc';record={'question_id':1,'answer':{'probability_yes':.123,'reasoning':'private reasoning'}}
    state=Checkpoint(path,'test-token');state.put(record);state.defer(90)
    assert b'private reasoning' not in path.read_bytes()
    restored=Checkpoint(path,'test-token')
    assert restored.get(1)==record and 89<restored.remaining()<=90
    with pytest.raises(InvalidToken):Checkpoint(path,'different-token')
    restored.remove(1)
    assert Checkpoint(path,'test-token').get(1) is None


def test_pending_generation_does_not_rerun_model(posts,monkeypatch,tmp_path):
    sample=next(copy.deepcopy(p) for p in posts if p.get('question',{}).get('type')=='binary')
    q=unpack(sample)[0]
    path=tmp_path/'checkpoint.enc'
    record={'question_id':q.id_of_question,'answer':{'probability_yes':.2},'question_sha256':question_hash(q),'generated_at':'2026-10-10T00:00:00Z'}
    Checkpoint(path,'test-token').put(record)
    monkeypatch.setenv('METACULUS_TOKEN','test-token')
    monkeypatch.setenv('METACULUS_PARTICIPATION_FORM_CONFIRMED','true')
    class Api:
        writes=[]
        def identity(self):return {'id':1,'username':'test'}
        def posts(self,project):return [sample]
        def comments(self,*args):return []
        def get(self,path):return sample
        def own_forecasts(self,ids):return set(ids) if 'questions/forecast/' in self.writes else set()
        def post(self,path,body):self.writes.append(path);return {}
    args=SimpleNamespace(scope='test',engine='llm',submit=True,max_questions=1,question_ids=[],checkpoint=str(path),report=str(tmp_path/'summary.json'))
    api=Api()
    assert run(args,api,lambda key:pytest.fail('Must reuse original generation'))['counts']=={'submitted':1}
    assert api.writes==['comments/create/','questions/forecast/'] and Checkpoint(path,'test-token').get(q.id_of_question) is None


def test_existing_cooldown_skips_all_metaculus_requests(monkeypatch,tmp_path):
    path=tmp_path/'checkpoint.enc';Checkpoint(path,'test-token').defer(600)
    monkeypatch.setenv('METACULUS_TOKEN','test-token')
    args=SimpleNamespace(checkpoint=str(path))
    with pytest.raises(RateLimited):run(args,SimpleNamespace(identity=lambda:pytest.fail('Must honor restored cooldown')))
