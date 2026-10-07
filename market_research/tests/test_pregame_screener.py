from market_research.pregame_screener import eligible, game_date, through_deadline, document, game_models, GAME_QUANTILES
from market_research.engine import stamp, Quote
from market_research.provider import QuanturaProvider, KalshiProvider
import pytest


def test_cleanup_splits_oversized_index_deletes_and_preserves_other_provider():
    from types import SimpleNamespace
    from market_research.pregame_screener import delete_expired_catalog, COLLECTION
    committed, sizes = [], []
    docs = [SimpleNamespace(reference=f"doc-{n}", to_dict=lambda n=n: {"provider": "polymarket_us" if n == 25 else "kalshi"}) for n in range(26)]
    class Query:
        def where(self, field, op, cutoff):
            assert (field, op, cutoff) == ("game_date", "<", "2026-10-03")
            return self
        def limit(self, count):
            assert count == 500
            return self
        def stream(self):
            return iter(docs)
    class Batch:
        def __init__(self):
            self.refs = []
        def delete(self, reference):
            self.refs.append(reference)
        def commit(self):
            sizes.append(len(self.refs))
            if len(self.refs) > 3:
                raise ValueError("400 Transaction too big. Decrease transaction size.")
            committed.extend(self.refs)
    db = SimpleNamespace(collection=lambda name: Query() if name == COLLECTION else None, batch=Batch)
    assert delete_expired_catalog(db, "2026-10-03", "kalshi") == 25
    assert len(committed) == len(set(committed)) == 25
    assert "doc-25" not in committed
    assert max(sizes) <= 20


def test_cleanup_does_not_hide_permission_or_network_errors():
    from types import SimpleNamespace
    from market_research.pregame_screener import delete_expired_catalog
    doc = SimpleNamespace(reference="doc", to_dict=lambda: {"provider": "kalshi"})
    query = SimpleNamespace(where=lambda *args: query, limit=lambda *args: query, stream=lambda: iter([doc]))
    batch = SimpleNamespace(delete=lambda *args: None, commit=lambda: (_ for _ in ()).throw(ValueError("permission denied")))
    db = SimpleNamespace(collection=lambda *args: query, batch=lambda: batch)
    with pytest.raises(ValueError, match="permission denied"):
        delete_expired_catalog(db, "2026-10-03", "kalshi")


def contract(start="2026-09-26T23:30:00Z"):
    return dict(eventStart=start,status="open",source="kalshi",contractId="GAME:yes",providerSymbol="GAME",eventId="GAME",eventTitle="A vs B",outcome="A")


def test_start_hour_stops_even_before_partial_hour_kickoff():
    c=contract()
    assert eligible(c,stamp("2026-09-26T22:59:59Z"))
    assert not eligible(c,stamp("2026-09-26T23:00:00Z"))
    assert not eligible(c,stamp("2026-09-26T23:29:59Z"))
    assert not eligible({**c,"eventStart":""},stamp("2026-09-26T22:00:00Z"))
    assert not eligible({**c,"live":True},stamp("2026-09-26T22:00:00Z"))


def test_new_york_today_includes_utc_next_day_and_dst_transition():
    assert game_date(stamp("2026-09-27T01:00:00Z"))=="2026-09-26"
    assert eligible(contract("2026-09-27T01:30:00Z"),stamp("2026-09-26T23:00:00Z"))
    assert not eligible(contract("2026-09-27T13:30:00Z"),stamp("2026-09-26T23:00:00Z"))
    assert game_date(stamp("2026-11-01T05:30:00Z"))==game_date(stamp("2026-11-01T06:30:00Z"))


def test_partial_hour_end_interpolates_model_only_and_preserves_ordered_bands():
    rows=[{"timestamp":3600,"quantiles":{"0.1":.1,"0.5":.3,"0.9":.6}}, {"timestamp":7200,"quantiles":{"0.1":.2,"0.5":.5,"0.9":.8}}]
    result=through_deadline(rows,0,.2,5400)
    assert result[-1]["timestamp"]==5400
    assert result[-1]["quantiles"]["0.5"]==pytest.approx(.4)
    assert result[-1]["interpolated_model_point"]
    assert rows[-1]["timestamp"]==7200


@pytest.mark.parametrize('provider,offset',[(QuanturaProvider,3600),(KalshiProvider,0)])
def test_hourly_history_has_no_incomplete_or_filled_observations(provider,offset):
    p=provider();start=stamp('2026-09-26T00:00:00Z');end=start+4*3600
    calls=[]
    def request(path,body):
        calls.append(body)
        return {"rows":[{"timestamp":"2026-09-26T01:00:00Z","price":.4,"ask":.4},
                        {"timestamp":"2026-09-26T02:00:00Z","price":.5,"ask":.5,"is_forward_filled":True},
                        {"timestamp":"2026-09-26T04:30:00Z","price":.6,"ask":.6}]}
    p.request=request
    quotes=p.hourly_history(contract(),start,end)
    assert quotes==[Quote(start+3600+offset,.4,.4)]
    assert calls[0]['frequency']=='1h' and calls[0]['missing']=='leave'
    assert calls[0]['history_phase']=='pregame'


def test_late_model_completion_never_materializes_a_published_document():
    with pytest.raises(ValueError,match='GAME_START_HOUR_REACHED'):
        document(contract(),{},stamp('2026-09-26T23:00:00Z'))


def test_kalshi_retains_genuine_book_candles_without_trade_prices():
    p=KalshiProvider();start=stamp('2026-09-26T00:00:00Z')
    p.request=lambda *_: {"rows":[{"timestamp":"2026-09-26T01:00:00Z","price":None,"ask":.6,"bid":.5}]}
    assert p.hourly_history(contract(),start,start+7200)==[Quote(start+3600,.6,.6)]


def test_hourly_ensemble_advances_hours_and_does_not_call_regular_hour_steps_gaps(monkeypatch):
    from market_research import forecast
    from ensemble_forecasting.worker import execute_job
    monkeypatch.setattr(forecast,'execute_job',lambda job,**kw:execute_job(job,mock=True,**kw))
    result=forecast.forecast_window([Quote(3600,.4,.4),Quote(7200,.42,.42)],7,models=('prophet','chronos'),frequency='1h',failure_policy='fail')
    assert result['rows'][0]['timestamp']==10800
    assert result['rows'][-1]['timestamp']==32400
    assert result['history_gap_count']==0 and result['imputed_context_steps']==0


def test_pregame_summary_is_accepted_by_encrypted_artifact_packager(tmp_path, monkeypatch):
    import json
    import zipfile
    from market_research.pregame_screener import REPORT_FILENAME
    from market_research.artifact import package, decrypt

    source = tmp_path / "output"
    source.mkdir()
    report = {"successful": 12, "failed": 0}
    (source / REPORT_FILENAME).write_text(json.dumps(report))
    monkeypatch.setenv("QUANTURA_RESEARCH_ARTIFACT_KEY", bytes(range(32)).hex())
    result = package(source, tmp_path / "summary.qra.enc")
    assert result["format"] == "AES-256-GCM encrypted ZIP"
    decrypt(tmp_path / "summary.qra.enc", tmp_path / "summary.zip")
    with zipfile.ZipFile(tmp_path / "summary.zip") as archive:
        assert json.loads(archive.read(REPORT_FILENAME)) == report
        assert archive.namelist() == [REPORT_FILENAME, "manifest.json"]


def test_four_or_five_models_depend_on_real_history_and_approved_timesfm():
    assert game_models(31, True) == ('prophet','granite','chronos','timesfm')
    assert len(game_models(32, True)) == 5
    assert len(game_models(32, False)) == 4
    with pytest.raises(ValueError,match='INSUFFICIENT_HISTORY'):
        game_models(31, False)
    assert GAME_QUANTILES == (.01,.25,.5,.75,.9,.99)


def test_polymarket_schedule_uses_game_start_not_market_listing_date():
    p=QuanturaProvider()
    c={**contract(),"source":"polymarket_us","providerSymbol":"aec-game","contractId":"side-1"}
    p._schedule_request=lambda _: {"market":{"id":"market-1","slug":"aec-game","startDate":"2026-09-25T01:00:00Z","gameStartTime":"2026-09-27T01:30:00Z","marketSides":[{"id":"side-1","long":True}]}}
    verified=p.verify_schedule(c)
    assert verified['marketId']=='market-1'
    assert stamp(verified['eventStart'])==stamp('2026-09-27T01:30:00Z')
    assert game_date(stamp(verified['eventStart']))=='2026-09-26'
    p._schedule_request=lambda _: {"market":{"id":"market-1","slug":"aec-game","startDate":"2026-09-25T01:00:00Z","marketSides":[{"id":"side-1","long":True}]}}
    with pytest.raises(ValueError,match='SCHEDULE_MISSING'):
        p.verify_schedule(c)


def test_kalshi_start_requires_a_linked_unambiguous_milestone():
    p=KalshiProvider()
    def request(url):
        if '/events/' in url:return {'event':{'event_ticker':'GAME','event_metadata':{'occurrence_datetime':'2026-09-28T01:00Z'}},'markets':[{'ticker':'GAME'}]}
        return {'milestones':[{'primary_event_tickers':['OTHER'],'start_date':'2026-09-28T01:00Z'},{'related_event_tickers':['GAME'],'start_date':'2026-09-27T01:30Z'}]}
    p._schedule_request=request
    assert stamp(p.verify_schedule(contract())['eventStart'])==stamp('2026-09-27T01:30Z')
    p._schedule_request=lambda url:request(url) if '/events/' in url else {'milestones':[{'related_event_tickers':['GAME'],'start_date':'2026-09-27T01:30Z'},{'primary_event_tickers':['GAME'],'start_date':'2026-09-28T01:30Z'}]}
    with pytest.raises(ValueError,match='SCHEDULE_MISSING_OR_CONFLICTING'):
        p.verify_schedule(contract())


def test_retrospective_refresh_keeps_original_pregame_cutoff_and_is_labeled():
    from market_research import forecast
    from ensemble_forecasting.worker import execute_job
    # Exercise the real ensemble projection/quantile weighting with mock adapters.
    original=forecast.execute_job
    forecast.execute_job=lambda job,**kw:execute_job(job,mock=True,**kw)
    try:
        rows=[Quote(stamp('2026-09-25T14:00Z')+i*3600,.4,.4) for i in range(32)]
        result=forecast.forecast_window(rows,7,models=('prophet','granite','chronos','toto'),quantiles=GAME_QUANTILES,frequency='1h',failure_policy='fail')
        saved={'generated_at':'2026-09-26T21:10Z'}
        doc=document(contract(),result,stamp('2026-09-27T02:00Z'),saved)
        assert doc['observations']==[{'timestamp':r['timestamp'],'price':r['target']} for r in result['input_snapshot']]
        assert len(doc['observations'])==32
        assert doc['recomputed_at']==doc['generated_at']
        assert stamp(doc['input_cutoff'])<=stamp(doc['original_generated_at'])
        assert set(doc['predictions'][-1]['quantiles'])=={'0.01','0.25','0.5','0.75','0.9','0.99'}
        with pytest.raises(ValueError,match='ORIGINAL_CUTOFF_NOT_PREGAME'):
            document(contract('2026-09-26T20:30Z'),result,stamp('2026-09-27T02:00Z'),saved)
    finally:
        forecast.execute_job=original


def test_kalshi_schedule_accepts_nested_markets_when_top_level_is_empty():
    p=KalshiProvider()
    p._schedule_request=lambda url: ({'event':{'event_ticker':'GAME','markets':[{'ticker':'GAME','status':'open'}]},'markets':[]} if '/events/' in url else {'milestones':[{'primary_event_tickers':['GAME'],'start_date':'2026-09-27T01:30Z'}]})
    assert stamp(p.verify_schedule(contract())['eventStart'])==stamp('2026-09-27T01:30Z')


def screener_dependencies(monkeypatch, tmp_path, discover, status_error=False):
    from types import SimpleNamespace
    from market_research import pregame_screener, store

    class Database:
        status = None
        def collection(self, name):
            return self
        def where(self, *args):
            return self
        def limit(self, *args):
            return self
        def stream(self):
            return []
        def batch(self):
            return self
        def document(self, name):
            return self
        def set(self, value):
            if status_error:
                raise RuntimeError("database is unavailable")
            self.status = value.copy()

    db = Database()
    monkeypatch.setattr(store, 'Store', lambda *args: SimpleNamespace(db=db))
    monkeypatch.setattr(pregame_screener, 'KalshiProvider', lambda: SimpleNamespace(discover=discover))
    monkeypatch.setenv('QUANTURA_RESEARCH_DIR', str(tmp_path / 'not-created-yet'))
    monkeypatch.setenv('QUANTURA_RESEARCH_ARTIFACT_KEY', '12' * 32)
    return pregame_screener, db


@pytest.mark.parametrize('status_error', [False, True])
def test_discovery_rate_limit_preserves_encrypted_failure_without_masking_root_cause(tmp_path, monkeypatch, status_error):
    import json
    import zipfile
    from market_research.artifact import package, decrypt

    def discover(*args):
        raise RuntimeError('DATA_HTTP_429_RATE_LIMITED')
    worker, db = screener_dependencies(monkeypatch, tmp_path, discover, status_error)
    with pytest.raises(RuntimeError, match='^DATA_HTTP_429_RATE_LIMITED$'):
        worker.run('kalshi', 500, shard=0, shards=2)
    source = tmp_path / 'not-created-yet'
    report = json.loads((source / worker.REPORT_FILENAME).read_text())
    assert report['run_status'] == 'failed' and report['partial']
    assert report['failure_stage'] == 'discovery'
    assert report['error_code'] == 'DATA_HTTP_429_RATE_LIMITED'
    assert report['successful'] == report['discovery_pages'] == 0
    if status_error:
        assert report['status_error_code'] == 'PREGAME_STATUS_PUBLICATION_FAILED'
    else:
        assert db.status == report
    package(source, tmp_path / 'failed.enc')
    decrypt(tmp_path / 'failed.enc', tmp_path / 'verified.zip')
    with zipfile.ZipFile(tmp_path / 'verified.zip') as archive:
        assert archive.namelist() == [worker.REPORT_FILENAME, 'manifest.json']
        assert json.loads(archive.read(worker.REPORT_FILENAME)) == report


def test_bootstrap_failure_preserves_report_without_exception_credentials(tmp_path, monkeypatch):
    import json
    from market_research import store
    worker, _ = screener_dependencies(monkeypatch, tmp_path, lambda *args: ([], {}))
    def fail(*args):
        raise RuntimeError('credential-value-must-never-be-archived')
    monkeypatch.setattr(store, 'Store', fail)
    with pytest.raises(RuntimeError):
        worker.run('kalshi', 500)
    text = (tmp_path / 'not-created-yet' / worker.REPORT_FILENAME).read_text()
    report = json.loads(text)
    assert report['failure_stage'] == 'initialize'
    assert report['error_code'] == 'PREGAME_WORKER_FAILED'
    assert 'credential-value' not in text


def test_discovery_is_paced_one_page_at_a_time_and_empty_day_completes(tmp_path, monkeypatch):
    import json
    calls, delays = [], []
    def discover(mode, pages, cursor):
        calls.append((mode, pages, cursor))
        return [], {'next_cursor': 'next-page' if cursor == '0' else None}
    worker, db = screener_dependencies(monkeypatch, tmp_path, discover)
    monkeypatch.setattr(worker.time, 'sleep', delays.append)
    worker.run('kalshi', 500)
    assert calls == [('premarket', 1, '0'), ('premarket', 1, 'next-page')]
    assert delays == [1.25]
    report = json.loads((tmp_path / 'not-created-yet' / worker.REPORT_FILENAME).read_text())
    assert report['discovery_pages'] == 2 and report['eligible'] == 0
    assert report['run_status'] == 'completed' and not report['partial']
    assert db.status == report


def test_status_publication_failure_keeps_local_report_and_fails_job(tmp_path, monkeypatch):
    import json
    worker, _ = screener_dependencies(monkeypatch, tmp_path, lambda *args: ([], {'next_cursor': None}), True)
    with pytest.raises(RuntimeError, match='^PREGAME_STATUS_PUBLICATION_FAILED$'):
        worker.run('kalshi', 500)
    report = json.loads((tmp_path / 'not-created-yet' / worker.REPORT_FILENAME).read_text())
    assert report['run_status'] == 'failed'
    assert report['failure_stage'] == 'status' and report['partial']
