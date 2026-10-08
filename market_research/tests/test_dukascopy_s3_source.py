from datetime import date
import hashlib
from io import BytesIO
import json
import lzma
from pathlib import Path

import pytest

from market_research import dukascopy_s3_source as source
from market_research.ftmo_dukas_data import digest, load_quotes
from market_research.ftmo_p99_study import daily_sessions, load_minutes


DAY = date(2026, 10, 6)


def archive(*records):
    return lzma.compress(b''.join(source.RECORD.pack(*r) for r in records))


class ProviderError(Exception):
    def __init__(self, code):
        self.response = {'Error': {'Code': code}}
        super().__init__('Untrusted provider details must not reach artifacts')


class ArchiveClient:
    def __init__(self, objects):
        self.objects = objects
        self.calls = []
        self.streams = []

    def get_object(self, **kwargs):
        self.calls.append(kwargs)
        value = self.objects.get(kwargs['Key'])
        if value is None:
            raise ProviderError('NoSuchKey')
        body = BytesIO(value)
        self.streams.append(body)
        return {'Body': body, 'ContentLength': len(value), 'VersionId': 'provider-version',
                'ETag': '"'+hashlib.md5(value, usedforsecurity=False).hexdigest()+'"'}


def keys(day=DAY, symbol='EURUSD'):
    return [f'{symbol}/{day.year}/{day.month-1:02d}/{day.day:02d}/{side}_candles_min_1.bi5'
            for side in ('BID', 'ASK')]


@pytest.mark.parametrize('scale,price', [(5,112167), (3,158316), (3,4136195), (1,857713)])
def test_native_field_order_and_instrument_scaling(scale, price):
    row = source.decode_candles(archive((120, price-2, price, price-3, price+1, 2.5)), DAY, scale)[0]
    assert row == {'start':'2026-10-06T00:02:00+00:00', 'o':(price-2)/10**scale,
                   'c':price/10**scale, 'l':(price-3)/10**scale, 'h':(price+1)/10**scale, 'observed':True}


def test_all_ten_provider_prefixes_use_catalog_scales():
    expected = {'EURUSD.sim':('EURUSD',5), 'USDJPY.sim':('USDJPY',3), 'GBPUSD.sim':('GBPUSD',5),
                'GBPJPY.sim':('GBPJPY',3), 'USDCAD.sim':('USDCAD',5), 'US500.sim':('USA500IDXUSD',3),
                'US30.sim':('USA30IDXUSD',3), 'US100.sim':('USATECHIDXUSD',3),
                'XAUUSD.sim':('XAUUSD',3), 'BTCUSD.sim':('BTCUSD',1)}
    for symbol, pair in expected.items():
        item = source.instrument(symbol)
        assert (item['archive_symbol'], item['priceScale']) == pair
        assert len(item['catalog_sha256']) == 64
    with pytest.raises(ValueError, match='APPROVED_FTMO'):
        source.instrument('ANYTHING')


def test_pairing_excludes_stale_placeholders_but_keeps_either_side_observed():
    bid = archive((0,100000,100000,100000,100000,0),
                  (60,100000,100002,100000,100003,0),
                  (120,100000,100000,100000,100000,0))
    ask = archive((0,100010,100010,100010,100010,0),
                  (60,100010,100010,100010,100010,0),
                  (120,100010,100010,100010,100010,1))
    s3 = ArchiveClient(dict(zip(keys(), (bid,ask))))
    rows,audit = source.download_day(s3,source.instrument('EURUSD.sim'),DAY)
    assert [r['start'] for r in rows] == ['2026-10-06T00:01:00+00:00','2026-10-06T00:02:00+00:00']
    assert rows[0]['end'] == '2026-10-06T00:02:00+00:00'
    assert audit['excluded_placeholder_minutes'] == 1
    assert all(r['version_id']=='provider-version' and len(r['compressed_sha256'])==64 for r in audit['files'])
    assert all(c['Bucket']==source.BUCKET and c['RequestPayer']=='requester' and c['ChecksumMode']=='ENABLED' for c in s3.calls)
    assert all(stream.closed for stream in s3.streams)


@pytest.mark.parametrize('records', [
    [(1,100,100,100,100,1)], [(86400,100,100,100,100,1)],
    [(60,100,100,100,100,1),(60,100,100,100,100,1)],
    [(0,100,102,103,104,1)], [(0,100,100,100,100,-1)],
    [(0,100,100,100,100,float('nan'))], [(0,0,0,0,0,1)],
])
def test_malformed_records_fail(records):
    with pytest.raises(ValueError):
        source.decode_candles(archive(*records),DAY,5)


@pytest.mark.parametrize('payload', [b'garbage', lzma.compress(b'odd'),
    archive((0,100,100,100,100,1))[:-5], lzma.compress(b'x'*(1440*24+1)),
    archive((0,100,100,100,100,1))+b'trailing'])
def test_corrupt_or_oversized_compression_fails(payload):
    with pytest.raises(ValueError):
        source.decode_candles(payload,DAY,5)


def test_missing_sides_and_unmatched_minutes_fail():
    bid = archive((0,100000,100000,100000,100000,1))
    ask = archive((60,100010,100010,100010,100010,1))
    with pytest.raises(ValueError,match='MISSING_ONE_SIDE'):
        source.download_day(ArchiveClient({keys()[0]:bid}),source.instrument('EURUSD.sim'),DAY)
    with pytest.raises(ValueError,match='UNMATCHED_NATIVE'):
        source.download_day(ArchiveClient(dict(zip(keys(),(bid,ask)))),source.instrument('EURUSD.sim'),DAY)
    rows,audit = source.download_day(ArchiveClient({}),source.instrument('EURUSD.sim'),DAY)
    assert rows==[] and all(r['absent'] for r in audit['files'])


def test_crossed_quote_fails():
    bid = archive((0,100,100,100,100,1))
    ask = archive((0,99,99,99,99,1))
    with pytest.raises(ValueError,match='INVALID_PAIRED'):
        source.download_day(ArchiveClient(dict(zip(keys(),(bid,ask)))),source.instrument('EURUSD.sim'),DAY)


def test_archive_integrity_and_closed_stream():
    body = BytesIO(b'payload')
    class BadETag:
        def get_object(self,**kwargs):
            return {'Body':body,'ContentLength':7,'ETag':'0'*32}
    with pytest.raises(ValueError,match='ARCHIVE_ETAG_INTEGRITY'):
        source.get_archive(BadETag(),'key')
    assert body.closed
    class Unauthorized:
        def get_object(self,**kwargs):
            raise ProviderError('AccessDenied')
    with pytest.raises(RuntimeError,match='^AUTHENTICATED_DUKASCOPY_ARCHIVE_FAILED$'):
        source.get_archive(Unauthorized(),'key')


def test_streamed_replay_digest_matches_existing_reader_and_no_placeholder_sessions(monkeypatch,tmp_path):
    bid = archive((0,100000,100000,100000,100000,0),
                  (16*3600,100000,100002,99999,100003,1),
                  (16*3600+60,100002,100001,100000,100004,1))
    ask = archive((0,100010,100010,100010,100010,0),
                  (16*3600,100010,100012,100009,100013,1),
                  (16*3600+60,100012,100011,100010,100014,1))
    s3 = ArchiveClient(dict(zip(keys(),(bid,ask))))
    monkeypatch.setattr(source,'client',lambda profile=None:s3)
    source.download('EURUSD.sim',DAY,DAY,tmp_path,workers=2,warmup_days=700)
    minutes=load_minutes(tmp_path)
    hours=load_quotes(tmp_path)
    assert len(minutes)==2 and len(hours)==1
    assert hours[0]['bid']=={'o':1.0,'h':1.00004,'l':.99999,'c':1.00001}
    assert len(daily_sessions(hours,'EURUSD.sim'))==1
    identity=json.loads((tmp_path/'minutes-source.json').read_text())
    assert identity['paired_minutes']==2 and identity['credentials_persisted'] is False
    assert identity['coverage']['excluded_placeholder_minutes']==1
    assert identity['coverage']['missing_intervals_filled'] is False
    assert identity['rows_sha256']==digest([{k:v for k,v in r.items() if not k.startswith('_')} for r in minutes])


def test_warming_history_does_not_accept_less_than_500_genuine_daily_candles(monkeypatch,tmp_path):
    from market_research import ftmo_p99_study as study
    monkeypatch.setattr(study,'download_authenticated',lambda *args,**kwargs:None)
    monkeypatch.setattr(study,'load_quotes',lambda folder:[])
    with pytest.raises(ValueError,match='INSUFFICIENT_500_DAILY_SESSION_WARMUP'):
        study.source('EURUSD.sim',DAY,DAY,tmp_path)


def test_oidc_trust_and_archive_only_policy_are_scoped():
    infra=Path(__file__).resolve().parents[1]/'infra'
    trust=json.loads((infra/'dukascopy-history-trust.json').read_text())['Statement'][0]
    assert trust['Action']=='sts:AssumeRoleWithWebIdentity'
    assert trust['Condition']['StringEquals']=={
        'token.actions.githubusercontent.com:aud':'sts.amazonaws.com',
        'token.actions.githubusercontent.com:sub':'repo:tamzid2001/stockssagemakerdata:ref:refs/heads/main'}
    policy=json.loads((infra/'dukascopy-history-read.json').read_text())['Statement'][0]
    assert policy['Action']=='s3:GetObject' and len(policy['Resource'])==20
    assert all(r.startswith('arn:aws:s3:::'+source.BUCKET+'/') and r.endswith('_candles_min_1.bi5') for r in policy['Resource'])
