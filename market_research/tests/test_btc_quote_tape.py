import asyncio
import base64
from copy import deepcopy
import json
import sys
from types import SimpleNamespace

import pytest

from market_research import btc_quote_tape as tape
from market_research.local_store import LocalStore
from market_research.tests.test_btc_horizons import MARKET,OPEN


def snapshot(seq=1):
    return {'type':'orderbook_snapshot','sid':2,'seq':seq,'msg':{'market_ticker':MARKET['ticker'],
        'yes_dollars_fp':[['0.4','10']],'no_dollars_fp':[['0.5','10']]}}


def delta(seq=2,ms=None,**fields):
    return {'type':'orderbook_delta','sid':2,'seq':seq,'msg':{'market_ticker':MARKET['ticker'],
        'price_dollars':'0.4','delta_fp':'1','side':'yes','ts_ms':OPEN*1000+seq if ms is None else ms,**fields}}


def book():
    b=tape.Book(deepcopy(MARKET),'test-connection')
    assert b.accept(snapshot(),OPEN*1000-1000)==[]
    return b


@pytest.mark.parametrize('count',tape.COUNTS)
def test_genuine_quote_prefix_preserves_milliseconds_and_count_not_seconds(count):
    b=book()
    for seq in range(2,count+2):b.accept(delta(seq,OPEN*1000+(seq//3)),OPEN*1000+seq)
    for side in ('yes','no'):
        result=b.first(side,count)
        assert len(result['rows'])==count and result['quote_count']==count
        assert result['elapsed_from_open_seconds']<1
        assert result['input_span_seconds']<1
        assert result['timebase']=='exchange_event_order'
        assert result['forecast_status']=='not_run_event_time_model_validation_required'
        assert len({r['sequence'] for r in result['rows']})==count
        assert len({r['exchange_timestamp_ms'] for r in result['rows']})<count
        assert all(not r['synthetic'] for r in result['rows'])
    assert b.rows['yes'][0]['ask']=='0.5' and b.rows['no'][0]['ask']=='0.6'


def test_quiet_seconds_deep_deltas_and_snapshot_do_not_invent_quotes():
    b=book()
    assert not b.rows['yes']
    assert b.accept(delta(price_dollars='0.1'),OPEN*1000+100)==[]
    assert b.accept({'type':'ticker','msg':{'volume':100}},OPEN*1000+1000)==[]
    assert not b.rows['yes']


@pytest.mark.parametrize('fault',[lambda:delta(4),lambda:delta(1),
                                 lambda:delta(2,OPEN*1000+100_000)])
def test_gap_replay_and_noncausal_timestamp_disqualify_prefix(fault):
    b=book()
    with pytest.raises(ValueError):b.accept(fault(),OPEN*1000+100)
    assert not b.eligible
    with pytest.raises(ValueError,match='COMPLETE_OPENING'):b.first('yes',32)


def test_late_join_cannot_claim_first_quotes_of_market():
    b=tape.Book(MARKET,'late')
    b.accept(snapshot(),OPEN*1000+100)
    for seq in range(2,34):b.accept(delta(seq,OPEN*1000+200+seq),OPEN*1000+300+seq)
    assert len(b.rows['yes'])==32 and not b.eligible
    with pytest.raises(ValueError,match='COMPLETE_OPENING'):b.first('yes',32)


def test_preopen_and_postclose_updates_never_count_as_ingame_context():
    b=book()
    b.accept(delta(2,OPEN*1000-500),OPEN*1000-400)
    b.accept(delta(3,(OPEN+900)*1000),(OPEN+900)*1000+1)
    assert not b.rows['yes']


def test_invalid_market_and_crossed_book_rejected():
    b=book();d=delta(market_ticker='KXBTC15M-OTHER')
    with pytest.raises(ValueError,match='MISMATCH'):b.accept(d,OPEN*1000+10)
    with pytest.raises(ValueError,match='CROSSED'):b.accept(delta(price_dollars='.7'),OPEN*1000+10)


def test_prefixes_saved_once_without_raw_private_fields(tmp_path):
    b=book();s=LocalStore('test','test',tmp_path)
    for seq in range(2,123):
        rows=b.accept(delta(seq,client_order_id='do-not-persist'),OPEN*1000+seq)
        tape.save_update(s,b,rows)
    assert len(s.values('quote_windows'))==8
    before=s.values('quote_windows')
    tape.save_update(s,b,[])
    assert s.values('quote_windows')==before
    assert 'do-not-persist' not in json.dumps(s.values('exchange_quotes'))
    assert s.values('quote_coverage')[0]['eligible_from_open']


def test_rsa_signer_matches_official_path_and_never_emits_private_key(monkeypatch):
    from cryptography.hazmat.primitives.asymmetric import rsa,padding
    from cryptography.hazmat.primitives import hashes,serialization
    key=rsa.generate_private_key(public_exponent=65537,key_size=2048)
    pem=key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,
                          serialization.NoEncryption()).decode()
    monkeypatch.setenv('KALSHI_PROD_API_KEY','test-key')
    monkeypatch.setenv('KALSHI_PRIVATE_KEY',pem.replace('\n','\\n'))
    h=tape.auth_headers(1234)
    key.public_key().verify(base64.b64decode(h['KALSHI-ACCESS-SIGNATURE']),
        b'1234GET/trade-api/ws/v2',padding.PSS(mgf=padding.MGF1(hashes.SHA256()),salt_length=32),hashes.SHA256())
    assert 'PRIVATE' not in json.dumps(h) and h['KALSHI-ACCESS-TIMESTAMP']=='1234'
    monkeypatch.delenv('KALSHI_PRIVATE_KEY')
    with pytest.raises(ValueError,match='CREDENTIALS_REQUIRED'):tape.auth_headers()


def test_capture_subscribes_read_only_and_persists_actual_events(tmp_path,monkeypatch):
    s=LocalStore('test','test',tmp_path);messages=[snapshot()]+[delta(i) for i in range(2,34)]
    clock=[float(OPEN)-1];sent=[]
    class Socket:
        async def __aenter__(self):return self
        async def __aexit__(self,*args):return False
        async def send(self,value):sent.append(json.loads(value))
        async def recv(self):
            if not messages:
                clock[0]=OPEN+901
                raise asyncio.TimeoutError()
            value=messages.pop(0)
            clock[0]=OPEN-1 if value['type']=='orderbook_snapshot' else OPEN+.1
            return json.dumps(value)
    def connect(url,**kw):
        assert url==tape.WS_URL and kw['additional_headers']=={'test':'auth'}
        return Socket()
    monkeypatch.setitem(sys.modules,'websockets',SimpleNamespace(connect=connect))
    monkeypatch.setattr(tape,'auth_headers',lambda:{'test':'auth'})
    monkeypatch.setattr(tape.time,'time',lambda:clock[0])
    monkeypatch.setattr(tape.time,'time_ns',lambda:int(clock[0]*1e9))
    asyncio.run(tape.capture(MARKET,s,tape.time.monotonic()+10))
    assert len(s.values('exchange_quotes'))==64
    assert len(s.values('quote_windows'))==2
    assert sent==[{'id':1,'cmd':'subscribe','params':{'channels':['orderbook_delta'],'market_tickers':[MARKET['ticker']]}}]
