import json
from pathlib import Path
import pytest

from market_research.cloud_checkpoint import write_pointer, read_pointer, compatible_configuration, MAGIC


def pointer():
    cid='p90-'+'a'*24;sha='b'*64
    return {'version':1,'campaign_id':cid,'archive':{'object':f'private-research/p90-campaigns/{cid}/checkpoint/{sha}.enc',
             'sha256':sha,'bytes':100,'generation':'123'}}


def test_authenticated_small_pointer_no_plaintext_credentials(tmp_path,monkeypatch):
    monkeypatch.setenv('QUANTURA_RESEARCH_ARTIFACT_KEY','45'*32)
    p=tmp_path/'pointer.enc';write_pointer(pointer(),p)
    assert p.stat().st_size<8192 and p.read_bytes()[:4]==MAGIC
    assert b'private-research' not in p.read_bytes()
    assert read_pointer(p)==pointer()
    bad=tmp_path/'bad';bad.write_bytes(p.read_bytes()[:-1]+bytes([p.read_bytes()[-1]^1]))
    with pytest.raises(Exception):read_pointer(bad)


@pytest.mark.parametrize('mutate',[
    lambda p:p['archive'].update(object='../outside'),
    lambda p:p['archive'].update(object='https://example.com/private.enc'),
    lambda p:p['archive'].update(bytes=2**40),
    lambda p:p.update(campaign_id='other'),
    lambda p:p['archive'].update(generation='not-a-generation')])
def test_cloud_pointer_cannot_escape_private_allowlist(tmp_path,monkeypatch,mutate):
    monkeypatch.setenv('QUANTURA_RESEARCH_ARTIFACT_KEY','45'*32)
    obj=pointer();mutate(obj);p=tmp_path/'bad.enc';write_pointer(obj,p)
    with pytest.raises(ValueError):read_pointer(p)


def test_migration_only_changes_execution_code_and_preserves_original_configuration(monkeypatch):
    old={'version':'p1','horizon':30,'code_sha':'a'*40}
    new={**old,'code_sha':'b'*40}
    with pytest.raises(RuntimeError):compatible_configuration([old],new)
    monkeypatch.setenv('QUANTURA_CLOUD_PAPER_CHECKPOINTS','true')
    assert compatible_configuration([old],new)==old
    with pytest.raises(RuntimeError):compatible_configuration([old],{**new,'horizon':15})
    with pytest.raises(RuntimeError):compatible_configuration([old,old],new)


def test_cloud_restore_routes_after_authentication_without_reset(monkeypatch,tmp_path):
    from market_research.artifact import restore
    calls=[]
    monkeypatch.setattr('market_research.cloud_checkpoint.restore_pointer',lambda s,d,p:calls.append((s,d,p)))
    source=tmp_path/'pointer';source.write_bytes(MAGIC+b'not-a-real-pointer')
    restore(source,tmp_path/'target',preserve_results=True)
    assert calls==[(source,tmp_path/'target',True)]
