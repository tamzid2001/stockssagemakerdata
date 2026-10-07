import gzip
import json
import pytest
from market_research.public_snapshots import write, unpack

def test_public_snapshot_roundtrip_and_feed_boundary(tmp_path):
    path = write("games-kalshi-0", {"items": [{"id": "a" * 32}], "status": {}}, tmp_path)
    value = unpack(path.read_bytes(), "games-kalshi-0")
    assert value["data"]["items"][0]["id"] == "a" * 32
    with pytest.raises(ValueError):
        unpack(path.read_bytes(), "perps-0")
    with pytest.raises(ValueError):
        write("uploaded_csv", {"items": []}, tmp_path)

def test_incomplete_or_nonfinite_publication_is_not_written(tmp_path):
    with pytest.raises(ValueError):
        write("stocks", {"items": [{"price": float("nan")}]}, tmp_path)
    assert not (tmp_path / "snapshot.json.gz").exists()


def test_previous_uses_upload_timestamp_when_artifact_ids_arrive_out_of_order(tmp_path, monkeypatch):
    import io
    import zipfile
    import requests
    from market_research.public_snapshots import previous
    packed=write('perps-0', {'items':[{'ticker':'NEW'}]},tmp_path).read_bytes()
    output=io.BytesIO()
    with zipfile.ZipFile(output,'w') as archive:archive.writestr('snapshot.json.gz',packed)
    downloaded=[]
    class Response:
        status_code=200
        def __init__(self,data=None,location=None):self.value=data;self.headers={'Location':location} if location else {};self.status_code=302 if location else 200
        def raise_for_status(self):pass
        def json(self):return self.value
        def iter_content(self,*_):yield output.getvalue()
    class Session:
        def get(self,url,**kwargs):
            if url.endswith('/actions/artifacts'):
                base={'expired':False,'workflow_run':{'id':2,'head_branch':'main','head_sha':'abc','head_repository_id':3,'repository_id':3}}
                assert kwargs['params']['per_page']==100
                return Response({'artifacts':[{**base,'id':99,'created_at':'2026-10-07T14:41:00Z'},{**base,'id':1,'created_at':'2026-10-07T14:49:00Z'}]})
            if '/actions/runs/' in url:return Response({'path':'.github/workflows/hourly-perpetual-screener.yml','head_sha':'abc'})
            downloaded.append(int(url.split('/')[-2]));return Response(location='https://example.blob.core.windows.net/latest')
    monkeypatch.setattr(requests,'Session',Session)
    monkeypatch.setattr(requests,'get',lambda *_args,**kwargs:Response())
    assert previous('perps-0')['items']==[{'ticker':'NEW'}]
    assert downloaded==[1]
