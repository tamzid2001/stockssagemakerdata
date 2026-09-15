from copy import deepcopy
import json
import zipfile
import pytest

from market_research.artifact import snapshot_package, decrypt, restore
from market_research.local_store import LocalStore
from market_research.p1_oco import VERSION, advance_book, arm_minute, initial_book, replace_orders, summary
from market_research.tests.test_p1_oco import q, forecasts


def test_encrypted_online_checkpoint_replays_without_duplicate_fills(tmp_path,monkeypatch):
    monkeypatch.setenv("QUANTURA_RESEARCH_ARTIFACT_KEY","12"*32)
    store=LocalStore("p1","first",tmp_path/"first")
    store.claim({"version":VERSION})
    book=initial_book()
    events=replace_orders(book,forecasts(),{"a":q(100,.5),"b":q(100,.5)},100)
    store.save("p1-game:g",book,None,[{**e,"game_id":"g"} for e in events],{})
    for t in (160,220):
        events=advance_book(book,{"a":q(t,.5),"b":q(t,.5)},t)
        events+=arm_minute(book,{"a":q(t,.5),"b":q(t,.5)},t)
        store.save("p1-game:g",book,None,[{**e,"game_id":"g"} for e in events],{})
    events=advance_book(book,{"a":q(280,.19),"b":q(280,.8)},280)
    store.save("p1-game:g",book,None,[{**e,"game_id":"g"} for e in events],{})
    store.report("r",{"levels":summary([book]),"paper_only":True})
    encrypted=tmp_path/"checkpoint.enc"
    metadata=snapshot_package(store.directory,encrypted)
    assert metadata["format"]=="AES-256-GCM encrypted ZIP"
    clear=tmp_path/"verified.zip";decrypt(encrypted,clear)
    with zipfile.ZipFile(clear) as archive:
        assert {"research.sqlite3","p1_orders_and_fills.csv.gz","p1_forecast_quantiles.csv.gz","p1_summary.json","manifest.json"}<=set(archive.namelist())
        report=json.loads(archive.read("p1_summary.json"))
        assert report["levels"]["0.5"]["averaging_fills"]==2
        assert report["levels"]["0.5"]["open_shares"]==3
    restore(encrypted,tmp_path/"resumed",preserve_results=True)
    resumed=LocalStore("p1","second",tmp_path/"resumed");resumed.claim({"version":VERSION})
    recovered=resumed.load("p1-game:g")["state"]
    assert recovered==book
    assert not advance_book(recovered,{"a":q(280,.19),"b":q(280,.8)},280)
    old_count=len(resumed.values("trades"))
    resumed.save("p1-game:g",recovered,None,[{**e,"game_id":"g"} for e in events],{})
    assert len(resumed.values("trades"))==old_count
    resumed.db.close();store.db.close()


def test_empty_failure_report_is_still_a_recoverable_artifact(tmp_path,monkeypatch):
    from market_research.p1_worker import export_report
    monkeypatch.setenv("QUANTURA_RESEARCH_ARTIFACT_KEY","34"*32)
    store=LocalStore("p1","holder",tmp_path/"output");store.claim({"version":VERSION})
    export_report(store,{"version":VERSION},{"forecasts":0},[],"PROVIDER_UNAVAILABLE")
    snapshot_package(store.directory,tmp_path/"failed.enc")
    assert (tmp_path/"failed.enc").stat().st_size>32
    assert store.values("reports")[-1]["report"]["levels"]["0.1"]["win_rate"] is None
    store.db.close()


def test_side_groups_require_complete_soccer_set():
    from market_research.p1_worker import game_groups
    rows=[{"eventId":"e","marketId":str(m),"contractId":f"{m}:{s}","side":s,
        "marketType":"SPORTS_MARKET_TYPE_DRAWABLE_OUTCOME"} for m in range(3) for s in ("long","short")]
    assert len(game_groups(rows)["e"])==6
    assert not game_groups(rows[:-1])


def test_live_p1_executes_locally_and_cloud_checkpoints_preserve_books():
    from pathlib import Path
    import yaml
    root=Path(__file__).resolve().parents[2]
    code=(root/"market_research/p1_worker.py").read_text()
    assert 'store=LocalStore(' in code and 'store=Store(' not in code
    assert 'choices=[32],default=32' in code
    assert 'arm_minute(book,quotes,timestamp' in code
    workflow=yaml.safe_load((root/".github/workflows/polymarket-live-paper.yml").read_text())
    p1=next(s for s in workflow["jobs"]["monitor"]["steps"] if s.get("id")=="p1_worker")
    assert p1['env']['QUANTURA_CLOUD_PAPER_CHECKPOINTS']=='true'
    assert 'FIREBASE_SERVICE_ACCOUNT_JSON' in p1['env']
    assert 'QUANTURA_RESEARCH_ARTIFACT_KEY' in p1["env"]
    script=(root/"market_research/node/run.mjs").read_text()
    assert 'paper || btc ? 5 * 60 * 1000' in script and 'await publish();' in script
