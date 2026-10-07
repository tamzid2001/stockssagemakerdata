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
