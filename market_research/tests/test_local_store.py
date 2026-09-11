import json
import zipfile
import pytest

from market_research.local_store import LocalStore


def test_local_history_immutable_forecasts_and_idempotent_trades(tmp_path):
    store = LocalStore("historical", "test", tmp_path)
    store.claim({"horizon": 30})
    forecast = {"forecast_id": "f1", "rows": [{"p10": 0.2}]}
    trade = {"net_pnl": -0.1}
    store.save("c", {"last_timestamp": 60}, forecast, [trade])
    store.save("c", {"last_timestamp": 120}, {**forecast, "rows": []}, [trade])
    assert store._get("forecasts", "f1") == forecast
    assert store.load("c")["state"]["last_timestamp"] == 120
    assert (
        store.db.execute("SELECT count(*) FROM records WHERE kind='trades'").fetchone()[
            0
        ]
        == 1
    )
    c = {"providerSymbol": "GAME", "side": "long"}
    rows = [
        {"timestamp": "2026-09-01T00:00:00Z", "long_price": 0.4, "short_price": 0.62}
    ]
    first = store.archive_history(c, 0, 10, rows)
    second = store.archive_history(c, 0, 10, [{**rows[0], "long_price": 0.41}])
    assert first["snapshot_id"] != second["snapshot_id"]
    assert store._get("snapshots", first["snapshot_id"])["rows"] == rows
    assert store.archived_history({**c, "source": "kalshi"}, 0, 10) is None
    store.report("test", {"coverage": 1})
    store.release()
    assert not store.at_capacity
    restored = LocalStore("historical", "retry", tmp_path)
    restored.claim({"horizon": 30})
    assert restored.load("c")["state"]["last_timestamp"] == 120
    with pytest.raises(RuntimeError, match="CONFIGURATION"):
        restored.claim({"horizon": 60})


def test_encrypted_zip_roundtrip_authentication_and_secret_exclusion(
    tmp_path, monkeypatch
):
    pytest.importorskip("cryptography")
    from cryptography.exceptions import InvalidTag
    from market_research.artifact import package, decrypt, restore

    monkeypatch.setenv("QUANTURA_RESEARCH_ARTIFACT_KEY", "a" * 64)
    directory = tmp_path / "data"
    store = LocalStore("s", "run", directory)
    store.report("run", {"net_pnl": -1.0})
    catalog = store.save_catalog([], {"next_cursor": None}, 1000)
    (directory / ".env").write_text("NOT_ALLOWED_IN_ARCHIVE")
    encrypted, clear = tmp_path / "result.enc", tmp_path / "clear.zip"
    result = package(directory, encrypted)
    assert result["bytes"] < 25 * 1024 * 1024
    assert b"net_pnl" not in encrypted.read_bytes()
    decrypt(encrypted, clear)
    with zipfile.ZipFile(clear) as archive:
        assert ".env" not in archive.namelist()
        assert "research.sqlite3" in archive.namelist()
        assert json.loads(archive.read("manifest.json"))["schema_version"] == 1
    restore(encrypted, tmp_path / "restored")
    recovered = LocalStore("s", "continuation", tmp_path / "restored")
    assert recovered.load_catalog(catalog)[0] == []
    assert (
        recovered._get("reports", "run") is None
    )  # prior results stay in original ZIP
    tampered = bytearray(encrypted.read_bytes())
    tampered[-1] ^= 1
    encrypted.write_bytes(tampered)
    with pytest.raises(InvalidTag):
        decrypt(encrypted, tmp_path / "bad.zip")
    assert not (tmp_path / "bad.zip").exists()


def test_missing_key_fails_before_creating_artifact(tmp_path, monkeypatch):
    from market_research.artifact import key_bytes

    monkeypatch.delenv("QUANTURA_RESEARCH_ARTIFACT_KEY", raising=False)
    with pytest.raises(ValueError, match="KEY_REQUIRED"):
        key_bytes()
    monkeypatch.setenv("QUANTURA_RESEARCH_ARTIFACT_KEY", "a" * 32 + " " * 32)
    with pytest.raises(ValueError, match="KEY_INVALID"):
        key_bytes()
