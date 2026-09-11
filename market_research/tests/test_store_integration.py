"""Real Firestore transactions against the isolated Firebase emulator only."""

import os
import uuid
import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("FIRESTORE_EMULATOR_HOST"), reason="Firestore emulator required"
)


def test_checkpoint_immutability_fencing_and_atomic_outcome_replay():
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import firestore
    from market_research.store import Store
    from market_research.engine import digest

    db = firestore.Client(
        project="quantura-forecast-integration", credentials=AnonymousCredentials()
    )
    session = "test-paper-" + uuid.uuid4().hex

    def make(holder):
        store = Store.__new__(Store)
        store.db = db
        store.fs = firestore
        store.session = session
        store.holder = holder
        store.fence = None
        store.ref = db.collection("market_research_sessions").document(session)
        return store

    first = make("first")
    second = make("second")
    config = {"models": ["prophet", "chronos"], "horizon": 30}
    first.claim(config)
    with pytest.raises(RuntimeError, match="LEASE_HELD"):
        second.claim(config)
    fid = digest(session)
    forecast = {"forecast_id": fid, "probability": 0.5}
    outcome = {"contract_id": "test", "net_pnl": 0.01}
    first.save("test", {"last_timestamp": 60}, forecast, [outcome])
    first.save("test", {"last_timestamp": 60}, forecast, [outcome])
    assert first.load("test")["state"]["last_timestamp"] == 60
    assert (
        len(
            list(
                db.collection("market_research_trades")
                .where("session", "==", session)
                .stream()
            )
        )
        == 1
    )
    first.save("test", {"last_timestamp": 120}, {**forecast, "probability": 0.9}, [])
    immutable = db.collection("market_research_forecasts").document(fid).get().to_dict()
    assert immutable["forecast"]["probability"] == 0.5
    contract = {"providerSymbol": session, "side": "long"}
    raw = [
        {
            "timestamp": "2026-09-01T00:00:00Z",
            "long_price": 0.6,
            "short_price": 0.42,
            "selected_position": "long",
            "raw": {"selected_position": "long"},
        }
    ]
    archived = first.archive_history(contract, 0, 1000, raw)
    assert first.archived_history(contract, 0, 1000) == raw
    opposite = first.archived_history({**contract, "side": "short"}, 0, 1000)
    assert opposite[0]["selected_position"] == "short"
    revised = [{**raw[0], "long_price": 0.61}]
    newer = first.archive_history(contract, 0, 1000, revised)
    assert newer["snapshot_id"] != archived["snapshot_id"]
    assert (
        first.archive_ref.collection("snapshots")
        .document(archived["snapshot_id"])
        .get()
        .exists
    )
    assert first.archived_history(contract, 0, 1000) == revised
    catalog = first.save_catalog([contract], {"next_cursor": None}, 1000)
    assert first.load_catalog(catalog)[0] == [contract]
    # A heartbeat renew and a write share one serialized local transaction path.
    import threading

    renewal = threading.Thread(target=first.renew)
    renewal.start()
    first.archive_progress(
        catalog, 0, contract, {"status": "downloaded"}, {"raw_rows": 1}
    )
    renewal.join(timeout=10)
    assert not renewal.is_alive()
    # Kalshi's same-named symbols cannot address the Polymarket archive.
    kalshi = make("kalshi-holder")
    kalshi.session = "kalshi-test-" + session
    kalshi.ref = db.collection("market_research_sessions").document(kalshi.session)
    kalshi.claim(config)
    assert kalshi.archived_history(contract, 0, 1000) is None
    kalshi.archive_history(
        {**contract, "source": "kalshi", "side": "yes"}, 0, 1000, raw
    )
    assert kalshi.archive_ref.path != first.archive_ref.path
    kalshi.release()
    first.release()
    second.claim(config)
    with pytest.raises(RuntimeError, match="LEASE_LOST"):
        first.save("test", {"last_timestamp": 180}, None, [])
    assert second.load("test")["state"]["last_timestamp"] == 120
    second.release()
    with pytest.raises(RuntimeError, match="SESSION_CONFIGURATION_CHANGED"):
        make("third").claim({"horizon": 60})
