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
    first.release()
    second.claim(config)
    with pytest.raises(RuntimeError, match="LEASE_LOST"):
        first.save("test", {"last_timestamp": 180}, None, [])
    assert second.load("test")["state"]["last_timestamp"] == 120
    second.release()
    with pytest.raises(RuntimeError, match="SESSION_CONFIGURATION_CHANGED"):
        make("third").claim({"horizon": 60})
