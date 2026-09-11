from types import SimpleNamespace

import pytest


def test_read_phase_aborts_retry_fresh_transactions_without_bypassing_fence(
    monkeypatch,
):
    exceptions = pytest.importorskip("google.api_core.exceptions")
    from market_research.store import Store

    store = Store.__new__(Store)
    store.db = SimpleNamespace(transaction=object)
    transactions = []
    monkeypatch.setattr("market_research.store.time.sleep", lambda _: None)

    def operation(tx):
        transactions.append(tx)
        if len(transactions) < 3:
            raise exceptions.Aborted("read conflict")
        return "ok"

    assert store.transact(operation) == "ok"
    assert len({id(t) for t in transactions}) == 3
    with pytest.raises(RuntimeError, match="LEASE_LOST"):
        store.transact(lambda _: (_ for _ in ()).throw(RuntimeError("LEASE_LOST")))
