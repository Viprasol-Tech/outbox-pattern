"""Tests for the transactional outbox store and unit of work."""

from __future__ import annotations

from outbox_pattern.outbox import OutboxStore


def test_state_and_event_persist_together_on_commit() -> None:
    store = OutboxStore()
    with store.begin() as txn:
        txn.set_state("order-1", {"status": "PLACED"})
        evt = txn.add_event("order.placed", {"order_id": "order-1"})
        txn.commit()

    # Both the domain state and the event are committed together.
    assert store.get_state("order-1") == {"status": "PLACED"}
    assert store.get_event(evt.id) is not None
    assert store.get_event(evt.id) is evt
    assert len(store.unpublished_events()) == 1


def test_uncommitted_transaction_rolls_back_both_state_and_event() -> None:
    store = OutboxStore()
    # Exit the context without committing: nothing should persist.
    with store.begin() as txn:
        txn.set_state("order-2", {"status": "PLACED"})
        txn.add_event("order.placed", {"order_id": "order-2"})
        # no commit

    assert store.get_state("order-2") is None
    assert store.all_events() == []


def test_exception_inside_transaction_rolls_everything_back() -> None:
    store = OutboxStore()
    try:
        with store.begin() as txn:
            txn.set_state("order-3", {"status": "PLACED"})
            txn.add_event("order.placed", {"order_id": "order-3"})
            raise ValueError("boom before commit")
    except ValueError:
        pass

    # Atomicity: a failure before commit leaves no state and no event.
    assert store.get_state("order-3") is None
    assert store.all_events() == []


def test_double_commit_raises() -> None:
    store = OutboxStore()
    txn = store.begin()
    txn.set_state("k", 1)
    txn.commit()
    try:
        txn.commit()
        raised = False
    except RuntimeError:
        raised = True
    assert raised


def test_events_returned_in_creation_order() -> None:
    store = OutboxStore()
    ids = []
    for i in range(3):
        with store.begin() as txn:
            ids.append(txn.add_event("t", {"i": i}).id)
            txn.commit()
    assert [e.id for e in store.all_events()] == ids
