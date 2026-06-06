"""Tests for the relay, at-least-once delivery, and consumer dedup."""

from __future__ import annotations

from outbox_pattern.models import OutboxEvent
from outbox_pattern.outbox import OutboxStore
from outbox_pattern.relay import InMemoryPublisher, Relay


def _seed(store: OutboxStore, n: int) -> list[OutboxEvent]:
    events = []
    for i in range(n):
        with store.begin() as txn:
            events.append(txn.add_event("order.placed", {"i": i}))
            txn.commit()
    return events


def test_relay_publishes_only_unpublished_and_marks_them() -> None:
    store = OutboxStore()
    events = _seed(store, 3)
    publisher = InMemoryPublisher()
    relay = Relay(store, publisher)

    result = relay.run_once()

    assert result.published_count == 3
    assert sorted(result.published) == sorted(e.id for e in events)
    assert all(store.get_event(e.id).published for e in events)  # type: ignore[union-attr]
    assert store.unpublished_events() == []

    # A second pass finds nothing new to publish.
    second = relay.run_once()
    assert second.published_count == 0
    assert len(publisher.deliveries) == 3


def test_failed_publish_leaves_event_unpublished_for_retry() -> None:
    store = OutboxStore()
    events = _seed(store, 3)
    fail_id = events[1].id
    publisher = InMemoryPublisher(fail_ids={fail_id})
    relay = Relay(store, publisher)

    first = relay.run_once()
    assert first.published_count == 2
    assert first.failed == [fail_id]
    # The failed event is still unpublished and available for retry.
    failed_event = store.get_event(fail_id)
    assert failed_event is not None
    assert failed_event.published is False
    assert store.unpublished_events() == [failed_event]
    assert failed_event.attempts == 1

    # Clear the failure and retry: now it goes through.
    publisher.fail_ids.clear()
    second = relay.run_once()
    assert second.published == [fail_id]
    assert store.unpublished_events() == []
    assert failed_event.attempts == 2


def test_consumer_dedup_by_id_on_redelivery() -> None:
    store = OutboxStore()
    events = _seed(store, 2)
    publisher = InMemoryPublisher()
    relay = Relay(store, publisher)

    relay.run_once()
    # Simulate at-least-once redelivery of an already-delivered event.
    publisher.publish(events[0])
    publisher.publish(events[0])

    # The broker saw 4 deliveries (2 original + 2 redeliveries)...
    assert len(publisher.deliveries) == 4
    # ...but the consumer deduplicated to exactly the 2 distinct events.
    assert set(publisher.consumed) == {e.id for e in events}
    assert publisher.duplicate_count == 2


def test_run_until_drained_retries_until_no_progress() -> None:
    store = OutboxStore()
    events = _seed(store, 2)
    publisher = InMemoryPublisher(fail_ids={events[0].id})
    relay = Relay(store, publisher)

    # First drain: one event publishes, one keeps failing -> stops after the
    # pass that makes zero progress.
    passes = relay.run_until_drained()
    assert passes[0].published_count == 1
    assert passes[-1].published_count == 0
    assert len(store.unpublished_events()) == 1

    # Recover and drain again.
    publisher.fail_ids.clear()
    passes2 = relay.run_until_drained()
    assert passes2[0].published == [events[0].id]
    assert store.unpublished_events() == []
