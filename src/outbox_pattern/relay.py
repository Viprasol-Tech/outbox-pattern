"""The relay: polls the outbox and publishes events at-least-once.

A :class:`Relay` reads unpublished events from an :class:`OutboxStore`, hands
each to a :class:`Publisher`, and marks the event published only when the
publisher confirms success. If publishing raises, the event stays unpublished
so the next poll retries it — this is the at-least-once guarantee.

Because retries (and broker-level redelivery) can publish the same event id
more than once, consumers must deduplicate. The bundled
:class:`InMemoryPublisher` plays both broker and consumer: it records every
delivery attempt but de-duplicates by event id, so the set of *distinct*
delivered ids matches the events exactly once.

Part of Outbox Pattern by Viprasol Tech Private Limited (https://viprasol.com).
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from pydantic import BaseModel

from outbox_pattern.models import OutboxEvent
from outbox_pattern.outbox import OutboxStore


class PublishError(RuntimeError):
    """Raised by a :class:`Publisher` when delivery fails.

    The relay treats this (and any other exception) as a transient failure and
    leaves the event unpublished for a later retry.
    """


@runtime_checkable
class Publisher(Protocol):
    """A sink the relay publishes events to (a broker, a webhook, a queue).

    Implementations should raise :class:`PublishError` (or any exception) to
    signal a failed delivery; the relay will then retry the event later.
    """

    def publish(self, event: OutboxEvent) -> None:
        """Deliver ``event`` or raise on failure."""
        ...


class InMemoryPublisher:
    """A fake broker + deduplicating consumer used for demos and tests.

    Every call to :meth:`publish` appends to :attr:`deliveries` (so redeliveries
    are visible), but the event is only added to :attr:`consumed` the *first*
    time its id is seen — modelling a consumer that deduplicates by event id.
    An optional ``fail_ids`` set makes chosen events fail, to exercise retries.
    """

    def __init__(self, *, fail_ids: set[str] | None = None) -> None:
        self.deliveries: list[str] = []
        """Every delivered event id, including duplicate redeliveries."""

        self.consumed: dict[str, OutboxEvent] = {}
        """Distinct events as seen by the deduplicating consumer."""

        self.fail_ids: set[str] = set(fail_ids or set())
        """Event ids that should raise :class:`PublishError` on publish."""

    def publish(self, event: OutboxEvent) -> None:
        """Deliver an event, deduplicating by id on the consumer side."""
        if event.id in self.fail_ids:
            raise PublishError(f"simulated delivery failure for {event.id}")
        self.deliveries.append(event.id)
        # Consumer-side idempotency: ignore an id we have already processed.
        if event.id not in self.consumed:
            self.consumed[event.id] = event

    @property
    def duplicate_count(self) -> int:
        """Number of redeliveries the consumer deduplicated away."""
        return len(self.deliveries) - len(self.consumed)


class RelayResult(BaseModel):
    """Outcome of a single :meth:`Relay.run_once` poll."""

    published: list[str] = []
    """Event ids published successfully in this pass."""

    failed: list[str] = []
    """Event ids whose publish failed and will be retried."""

    @property
    def published_count(self) -> int:
        """How many events were published in this pass."""
        return len(self.published)

    @property
    def failed_count(self) -> int:
        """How many events failed to publish in this pass."""
        return len(self.failed)


class Relay:
    """Polls an outbox store and publishes its unpublished events.

    The relay is safe to run repeatedly: each pass only touches events that are
    still unpublished, and an event is marked published *only* after the
    publisher confirms success.
    """

    def __init__(self, store: OutboxStore, publisher: Publisher) -> None:
        self._store = store
        self._publisher = publisher

    def run_once(self) -> RelayResult:
        """Publish all currently-unpublished events, once.

        Returns:
            A :class:`RelayResult` listing published and failed event ids.
        """
        result = RelayResult()
        for event in self._store.unpublished_events():
            event.attempts += 1
            try:
                self._publisher.publish(event)
            except Exception:
                # Transient failure: leave unpublished so a later pass retries.
                result.failed.append(event.id)
                continue
            event.mark_published()
            result.published.append(event.id)
        return result

    def run_until_drained(self, max_passes: int = 100) -> list[RelayResult]:
        """Repeatedly poll until no event is published in a pass.

        Useful when a previous pass failed some events and you want to retry
        until things stop making progress (e.g. after clearing ``fail_ids``).

        Args:
            max_passes: Hard cap on passes to guarantee termination.

        Returns:
            The :class:`RelayResult` from each pass performed.
        """
        passes: list[RelayResult] = []
        for _ in range(max_passes):
            result = self.run_once()
            passes.append(result)
            if result.published_count == 0:
                break
        return passes
