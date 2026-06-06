"""The outbox store and its atomic unit of work, :class:`Transaction`.

The core idea of the transactional outbox pattern: instead of writing domain
state to the database and *then* publishing to a message broker (two systems
that can fail independently and leave you inconsistent), you write the domain
state **and** the event to record in *one* local transaction. A separate relay
later reads the outbox and publishes the events. State and events commit or
roll back together, so an event is never lost and never published for state
that did not commit.

This module models that with an in-memory, fully offline implementation. A
:class:`Transaction` buffers both state mutations and outbox events; calling
:meth:`Transaction.commit` applies them atomically, and leaving the context
manager without committing discards everything.

Part of Outbox Pattern by Viprasol Tech Private Limited (https://viprasol.com).
"""

from __future__ import annotations

from types import TracebackType
from typing import Any

from outbox_pattern.models import OutboxEvent


class OutboxStore:
    """In-memory store holding both domain state and the outbox table.

    In a real system the "state" and "outbox" would be two tables in the same
    relational database, written inside a single SQL transaction. Here they are
    two dicts mutated inside a single :class:`Transaction` that applies its
    buffered changes atomically on commit.
    """

    def __init__(self) -> None:
        self._state: dict[str, Any] = {}
        self._outbox: dict[str, OutboxEvent] = {}

    # -- read helpers -----------------------------------------------------
    def get_state(self, key: str) -> Any:
        """Return the committed domain value for ``key`` (or ``None``)."""
        return self._state.get(key)

    def all_state(self) -> dict[str, Any]:
        """Return a shallow copy of all committed domain state."""
        return dict(self._state)

    def get_event(self, event_id: str) -> OutboxEvent | None:
        """Return the committed event with ``event_id`` (or ``None``)."""
        return self._outbox.get(event_id)

    def all_events(self) -> list[OutboxEvent]:
        """Return committed events ordered by creation time."""
        return sorted(self._outbox.values(), key=lambda e: e.seq)

    def unpublished_events(self) -> list[OutboxEvent]:
        """Return committed, not-yet-published events in creation order.

        This is exactly what the relay polls.
        """
        return [e for e in self.all_events() if not e.published]

    # -- unit of work -----------------------------------------------------
    def begin(self) -> Transaction:
        """Open a new unit of work over this store."""
        return Transaction(self)

    # -- internal: applied by Transaction.commit --------------------------
    def _apply(self, state: dict[str, Any], events: list[OutboxEvent]) -> None:
        """Atomically merge buffered state and events into the store.

        Because this runs single-threaded and performs only dict assignments
        with no intervening yield points, it cannot partially apply.
        """
        self._state.update(state)
        for event in events:
            self._outbox[event.id] = event


class Transaction:
    """A single atomic unit of work over an :class:`OutboxStore`.

    Buffer domain writes with :meth:`set_state` and events with
    :meth:`add_event`, then call :meth:`commit` to apply them together. Used as
    a context manager, an un-committed transaction is discarded on exit (so an
    exception rolls back both the state *and* the events).
    """

    def __init__(self, store: OutboxStore) -> None:
        self._store = store
        self._staged_state: dict[str, Any] = {}
        self._staged_events: list[OutboxEvent] = []
        self._committed = False

    def set_state(self, key: str, value: Any) -> None:
        """Stage a domain-state write to apply on commit."""
        self._staged_state[key] = value

    def add_event(
        self,
        topic: str,
        payload: dict[str, Any] | None = None,
        *,
        event: OutboxEvent | None = None,
    ) -> OutboxEvent:
        """Stage an outbox event to record on commit.

        Args:
            topic: Logical channel for the event.
            payload: JSON-serialisable event body.
            event: A pre-built event to stage instead of ``topic``/``payload``.

        Returns:
            The staged :class:`OutboxEvent` (with its generated id).
        """
        evt = event or OutboxEvent(topic=topic, payload=payload or {})
        self._staged_events.append(evt)
        return evt

    @property
    def committed(self) -> bool:
        """Whether this transaction has been committed."""
        return self._committed

    def commit(self) -> None:
        """Atomically apply staged state and events to the store.

        Raises:
            RuntimeError: If the transaction was already committed.
        """
        if self._committed:
            raise RuntimeError("transaction already committed")
        self._store._apply(self._staged_state, self._staged_events)
        self._committed = True

    def rollback(self) -> None:
        """Discard all staged changes without touching the store."""
        self._staged_state.clear()
        self._staged_events.clear()

    def __enter__(self) -> Transaction:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        # Leaving the block without an explicit commit discards everything,
        # so an exception rolls back state and events together.
        if not self._committed:
            self.rollback()
