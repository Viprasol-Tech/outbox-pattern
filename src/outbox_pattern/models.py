"""Domain models for the transactional outbox.

An :class:`OutboxEvent` is the unit that the relay publishes. It carries a
stable, unique ``id`` (used for consumer-side deduplication), a logical
``topic``, an arbitrary JSON-serialisable ``payload``, and bookkeeping fields
the relay uses to track delivery (``published``, ``attempts``).

Part of Outbox Pattern by Viprasol Tech Private Limited (https://viprasol.com).
"""

from __future__ import annotations

import itertools
import uuid
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field

_seq_counter = itertools.count(1)


def _new_id() -> str:
    """Return a fresh, globally-unique event id."""
    return uuid.uuid4().hex


def _next_seq() -> int:
    """Return the next monotonic creation sequence number.

    Provides a stable total order for events created within the same clock
    tick, where ``created_at`` alone would be ambiguous.
    """
    return next(_seq_counter)


def _utcnow() -> datetime:
    """Return the current time as a timezone-aware UTC ``datetime``."""
    return datetime.now(UTC)


class OutboxEvent(BaseModel):
    """A single event recorded in the outbox alongside domain state.

    The event id is the idempotency key: a consumer that has already
    processed an id must ignore any redelivery of the same id. This is what
    makes at-least-once delivery safe to use as effectively exactly-once at
    the application level.
    """

    id: str = Field(default_factory=_new_id)
    """Stable unique identifier; the consumer-side deduplication key."""

    seq: int = Field(default_factory=_next_seq)
    """Monotonic creation sequence; gives events a stable total order."""

    topic: str
    """Logical channel the event is published to."""

    payload: dict[str, Any] = Field(default_factory=dict)
    """Arbitrary JSON-serialisable event body."""

    created_at: datetime = Field(default_factory=_utcnow)
    """When the event was first recorded in the outbox (UTC)."""

    published: bool = False
    """Whether the relay has successfully published this event."""

    published_at: datetime | None = None
    """When the event was published, or ``None`` if not yet published."""

    attempts: int = 0
    """Number of publish attempts made by the relay (success or failure)."""

    def mark_published(self, *, at: datetime | None = None) -> None:
        """Mark this event as successfully published.

        Args:
            at: Publish timestamp; defaults to the current UTC time.
        """
        self.published = True
        self.published_at = at or _utcnow()
