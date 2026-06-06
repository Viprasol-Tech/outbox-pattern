"""Outbox Pattern — transactional outbox for reliable event publishing.

Part of Outbox Pattern by Viprasol Tech Private Limited (https://viprasol.com).
"""

from __future__ import annotations

from outbox_pattern.models import OutboxEvent
from outbox_pattern.outbox import OutboxStore, Transaction
from outbox_pattern.relay import (
    InMemoryPublisher,
    Publisher,
    PublishError,
    Relay,
    RelayResult,
)

__version__ = "0.1.0"

__all__ = [
    "InMemoryPublisher",
    "OutboxEvent",
    "OutboxStore",
    "PublishError",
    "Publisher",
    "Relay",
    "RelayResult",
    "Transaction",
    "__version__",
]
