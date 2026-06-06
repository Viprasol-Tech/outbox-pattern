"""Command-line interface for Outbox Pattern.

``outbox-pattern demo`` writes domain state and outbox events transactionally,
then runs the relay to publish them to an in-memory broker — fully offline,
demonstrating at-least-once delivery and consumer-side deduplication.

Part of Outbox Pattern by Viprasol Tech Private Limited (https://viprasol.com).
"""

from __future__ import annotations

import typer
from rich.console import Console
from rich.table import Table

from outbox_pattern import __version__
from outbox_pattern.outbox import OutboxStore
from outbox_pattern.relay import InMemoryPublisher, Relay

app = typer.Typer(add_completion=False, help="Outbox Pattern — by Viprasol Tech.")
console = Console()


@app.command()
def version() -> None:
    """Print the installed version."""
    console.print(f"outbox-pattern [bold cyan]{__version__}[/] - by Viprasol Tech")


@app.command()
def demo(orders: int = typer.Option(3, help="Number of orders to place transactionally.")) -> None:
    """Write events transactionally, then relay them to an in-memory broker.

    The demo simulates ``orders`` order placements. Each one commits the order
    state and an ``order.placed`` event together. The relay then publishes the
    events. To show at-least-once + dedup, the relay is run twice: the second
    pass finds nothing new, but we also redeliver one event by hand so the
    consumer can deduplicate it.
    """
    store = OutboxStore()

    # 1. Write domain state and outbox events in the SAME transaction.
    for i in range(1, orders + 1):
        with store.begin() as txn:
            order_id = f"order-{i}"
            txn.set_state(order_id, {"status": "PLACED", "total": i * 100})
            txn.add_event("order.placed", {"order_id": order_id, "total": i * 100})
            txn.commit()

    console.print(f"Committed [bold]{orders}[/] orders, each with a paired outbox event.")
    console.print(f"Unpublished events in outbox: [yellow]{len(store.unpublished_events())}[/]")

    # 2. Relay publishes only unpublished events and marks them published.
    publisher = InMemoryPublisher()
    relay = Relay(store, publisher)

    first = relay.run_once()
    console.print(f"Relay pass 1: published [green]{first.published_count}[/] events.")

    second = relay.run_once()
    console.print(
        f"Relay pass 2: published [green]{second.published_count}[/] events (nothing new)."
    )

    # 3. Simulate a broker redelivery; the consumer deduplicates by event id.
    if publisher.consumed:
        replayed = next(iter(publisher.consumed.values()))
        publisher.publish(replayed)
        console.print(f"Redelivered event [dim]{replayed.id[:8]}[/] - consumer dedups it.")

    # Summary table.
    table = Table(title="Delivery summary", show_header=True, header_style="bold")
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    table.add_row("Events committed", str(len(store.all_events())))
    table.add_row("Delivery attempts", str(len(publisher.deliveries)))
    table.add_row("Distinct consumed", str(len(publisher.consumed)))
    table.add_row("Duplicates dropped", str(publisher.duplicate_count))
    table.add_row("Unpublished remaining", str(len(store.unpublished_events())))
    console.print(table)

    console.print(
        "[bold green]OK[/] - state and events committed together; delivery is idempotent."
    )


if __name__ == "__main__":
    app()
