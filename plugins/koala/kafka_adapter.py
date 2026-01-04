"""Placeholder Kafka adapter interface.

This is a tiny design placeholder showing how a Kafka adapter could be
implemented. It's not functional; it's a starting point for later integration.
"""

from __future__ import annotations

from typing import Any, Iterable, Optional


class KafkaAdapter:
    """Minimal interface for a Kafka producer/consumer adapter.

    Implementations should provide connect(), publish(topic, message), and
    consume(topic) iterators or callbacks.
    """

    def __init__(self, bootstrap_servers: Optional[Iterable[str]] = None):
        self.bootstrap_servers = list(bootstrap_servers or [])

    def connect(self) -> None:
        """Open connections to Kafka brokers. Not implemented in placeholder."""
        raise NotImplementedError(
            "KafkaAdapter.connect must be implemented by a concrete adapter"
        )

    def publish(self, topic: str, message: Any) -> None:
        """Publish a message to a topic."""
        raise NotImplementedError(
            "KafkaAdapter.publish must be implemented by a concrete adapter"
        )

    def consume(self, topic: str):
        """Return an iterator of messages from topic."""
        raise NotImplementedError(
            "KafkaAdapter.consume must be implemented by a concrete adapter"
        )
