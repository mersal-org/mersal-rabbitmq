from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import aio_pika
import aio_pika.abc
import anyio

from mersal.subscription import SubscriptionStorage

if TYPE_CHECKING:
    from pamqp import common as pamqp_common

__all__ = (
    "RabbitMqSubscriptionStorage",
    "RabbitMqSubscriptionStorageConfig",
)


@dataclass
class RabbitMqSubscriptionStorageConfig:
    connection_uri: str
    topic_exchange_name: str = "mersal.topics"
    """Must match the `topic_exchange_name` of the `RabbitMqTransport` instances sharing
    this broker - subscribing and publishing only agree on where events land if both
    sides bind to and publish through the same exchange.
    """
    should_declare_topic_exchange: bool = True
    topic_exchange_arguments: pamqp_common.Arguments | None = None
    subscriber_queue_durable: bool = True
    """Durability used when defensively declaring a subscriber's queue before binding
    it. Should match the durability the subscriber's own `RabbitMqTransport` declares
    its input queue with, or RabbitMQ will reject the (re)declaration with a
    channel-closing PRECONDITION_FAILED error.
    """


class RabbitMqSubscriptionStorage(SubscriptionStorage):
    """A `SubscriptionStorage` that stores subscriptions as native RabbitMQ topic-exchange bindings."""

    def __init__(self, config: RabbitMqSubscriptionStorageConfig) -> None:
        self._config = config
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._connect_lock = anyio.Lock()

    @property
    def is_centralized(self) -> bool:
        return True

    async def connect(self) -> None:
        """Eagerly establish the underlying connection.

        Optional: every public method also connects lazily on first use, so calling
        this isn't required, but doing so up front (e.g. from an app's startup hook)
        surfaces connection failures before the first `register_subscriber` call.
        """
        await self._ensure_connected()

    async def close(self) -> None:
        """Close the underlying connection.

        The storage can be used again afterwards; the next use reconnects.
        """
        connection = self._connection
        self._connection = None
        if connection is not None:
            await connection.close()

    async def register_subscriber(self, topic: str, subscriber_address: str) -> None:
        connection = await self._ensure_connected()

        channel = await connection.channel()
        async with channel:
            exchange = await channel.get_exchange(self._config.topic_exchange_name, ensure=False)
            queue = await channel.declare_queue(subscriber_address, durable=self._config.subscriber_queue_durable)
            await queue.bind(exchange, routing_key=topic)

    async def unregister_subscriber(self, topic: str, subscriber_address: str) -> None:
        connection = await self._ensure_connected()

        channel = await connection.channel()
        async with channel:
            exchange = await channel.get_exchange(self._config.topic_exchange_name, ensure=False)
            queue = await channel.get_queue(subscriber_address, ensure=False)
            await queue.unbind(exchange, routing_key=topic)

    async def get_subscriber_addresses(self, topic: str) -> set[str]:
        return {f"{topic}@{self._config.topic_exchange_name}"}

    async def _ensure_connected(self) -> aio_pika.abc.AbstractRobustConnection:
        if self._connection is not None:
            return self._connection
        async with self._connect_lock:
            if self._connection is not None:
                # Another task finished connecting while we waited on the lock; mypy's
                # narrowing from the fast-path check above can't see that.
                return self._connection  # type: ignore[unreachable]
            connection = await aio_pika.connect_robust(self._config.connection_uri)
            try:
                if self._config.should_declare_topic_exchange:
                    channel = await connection.channel()
                    async with channel:
                        await channel.declare_exchange(
                            self._config.topic_exchange_name,
                            type=aio_pika.ExchangeType.TOPIC,
                            durable=True,
                            arguments=self._config.topic_exchange_arguments or {},
                        )
            except BaseException:
                # A failed exchange declare must not leak the connection; shielded so
                # cancellation can't abandon it either.
                with anyio.CancelScope(shield=True):
                    await connection.close()
                raise
            self._connection = connection
            return connection
