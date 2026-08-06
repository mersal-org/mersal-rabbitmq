from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

import aio_pika
import aio_pika.abc
import anyio

from mersal.subscription import SubscriptionStorage

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

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


class RabbitMqSubscriptionStorage(SubscriptionStorage):
    """A `SubscriptionStorage` that stores subscriptions as native RabbitMQ topic-exchange bindings."""

    def __init__(
        self,
        config: RabbitMqSubscriptionStorageConfig,
        connection_provider: Callable[[], Awaitable[aio_pika.abc.AbstractRobustConnection]] | None = None,
    ) -> None:
        """
        Args:
            config: Connection and topology settings.
            connection_provider: If given, called to obtain the connection instead of
                opening one of its own - lets this storage share a connection with a
                `RabbitMqTransport` on the same broker (`RabbitMQPlugin` wires this up
                using the transport's `ensure_connection`). Since the provider's owner
                is responsible for the connection's lifetime, `close` becomes a no-op.
        """
        self._config = config
        self._connection_provider = connection_provider
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._connect_lock = anyio.Lock()
        self._subscriptions: dict[str, set[str]] = defaultdict(set)

    @property
    def is_centralized(self) -> bool:
        return True

    async def connect(self) -> None:
        await self._ensure_connected()

    async def close(self) -> None:
        connection = self._connection
        self._connection = None
        if connection is not None and self._connection_provider is None:
            await connection.close()

    async def register_subscriber(self, topic: str, subscriber_address: str) -> None:
        """Bind `subscriber_address`'s queue to `topic` on the topic exchange.

        The queue itself is never declared here - it's expected to already exist,
        declared once by its owning `RabbitMqTransport` at that app's own startup
        (`subscriber_address` is that transport's own address). Declaring it a second
        place would need every declaration option kept in sync with the transport's
        or risk a channel-closing PRECONDITION_FAILED on mismatch.
        """
        connection = await self._ensure_connected()

        channel = await connection.channel()
        async with channel:
            exchange = await channel.get_exchange(self._config.topic_exchange_name, ensure=False)
            queue = await channel.get_queue(subscriber_address, ensure=False)
            await queue.bind(exchange, routing_key=topic)
        self._subscriptions[subscriber_address].add(topic)

    async def unregister_subscriber(self, topic: str, subscriber_address: str) -> None:
        connection = await self._ensure_connected()

        channel = await connection.channel()
        try:
            async with channel:
                exchange = await channel.get_exchange(self._config.topic_exchange_name, ensure=False)
                queue = await channel.get_queue(subscriber_address, ensure=False)
                await queue.unbind(exchange, routing_key=topic)
        except aio_pika.exceptions.ChannelNotFoundEntity:
            # Unsubscribing something that's already gone (e.g. the subscriber's queue
            # was deleted) is a no-op, not an error.
            pass
        self._subscriptions[subscriber_address].discard(topic)

    async def get_subscriber_addresses(self, topic: str) -> set[str]:
        return {f"{topic}@{self._config.topic_exchange_name}"}

    async def rebind_subscriptions(self, subscriber_address: str) -> None:
        topics = self._subscriptions.get(subscriber_address)
        if not topics:
            return
        connection = await self._ensure_connected()
        channel = await connection.channel()
        async with channel:
            exchange = await channel.get_exchange(self._config.topic_exchange_name, ensure=False)
            queue = await channel.get_queue(subscriber_address, ensure=False)
            for topic in topics:
                await queue.bind(exchange, routing_key=topic)

    async def _ensure_connected(self) -> aio_pika.abc.AbstractRobustConnection:
        if self._connection is not None:
            return self._connection
        async with self._connect_lock:
            if self._connection_provider is not None:
                connection = await self._connection_provider()
                await self._declare_topic_exchange(connection)
                self._connection = connection
                return connection

            connection = await aio_pika.connect_robust(self._config.connection_uri)
            try:
                await self._declare_topic_exchange(connection)
            except BaseException:
                # A failed exchange declare must not leak the connection; shielded so
                # cancellation can't abandon it either.
                with anyio.CancelScope(shield=True):
                    await connection.close()
                raise
            self._connection = connection
            return connection

    async def _declare_topic_exchange(self, connection: aio_pika.abc.AbstractRobustConnection) -> None:
        if not self._config.should_declare_topic_exchange:
            return
        channel = await connection.channel()
        async with channel:
            await channel.declare_exchange(
                self._config.topic_exchange_name,
                type=aio_pika.ExchangeType.TOPIC,
                durable=True,
                arguments=self._config.topic_exchange_arguments or {},
            )
