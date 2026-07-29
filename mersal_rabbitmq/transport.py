from __future__ import annotations

import asyncio
from collections import deque
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from functools import partial
from typing import TYPE_CHECKING, Any

import aio_pika
import aio_pika.abc
import anyio

from mersal.logging import Logger, NullLogger
from mersal.messages import TransportMessage
from mersal.messages.message_headers import MessageHeaders
from mersal.threading import AnyIOPeriodicTaskFactory, PeriodicAsyncTask, PeriodicAsyncTaskFactory
from mersal.transport.base_transport import BaseTransport
from mersal.utils import AsyncRetrier

if TYPE_CHECKING:
    from pamqp import common as pamqp_common

    from mersal.transport import TransactionContext
    from mersal.transport.outgoing_message import OutgoingMessage

__all__ = (
    "QueueDeclarationOptions",
    "RabbitMqTransport",
    "RabbitMqTransportConfig",
)


_RETRY_DELAYS = [0.0, 0.0]

# Types pamqp encodes natively into an AMQP field table. Everything else is
# stringified as a last resort (see `_to_header_value`).
_NATIVE_HEADER_TYPES = (bool, int, float, str, bytes, datetime, Decimal)


def _to_header_value(value: Any) -> Any:
    """Coerce a header value into something pamqp can encode into an AMQP field table.

    AMQP field tables natively carry bools, ints, floats, strings, bytes, timestamps,
    decimals, and nested lists/tables, so those pass through unchanged - stringifying
    them would mean e.g. an int header sent through RabbitMQ comes back as a str,
    unlike the same message sent through any other transport. Only values pamqp has no
    encoding for (e.g. UUID) fall back to `str`.
    """
    if value is None or isinstance(value, _NATIVE_HEADER_TYPES):
        return value
    if isinstance(value, dict):
        return {str(key): _to_header_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_header_value(item) for item in value]
    return str(value)


@dataclass
class QueueDeclarationOptions:
    durable: bool
    exclusive: bool
    arguments: pamqp_common.Arguments
    auto_delete: bool


@dataclass
class RabbitMqTransportConfig:
    connection_uri: str
    input_queue_name: str
    should_declare_exchanges: bool = True
    should_declare_input_queue: bool = True
    should_bind_input_queue: bool = True
    direct_exchange_arguments: pamqp_common.Arguments | None = None
    topic_exchange_arguments: pamqp_common.Arguments | None = None
    direct_exchange_name: str = "mersal.direct"
    topic_exchange_name: str = "mersal.topics"
    input_queue_declaration_options: QueueDeclarationOptions | None = None
    default_queue_declaration_options: QueueDeclarationOptions | None = None
    prefetch_count: int = 50
    """Maximum number of unacked messages the broker will push to this transport's
    consumer at once. Sized a few times larger than the app's expected processing
    concurrency (see `max_parallelism` on the worker) keeps the pipeline fed without
    letting one slow consumer hoard the whole queue.
    """
    passive_queue_check_interval: float | None = 60.0
    """How often, in seconds, to passively verify the input queue still exists on the
    broker (e.g. it wasn't deleted by a TTL/`x-expires` policy) and proactively
    recreate it if not.

    This exists on top of the reactive self-heal in `receive` (which only notices a
    dead *consumer*, not a vanished queue) because a queue nobody is currently
    consuming from or publishing to wouldn't otherwise be noticed missing until
    something tried to use it again. Set to `None` to disable the check entirely.
    """


@dataclass
class _Consumer:
    """A live consumer subscription plus the local buffer it feeds.

    `pump_task` drains the aio-pika queue iterator into `buffer` so that `receive`
    never awaits (and therefore never cancels) the iterator's `__anext__` directly:
    aio-pika interprets cancellation of `__anext__` as a shutdown request and responds
    by closing the whole consumer. Waiting on our own buffer instead makes `receive`
    safe to cancel from outside (worker shutdown, or a caller-imposed deadline): the
    subscription and any prefetched messages are untouched.
    """

    iterator: aio_pika.abc.AbstractQueueIterator
    pump_task: asyncio.Task[None] | None = None
    buffer: deque[aio_pika.abc.AbstractIncomingMessage] = field(default_factory=deque)
    message_available: anyio.Event = field(default_factory=anyio.Event)
    dead: bool = False


@dataclass
class _StartedState:
    """Everything that only exists once the transport has connected.

    Grouping these means the started/not-started invariant lives in one
    `_StartedState | None` field instead of one Optional per resource, and
    `_ensure_started` can hand back a fully-typed state with no unwrapping at
    every use site.
    """

    connection: aio_pika.abc.AbstractRobustConnection
    publish_channel: aio_pika.abc.AbstractChannel
    consume_channel: aio_pika.abc.AbstractChannel
    direct_exchange: aio_pika.abc.AbstractExchange
    topic_exchange: aio_pika.abc.AbstractExchange
    input_queue: aio_pika.abc.AbstractQueue
    consumer: _Consumer


class RabbitMqTransport(BaseTransport):
    """A RabbitMQ transport backed by a single persistent, auto-recovering connection.

    Topology:
        Two durable exchanges are declared: a ``direct`` exchange used for
        point-to-point sends (each app's input queue is bound to it with a routing key
        equal to its own address), and a ``topic`` exchange used for pub/sub (see
        `mersal_rabbitmq.subscription_storage.RabbitMqSubscriptionStorage`).

    Connections and channels:
        A single `aio_pika.connect_robust` connection is opened once and reused for the
        lifetime of the transport; `aio_pika`'s robust connection/channel classes
        transparently redeclare topology and resume consuming after a dropped
        connection, so we don't hand-roll reconnection logic. Two long-lived channels
        are kept open on top of it - one dedicated to publishing (with publisher
        confirms enabled, so `send` only returns once the broker has actually accepted
        the message) and one dedicated to consuming (with a QoS prefetch limit) - so a
        failure on one side can't take down the other.

    Startup:
        Connecting is lazy: the first call to `send`, `receive`, or `create_queue`
        establishes the connection and topology if it hasn't happened yet. `__call__`
        (registered as an on-startup lifespan hook by `RabbitMQPlugin`) simply
        triggers this eagerly, so the cost of connecting is paid at app startup rather
        than on the first message - but nothing breaks if `__call__` is never invoked.
        After `close`, the next use starts the transport again from scratch.

    Self-healing:
        A dead *consumer* (e.g. its channel closed and wasn't - or couldn't be -
        transparently recovered) is noticed reactively, the next time `receive` is
        called. A vanished *queue* (e.g. deleted by a TTL/`x-expires` policy) is
        noticed proactively instead, by a periodic passive-existence check - see
        `RabbitMqTransportConfig.passive_queue_check_interval`.
    """

    def __init__(
        self,
        config: RabbitMqTransportConfig,
        periodic_task_factory: PeriodicAsyncTaskFactory | None = None,
        logger: Logger | None = None,
    ):
        super().__init__(config.input_queue_name)

        self._logger = logger or NullLogger()

        self._connection_uri = config.connection_uri

        self._direct_exchange_name = config.direct_exchange_name
        self._topic_exchange_name = config.topic_exchange_name
        self._input_queue_declaration_options = config.input_queue_declaration_options or QueueDeclarationOptions(
            durable=True,
            exclusive=False,
            auto_delete=False,
            arguments={},
        )
        self._default_queue_declaration_options = config.default_queue_declaration_options or QueueDeclarationOptions(
            durable=True,
            exclusive=False,
            auto_delete=False,
            arguments={},
        )

        self._should_declare_exchanges = config.should_declare_exchanges
        self._should_declare_input_queue = config.should_declare_input_queue
        self._should_bind_input_queue = config.should_bind_input_queue
        self._direct_exchange_arguments = config.direct_exchange_arguments or {}
        self._topic_exchange_arguments = config.topic_exchange_arguments or {}
        self._prefetch_count = config.prefetch_count

        periodic_task_factory = periodic_task_factory or AnyIOPeriodicTaskFactory(logger=self._logger)
        self._passive_queue_check_task: PeriodicAsyncTask | None = (
            periodic_task_factory(
                f"RabbitMQ-PassiveQueueCheck-{config.input_queue_name}",
                self._check_input_queue_exists,
                config.passive_queue_check_interval,
            )
            if config.passive_queue_check_interval is not None
            else None
        )

        self._exchange_cache: dict[str, aio_pika.abc.AbstractExchange] = {}
        self._retrier = AsyncRetrier(_RETRY_DELAYS)

        self._state: _StartedState | None = None
        self._start_lock = anyio.Lock()
        self._consumer_lock = anyio.Lock()

    async def __call__(self) -> None:
        await self._ensure_started()

    async def close(self) -> None:
        """Gracefully tear down the consumer and connection.

        Wired up as an on-shutdown lifespan hook by `RabbitMQPlugin` so the
        underlying connection isn't just abandoned when the app stops. The transport
        can be used again afterwards; the next use starts it from scratch.
        """
        if self._passive_queue_check_task is not None:
            await self._passive_queue_check_task.stop()
        state = self._state
        self._state = None
        self._exchange_cache.clear()
        if state is not None:
            await self._close_consumer(state.consumer)
            await state.connection.close()

    async def create_queue(self, address: str) -> None:
        state = await self._ensure_started()
        if address != self.address:
            await self._declare_and_bind(state.connection, address)

    async def send_outgoing_messages(
        self,
        outgoing_message: list[OutgoingMessage],
        transaction_context: TransactionContext,
    ) -> None:
        state = await self._ensure_started()

        for message in outgoing_message:
            exchange, routing_key, mandatory = await self._resolve_publish_target(state, message.destination_address)
            amqp_message = self._to_amqp_message(message.transport_message)
            await self._retrier.run(partial(exchange.publish, amqp_message, routing_key, mandatory=mandatory))

    async def receive(self, transaction_context: TransactionContext) -> TransportMessage | None:
        """Wait for the next message; blocks until one arrives.

        There is deliberately no built-in timeout: the worker stops a blocked receive
        by cancelling it, and any caller needing a deadline can impose one externally
        (e.g. `anyio.move_on_after`) - the wait is cancellation-safe and leaves the
        underlying consumer untouched.
        """
        state = await self._ensure_started()
        consumer = state.consumer

        incoming_message = await self._next_message(consumer)

        if incoming_message is None:
            # The consumer was torn down (e.g. the channel closed and wasn't - or
            # couldn't be - transparently recovered by aio_pika's robust machinery).
            # Self-heal by re-establishing it.
            # Guarded so that concurrent receives (or the passive queue check) don't
            # each spin up their own replacement consumer for the same death.
            async with self._consumer_lock:
                if self._state is state and state.consumer is consumer:
                    self._logger.warning("rabbitmq.consumer.reinitializing", address=self.address)
                    await self._close_consumer(consumer)
                    state.consumer = await self._create_consumer(state.input_queue)
            return None

        async def on_ack(_: TransactionContext) -> None:
            await self._retrier.run(incoming_message.ack)

        async def on_nack(_: TransactionContext) -> None:
            await self._retrier.run(partial(incoming_message.nack, requeue=True))

        transaction_context.on_ack(on_ack)
        transaction_context.on_nack(on_nack)

        return self._to_transport_message(incoming_message)

    async def _next_message(self, consumer: _Consumer) -> aio_pika.abc.AbstractIncomingMessage | None:
        """Wait for the next buffered message; `None` means the consumer is dead.

        Cancellation-safe by construction: a message is only ever taken out of the
        buffer synchronously after a wait completes, so cancelling the wait (worker
        shutdown, or a deadline imposed by the caller) can never drop one.
        """
        while True:
            if consumer.buffer:
                return consumer.buffer.popleft()
            if consumer.dead:
                return None
            event = consumer.message_available
            if event.is_set():
                # Stale from an earlier delivery whose message was already taken;
                # arm a fresh event, then re-check the buffer before waiting.
                consumer.message_available = anyio.Event()
                continue
            await event.wait()

    async def _create_consumer(self, input_queue: aio_pika.abc.AbstractQueue) -> _Consumer:
        iterator = input_queue.iterator()
        await iterator.consume()
        consumer = _Consumer(iterator=iterator)
        consumer.pump_task = asyncio.create_task(self._pump(consumer))
        return consumer

    async def _pump(self, consumer: _Consumer) -> None:
        try:
            async for message in consumer.iterator:
                consumer.buffer.append(message)
                consumer.message_available.set()
        except (aio_pika.exceptions.AMQPError, aio_pika.exceptions.ChannelInvalidStateError) as exc:
            # A dying channel/connection is an ordinary way for a consumer to end -
            # the self-heal in `receive` deals with it, so no stack trace needed.
            self._logger.warning("rabbitmq.consumer.pump.stopped", address=self.address, error=repr(exc))
        except Exception:
            self._logger.exception("rabbitmq.consumer.pump.crashed", address=self.address)
        finally:
            # Reached on iterator exhaustion (consumer closed / connection gone),
            # crash, or cancellation - all of which mean "this consumer is done".
            consumer.dead = True
            consumer.message_available.set()

    async def _close_consumer(self, consumer: _Consumer) -> None:
        with suppress(Exception):
            await consumer.iterator.close()
        if consumer.pump_task is not None:
            consumer.pump_task.cancel()
            with suppress(asyncio.CancelledError):
                await consumer.pump_task
        # Requeue anything still sitting in the local buffer so it's redelivered
        # promptly instead of dangling unacked until the channel dies. Best-effort:
        # if the channel is already gone, the broker requeues these by itself.
        while consumer.buffer:
            message = consumer.buffer.popleft()
            with suppress(Exception):
                await message.nack(requeue=True)

    async def _ensure_started(self) -> _StartedState:
        if self._state is not None:
            return self._state
        async with self._start_lock:
            if self._state is not None:
                # Another task finished starting while we waited on the lock; mypy's
                # narrowing from the fast-path check above can't see that.
                return self._state  # type: ignore[unreachable]

            connection = await aio_pika.connect_robust(self._connection_uri)
            try:
                await self._declare_and_bind(connection, self.address)

                publish_channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
                direct_exchange = await publish_channel.get_exchange(self._direct_exchange_name, ensure=False)
                topic_exchange = await publish_channel.get_exchange(self._topic_exchange_name, ensure=False)

                consume_channel = await connection.channel()
                await consume_channel.set_qos(prefetch_count=self._prefetch_count)
                input_queue = await consume_channel.get_queue(self.address, ensure=True)
                consumer = await self._create_consumer(input_queue)
            except BaseException:
                # A partial start (e.g. a PRECONDITION_FAILED on redeclare) must not
                # leak the connection; shielded so cancellation can't abandon it either.
                with anyio.CancelScope(shield=True):
                    await connection.close()
                raise

            if self._passive_queue_check_task is not None:
                await self._passive_queue_check_task.start()

            self._state = _StartedState(
                connection=connection,
                publish_channel=publish_channel,
                consume_channel=consume_channel,
                direct_exchange=direct_exchange,
                topic_exchange=topic_exchange,
                input_queue=input_queue,
                consumer=consumer,
            )
            return self._state

    async def _check_input_queue_exists(self) -> None:
        """Passively verify the input queue is still there, and recreate it if not.

        Runs on `_passive_queue_check_task`'s own timer. Uses a throwaway channel - a
        failed passive declare closes whatever channel it was attempted on, and this
        must not take down `_consume_channel`/`_publish_channel` as a side effect of
        just checking.
        """
        state = self._state
        if state is None:
            return
        channel = await state.connection.channel()
        try:
            await channel.declare_queue(self.address, passive=True)
        except aio_pika.exceptions.ChannelClosed:
            self._logger.warning("rabbitmq.queue.missing", address=self.address)
            await self._declare_and_bind(state.connection, self.address)
            async with self._consumer_lock:
                if self._state is state:
                    await self._close_consumer(state.consumer)
                    state.consumer = await self._create_consumer(state.input_queue)
        finally:
            if not channel.is_closed:
                await channel.close()

    async def _declare_and_bind(self, connection: aio_pika.abc.AbstractRobustConnection, address: str) -> None:
        if not any((self._should_declare_exchanges, self._should_declare_input_queue, self._should_bind_input_queue)):
            return

        channel = await connection.channel()
        async with channel:
            if self._should_declare_exchanges:
                await self._declare_exchanges(channel)

            queue: aio_pika.abc.AbstractQueue | None = None
            if self._should_declare_input_queue:
                queue = await self._declare_queue(address, channel)

            if self._should_bind_input_queue:
                if queue is None:
                    queue = await channel.get_queue(address, ensure=False)
                direct_exchange = await channel.get_exchange(self._direct_exchange_name, ensure=False)
                await queue.bind(direct_exchange, routing_key=address)

    async def _declare_exchanges(self, channel: aio_pika.abc.AbstractChannel) -> None:
        await channel.declare_exchange(
            self._direct_exchange_name,
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
            arguments=self._direct_exchange_arguments,
        )
        await channel.declare_exchange(
            self._topic_exchange_name,
            type=aio_pika.ExchangeType.TOPIC,
            durable=True,
            arguments=self._topic_exchange_arguments,
        )

    async def _declare_queue(self, address: str, channel: aio_pika.abc.AbstractChannel) -> aio_pika.abc.AbstractQueue:
        options = (
            self._input_queue_declaration_options
            if address == self.address
            else self._default_queue_declaration_options
        )
        return await channel.declare_queue(
            address,
            exclusive=options.exclusive,
            durable=options.durable,
            auto_delete=options.auto_delete,
            arguments=options.arguments,
        )

    async def _resolve_publish_target(
        self, state: _StartedState, destination_address: str
    ) -> tuple[aio_pika.abc.AbstractExchange, str, bool]:
        """Resolve where and how to publish a message for the given destination address.

        A plain address (e.g. ``"billing"``) is a point-to-point destination: it's
        published to the direct exchange with the address itself as the routing key,
        and marked ``mandatory`` so an unroutable message raises loudly instead of
        vanishing silently.

        An address of the form ``"topic@exchange"`` - as produced by
        `mersal_rabbitmq.subscription_storage.RabbitMqSubscriptionStorage` for pub/sub -
        is published to the named exchange with the topic as the routing key, and is
        *not* mandatory: publishing an event nobody has subscribed to yet is normal,
        not an error.
        """
        if "@" in destination_address:
            routing_key, exchange_name = destination_address.rsplit("@", 1)
            exchange = await self._get_exchange(state, exchange_name)
            return exchange, routing_key, False

        return state.direct_exchange, destination_address, True

    async def _get_exchange(self, state: _StartedState, exchange_name: str) -> aio_pika.abc.AbstractExchange:
        if exchange_name == self._topic_exchange_name:
            return state.topic_exchange
        if exchange_name == self._direct_exchange_name:
            return state.direct_exchange

        if exchange_name not in self._exchange_cache:
            self._exchange_cache[exchange_name] = await state.publish_channel.get_exchange(exchange_name, ensure=True)
        return self._exchange_cache[exchange_name]

    def _to_amqp_message(self, transport_message: TransportMessage) -> aio_pika.Message:
        headers = transport_message.headers
        message_id = headers.message_id
        correlation_id = headers.correlation_id
        return aio_pika.Message(
            body=transport_message.body,
            headers={key: _to_header_value(value) for key, value in headers.items()},
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=str(message_id) if message_id is not None else None,
            correlation_id=str(correlation_id) if correlation_id is not None else None,
        )

    def _to_transport_message(self, message: aio_pika.abc.AbstractIncomingMessage) -> TransportMessage:
        return TransportMessage(body=message.body, headers=MessageHeaders(message.headers or {}))
