from __future__ import annotations

import math
from collections.abc import Awaitable, Callable
from contextlib import AsyncExitStack, suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from functools import partial
from typing import TYPE_CHECKING, Any

import aio_pika
import aio_pika.abc
import anyio
import anyio.abc

from mersal.logging import Logger, NullLogger
from mersal.messages import TransportMessage
from mersal.messages.message_headers import MessageHeaders
from mersal.threading import AnyIOPeriodicTaskFactory, PeriodicAsyncTask, PeriodicAsyncTaskFactory
from mersal.transport.base_transport import BaseTransport

if TYPE_CHECKING:
    from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
    from pamqp import common as pamqp_common

    from mersal.transport import TransactionContext
    from mersal.transport.outgoing_message import OutgoingMessage

__all__ = (
    "QueueDeclarationOptions",
    "RabbitMqTransport",
    "RabbitMqTransportConfig",
)


_RETRY_DELAYS = [0.1, 0.5, 2.0]

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
    send_only: bool = False
    """Set by `RabbitMQPlugin` from the owning app's `send_only` configuration - a
    send-only app never receives, so this transport skips declaring/binding its own
    input queue and never opens a consume channel or consumer for it.
    """
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

    Also size this against processing time, not just parallelism: RabbitMQ (3.8.15+)
    forcibly closes a channel whose oldest unacked delivery exceeds `consumer_timeout`
    (broker-side setting, default 30 minutes) - and a message can sit unacked in
    `_Consumer.receive_stream` (this transport's local prefetch buffer, ahead of the app
    actually processing it) for a while under a large `prefetch_count` and slow
    handlers. Hitting the timeout kills the consume channel; the transport self-heals
    it (see `receive`), but every message still in flight at that point gets
    redelivered.
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
    """A live consumer subscription plus the memory object stream it feeds."""

    iterator: aio_pika.abc.AbstractQueueIterator
    send_stream: MemoryObjectSendStream[aio_pika.abc.AbstractIncomingMessage]
    receive_stream: MemoryObjectReceiveStream[aio_pika.abc.AbstractIncomingMessage]
    cancel_scope: anyio.CancelScope = field(default_factory=anyio.CancelScope)
    pump_done: anyio.Event = field(default_factory=anyio.Event)


@dataclass
class _ReceiveState:
    """Everything that only exists for consuming from this app's own input queue.

    `None` on `_StartedState` for a send-only transport, which never declares/binds
    its own input queue or consumes from it.
    """

    consume_channel: aio_pika.abc.AbstractChannel
    input_queue: aio_pika.abc.AbstractQueue
    consumer: _Consumer
    pump_task_group: anyio.abc.TaskGroup


@dataclass
class _StartedState:
    """Everything that only exists once the transport has connected.

    `_ensure_started` hands back this fully-typed state.
    """

    connection: aio_pika.abc.AbstractRobustConnection
    publish_channel: aio_pika.abc.AbstractChannel
    direct_exchange: aio_pika.abc.AbstractExchange
    topic_exchange: aio_pika.abc.AbstractExchange
    pump_exit_stack: AsyncExitStack
    receive: _ReceiveState | None


class RabbitMqTransport(BaseTransport):
    """A RabbitMQ transport backed by a single persistent, auto-recovering connection.

    Topology:
        Two durable exchanges are declared: a ``direct`` exchange used for
        point-to-point sends (each app's input queue is bound to it with a routing key
        equal to its own address), and a ``topic`` exchange used for pub/sub (see
        `mersal_rabbitmq.subscription_storage.RabbitMqSubscriptionStorage`).

    Backend:
        Anyio-native, including the consumer pump: `_pump` runs in a long-lived task
        group (`_StartedState.pump_task_group`) that outlives any single `receive`
        call, with a per-consumer `anyio.CancelScope` (see `_Consumer`) used to cancel
        just one consumer's pump - e.g. on self-heal - without tearing down the group
        itself. `send_outgoing_messages` fires concurrent publishes via an anyio task
        group too.

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
        `RabbitMqTransportConfig.passive_queue_check_interval`. Recreating a vanished
        queue only restores its direct-exchange binding; the `queue_recreated_hook`
        property lets a subscription storage re-establish topic-exchange bindings too
        (see `RabbitMQPlugin`).

    Send-only apps:
        See `RabbitMqTransportConfig.send_only`: this app's own input queue is only
        ever declared, bound, and consumed from when it isn't send-only, since a
        send-only app never receives.
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
        self._send_only = config.send_only

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
            if config.passive_queue_check_interval is not None and not self._send_only
            else None
        )

        self._exchange_cache: dict[str, aio_pika.abc.AbstractExchange] = {}

        self._state: _StartedState | None = None
        self._start_lock = anyio.Lock()
        self._consumer_lock = anyio.Lock()
        self._on_queue_recreated: Callable[[str], Awaitable[None]] | None = None

    async def __call__(self) -> None:
        await self._ensure_started()

    @property
    def queue_recreated_hook(self) -> Callable[[str], Awaitable[None]] | None:
        """Callback invoked with `self.address` whenever the passive queue check
        finds the input queue gone and recreates it.

        Recreating the queue only restores its direct-exchange binding (see
        `_declare_and_bind`) - any topic-exchange bindings it had (i.e. this app's
        subscriptions) are gone with the deleted queue. `RabbitMQPlugin` wires this to
        `RabbitMqSubscriptionStorage.rebind_subscriptions` so those get re-established
        too.
        """
        return self._on_queue_recreated

    @queue_recreated_hook.setter
    def queue_recreated_hook(self, hook: Callable[[str], Awaitable[None]] | None) -> None:
        self._on_queue_recreated = hook

    async def ensure_connection(self) -> aio_pika.abc.AbstractRobustConnection:
        """Return the transport's underlying connection, connecting first if necessary.

        Lets other RabbitMQ-backed components sharing this broker (e.g.
        `RabbitMqSubscriptionStorage`) reuse this connection instead of opening one of
        their own - one fewer TCP connection and reconnect state machine against the
        broker.
        """
        state = await self._ensure_started()
        return state.connection

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
            if state.receive is not None:
                await self._close_consumer(state.receive.consumer)
                state.receive.pump_task_group.cancel_scope.cancel()
            await state.pump_exit_stack.aclose()
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

        async def _publish(message: OutgoingMessage) -> None:
            exchange, routing_key, mandatory = await self._resolve_publish_target(state, message.destination_address)
            amqp_message = self._to_amqp_message(message.transport_message)
            await self._retry(partial(exchange.publish, amqp_message, routing_key, mandatory=mandatory))

        async with anyio.create_task_group() as task_group:
            for message in outgoing_message:
                _ = task_group.start_soon(_publish, message)

    async def receive(self, transaction_context: TransactionContext) -> TransportMessage | None:
        """Wait for the next message; blocks until one arrives."""
        state = await self._ensure_started()
        if state.receive is None:
            raise RuntimeError("This transport is send-only; it has no input queue to receive from.")
        consumer = state.receive.consumer

        try:
            incoming_message = await consumer.receive_stream.receive()
        except anyio.EndOfStream:
            incoming_message = None

        if incoming_message is None:
            # The consumer was torn down (e.g. the channel closed and wasn't - or
            # couldn't be - transparently recovered by aio_pika's robust machinery).
            # Self-heal by re-establishing it.
            # Guarded so that concurrent receives (or the passive queue check) don't
            # each spin up their own replacement consumer for the same death.
            async with self._consumer_lock:
                if self._state is state and state.receive.consumer is consumer:
                    self._logger.warning("rabbitmq.consumer.reinitializing", address=self.address)
                    await self._close_consumer(consumer)
                    state.receive.consumer = await self._create_consumer(
                        state.receive.input_queue, state.receive.pump_task_group
                    )
            return None

        # A message delivered before a connection drop is bound to that dead
        # connection's channel; ack/nack against it raises `ChannelInvalidStateError`
        # on every attempt, including retries - `_retry` treats that as a retryable
        # broker hiccup, but this particular failure can't be fixed by waiting, only by
        # a fresh redelivery on the new channel (which the broker sends on its own, per
        # normal at-least-once semantics). Expected log noise, not a bug.
        async def on_ack(_: TransactionContext) -> None:
            await self._retry(incoming_message.ack)

        async def on_nack(_: TransactionContext) -> None:
            await self._retry(partial(incoming_message.nack, requeue=True))

        transaction_context.on_ack(on_ack)
        transaction_context.on_nack(on_nack)

        return self._to_transport_message(incoming_message)

    async def _retry(self, func: Callable[[], Awaitable[Any]]) -> None:
        """Retry `func` with backoff."""
        for delay in (*_RETRY_DELAYS, None):
            try:
                await func()
                return
            except BaseException:
                if delay is None:
                    raise
                await anyio.sleep(delay)

    async def _create_consumer(
        self, input_queue: aio_pika.abc.AbstractQueue, pump_task_group: anyio.abc.TaskGroup
    ) -> _Consumer:
        iterator = input_queue.iterator()
        await iterator.consume()
        send_stream: MemoryObjectSendStream[aio_pika.abc.AbstractIncomingMessage]
        receive_stream: MemoryObjectReceiveStream[aio_pika.abc.AbstractIncomingMessage]

        send_stream, receive_stream = anyio.create_memory_object_stream(max_buffer_size=math.inf)
        consumer = _Consumer(iterator=iterator, send_stream=send_stream, receive_stream=receive_stream)
        _ = pump_task_group.start_soon(self._run_pump, consumer)
        return consumer

    async def _run_pump(self, consumer: _Consumer) -> None:
        try:
            with consumer.cancel_scope:
                await self._pump(consumer)
        finally:
            consumer.pump_done.set()

    async def _pump(self, consumer: _Consumer) -> None:
        try:
            async for message in consumer.iterator:
                consumer.send_stream.send_nowait(message)
        except (aio_pika.exceptions.AMQPError, aio_pika.exceptions.ChannelInvalidStateError) as exc:
            # A dying channel/connection is an ordinary way for a consumer to end -
            # the self-heal in `receive` deals with it, so no stack trace needed.
            self._logger.warning("rabbitmq.consumer.pump.stopped", address=self.address, error=repr(exc))
        except Exception:
            self._logger.exception("rabbitmq.consumer.pump.crashed", address=self.address)
        finally:
            # Reached on iterator exhaustion (consumer closed / connection gone),
            # crash, or cancellation - all of which mean "this consumer is done".
            # Closing the send end turns the next (or a currently pending)
            # `receive_stream.receive()` into `EndOfStream`, once anything already
            # buffered is drained.
            consumer.send_stream.close()

    async def _close_consumer(self, consumer: _Consumer) -> None:
        with suppress(Exception):
            await consumer.iterator.close()
        consumer.cancel_scope.cancel()
        await consumer.pump_done.wait()
        # Requeue anything still sitting in the stream so it's redelivered promptly
        # instead of dangling unacked until the channel dies. Best-effort: if the
        # channel is already gone, the broker requeues these by itself.
        while True:
            try:
                message = consumer.receive_stream.receive_nowait()
            except (anyio.WouldBlock, anyio.EndOfStream):
                break
            with suppress(Exception):
                await message.nack(requeue=True)
        consumer.receive_stream.close()

    async def _ensure_started(self) -> _StartedState:
        if self._state is not None:
            return self._state
        async with self._start_lock:
            connection = await aio_pika.connect_robust(self._connection_uri)
            pump_exit_stack = AsyncExitStack()
            try:
                if self._send_only:
                    await self._declare_exchanges_only(connection)
                else:
                    await self._declare_and_bind(connection, self.address)

                publish_channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
                direct_exchange = await publish_channel.get_exchange(self._direct_exchange_name, ensure=False)
                topic_exchange = await publish_channel.get_exchange(self._topic_exchange_name, ensure=False)

                receive_state: _ReceiveState | None = None
                if not self._send_only:
                    consume_channel = await connection.channel()
                    await consume_channel.set_qos(prefetch_count=self._prefetch_count)
                    input_queue = await consume_channel.get_queue(self.address, ensure=True)

                    pump_task_group = await pump_exit_stack.enter_async_context(anyio.create_task_group())
                    consumer = await self._create_consumer(input_queue, pump_task_group)
                    receive_state = _ReceiveState(
                        consume_channel=consume_channel,
                        input_queue=input_queue,
                        consumer=consumer,
                        pump_task_group=pump_task_group,
                    )
            except BaseException:
                # A partial start (e.g. a PRECONDITION_FAILED on redeclare) must not
                # leak the connection or the pump task group; shielded so cancellation
                # can't abandon them either.
                with anyio.CancelScope(shield=True):
                    await pump_exit_stack.aclose()
                    await connection.close()
                raise

            if self._passive_queue_check_task is not None:
                await self._passive_queue_check_task.start()

            self._state = _StartedState(
                connection=connection,
                publish_channel=publish_channel,
                direct_exchange=direct_exchange,
                topic_exchange=topic_exchange,
                pump_exit_stack=pump_exit_stack,
                receive=receive_state,
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
        if state is None or state.receive is None:
            return
        channel = await state.connection.channel()
        try:
            await channel.declare_queue(self.address, passive=True)
        except aio_pika.exceptions.ChannelClosed:
            self._logger.warning("rabbitmq.queue.missing", address=self.address)
            await self._declare_and_bind(state.connection, self.address)
            if self._on_queue_recreated is not None:
                await self._on_queue_recreated(self.address)
            async with self._consumer_lock:
                if self._state is state:
                    await self._close_consumer(state.receive.consumer)
                    state.receive.consumer = await self._create_consumer(
                        state.receive.input_queue, state.receive.pump_task_group
                    )
        finally:
            if not channel.is_closed:
                await channel.close()

    async def _declare_exchanges_only(self, connection: aio_pika.abc.AbstractRobustConnection) -> None:
        """Declare the exchanges without touching an input queue - used by
        `_ensure_started` for a send-only transport, which never declares/binds one
        for itself.
        """
        if not self._should_declare_exchanges:
            return
        channel = await connection.channel()
        async with channel:
            await self._declare_exchanges(channel)

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
            # Mersal doesn't track a serialization format, so this only claims the
            # body is opaque bytes (true regardless of serializer) - still useful to
            # interop tooling (management UI, shovels, other consumers) that would
            # otherwise have to guess. `timestamp` likewise costs nothing and is
            # commonly surfaced by the same tooling.
            content_type="application/octet-stream",
            timestamp=datetime.now(UTC),
        )

    def _to_transport_message(self, message: aio_pika.abc.AbstractIncomingMessage) -> TransportMessage:
        return TransportMessage(body=message.body, headers=MessageHeaders(message.headers or {}))
