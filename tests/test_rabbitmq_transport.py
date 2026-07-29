import uuid
from collections.abc import AsyncGenerator
from typing import cast

import aio_pika
import anyio
import pytest

from mersal.logging import NullLogger
from mersal.transport import DefaultTransactionContext
from mersal.types.callable_types import AsyncAnyCallable
from mersal_rabbitmq.transport import RabbitMqTransport, RabbitMqTransportConfig
from mersal_testing.test_doubles import TransportMessageBuilder
from mersal_testing.testing_utils import is_docker_available
from mersal_testing.transport.basic_transport_tests import BasicTransportTest, TransportMaker

__all__ = (
    "TestRabbitMQTransport",
    "TestRabbitMQTransportContract",
    "TestRabbitMQTransportSpecificBehaviour",
)


pytestmark = [
    pytest.mark.anyio,
    pytest.mark.usefixtures("rabbitmq_service"),
    pytest.mark.skipif(not is_docker_available(), reason="docker not available on this platform"),
]


class TestRabbitMQTransport:
    @pytest.fixture
    async def queue_name(self, connection_uri: str) -> AsyncGenerator[str, None]:
        n = str(uuid.uuid4())
        yield n
        await self._delete_queue(connection_uri, n)

    async def test_create_queue(self, connection_uri: str, queue_name: str):
        config = RabbitMqTransportConfig(
            connection_uri=connection_uri,
            input_queue_name=queue_name,
        )
        transport = RabbitMqTransport(config=config, logger=NullLogger())
        await transport()

        assert await self._check_queue_exists(connection_uri, queue_name=queue_name)
        await transport.close()

    async def _check_queue_exists(self, connection_uri: str, queue_name: str) -> bool:
        connection = await aio_pika.connect_robust(connection_uri)
        async with connection:
            channel = await connection.channel()
            try:
                await channel.get_queue(queue_name, ensure=True)
                return True
            except aio_pika.exceptions.ChannelClosed:
                return False

    async def _delete_queue(self, connection_uri: str, queue_name: str):
        connection = await aio_pika.connect_robust(connection_uri)
        async with connection:
            channel = await connection.channel()
            await channel.queue_delete(queue_name)


class TestRabbitMQTransportContract(BasicTransportTest):
    """Runs mersal_testing's generic transport contract against `RabbitMqTransport`."""

    @pytest.fixture
    def transport_maker(self, rabbitmq_transport_maker: TransportMaker) -> TransportMaker:
        return rabbitmq_transport_maker


class TestRabbitMQTransportSpecificBehaviour:
    """RabbitMQ-specific behaviour that the generic transport contract doesn't (and
    shouldn't) know about: broker-native pub/sub fan-out via the topic exchange,
    fail-loud delivery guarantees for point-to-point sends, and self-healing after the
    underlying consumer is torn down.
    """

    @pytest.fixture
    def transport_maker(self, rabbitmq_transport_maker: TransportMaker) -> TransportMaker:
        return rabbitmq_transport_maker

    #: `receive()` has no built-in timeout - it blocks until a message arrives - so
    #: every receive expecting a message runs under this external deadline, turning a
    #: delivery regression into a fast `TimeoutError` instead of a hung test.
    receive_deadline: float = 5.0

    async def assert_with_context(
        self,
        assertions_call: AsyncAnyCallable,
        commit: bool = True,
        ack: bool = True,
    ) -> None:
        async with DefaultTransactionContext() as context:
            await assertions_call(context)
            context.set_result(commit=commit, ack=ack)
            await context.complete()

    async def test_send_fails_loud_when_topology_management_is_disabled_and_destination_missing(
        self, transport_maker: TransportMaker, connection_uri: str
    ) -> None:
        """Defensive provisioning is itself gated by the transport's own topology flags:
        if a transport is configured to manage no topology at all
        (`should_declare_exchanges`/`should_declare_input_queue`/`should_bind_input_queue`
        all `False` - topology fully managed externally, e.g. by infrastructure-as-code),
        it doesn't declare anything on send either, so a mandatory publish to a
        destination nothing has bound still fails loud rather than silently creating an
        orphan queue for what might just be a typo.
        """
        # A real deployment with topology management disabled would have its exchanges
        # and its own input queue pre-declared by infrastructure rather than left to
        # the transport - including the sender's own queue, since `_ensure_started`
        # always verifies its own input queue exists, regardless of these flags.
        connection = await aio_pika.connect_robust(connection_uri)
        async with connection:
            channel = await connection.channel()
            direct_exchange = await channel.declare_exchange(
                "mersal.direct", type=aio_pika.ExchangeType.DIRECT, durable=True
            )
            sender_queue = await channel.declare_queue("unmanaged-sender", durable=True)
            await sender_queue.bind(direct_exchange, routing_key="unmanaged-sender")

        sender = transport_maker(
            input_queue_address="unmanaged-sender",
            should_declare_exchanges=False,
            should_declare_input_queue=False,
            should_bind_input_queue=False,
        )
        message = TransportMessageBuilder.build()

        async def _send(context: DefaultTransactionContext) -> None:
            await sender.send("this-destination-was-never-created", message, context)

        with pytest.raises(aio_pika.exceptions.DeliveryError):
            await self.assert_with_context(_send)

    async def test_custom_headers_round_trip(self, transport_maker: TransportMaker) -> None:
        sender = transport_maker(input_queue_address="header-sender")
        receiver = transport_maker(input_queue_address="header-receiver")
        message = TransportMessageBuilder.build()
        message.headers["x-custom-header"] = "some-value"

        # Starts the receiver so its queue exists before the sender addresses it.
        await receiver()

        async def _send(context: DefaultTransactionContext) -> None:
            await sender.send("header-receiver", message, context)

        await self.assert_with_context(_send)

        async def _receive(context: DefaultTransactionContext) -> None:
            with anyio.fail_after(self.receive_deadline):
                received = await receiver.receive(context)
            assert received is not None
            assert received.headers["x-custom-header"] == "some-value"
            assert str(received.headers.message_id) == str(message.headers.message_id)

        await self.assert_with_context(_receive)

    async def test_publish_to_topic_exchange_fans_out_to_every_bound_queue(
        self,
        transport_maker: TransportMaker,
        connection_uri: str,
        topic_exchange_name: str,
    ) -> None:
        """Exercises the "magic address" pub/sub path directly at the transport level:
        an address of the form `topic@exchange` is published once to that exchange,
        and every queue bound to it (simulating what
        `RabbitMqSubscriptionStorage.register_subscriber` does) receives its own copy -
        broker-native fan-out, not N point-to-point sends.
        """
        publisher = transport_maker(input_queue_address="fanout-publisher")
        subscriber1 = transport_maker(input_queue_address="fanout-sub-1")
        subscriber2 = transport_maker(input_queue_address="fanout-sub-2")

        # Starts each subscriber so its queue/exchanges exist before we bind.
        await subscriber1()
        await subscriber2()

        topic = "some.event.topic"
        connection = await aio_pika.connect_robust(connection_uri)
        async with connection:
            channel = await connection.channel()
            exchange = await channel.get_exchange(topic_exchange_name, ensure=True)
            for address in ("fanout-sub-1", "fanout-sub-2"):
                queue = await channel.get_queue(address, ensure=True)
                await queue.bind(exchange, routing_key=topic)

        message = TransportMessageBuilder.build()
        magic_address = f"{topic}@{topic_exchange_name}"

        async def _publish(context: DefaultTransactionContext) -> None:
            await publisher.send(magic_address, message, context)

        await self.assert_with_context(_publish)

        async def _assert_delivered(context: DefaultTransactionContext) -> None:
            with anyio.fail_after(self.receive_deadline):
                received1 = await subscriber1.receive(context)
                received2 = await subscriber2.receive(context)
            assert received1 is not None
            assert received2 is not None
            assert str(received1.headers.message_id) == str(message.headers.message_id)
            assert str(received2.headers.message_id) == str(message.headers.message_id)

        await self.assert_with_context(_assert_delivered)

    async def test_publish_to_topic_with_no_subscribers_does_not_raise(
        self,
        transport_maker: TransportMaker,
        topic_exchange_name: str,
    ) -> None:
        """Unlike point-to-point sends, publishing an event nobody has subscribed to
        yet is normal, not an error - pub/sub sends are not `mandatory`.
        """
        publisher = transport_maker(input_queue_address="fanout-publisher-lonely")
        message = TransportMessageBuilder.build()
        magic_address = f"nobody.listens.to.this@{topic_exchange_name}"

        async def _publish(context: DefaultTransactionContext) -> None:
            await publisher.send(magic_address, message, context)

        await self.assert_with_context(_publish)

    async def test_receive_self_heals_after_consumer_is_torn_down(self, transport_maker: TransportMaker) -> None:
        """If the underlying consumer dies (e.g. its channel was closed) `receive()`
        transparently reinitializes it on the next call rather than returning `None`
        forever.
        """
        sender = transport_maker(input_queue_address="healer-sender")
        receiver = cast("RabbitMqTransport", transport_maker(input_queue_address="healer"))

        await receiver()

        assert receiver._state is not None
        await receiver._state.consumer.iterator.close()

        message = TransportMessageBuilder.build()

        async def _send(context: DefaultTransactionContext) -> None:
            await sender.send("healer", message, context)

        await self.assert_with_context(_send)

        async def _first_receive_after_death(context: DefaultTransactionContext) -> None:
            # A dead consumer is noticed immediately - no message wait involved.
            with anyio.fail_after(self.receive_deadline):
                assert await receiver.receive(context) is None

        await self.assert_with_context(_first_receive_after_death)

        async def _second_receive(context: DefaultTransactionContext) -> None:
            with anyio.fail_after(self.receive_deadline):
                received = await receiver.receive(context)
            assert received is not None
            assert str(received.headers.message_id) == str(message.headers.message_id)

        await self.assert_with_context(_second_receive)

    async def test_passive_queue_check_recreates_a_deleted_input_queue(
        self, transport_maker: TransportMaker, connection_uri: str
    ) -> None:
        """The reactive self-heal above only notices a dead *consumer*; it can't help
        if the queue itself is gone (e.g. deleted by a TTL/`x-expires` policy) while
        nothing happens to be receiving. `_check_input_queue_exists` - normally run
        periodically by `_passive_queue_check_task` - is what notices and recreates the
        queue in that case. Invoked directly here rather than waiting on the task's
        real interval, for a deterministic test.
        """
        sender = transport_maker(input_queue_address="passive-check-sender")
        receiver = cast("RabbitMqTransport", transport_maker(input_queue_address="passive-check-target"))

        await receiver()

        connection = await aio_pika.connect_robust(connection_uri)
        async with connection:
            channel = await connection.channel()
            await channel.queue_delete("passive-check-target")

        await receiver._check_input_queue_exists()

        message = TransportMessageBuilder.build()

        async def _send(context: DefaultTransactionContext) -> None:
            await sender.send("passive-check-target", message, context)

        await self.assert_with_context(_send)

        async def _receive(context: DefaultTransactionContext) -> None:
            with anyio.fail_after(self.receive_deadline):
                received = await receiver.receive(context)
            assert received is not None
            assert str(received.headers.message_id) == str(message.headers.message_id)

        await self.assert_with_context(_receive)

    async def test_passive_queue_check_disabled_when_interval_is_none(self, transport_maker: TransportMaker) -> None:
        transport = cast(
            "RabbitMqTransport",
            transport_maker(input_queue_address="passive-check-disabled", passive_queue_check_interval=None),
        )
        assert transport._passive_queue_check_task is None
