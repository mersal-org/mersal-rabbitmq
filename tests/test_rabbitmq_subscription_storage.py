import uuid
from collections.abc import Awaitable, Callable
from typing import Any, cast

import aio_pika
import pytest

from mersal.subscription import SubscriptionStorage
from mersal_rabbitmq.subscription_storage import (
    RabbitMqSubscriptionStorage,
    RabbitMqSubscriptionStorageConfig,
)
from mersal_testing.subscription.basic_subscription_storage_tests import (
    BasicSubscriptionStorageTest,
    SubscriptionStorageMaker,
)
from mersal_testing.testing_utils import is_docker_available

__all__ = ("TestRabbitMQSubscriptionStorage",)


pytestmark = [
    pytest.mark.anyio,
    pytest.mark.usefixtures("rabbitmq_service"),
    pytest.mark.skipif(not is_docker_available(), reason="docker not available on this platform"),
]


class TestRabbitMQSubscriptionStorage(BasicSubscriptionStorageTest):
    # RabbitMqSubscriptionStorage has no notion of a topic "owner" to route
    # subscribe/unsubscribe control messages to - every instance can bind or unbind
    # the shared topic exchange directly - so decentralized mode isn't offered.
    supports_decentralized = False

    @pytest.fixture
    def centralized_storage_maker(self, connection_uri: str, topic_exchange_name: str) -> SubscriptionStorageMaker:
        def maker(**kwargs: Any) -> SubscriptionStorage:
            config = RabbitMqSubscriptionStorageConfig(
                connection_uri=connection_uri,
                topic_exchange_name=topic_exchange_name,
                **kwargs,
            )
            return RabbitMqSubscriptionStorage(config)

        return maker

    async def test_centralized_storage_is_shared_across_instances(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        centralized_storage_maker: SubscriptionStorageMaker,
        connection_uri: str,
        topic_exchange_name: str,
        delete_queues: Callable[..., Awaitable[None]],
    ) -> None:
        """Overrides the base assertion.

        `RabbitMqSubscriptionStorage.get_subscriber_addresses` returns a single
        synthetic `"{topic}@{exchange}"` address rather than the literal subscriber
        addresses (see the class docstring for why), so it can't satisfy the base
        test's literal address-equality check. This verifies the same underlying
        guarantee instead - a binding made through one instance is visible and
        effective broker-wide, not cached per-instance - via an actual publish/consume
        round trip through a queue that a *second*, independently constructed instance
        unregisters.
        """
        subject1 = centralized_storage_maker()
        subject2 = centralized_storage_maker()
        assert subject1.is_centralized
        assert subject2.is_centralized

        topic = "topic"
        queue_name = f"subscriber-{uuid.uuid4()}"

        try:
            connection = await aio_pika.connect_robust(connection_uri)
            async with connection:
                channel = await connection.channel()
                exchange = await channel.get_exchange(topic_exchange_name, ensure=True)
                # `register_subscriber` no longer declares the queue itself - it
                # expects the subscriber's own transport to have already done so at
                # its startup - so this stands in for that transport here.
                queue = await channel.declare_queue(queue_name, durable=True)

                await subject1.register_subscriber(topic, queue_name)

                magic_address = f"{topic}@{topic_exchange_name}"
                assert await subject1.get_subscriber_addresses(topic) == {magic_address}
                assert await subject2.get_subscriber_addresses(topic) == {magic_address}

                await exchange.publish(aio_pika.Message(body=b"hello"), routing_key=topic)
                message = await queue.get(fail=False, timeout=2)
                assert message is not None
                assert message.body == b"hello"
                await message.ack()

                # Unregister through the *other* instance, proving the binding isn't
                # cached per-instance.
                await subject2.unregister_subscriber(topic, queue_name)

                await exchange.publish(aio_pika.Message(body=b"world"), routing_key=topic)
                message = await queue.get(fail=False, timeout=1)
                assert message is None
        finally:
            await delete_queues(queue_name)
            await cast("RabbitMqSubscriptionStorage", subject1).close()
            await cast("RabbitMqSubscriptionStorage", subject2).close()

    async def test_register_and_unregister_are_idempotent(
        self,
        centralized_storage_maker: SubscriptionStorageMaker,
        connection_uri: str,
        delete_queues: Callable[..., Awaitable[None]],
    ) -> None:
        """Registering/unregistering twice mirrors plain AMQP `queue.bind`/`unbind`
        semantics: binding (or unbinding) the same routing key twice is a no-op, not
        an error - useful since apps may resubscribe on every restart.
        """
        subject = centralized_storage_maker()
        topic = "topic"
        queue_name = f"subscriber-{uuid.uuid4()}"

        try:
            # `register_subscriber` expects the subscriber's own transport to have
            # already declared its queue at startup - stand in for that here.
            connection = await aio_pika.connect_robust(connection_uri)
            async with connection:
                channel = await connection.channel()
                await channel.declare_queue(queue_name, durable=True)

            await subject.register_subscriber(topic, queue_name)
            await subject.register_subscriber(topic, queue_name)

            await subject.unregister_subscriber(topic, queue_name)
            await subject.unregister_subscriber(topic, queue_name)
        finally:
            await delete_queues(queue_name)
            await cast("RabbitMqSubscriptionStorage", subject).close()
