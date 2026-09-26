import json
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
from datetime import timedelta
from typing import Any

import anyio
import pytest

from mersal.activation import BuiltinHandlerActivator
from mersal.core.app import Mersal
from mersal.persistence.in_memory import InMemoryTimeoutManager
from mersal.rabbitmq.plugin import RabbitMQPluginConfig
from mersal.testing.core.testing_utils import is_docker_available
from mersal.timeouts import TimeoutsConfig

__all__ = ("TestRabbitMQPlugin",)


pytestmark = [
    pytest.mark.anyio,
    pytest.mark.usefixtures("rabbitmq_service"),
    pytest.mark.skipif(not is_docker_available(), reason="docker not available on this platform"),
]


@dataclass
class Greeting:
    text: str


class _JsonSerializer:
    """Bytes-on-the-wire serializer for this test only.

    The transport/subscription-storage tests build `TransportMessage`s directly
    (`TransportMessageBuilder`), bypassing app-level serialization entirely. This test
    goes through a real `Mersal` app instead, so it needs a `Serializer` that actually
    turns a message into the bytes `aio_pika.Message` requires.
    """

    def __init__(self, types: set[type]) -> None:
        self._types = {t.__name__: t for t in types}

    def serialize(self, obj: Any) -> bytes:
        return json.dumps({"type": type(obj).__name__, "data": asdict(obj)}).encode()

    def deserialize(self, data: bytes) -> Any:
        payload = json.loads(data)
        return self._types[payload["type"]](**payload["data"])


class TestRabbitMQPlugin:
    """End-to-end check that `RabbitMQPluginConfig` (the way `docs/usage.rst` tells
    users to configure this package) wires a working transport and subscription
    storage into a real `Mersal` app, on top of the transport/subscription-storage
    tests exercising those pieces directly.
    """

    async def test_send_local_is_delivered_to_the_registered_handler(
        self,
        connection_uri: str,
        topic_exchange_name: str,
        delete_queues: Callable[..., Awaitable[None]],
    ) -> None:
        received: list[Greeting] = []
        done = anyio.Event()

        activator = BuiltinHandlerActivator()

        def handler_factory(message_context: Any, app: Mersal) -> Callable[[Greeting], Awaitable[None]]:
            async def handler(message: Greeting) -> None:
                received.append(message)
                done.set()

            return handler

        activator.register(Greeting, handler_factory)

        queue_name = f"plugin-test-{uuid.uuid4()}"
        plugin_config = RabbitMQPluginConfig(
            connection_uri=connection_uri,
            input_queue_name=queue_name,
            topic_exchange_name=topic_exchange_name,
        )

        app = Mersal(
            "plugin-test-app",
            activator,
            plugins=[plugin_config.plugin()],
            serializer=_JsonSerializer(types={Greeting}),
        )

        try:
            await app.start()
            await app.send_local(Greeting(text="hello"))

            with anyio.fail_after(5.0):
                await done.wait()

            assert received == [Greeting(text="hello")]
        finally:
            await app.stop()
            await delete_queues(queue_name)

    async def test_send_only_app_can_send_to_a_receiving_app(
        self,
        connection_uri: str,
        topic_exchange_name: str,
        delete_queues: Callable[..., Awaitable[None]],
    ) -> None:
        """A send-only app (`Mersal(..., send_only=True)`) still gets a working
        transport wired up by `RabbitMQPlugin`, proving `configurator.send_only`
        reaches `RabbitMqTransportConfig`.
        """
        received: list[Greeting] = []
        done = anyio.Event()

        receiver_activator = BuiltinHandlerActivator()

        def handler_factory(message_context: Any, app: Mersal) -> Callable[[Greeting], Awaitable[None]]:
            async def handler(message: Greeting) -> None:
                received.append(message)
                done.set()

            return handler

        receiver_activator.register(Greeting, handler_factory)

        receiver_queue_name = f"plugin-test-receiver-{uuid.uuid4()}"
        sender_queue_name = f"plugin-test-sender-{uuid.uuid4()}"

        receiver = Mersal(
            "plugin-test-receiver",
            receiver_activator,
            plugins=[
                RabbitMQPluginConfig(
                    connection_uri=connection_uri,
                    input_queue_name=receiver_queue_name,
                    topic_exchange_name=topic_exchange_name,
                ).plugin()
            ],
            serializer=_JsonSerializer(types={Greeting}),
        )
        sender = Mersal(
            "plugin-test-sender",
            BuiltinHandlerActivator(),
            plugins=[
                RabbitMQPluginConfig(
                    connection_uri=connection_uri,
                    input_queue_name=sender_queue_name,
                    topic_exchange_name=topic_exchange_name,
                ).plugin()
            ],
            serializer=_JsonSerializer(types={Greeting}),
            send_only=True,
        )

        try:
            await receiver.start()
            await sender.start()

            assert sender.worker is None

            await sender.send(Greeting(text="hello"), addresses={receiver_queue_name})

            with anyio.fail_after(5.0):
                await done.wait()

            assert received == [Greeting(text="hello")]
        finally:
            await sender.stop()
            await receiver.stop()
            await delete_queues(receiver_queue_name)

    async def test_defer_local_is_delivered_after_the_delay(
        self,
        connection_uri: str,
        topic_exchange_name: str,
        delete_queues: Callable[..., Awaitable[None]],
    ) -> None:
        received_at: list[float] = []
        done = anyio.Event()

        activator = BuiltinHandlerActivator()

        def handler_factory(message_context: Any, app: Mersal) -> Callable[[Greeting], Awaitable[None]]:
            async def handler(message: Greeting) -> None:
                received_at.append(anyio.current_time())
                done.set()

            return handler

        activator.register(Greeting, handler_factory)

        queue_name = f"plugin-defer-test-{uuid.uuid4()}"
        app = Mersal(
            "plugin-defer-test-app",
            activator,
            plugins=[
                RabbitMQPluginConfig(
                    connection_uri=connection_uri,
                    input_queue_name=queue_name,
                    topic_exchange_name=topic_exchange_name,
                    delayed_exchange_name="mersal.delayed",
                ).plugin()
            ],
            serializer=_JsonSerializer(types={Greeting}),
        )

        try:
            await app.start()
            sent_at = anyio.current_time()
            await app.defer_local(timedelta(seconds=1.5), Greeting(text="later"))

            with anyio.fail_after(10.0):
                await done.wait()

            assert received_at[0] - sent_at >= 1.4
        finally:
            await app.stop()
            await delete_queues(queue_name)

    async def test_defer_local_through_a_timeout_manager_without_a_delayed_exchange(
        self,
        connection_uri: str,
        topic_exchange_name: str,
        delete_queues: Callable[..., Awaitable[None]],
    ) -> None:
        done = anyio.Event()
        activator = BuiltinHandlerActivator()

        def handler_factory(message_context: Any, app: Mersal) -> Callable[[Greeting], Awaitable[None]]:
            async def handler(message: Greeting) -> None:
                done.set()

            return handler

        activator.register(Greeting, handler_factory)

        queue_name = f"plugin-timeouts-test-{uuid.uuid4()}"
        timeout_manager = InMemoryTimeoutManager()
        app = Mersal(
            "plugin-timeouts-test-app",
            activator,
            plugins=[
                RabbitMQPluginConfig(
                    connection_uri=connection_uri,
                    input_queue_name=queue_name,
                    topic_exchange_name=topic_exchange_name,
                ).plugin()
            ],
            serializer=_JsonSerializer(types={Greeting}),
            timeouts=TimeoutsConfig(storage=timeout_manager, poll_interval=0.1),
        )

        try:
            await app.start()
            await app.defer_local(timedelta(seconds=1), Greeting(text="later"))

            with anyio.fail_after(5.0):
                while not len(timeout_manager):
                    await anyio.sleep(0.05)
            assert not done.is_set()
            with anyio.fail_after(10.0):
                await done.wait()
            # marked as completed only after the send, which can race the handler
            with anyio.fail_after(5.0):
                while len(timeout_manager):
                    await anyio.sleep(0.05)
        finally:
            await app.stop()
            await delete_queues(queue_name)
