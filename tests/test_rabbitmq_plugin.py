import json
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
from typing import Any

import anyio
import pytest

from mersal.activation import BuiltinHandlerActivator
from mersal.app import Mersal
from mersal_rabbitmq.plugin import RabbitMQPluginConfig
from mersal_testing.testing_utils import is_docker_available

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
