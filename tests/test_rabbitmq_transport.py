import uuid
from collections.abc import AsyncGenerator

import aio_pika
import pytest

from mersal_rabbitmq.transport import RabbitMqTransport, RabbitMqTransportConfig
from mersal_testing.testing_utils import is_docker_available

__all__ = ("TestRabbitMQTransport",)


pytestmark = [
    pytest.mark.anyio,
    pytest.mark.usefixtures("rabbitmq_service"),
    pytest.mark.skipif(not is_docker_available(), reason="docker not available on this platform"),
]


class TestRabbitMQTransport:
    @pytest.fixture
    def connection_uri(self) -> str:
        return "amqp://guest:guest@127.0.0.1:5672"

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
        transport = RabbitMqTransport(config=config)
        await transport()

        assert await self._check_queue_existsnce(connection_uri, queue_name=queue_name)

    async def _check_queue_existsnce(self, connection_uri: str, queue_name: str) -> bool:
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
