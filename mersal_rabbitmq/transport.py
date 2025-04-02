import logging
from dataclasses import dataclass
from urllib.parse import urlparse

import aio_pika
import aio_pika.abc
from pamqp import common as pamqp_common

from mersal.transport.base_transport import BaseTransport

__all__ = (
    "QueueDeclerationOptions",
    "RabbitMqTransport",
    "RabbitMqTransportConfig",
)


logger = logging.getLogger(__name__)


@dataclass
class QueueDeclerationOptions:
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
    direct_exchage_arguments: pamqp_common.Arguments | None = None
    topic_exchage_arguments: pamqp_common.Arguments | None = None
    direct_exchange_name: str = "mersal.direct"
    topic_exchange_name: str = "mersal.topics"
    input_queue_decleration_options: QueueDeclerationOptions | None = None
    default_queue_decleration_options: QueueDeclerationOptions | None = None


class RabbitMqTransport(BaseTransport):
    def __init__(
        self,
        config: RabbitMqTransportConfig,
    ):
        super().__init__(config.input_queue_name)

        self._connection_uri = config.connection_uri
        self._parsed_uri = urlparse(config.connection_uri)

        self._direct_exchange_name = config.direct_exchange_name
        self._topic_exchange_name = config.topic_exchange_name
        self.input_queue_decleration_options = config.input_queue_decleration_options or QueueDeclerationOptions(
            durable=True,
            exclusive=False,
            auto_delete=False,
            arguments={},
        )
        self.default_queue_decleration_options = config.default_queue_decleration_options or QueueDeclerationOptions(
            durable=True,
            exclusive=False,
            auto_delete=False,
            arguments={},
        )

        self.should_declare_exchanges = config.should_declare_exchanges
        self.should_declare_input_queue = config.should_declare_input_queue
        self.should_bind_input_queue = config.should_bind_input_queue
        self.direct_exchange_arguments = config.direct_exchage_arguments or {}
        self.topic_exchange_arguments = config.topic_exchage_arguments or {}

        self._input_queue: aio_pika.abc.AbstractQueue | None = None
        self._direct_exchange: aio_pika.abc.AbstractExchange | None = None

    async def __call__(self) -> None:
        await self.create_queue(self.address)

    async def create_queue(self, address: str) -> None:
        if not any([self.should_declare_exchanges, self.should_declare_input_queue, self.should_bind_input_queue]):
            return
        connection = await aio_pika.connect_robust(self._connection_uri)
        async with connection:
            channel = await connection.channel()
            if self.should_declare_exchanges:
                await self._declerate_exchanges(channel, True)
            if self.should_declare_input_queue:
                await self._declerate_queue(self.address, channel)

    async def _declerate_exchanges(self, channel: aio_pika.abc.AbstractChannel, durable: bool) -> None:
        self._direct_exchange = await channel.declare_exchange(
            self._direct_exchange_name,
            type=aio_pika.ExchangeType.DIRECT,
            durable=durable,
            arguments=self.direct_exchange_arguments,
        )
        await channel.declare_exchange(
            self._topic_exchange_name,
            type=aio_pika.ExchangeType.TOPIC,
            durable=durable,
            arguments=self.topic_exchange_arguments,
        )

    async def _declerate_queue(self, address: str, channel: aio_pika.abc.AbstractChannel) -> None:
        if address == self.address:
            self._input_queue = await channel.declare_queue(
                address,
                exclusive=self.input_queue_decleration_options.exclusive,
                durable=self.input_queue_decleration_options.durable,
                auto_delete=self.input_queue_decleration_options.auto_delete,
                arguments=self.input_queue_decleration_options.arguments,
            )
        else:
            await channel.declare_queue(
                address,
                exclusive=self.default_queue_decleration_options.exclusive,
                durable=self.default_queue_decleration_options.durable,
                auto_delete=self.default_queue_decleration_options.auto_delete,
                arguments=self.default_queue_decleration_options.arguments,
            )

    async def _bind_input_queue(self, address: str, channel: aio_pika.abc.AbstractChannel) -> None:
        if self._direct_exchange is None:
            direct_exchange = await channel.get_exchange(self._direct_exchange_name)
        else:
            direct_exchange = self._direct_exchange

        if self._input_queue is None:
            input_queue = await channel.get_queue(address)
        else:
            input_queue = self._input_queue

        await input_queue.bind(direct_exchange, routing_key=self.address)
