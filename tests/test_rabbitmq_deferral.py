from datetime import UTC, datetime, timedelta

import anyio
import pytest

from mersal.exceptions import DeferralNotSupportedError
from mersal.messages import MessageHeaders, TransportMessage
from mersal.testing.core.test_doubles import TransportMessageBuilder
from mersal.testing.core.testing_utils import is_docker_available
from mersal.testing.core.transport.basic_transport_tests import TransportMaker
from mersal.transport import DefaultTransactionContext, Transport

__all__ = ("TestRabbitMQTransportDeferral",)


pytestmark = [
    pytest.mark.anyio,
    pytest.mark.usefixtures("rabbitmq_service"),
    pytest.mark.skipif(not is_docker_available(), reason="docker not available on this platform"),
]

DELAYED_EXCHANGE = "mersal.delayed"


def _deferred_message(delay: timedelta) -> TransportMessage:
    message = TransportMessageBuilder.build()
    message.headers[MessageHeaders.deferred_until_key] = (datetime.now(UTC) + delay).isoformat()
    return message


class TestRabbitMQTransportDeferral:
    @pytest.fixture
    def transport_maker(self, rabbitmq_transport_maker: TransportMaker) -> TransportMaker:
        return rabbitmq_transport_maker

    async def _send(self, sender: Transport, address: str, message: TransportMessage) -> None:
        async with DefaultTransactionContext() as context:
            await sender.send(address, message, context)
            context.set_result(commit=True, ack=True)
            await context.complete()

    async def _receive(self, receiver: Transport) -> TransportMessage | None:
        async with DefaultTransactionContext() as context:
            received = await receiver.receive(context)
            context.set_result(commit=True, ack=True)
            await context.complete()
        return received

    async def test_supports_deferral_only_with_a_delayed_exchange(self, transport_maker: TransportMaker) -> None:
        plain = transport_maker(input_queue_address="deferral-plain")
        delayed = transport_maker(input_queue_address="deferral-delayed", delayed_exchange_name=DELAYED_EXCHANGE)

        assert not plain.supports_deferral
        assert delayed.supports_deferral

    async def test_deferred_message_is_only_delivered_once_due(self, transport_maker: TransportMaker) -> None:
        sender = transport_maker(input_queue_address="deferral-sender", delayed_exchange_name=DELAYED_EXCHANGE)
        receiver = transport_maker(
            input_queue_address="deferral-receiver",
            delayed_exchange_name=DELAYED_EXCHANGE,
            receive_timeout=0.5,
        )
        bystander = transport_maker(
            input_queue_address="deferral-bystander",
            delayed_exchange_name=DELAYED_EXCHANGE,
            receive_timeout=0.5,
        )
        await receiver()
        await bystander()
        message = _deferred_message(timedelta(seconds=1.5))

        await self._send(sender, "deferral-receiver", message)

        assert await self._receive(receiver) is None
        received = None
        with anyio.fail_after(5.0):
            while received is None:
                received = await self._receive(receiver)
        assert str(received.headers.message_id) == str(message.headers.message_id)
        assert MessageHeaders.deferred_until_key not in received.headers
        assert MessageHeaders.deferred_recipient_key not in received.headers
        assert "x-delay" not in received.headers
        # only the addressed queue gets it
        assert await self._receive(bystander) is None

    async def test_the_sent_message_is_not_modified(self, transport_maker: TransportMaker) -> None:
        """The same message may be sent again (e.g. retried by the outbox forwarder)."""
        sender = transport_maker(input_queue_address="unmodified-sender", delayed_exchange_name=DELAYED_EXCHANGE)
        message = _deferred_message(timedelta(minutes=1))
        headers_before = dict(message.headers)

        await self._send(sender, "unmodified-sender", message)

        assert dict(message.headers) == headers_before

    async def test_past_due_message_is_delivered_right_away_without_defer_headers(
        self, transport_maker: TransportMaker
    ) -> None:
        sender = transport_maker(input_queue_address="due-sender", delayed_exchange_name=DELAYED_EXCHANGE)
        receiver = transport_maker(input_queue_address="due-receiver", delayed_exchange_name=DELAYED_EXCHANGE)
        await receiver()

        await self._send(sender, "due-receiver", _deferred_message(timedelta(seconds=-1)))

        with anyio.fail_after(5.0):
            received = await self._receive(receiver)
        assert received is not None
        assert MessageHeaders.deferred_until_key not in received.headers

    async def test_without_a_delayed_exchange_deferred_messages_are_sent_as_they_are(
        self, transport_maker: TransportMaker
    ) -> None:
        """So they reach a timeout manager (`mersal.timeouts`) intact."""
        sender = transport_maker(input_queue_address="timeouts-sender")
        timeout_manager_queue = transport_maker(input_queue_address="timeouts-queue")
        await timeout_manager_queue()
        message = _deferred_message(timedelta(minutes=1))
        message.headers[MessageHeaders.deferred_recipient_key] = "somewhere"

        await self._send(sender, "timeouts-queue", message)

        with anyio.fail_after(5.0):
            received = await self._receive(timeout_manager_queue)
        assert received is not None
        assert received.headers.deferred_until == message.headers.deferred_until
        assert received.headers.deferred_recipient == "somewhere"

    async def test_deferring_to_a_topic_address_raises(self, transport_maker: TransportMaker) -> None:
        sender = transport_maker(input_queue_address="topic-delay-sender", delayed_exchange_name=DELAYED_EXCHANGE)

        with pytest.raises(BaseExceptionGroup) as exc_info:
            await self._send(sender, "some.topic@mersal.topics", _deferred_message(timedelta(minutes=1)))

        assert exc_info.group_contains(DeferralNotSupportedError)

    async def test_delay_beyond_the_plugin_maximum_raises(self, transport_maker: TransportMaker) -> None:
        sender = transport_maker(input_queue_address="long-delay-sender", delayed_exchange_name=DELAYED_EXCHANGE)

        with pytest.raises(BaseExceptionGroup) as exc_info:
            await self._send(sender, "long-delay-sender", _deferred_message(timedelta(days=60)))

        assert exc_info.group_contains(DeferralNotSupportedError)
