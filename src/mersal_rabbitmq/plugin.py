from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from mersal.lifespan.lifespan_hooks_registration_plugin import LifespanHooksRegistrationPluginConfig
from mersal.logging import Logger
from mersal.plugins import Plugin
from mersal.subscription import SubscriptionStorage
from mersal.threading import AnyIOPeriodicTaskFactory
from mersal.transport.transport import Transport
from mersal.utils.sync import AsyncCallable
from mersal_rabbitmq.subscription_storage import RabbitMqSubscriptionStorage, RabbitMqSubscriptionStorageConfig
from mersal_rabbitmq.transport import QueueDeclarationOptions, RabbitMqTransport, RabbitMqTransportConfig

if TYPE_CHECKING:
    from pamqp import common as pamqp_common

    from mersal.configuration import StandardConfigurator

__all__ = (
    "RabbitMQPlugin",
    "RabbitMQPluginConfig",
)


@dataclass
class RabbitMQPluginConfig:
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

    def plugin(self) -> RabbitMQPlugin:
        return RabbitMQPlugin(self)


class RabbitMQPlugin(Plugin):
    def __init__(
        self,
        config: RabbitMQPluginConfig,
    ) -> None:
        self._config = config

    def __call__(self, configurator: StandardConfigurator) -> None:
        def register_transport(configurator: StandardConfigurator) -> RabbitMqTransport:
            logger = configurator.get(Logger)  # type: ignore[type-abstract]
            transport_config = RabbitMqTransportConfig(
                connection_uri=self._config.connection_uri,
                input_queue_name=self._config.input_queue_name,
                send_only=configurator.send_only,
                should_declare_exchanges=self._config.should_declare_exchanges,
                should_declare_input_queue=self._config.should_declare_input_queue,
                should_bind_input_queue=self._config.should_bind_input_queue,
                direct_exchange_arguments=self._config.direct_exchange_arguments,
                topic_exchange_arguments=self._config.topic_exchange_arguments,
                direct_exchange_name=self._config.direct_exchange_name,
                topic_exchange_name=self._config.topic_exchange_name,
                input_queue_declaration_options=self._config.input_queue_declaration_options,
                default_queue_declaration_options=self._config.default_queue_declaration_options,
                prefetch_count=self._config.prefetch_count,
                passive_queue_check_interval=self._config.passive_queue_check_interval,
            )
            return RabbitMqTransport(
                transport_config,
                periodic_task_factory=AnyIOPeriodicTaskFactory(logger=logger),
                logger=logger,
            )

        def register_subscription_configuration(
            configurator: StandardConfigurator,
        ) -> RabbitMqSubscriptionStorage:
            # Sharing the transport's connection (rather than opening a second one to
            # the same broker) also lets us wire `rebind_subscriptions` as the
            # transport's queue-recreation hook, since both ends of that hand-off need
            # to agree on the same underlying queue/broker state.
            # `register_transport` is this plugin's own factory for `Transport`, so
            # the instance it returns is always a `RabbitMqTransport`.
            transport = cast("RabbitMqTransport", configurator.get(Transport))  # type: ignore[type-abstract]
            subscription_config = RabbitMqSubscriptionStorageConfig(
                connection_uri=self._config.connection_uri,
                topic_exchange_name=self._config.topic_exchange_name,
                should_declare_topic_exchange=self._config.should_declare_exchanges,
                topic_exchange_arguments=self._config.topic_exchange_arguments,
            )
            storage = RabbitMqSubscriptionStorage(
                config=subscription_config,
                connection_provider=transport.ensure_connection,
            )
            transport.queue_recreated_hook = storage.rebind_subscriptions
            return storage

        configurator.register(Transport, register_transport)
        configurator.register(SubscriptionStorage, register_subscription_configuration)

        startup_hooks = [
            lambda config: AsyncCallable(config.get(Transport)),
            lambda config: AsyncCallable(config.get(SubscriptionStorage).connect),
        ]
        shutdown_hooks = [
            lambda config: AsyncCallable(config.get(SubscriptionStorage).close),
            lambda config: AsyncCallable(config.get(Transport).close),
        ]
        plugin = LifespanHooksRegistrationPluginConfig(
            on_startup_hooks=startup_hooks,
            on_shutdown_hooks=shutdown_hooks,
        ).plugin
        plugin(configurator)
