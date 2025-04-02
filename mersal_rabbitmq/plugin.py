from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from mersal.lifespan.lifespan_hooks_registration_plugin import LifespanHooksRegistrationPluginConfig
from mersal.plugins import Plugin
from mersal.transport.transport import Transport
from mersal.utils.sync import AsyncCallable
from mersal_rabbitmq.transport import RabbitMqTransport, RabbitMqTransportConfig

if TYPE_CHECKING:
    from mersal.configuration import StandardConfigurator

__all__ = (
    "RabbitMQTransportPlugin",
    "RabbitMQTransportPluginConfig",
)


@dataclass
class RabbitMQTransportPluginConfig:
    transport_config: RabbitMqTransportConfig

    def plugin(self) -> RabbitMQTransportPlugin:
        return RabbitMQTransportPlugin(self)


class RabbitMQTransportPlugin(Plugin):
    def __init__(
        self,
        config: RabbitMQTransportPluginConfig,
    ) -> None:
        self._config = config
        self._transport = RabbitMqTransport(self._config.transport_config)

    def __call__(self, configurator: StandardConfigurator) -> None:
        def register_transport(_: StandardConfigurator) -> Any:
            return self._transport

        configurator.register(Transport, register_transport)

        startup_hooks = [
            lambda config: AsyncCallable(self._transport),
        ]
        plugin = LifespanHooksRegistrationPluginConfig(
            on_startup_hooks=startup_hooks,  # type: ignore[arg-type]
        ).plugin
        plugin(configurator)
