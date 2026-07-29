# pyright: reportWildcardImportFromLibrary=false

import asyncio
import os
import re
import subprocess
import timeit
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator
from pathlib import Path
from typing import Any

import aio_pika
import pytest
from aiormq import AMQPConnectionError

from mersal.logging import NullLogger
from mersal.transport import Transport
from mersal.utils import AsyncCallable
from mersal_rabbitmq.transport import RabbitMqTransport, RabbitMqTransportConfig
from mersal_testing._internal.conftest import *
from mersal_testing.transport.basic_transport_tests import TransportMaker

__all__ = (
    "DockerServiceRegistry",
    "connection_uri",
    "delete_queues",
    "docker_ip",
    "docker_services",
    "rabbitmq_responsive",
    "rabbitmq_service",
    "rabbitmq_transport_maker",
    "topic_exchange_name",
    "wait_until_responsive",
)


class DockerServiceTimeoutError(Exception):
    """Timeout error."""


async def wait_until_responsive(
    check: Callable[..., Awaitable],
    timeout: float,
    pause: float,
    **kwargs: Any,
) -> None:
    """Wait until a service is responsive.

    Args:
        check: Coroutine, return truthy value when waiting should stop.
        timeout: Maximum seconds to wait.
        pause: Seconds to wait between calls to `check`.
        **kwargs: Given as kwargs to `check`.
    """
    ref = timeit.default_timer()
    now = ref
    while (now - ref) < timeout:
        if await check(**kwargs):
            return
        await asyncio.sleep(pause)
        now = timeit.default_timer()

    raise DockerServiceTimeoutError("Timeout reached while waiting on service!")


class DockerServiceRegistry:
    def __init__(self) -> None:
        self._running_services: set[str] = set()
        self.docker_ip = self._get_docker_ip()
        file_name = Path(__file__).resolve().parent / "docker-compose.yml"
        self._base_command = [
            "docker",
            "compose",
            f"--file={file_name!s}",
            "--project-name=mersal_rabbitmq_pytest",
        ]

    def _get_docker_ip(self) -> str:
        docker_host = os.environ.get("DOCKER_HOST", "").strip()
        if not docker_host or docker_host.startswith("unix://"):
            return "127.0.0.1"

        match = re.match(r"^tcp://(.+?):\d+$", docker_host)
        if not match:
            raise ValueError(f'Invalid value for DOCKER_HOST: "{docker_host}".')
        return match.group(1)

    def run_command(self, *args: str) -> None:
        subprocess.run([*self._base_command, *args], check=True, capture_output=True)

    async def start(
        self,
        name: str,
        *,
        check: Callable[..., Awaitable],
        timeout: float = 30,
        pause: float = 0.1,
        **kwargs: Any,
    ) -> None:
        if name not in self._running_services:
            self.run_command("up", "-d", name)
            self._running_services.add(name)

            await wait_until_responsive(
                check=AsyncCallable(check),
                timeout=timeout,
                pause=pause,
                host=self.docker_ip,
                **kwargs,
            )

    def stop(self, name: str) -> None:
        pass

    def down(self) -> None:
        self.run_command("down", "-t", "5")


@pytest.fixture(scope="session")
def docker_services() -> Generator[DockerServiceRegistry, None, None]:
    registry = DockerServiceRegistry()
    yield registry
    registry.down()


@pytest.fixture(scope="session")
def docker_ip(docker_services: DockerServiceRegistry) -> str:
    return docker_services.docker_ip


async def rabbitmq_responsive(host: str, port: int = 5672, timeout: float = 5.0) -> bool:
    """Attempts to establish a basic AMQP connection."""
    url = f"amqp://guest:guest@{host}:{port}/"
    try:
        connection = await asyncio.wait_for(
            aio_pika.connect_robust(url, timeout=timeout),  # Internal timeout for connection logic
            timeout=timeout + 1.0,  # Overall timeout for the wait_for
        )
        await connection.close()
        return True
    except (TimeoutError, AMQPConnectionError, ConnectionRefusedError, OSError):
        return False


@pytest.fixture()
async def rabbitmq_service(docker_services: DockerServiceRegistry) -> None:
    await docker_services.start("rabbitmq", check=rabbitmq_responsive)


@pytest.fixture
def connection_uri() -> str:
    return "amqp://guest:guest@127.0.0.1:5672"


@pytest.fixture
def topic_exchange_name() -> str:
    return "mersal.topics"


@pytest.fixture
def delete_queues(connection_uri: str) -> Callable[..., Awaitable[None]]:
    """Best-effort teardown helper for tests that declare fixed-name queues.

    The generic transport/subscription-storage contract tests reuse a handful of
    hardcoded addresses (e.g. "ad1", "moon") across runs; since RabbitMQ queues are
    durable by default, leftover messages from a prior run (e.g. after a failed test)
    could otherwise bleed into the next one. Deleting is attempted on a fresh channel
    per queue, since a queue that doesn't exist closes the channel it was attempted on.
    """

    async def _delete_queues(*queue_names: str) -> None:
        connection = await aio_pika.connect_robust(connection_uri)
        async with connection:
            for name in queue_names:
                channel = await connection.channel()
                try:
                    await channel.queue_delete(name)
                except aio_pika.exceptions.ChannelClosed:
                    pass
                finally:
                    if not channel.is_closed:
                        await channel.close()

    return _delete_queues


@pytest.fixture
async def rabbitmq_transport_maker(
    connection_uri: str,
    topic_exchange_name: str,
    delete_queues: Callable[..., Awaitable[None]],
) -> AsyncGenerator[TransportMaker, None]:
    """Builds `RabbitMqTransport` instances against the test broker.

    Tracks every address it's asked to build a transport for and deletes those queues
    on teardown - the generic transport contract test suite (and ours) reuse a handful
    of fixed addresses across test runs, and RabbitMQ queues are durable by default, so
    leftover messages from a previous run could otherwise bleed into the next one.

    Named distinctly from `transport_maker` on purpose: `BasicTransportTest` defines its
    own (raising) `transport_maker` fixture directly on the class, and a class-level
    fixture always shadows a same-named one from conftest.py regardless of where it
    sits in the MRO - so each test class below explicitly overrides `transport_maker`
    to return this one, rather than relying on conftest.py alone.
    """
    created_addresses: set[str] = set()
    created_transports: list[RabbitMqTransport] = []

    def maker(**kwargs: Any) -> Transport:
        address = kwargs.pop("input_queue_address")
        created_addresses.add(address)
        config = RabbitMqTransportConfig(
            connection_uri=connection_uri,
            input_queue_name=address,
            topic_exchange_name=topic_exchange_name,
            # `AnyIOPeriodicTask` must be stopped from the task that started it, and
            # under pytest-anyio the test body (which lazily starts the transport) and
            # this fixture's teardown (which closes it) run in different tasks. In
            # production both happen in the lifespan task, so this only matters here.
            # Tests of the passive check itself invoke `_check_input_queue_exists()`
            # directly rather than waiting on the timer anyway.
            **{"passive_queue_check_interval": None, **kwargs},
        )
        transport = RabbitMqTransport(config=config, logger=NullLogger())
        created_transports.append(transport)
        return transport

    yield maker

    # Close transports before the event loop goes away - a still-live consumer's
    # background pump would otherwise be cancelled abruptly at loop teardown.
    for transport in created_transports:
        await transport.close()
    await delete_queues(*created_addresses)
