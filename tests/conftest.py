# pyright: reportWildcardImportFromLibrary=false

import asyncio
import os
import re
import subprocess
import timeit
from collections.abc import Awaitable, Callable, Generator
from pathlib import Path
from typing import Any

import aio_pika
import pytest
from aiormq import AMQPConnectionError

from mersal.utils import AsyncCallable
from mersal_testing._internal.conftest import *

__all__ = (
    "DockerServiceRegistry",
    "docker_ip",
    "docker_services",
    "rabbitmq_responsive",
    "rabbitmq_service",
    "wait_until_responsive",
)


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

    raise Exception("Timeout reached while waiting on service!")


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
        subprocess.run([*self._base_command, *args], check=True, capture_output=True)  # noqa: S603

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
    except (AMQPConnectionError, ConnectionRefusedError, asyncio.TimeoutError, OSError):
        return False


@pytest.fixture()
async def rabbitmq_service(docker_services: DockerServiceRegistry) -> None:
    await docker_services.start("rabbitmq", check=rabbitmq_responsive)
