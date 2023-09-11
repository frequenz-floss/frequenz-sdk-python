# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Microgrid Connection Manager singleton abstraction.

This module provides a singleton abstraction over the microgrid. The main
purpose is to provide the connection the microgrid API client and the microgrid
component graph.
"""

import logging
from abc import ABC, abstractmethod

import grpc.aio as grpcaio

from ._graph import ComponentGraph, _MicrogridComponentGraph
from .client import MicrogridApiClient
from .client._client import MicrogridGrpcClient

# Not public default host and port
_DEFAULT_MICROGRID_HOST = "127.0.0.1"
_DEFAULT_MICROGRID_PORT = 443

_logger = logging.getLogger(__name__)


class ConnectionManager(ABC):
    """Creates and stores core features."""

    def __init__(self, host: str, port: int) -> None:
        """Create object instance.

        Args:
            host: server host
            port: server port
        """
        super().__init__()
        self._host: str = host
        self._port: int = port

    @property
    def host(self) -> str:
        """Get host of the currently connected server.

        Returns:
            host
        """
        return self._host

    @property
    def port(self) -> int:
        """Get port of the currently connected server.

        Returns:
            port
        """
        return self._port

    @property
    @abstractmethod
    def api_client(self) -> MicrogridApiClient:
        """Get MicrogridApiClient.

        Returns:
            api client
        """

    @property
    @abstractmethod
    def component_graph(self) -> ComponentGraph:
        """Get component graph.

        Returns:
            component graph
        """

    async def _update_api(self, host: str, port: int) -> None:
        self._host = host
        self._port = port

    @abstractmethod
    async def _initialize(self) -> None:
        """Initialize the object. This function should be called only once."""


class _InsecureConnectionManager(ConnectionManager):
    """Microgrid Api with insecure channel implementation."""

    def __init__(
        self, host: str = _DEFAULT_MICROGRID_HOST, port: int = _DEFAULT_MICROGRID_PORT
    ) -> None:
        """Create and stores core features.

        Args:
            host: host. Defaults to _DEFAULT_MICROGRID_HOST.
            port: port. Defaults to _DEFAULT_MICROGRID_PORT.
        """
        super().__init__(host, port)
        target = f"{host}:{port}"
        grpc_channel = grpcaio.insecure_channel(target)
        self._api = MicrogridGrpcClient(grpc_channel, target)
        # To create graph from the api we need await.
        # So create empty graph here, and update it in `run` method.
        self._graph = _MicrogridComponentGraph()

    @property
    def api_client(self) -> MicrogridApiClient:
        """Get MicrogridApiClient.

        Returns:
            api client
        """
        return self._api

    @property
    def component_graph(self) -> ComponentGraph:
        """Get component graph.

        Returns:
            component graph
        """
        return self._graph

    async def _update_api(self, host: str, port: int) -> None:
        """Update api with new host and port.

        Args:
            host: new host
            port: new port
        """
        await super()._update_api(host, port)  # pylint: disable=protected-access

        target = f"{host}:{port}"
        grpc_channel = grpcaio.insecure_channel(target)
        self._api = MicrogridGrpcClient(grpc_channel, target)
        await self._graph.refresh_from_api(self._api)

    async def _initialize(self) -> None:
        await self._graph.refresh_from_api(self._api)


_CONNECTION_MANAGER: ConnectionManager | None = None
"""The ConnectionManager singleton instance."""


async def initialize(host: str, port: int) -> None:
    """Initialize the MicrogridApi. This function should be called only once.

    Args:
        host: Microgrid host
        port: Microgrid port

    Raises:
        AssertionError: If method was called more then once.
    """
    # From Doc: pylint just try to discourage this usage.
    # That doesn't mean you cannot use it.
    global _CONNECTION_MANAGER  # pylint: disable=global-statement

    if _CONNECTION_MANAGER is not None:
        raise AssertionError("MicrogridApi was already initialized.")

    _logger.info("Connecting to microgrid at %s:%s", host, port)

    microgrid_api = _InsecureConnectionManager(host, port)
    await microgrid_api._initialize()  # pylint: disable=protected-access

    # Check again that _MICROGRID_API is None in case somebody had the great idea of
    # calling initialize() twice and in parallel.
    if _CONNECTION_MANAGER is not None:
        raise AssertionError("MicrogridApi was already initialized.")

    _CONNECTION_MANAGER = microgrid_api


def get() -> ConnectionManager:
    """Get the MicrogridApi instance created by initialize().

    This function should be only called after initialize().

    Raises:
        RuntimeError: Raised when:
            * If `initialize()` method was not called before this call.
            * If `initialize()` methods was called but was not awaited and instance was
                not created yet.

    Returns:
        MicrogridApi instance.
    """
    if _CONNECTION_MANAGER is None:
        raise RuntimeError(
            "ConnectionManager is not initialized. "
            "Call `await microgrid.initialize()` first."
        )

    return _CONNECTION_MANAGER
