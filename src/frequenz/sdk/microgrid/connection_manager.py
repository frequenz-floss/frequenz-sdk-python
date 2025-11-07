# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Microgrid Connection Manager singleton abstraction.

This module provides a singleton abstraction over the microgrid. The main
purpose is to provide the connection the microgrid API client and the microgrid
component graph.
"""

import logging
from abc import ABC, abstractmethod

from frequenz.client.common.microgrid import MicrogridId
from frequenz.client.microgrid import Location, MicrogridApiClient, MicrogridInfo

from .component_graph import ComponentGraph, _MicrogridComponentGraph

_logger = logging.getLogger(__name__)


class ConnectionManager(ABC):
    """Creates and stores core features."""

    def __init__(self, server_url: str) -> None:
        """Create object instance.

        Args:
            server_url: The location of the microgrid API server in the form of a URL.
                The following format is expected: `grpc://hostname{:port}{?ssl=ssl}`,
                where the port should be an int between `0` and `65535` (defaulting to
                `9090`) and ssl should be a boolean (defaulting to false). For example:
                `grpc://localhost:1090?ssl=true`.
        """
        super().__init__()
        self._server_url = server_url

    @property
    def server_url(self) -> str:
        """The location of the microgrid API server in the form of a URL."""
        return self._server_url

    @property
    @abstractmethod
    def api_client(self) -> MicrogridApiClient:
        """Get the MicrogridApiClient.

        Returns:
            The microgrid API client used by this connection manager.
        """

    @property
    @abstractmethod
    def component_graph(self) -> ComponentGraph:
        """Get component graph.

        Returns:
            component graph
        """

    @property
    @abstractmethod
    def microgrid_id(self) -> MicrogridId | None:
        """Get the ID of the microgrid if available.

        Returns:
            the ID of the microgrid if available, None otherwise.
        """

    @property
    @abstractmethod
    def location(self) -> Location | None:
        """Get the location of the microgrid if available.

        Returns:
            the location of the microgrid if available, None otherwise.
        """

    async def _update_client(self, server_url: str) -> None:
        self._server_url = server_url

    @abstractmethod
    async def _initialize(self) -> None:
        """Initialize the object. This function should be called only once."""


class _InsecureConnectionManager(ConnectionManager):
    """Microgrid Api with insecure channel implementation."""

    def __init__(self, server_url: str) -> None:
        """Create and stores core features.

        Args:
            server_url: The location of the microgrid API server in the form of a URL.
                The following format is expected: `grpc://hostname{:port}{?ssl=ssl}`,
                where the port should be an int between `0` and `65535` (defaulting to
                `9090`) and ssl should be a boolean (defaulting to false). For example:
                `grpc://localhost:1090?ssl=true`.
        """
        super().__init__(server_url)
        self._client = MicrogridApiClient(server_url)
        # To create graph from the API client we need await.
        # So create empty graph here, and update it in `run` method.
        self._graph = _MicrogridComponentGraph()

        self._microgrid: MicrogridInfo
        """The microgrid information."""

    @property
    def api_client(self) -> MicrogridApiClient:
        """The microgrid API client used by this connection manager."""
        return self._client

    @property
    def microgrid_id(self) -> MicrogridId | None:
        """Get the ID of the microgrid if available.

        Returns:
            the ID of the microgrid if available, None otherwise.
        """
        return self._microgrid.id

    @property
    def location(self) -> Location | None:
        """Get the location of the microgrid if available.

        Returns:
            the location of the microgrid if available, None otherwise.
        """
        return self._microgrid.location

    @property
    def component_graph(self) -> ComponentGraph:
        """Get component graph.

        Returns:
            component graph
        """
        return self._graph

    async def _update_client(self, server_url: str) -> None:
        """Update the API client with a new server URL.

        Args:
            server_url: The new location of the microgrid API server in the form of a
                URL. The following format is expected:
                `grpc://hostname{:port}{?ssl=ssl}`, where the port should be an int
                between `0` and `65535` (defaulting to `9090`) and ssl should be
                a boolean (defaulting to false). For example:
                `grpc://localhost:1090?ssl=true`.
        """
        await super()._update_client(server_url)  # pylint: disable=protected-access

        self._client = MicrogridApiClient(server_url)
        await self._initialize()

    async def _initialize(self) -> None:
        self._microgrid = await self._client.get_microgrid_info()
        await self._graph.refresh_from_client(self._client)


_CONNECTION_MANAGER: ConnectionManager | None = None
"""The ConnectionManager singleton instance."""


async def initialize(server_url: str) -> None:
    """Initialize the MicrogridApi. This function should be called only once.

    Args:
        server_url: The location of the microgrid API server in the form of a URL.
            The following format is expected: `grpc://hostname{:port}{?ssl=ssl}`,
            where the port should be an int between `0` and `65535` (defaulting to
            `9090`) and ssl should be a boolean (defaulting to false). For example:
            `grpc://localhost:1090?ssl=true`.
    """
    # From Doc: pylint just try to discourage this usage.
    # That doesn't mean you cannot use it.
    global _CONNECTION_MANAGER  # pylint: disable=global-statement

    assert _CONNECTION_MANAGER is None, "MicrogridApi was already initialized."

    _logger.info("Connecting to microgrid at %s", server_url)

    connection_manager = _InsecureConnectionManager(server_url)
    await connection_manager._initialize()  # pylint: disable=protected-access

    # Check again that _MICROGRID_API is None in case somebody had the great idea of
    # calling initialize() twice and in parallel.
    assert _CONNECTION_MANAGER is None, "MicrogridApi was already initialized."

    _CONNECTION_MANAGER = connection_manager


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
