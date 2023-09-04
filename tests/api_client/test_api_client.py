# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for the `ApiClient` class."""

from abc import abstractmethod
from typing import Any

from frequenz.sdk._api_client import ApiClient, ApiProtocol


class FakeApiClient(ApiClient):
    """An abstract mock api client."""

    @classmethod
    def api_major_version(cls) -> int:
        """Return the major version of the API supported by the client.

        Returns:
            The major version of the API supported by the client.
        """
        # Specifying the targeted API version here.
        return 1

    @classmethod
    @abstractmethod
    def api_type(cls) -> ApiProtocol:
        """Return the API type."""

    @abstractmethod
    async def connect(self, connection_params: Any) -> None:
        """Connect to the API."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the API."""

    @abstractmethod
    def get_data(self) -> str:
        """Get data from the API."""


class FakeGrpcClient(FakeApiClient):
    """Supported API version is defined in the `FakeApiClient` class."""

    is_connected: bool

    @classmethod
    def api_type(cls) -> ApiProtocol:
        """Return the API type."""
        # Specifying the API protocol here as gRPC.
        return ApiProtocol.GRPC

    async def connect(self, connection_params: str) -> None:
        """Connect to the API."""
        self.is_connected = True

    async def disconnect(self) -> None:
        """Disconnect from the API."""
        self.is_connected = False

    def get_data(self) -> str:
        """Get data from the API."""
        return "grpc data"


class FakeRestClient(FakeApiClient):
    """Supported API version is defined in the `FakeApiClient` class."""

    is_connected: bool

    @classmethod
    def api_type(cls) -> ApiProtocol:
        """Return the API type."""
        # Same as `FakeGrpcClient`, but targeting REST protocol here.
        return ApiProtocol.REST

    async def connect(self, connection_params: str) -> None:
        """Connect to the API."""
        self.is_connected = True

    async def disconnect(self) -> None:
        """Disconnect from the API."""
        self.is_connected = False

    def get_data(self) -> str:
        """Get data from the API."""
        return "rest data"


async def test_fake_grpc_client() -> None:
    """Test fake grpc client."""
    assert FakeGrpcClient.api_major_version() == 1
    assert FakeGrpcClient.api_type() == ApiProtocol.GRPC

    client = FakeGrpcClient()

    await client.connect("[::1]:80")
    assert client.is_connected

    await client.disconnect()
    assert not client.is_connected

    assert client.get_data() == "grpc data"


async def test_fake_rest_client() -> None:
    """Test fake rest client."""
    assert FakeRestClient.api_major_version() == 1
    assert FakeRestClient.api_type() == ApiProtocol.REST

    client = FakeRestClient()

    await client.connect("[::1]:80")
    assert client.is_connected

    await client.disconnect()
    assert not client.is_connected

    assert client.get_data() == "rest data"
