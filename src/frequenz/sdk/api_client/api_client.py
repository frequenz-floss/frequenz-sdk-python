# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""An abstract API client."""

from abc import ABC, abstractmethod
from enum import Enum


class ApiProtocol(Enum):
    """Enumerated values of supported API types."""

    GRPC = 1
    REST = 2
    FILESYSTEM = 3


class ApiClient(ABC):
    """An abstract API client, with general purpose functions that all APIs should implement.

    The methods defined here follow the principle that each client
    implementation should clearly and consistently specify the following
    information:
    a. which minimum version of the API it intends to target,
    b. what is the communication protocol.
    """

    @classmethod
    @abstractmethod
    def api_major_version(cls) -> int:
        """Return the major version of the API supported by the client.

        Returns:
            The major version of the API supported by the client.
        """

    @classmethod
    @abstractmethod
    def api_type(cls) -> ApiProtocol:
        """Return the API type supported by the client.

        Returns:
            The ApiProtocol value representing the API type being targeted in a
                concrete implementation.
        """
