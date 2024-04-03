# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Manage batteries and inverters for the power distributor."""

import abc
import collections.abc

from frequenz.channels import Sender

from .._component_status import ComponentPoolStatus
from ..request import Request
from ..result import Result


class ComponentManager(abc.ABC):
    """Abstract class to manage the data streams for components."""

    @abc.abstractmethod
    def __init__(
        self,
        component_pool_status_sender: Sender[ComponentPoolStatus],
        results_sender: Sender[Result],
    ):
        """Initialize the component data manager.

        Args:
            component_pool_status_sender: Channel for sending information about which
                components are expected to be working.
            results_sender: Channel for sending the results of power distribution.
        """

    @abc.abstractmethod
    def component_ids(self) -> collections.abc.Set[int]:
        """Return the set of component ids."""

    @abc.abstractmethod
    async def start(self) -> None:
        """Start the component data manager."""

    @abc.abstractmethod
    async def distribute_power(self, request: Request) -> None:
        """Distribute the requested power to the components.

        Args:
            request: Request to get the distribution for.

        Returns:
            Result of the distribution.
        """

    @abc.abstractmethod
    async def stop(self) -> None:
        """Stop the component data manager."""
