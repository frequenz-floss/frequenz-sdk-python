# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Wrapper around the power managing and power distributing actors."""

from __future__ import annotations

import logging
from datetime import timedelta

from frequenz.channels import Broadcast
from frequenz.client.microgrid.component import Battery, EvCharger, SolarInverter

from .._internal._channels import ChannelRegistry, ReceiverFetcher

# pylint seems to think this is a cyclic import, but it is not.
#
# pylint: disable-next=cyclic-import
from . import _power_managing, connection_manager

# pylint: disable-next=cyclic-import
from ._power_distributing import (
    ComponentPoolStatus,
    PowerDistributingActor,
    Request,
    Result,
)
from ._power_managing._base_classes import Algorithm, DefaultPower

_logger = logging.getLogger(__name__)


class PowerWrapper:  # pylint: disable=too-many-instance-attributes
    """Wrapper around the power managing and power distributing actors."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        channel_registry: ChannelRegistry,
        *,
        api_power_request_timeout: timedelta,
        power_manager_algorithm: Algorithm,
        default_power: DefaultPower,
        component_class: type[Battery | EvCharger | SolarInverter],
    ):
        """Initialize the power control.

        Args:
            channel_registry: A channel registry for use in the actors.
            api_power_request_timeout: Timeout to use when making power requests to
                the microgrid API.
            power_manager_algorithm: The power management algorithm to use.
            default_power: The default power to use for the components.
            component_class: The class of the component to manage.
        """
        self._default_power = default_power
        self._power_manager_algorithm = power_manager_algorithm
        self._component_class = component_class
        self._channel_registry = channel_registry
        self._api_power_request_timeout = api_power_request_timeout

        self.status_channel: Broadcast[ComponentPoolStatus] = Broadcast(
            name="Component Status Channel", resend_latest=True
        )
        self._power_distribution_requests_channel: Broadcast[Request] = Broadcast(
            name="Power Distributing Actor, Requests Broadcast Channel"
        )
        self._power_distribution_results_channel: Broadcast[Result] = Broadcast(
            name="Power Distributing Actor, Results Broadcast Channel"
        )

        self.proposal_channel: Broadcast[_power_managing.Proposal] = Broadcast(
            name="Power Managing Actor, Requests Broadcast Channel"
        )
        self.bounds_subscription_channel: Broadcast[_power_managing.ReportRequest] = (
            Broadcast(name="Power Managing Actor, Bounds Subscription Channel")
        )

        self._power_distributing_actor: PowerDistributingActor | None = None
        self._power_managing_actor: _power_managing.PowerManagingActor | None = None

    def _start_power_managing_actor(self) -> None:
        """Start the power managing actor if it is not already running."""
        if self._power_managing_actor:
            return

        component_graph = connection_manager.get().component_graph
        # Currently the power managing actor only supports batteries.  The below
        # constraint needs to be relaxed if the actor is extended to support other
        # components.
        if not component_graph.components(matching_types=self._component_class):
            _logger.warning(
                "No %s found in the component graph. "
                "The power managing actor will not be started.",
                self._component_class.__name__,
            )
            return

        self._power_managing_actor = _power_managing.PowerManagingActor(
            default_power=self._default_power,
            algorithm=self._power_manager_algorithm,
            component_class=self._component_class,
            proposals_receiver=self.proposal_channel.new_receiver(),
            bounds_subscription_receiver=(
                self.bounds_subscription_channel.new_receiver()
            ),
            power_distributing_requests_sender=(
                self._power_distribution_requests_channel.new_sender()
            ),
            power_distributing_results_receiver=(
                self._power_distribution_results_channel.new_receiver()
            ),
            channel_registry=self._channel_registry,
        )
        self._power_managing_actor.start()

    def _start_power_distributing_actor(self) -> None:
        """Start the power distributing actor if it is not already running."""
        if self._power_distributing_actor:
            return

        component_graph = connection_manager.get().component_graph
        if not component_graph.components(matching_types=self._component_class):
            _logger.warning(
                "No %s found in the component graph. "
                "The power distributing actor will not be started.",
                self._component_class.__name__,
            )
            return

        # The PowerDistributingActor is started with only a single default user channel.
        # Until the PowerManager is implemented, support for multiple use-case actors
        # will not be available in the high level interface.
        self._power_distributing_actor = PowerDistributingActor(
            component_type=self._component_class,
            api_power_request_timeout=self._api_power_request_timeout,
            requests_receiver=self._power_distribution_requests_channel.new_receiver(),
            results_sender=self._power_distribution_results_channel.new_sender(),
            component_pool_status_sender=self.status_channel.new_sender(),
        )
        self._power_distributing_actor.start()

    @property
    def started(self) -> bool:
        """Return True if power managing and power distributing actors are started."""
        return (
            self._power_managing_actor is not None
            and self._power_distributing_actor is not None
        )

    def start(self) -> None:
        """Start the power managing and power distributing actors."""
        if self.started:
            return
        self._start_power_distributing_actor()
        self._start_power_managing_actor()

    async def stop(self) -> None:
        """Stop the power managing and power distributing actors."""
        if self._power_distributing_actor:
            await self._power_distributing_actor.stop()
        if self._power_managing_actor:
            await self._power_managing_actor.stop()

    def distribution_results_fetcher(self) -> ReceiverFetcher[Result]:
        """Return a fetcher for the power distribution results."""
        return self._power_distribution_results_channel
