# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Wrapper around the power managing and power distributing actors."""

from __future__ import annotations

import logging
from datetime import timedelta

from frequenz.channels import Broadcast

# pylint seems to think this is a cyclic import, but it is not.
#
# pylint: disable=cyclic-import
from frequenz.client.microgrid import ComponentCategory, ComponentType

from .._internal._channels import ChannelRegistry, ReceiverFetcher
from . import _power_managing, connection_manager
from ._power_distributing import (
    ComponentPoolStatus,
    PowerDistributingActor,
    Request,
    Result,
)

_logger = logging.getLogger(__name__)


class PowerWrapper:
    """Wrapper around the power managing and power distributing actors."""

    def __init__(
        self,
        channel_registry: ChannelRegistry,
        *,
        api_power_request_timeout: timedelta,
        component_category: ComponentCategory,
        component_type: ComponentType | None = None,
    ):
        """Initialize the power control.

        Args:
            channel_registry: A channel registry for use in the actors.
            api_power_request_timeout: Timeout to use when making power requests to
                the microgrid API.
            component_category: The category of the components that actors started by
                this instance of the PowerWrapper will be responsible for.
            component_type: The type of the component of the given category that this
                actor is responsible for.  This is used only when the component category
                is not enough to uniquely identify the component.  For example, when the
                category is `ComponentCategory.INVERTER`, the type is needed to identify
                the inverter as a solar inverter or a battery inverter.  This can be
                `None` when the component category is enough to uniquely identify the
                component.
        """
        self._component_category = component_category
        self._component_type = component_type
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
        if not component_graph.components(
            component_categories={self._component_category}
        ):
            _logger.warning(
                "No %s found in the component graph. "
                "The power managing actor will not be started.",
                self._component_category,
            )
            return

        self._power_managing_actor = _power_managing.PowerManagingActor(
            component_category=self._component_category,
            component_type=self._component_type,
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
        if not component_graph.components(
            component_categories={self._component_category}
        ):
            _logger.warning(
                "No %s found in the component graph. "
                "The power distributing actor will not be started.",
                self._component_category,
            )
            return

        # The PowerDistributingActor is started with only a single default user channel.
        # Until the PowerManager is implemented, support for multiple use-case actors
        # will not be available in the high level interface.
        self._power_distributing_actor = PowerDistributingActor(
            component_category=self._component_category,
            component_type=self._component_type,
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
