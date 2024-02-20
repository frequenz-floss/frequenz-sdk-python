# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Wrapper around the power managing and power distributing actors."""

from __future__ import annotations

import logging
import typing

from frequenz.channels import Broadcast

# pylint seems to think this is a cyclic import, but it is not.
#
# pylint: disable=cyclic-import
from .component import ComponentCategory

# A number of imports had to be done inside functions where they are used, to break
# import cycles.
#
# pylint: disable=import-outside-toplevel
if typing.TYPE_CHECKING:
    from ..actor import ChannelRegistry, _power_managing
    from ..actor.power_distributing import (  # noqa: F401 (imports used by string type hints)
        ComponentPoolStatus,
        PowerDistributingActor,
        Request,
        Result,
    )

_logger = logging.getLogger(__name__)


class PowerWrapper:
    """Wrapper around the power managing and power distributing actors."""

    def __init__(self, channel_registry: ChannelRegistry):
        """Initialize the power control.

        Args:
            channel_registry: A channel registry for use in the actors.
        """
        self._channel_registry = channel_registry

        self.status_channel: Broadcast[ComponentPoolStatus] = Broadcast(
            "Component Status Channel", resend_latest=True
        )
        self._power_distribution_requests_channel: Broadcast[Request] = Broadcast(
            "Power Distributing Actor, Requests Broadcast Channel"
        )
        self._power_distribution_results_channel: Broadcast[Result] = Broadcast(
            "Power Distributing Actor, Results Broadcast Channel"
        )

        self.proposal_channel: Broadcast[_power_managing.Proposal] = Broadcast(
            "Power Managing Actor, Requests Broadcast Channel"
        )
        self.bounds_subscription_channel: Broadcast[_power_managing.ReportRequest] = (
            Broadcast("Power Managing Actor, Bounds Subscription Channel")
        )

        self._power_distributing_actor: PowerDistributingActor | None = None
        self._power_managing_actor: _power_managing.PowerManagingActor | None = None

    def _start_power_managing_actor(self) -> None:
        """Start the power managing actor if it is not already running."""
        if self._power_managing_actor:
            return

        from .. import microgrid

        component_graph = microgrid.connection_manager.get().component_graph
        # Currently the power managing actor only supports batteries.  The below
        # constraint needs to be relaxed if the actor is extended to support other
        # components.
        if not component_graph.components(
            component_categories={ComponentCategory.BATTERY}
        ):
            _logger.warning(
                "No batteries found in the component graph. "
                "The power managing actor will not be started."
            )
            return

        from ..actor._power_managing._power_managing_actor import PowerManagingActor
        from ..timeseries._base_types import PoolType

        self._power_managing_actor = PowerManagingActor(
            pool_type=PoolType.BATTERY_POOL,
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

        from .. import microgrid

        component_graph = microgrid.connection_manager.get().component_graph
        if not component_graph.components(
            component_categories={ComponentCategory.BATTERY}
        ):
            _logger.warning(
                "No batteries found in the component graph. "
                "The power distributing actor will not be started."
            )
            return

        from ..actor.power_distributing import PowerDistributingActor

        # The PowerDistributingActor is started with only a single default user channel.
        # Until the PowerManager is implemented, support for multiple use-case actors
        # will not be available in the high level interface.
        self._power_distributing_actor = PowerDistributingActor(
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
