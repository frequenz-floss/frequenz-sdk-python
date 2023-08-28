# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Create, connect and own instances of data pipeline components.

Provides SDK users direct access to higher level components of the data pipeline,
eliminating the boiler plate code required to setup the DataSourcingActor and the
ResamplingActor.
"""

from __future__ import annotations

import logging
import typing
from collections import abc
from dataclasses import dataclass

from frequenz.channels import Broadcast, Sender

from . import connection_manager
from .component import ComponentCategory

_logger = logging.getLogger(__name__)

# A number of imports had to be done inside functions where they are used, to break
# import cycles.
#
# pylint: disable=import-outside-toplevel
if typing.TYPE_CHECKING:
    from ..actor import (
        ComponentMetricRequest,
        ComponentMetricsResamplingActor,
        DataSourcingActor,
        ResamplerConfig,
    )
    from ..actor.power_distributing import (
        BatteryStatus,
        PowerDistributingActor,
        Request,
    )
    from ..timeseries.battery_pool import BatteryPool
    from ..timeseries.ev_charger_pool import EVChargerPool
    from ..timeseries.logical_meter import LogicalMeter


_REQUEST_RECV_BUFFER_SIZE = 500
"""The maximum number of requests that can be queued in the request receiver.

A larger buffer size means that the DataSourcing and Resampling actors don't drop
requests and will be able to keep up with higher request rates in larger installations.
"""

_T = typing.TypeVar("_T")


@dataclass
class _ActorInfo(typing.Generic[_T]):
    """Holds instances of core data pipeline actors and their request channels."""

    actor: _T
    channel: Broadcast["ComponentMetricRequest"]


class _DataPipeline:
    """Create, connect and own instances of data pipeline components.

    Provides SDK users direct access to higher level components of the data pipeline,
    eliminating the boiler plate code required to setup the DataSourcingActor and the
    ResamplingActor.
    """

    def __init__(
        self,
        resampler_config: ResamplerConfig,
    ) -> None:
        """Create a `DataPipeline` instance.

        Args:
            resampler_config: Config to pass on to the resampler.
        """
        from ..actor import ChannelRegistry

        self._resampler_config = resampler_config

        self._channel_registry = ChannelRegistry(name="Data Pipeline Registry")

        self._data_sourcing_actor: _ActorInfo[DataSourcingActor] | None = None
        self._resampling_actor: _ActorInfo[
            ComponentMetricsResamplingActor
        ] | None = None

        self._battery_status_channel = Broadcast["BatteryStatus"](
            "battery-status", resend_latest=True
        )
        self._power_distribution_channel = Broadcast["Request"](
            "Power Distributing Actor, Broadcast Channel"
        )

        self._power_distributing_actor: "PowerDistributingActor" | None = None

        self._logical_meter: "LogicalMeter" | None = None
        self._ev_charger_pools: dict[frozenset[int], "EVChargerPool"] = {}
        self._battery_pools: dict[frozenset[int], "BatteryPool"] = {}

    def logical_meter(self) -> LogicalMeter:
        """Return the logical meter instance.

        If a LogicalMeter instance doesn't exist, a new one is created and returned.

        Returns:
            A logical meter instance.
        """
        from ..timeseries.logical_meter import LogicalMeter

        if self._logical_meter is None:
            self._logical_meter = LogicalMeter(
                channel_registry=self._channel_registry,
                resampler_subscription_sender=self._resampling_request_sender(),
            )
        return self._logical_meter

    def ev_charger_pool(
        self,
        ev_charger_ids: set[int] | None = None,
    ) -> EVChargerPool:
        """Return the corresponding EVChargerPool instance for the given ids.

        If an EVChargerPool instance for the given ids doesn't exist, a new one is
        created and returned.

        Args:
            ev_charger_ids: Optional set of IDs of EV Chargers to be managed by the
                EVChargerPool.

        Returns:
            An EVChargerPool instance.
        """
        from ..timeseries.ev_charger_pool import EVChargerPool

        # We use frozenset to make a hashable key from the input set.
        key: frozenset[int] = frozenset()
        if ev_charger_ids is not None:
            key = frozenset(ev_charger_ids)

        if key not in self._ev_charger_pools:
            self._ev_charger_pools[key] = EVChargerPool(
                channel_registry=self._channel_registry,
                resampler_subscription_sender=self._resampling_request_sender(),
                component_ids=ev_charger_ids,
            )
        return self._ev_charger_pools[key]

    def battery_pool(
        self,
        battery_ids: abc.Set[int] | None = None,
    ) -> BatteryPool:
        """Return the corresponding BatteryPool instance for the given ids.

        If a BatteryPool instance for the given ids doesn't exist, a new one is created
        and returned.

        Args:
            battery_ids: Optional set of IDs of batteries to be managed by the
                BatteryPool.

        Returns:
            A BatteryPool instance.
        """
        from ..timeseries.battery_pool import BatteryPool

        if not self._power_distributing_actor:
            self._start_power_distributing_actor()

        # We use frozenset to make a hashable key from the input set.
        key: frozenset[int] = frozenset()
        if battery_ids is not None:
            key = frozenset(battery_ids)

        if key not in self._battery_pools:
            self._battery_pools[key] = BatteryPool(
                channel_registry=self._channel_registry,
                resampler_subscription_sender=self._resampling_request_sender(),
                batteries_status_receiver=self._battery_status_channel.new_receiver(
                    maxsize=1
                ),
                power_distributing_sender=self._power_distribution_channel.new_sender(),
                min_update_interval=self._resampler_config.resampling_period,
                batteries_id=battery_ids,
            )

        return self._battery_pools[key]

    def _start_power_distributing_actor(self) -> None:
        """Start the power distributing actor if it is not already running."""
        if self._power_distributing_actor:
            return

        component_graph = connection_manager.get().component_graph
        if not component_graph.components(
            component_category={ComponentCategory.BATTERY}
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
            requests_receiver=self._power_distribution_channel.new_receiver(),
            channel_registry=self._channel_registry,
            battery_status_sender=self._battery_status_channel.new_sender(),
        )
        self._power_distributing_actor.start()

    def _data_sourcing_request_sender(self) -> Sender[ComponentMetricRequest]:
        """Return a Sender for sending requests to the data sourcing actor.

        If the data sourcing actor is not already running, this function also starts it.

        Returns:
            A Sender for sending requests to the data sourcing actor.
        """
        from ..actor import DataSourcingActor

        if self._data_sourcing_actor is None:
            channel: Broadcast[ComponentMetricRequest] = Broadcast(
                "Data Pipeline: Data Sourcing Actor Request Channel"
            )
            actor = DataSourcingActor(
                request_receiver=channel.new_receiver(
                    maxsize=_REQUEST_RECV_BUFFER_SIZE
                ),
                registry=self._channel_registry,
            )
            self._data_sourcing_actor = _ActorInfo(actor, channel)
            self._data_sourcing_actor.actor.start()
        return self._data_sourcing_actor.channel.new_sender()

    def _resampling_request_sender(self) -> Sender[ComponentMetricRequest]:
        """Return a Sender for sending requests to the resampling actor.

        If the resampling actor is not already running, this function also starts it.

        Returns:
            A Sender for sending requests to the resampling actor.
        """
        from ..actor import ComponentMetricsResamplingActor

        if self._resampling_actor is None:
            channel: Broadcast[ComponentMetricRequest] = Broadcast(
                "Data Pipeline: Component Metric Resampling Actor Request Channel"
            )
            actor = ComponentMetricsResamplingActor(
                channel_registry=self._channel_registry,
                data_sourcing_request_sender=self._data_sourcing_request_sender(),
                resampling_request_receiver=channel.new_receiver(
                    maxsize=_REQUEST_RECV_BUFFER_SIZE
                ),
                config=self._resampler_config,
            )
            self._resampling_actor = _ActorInfo(actor, channel)
            self._resampling_actor.actor.start()
        return self._resampling_actor.channel.new_sender()

    async def _stop(self) -> None:
        """Stop the data pipeline actors."""
        if self._data_sourcing_actor:
            await self._data_sourcing_actor.actor.stop()
        if self._resampling_actor:
            await self._resampling_actor.actor.stop()
        if self._power_distributing_actor:
            await self._power_distributing_actor.stop()


_DATA_PIPELINE: _DataPipeline | None = None


async def initialize(resampler_config: ResamplerConfig) -> None:
    """Initialize a `DataPipeline` instance.

    Args:
        resampler_config: Config to pass on to the resampler.

    Raises:
        RuntimeError: if the DataPipeline is already initialized.
    """
    global _DATA_PIPELINE  # pylint: disable=global-statement

    if _DATA_PIPELINE is not None:
        raise RuntimeError("DataPipeline is already initialized.")
    _DATA_PIPELINE = _DataPipeline(resampler_config)


def logical_meter() -> LogicalMeter:
    """Return the logical meter instance.

    If a LogicalMeter instance doesn't exist, a new one is created and returned.

    Returns:
        A logical meter instance.
    """
    return _get().logical_meter()


def ev_charger_pool(ev_charger_ids: set[int] | None = None) -> EVChargerPool:
    """Return the corresponding EVChargerPool instance for the given ids.

    If an EVChargerPool instance for the given ids doesn't exist, a new one is
    created and returned.

    Args:
        ev_charger_ids: Optional set of IDs of EV Chargers to be managed by the
            EVChargerPool.  If not specified, all EV Chargers available in the
            component graph are used.

    Returns:
        An EVChargerPool instance.
    """
    return _get().ev_charger_pool(ev_charger_ids)


def battery_pool(battery_ids: abc.Set[int] | None = None) -> BatteryPool:
    """Return the corresponding BatteryPool instance for the given ids.

    If a BatteryPool instance for the given ids doesn't exist, a new one is
    created and returned.

    Args:
        battery_ids: Optional set of IDs of batteries to be managed by the
            BatteryPool.  If not specified, all batteries available in the
            component graph are used.

    Returns:
        A BatteryPool instance.
    """
    return _get().battery_pool(battery_ids)


def _get() -> _DataPipeline:
    if _DATA_PIPELINE is None:
        raise RuntimeError(
            "DataPipeline is not initialized. "
            "Call `await microgrid.initialize()` first."
        )
    return _DATA_PIPELINE
