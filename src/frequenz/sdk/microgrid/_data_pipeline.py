# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Create, connect and own instances of data pipeline components.

Provides SDK users direct access to higher level components of the data pipeline,
eliminating the boiler plate code required to setup the DataSourcingActor and the
ResamplingActor.
"""

from __future__ import annotations

import logging
import sys
import typing
from collections import abc
from dataclasses import dataclass

from frequenz.channels import Broadcast, Sender

from ..actor._actor import Actor
from ..timeseries._base_types import PoolType
from ..timeseries._grid_frequency import GridFrequency
from ..timeseries._voltage_streamer import VoltageStreamer
from ..timeseries.grid import Grid
from ..timeseries.grid import get as get_grid
from ..timeseries.grid import initialize as initialize_grid
from . import connection_manager
from .component import ComponentCategory

_logger = logging.getLogger(__name__)

# A number of imports had to be done inside functions where they are used, to break
# import cycles.
#
# pylint: disable=import-outside-toplevel
if typing.TYPE_CHECKING:
    from ..actor import ComponentMetricRequest, ResamplingActorConfig, _power_managing
    from ..actor.power_distributing import (  # noqa: F401 (imports used by string type hints)
        ComponentPoolStatus,
        PowerDistributingActor,
        Request,
        Result,
    )
    from ..timeseries.battery_pool import BatteryPool
    from ..timeseries.battery_pool._battery_pool_reference_store import (
        BatteryPoolReferenceStore,
    )
    from ..timeseries.consumer import Consumer
    from ..timeseries.ev_charger_pool import EVChargerPool
    from ..timeseries.logical_meter import LogicalMeter
    from ..timeseries.producer import Producer


_REQUEST_RECV_BUFFER_SIZE = 500
"""The maximum number of requests that can be queued in the request receiver.

A larger buffer size means that the DataSourcing and Resampling actors don't drop
requests and will be able to keep up with higher request rates in larger installations.
"""


@dataclass
class _ActorInfo:
    """Holds instances of core data pipeline actors and their request channels."""

    actor: Actor
    """The actor instance."""

    channel: Broadcast[ComponentMetricRequest]
    """The request channel for the actor."""


class _DataPipeline:  # pylint: disable=too-many-instance-attributes
    """Create, connect and own instances of data pipeline components.

    Provides SDK users direct access to higher level components of the data pipeline,
    eliminating the boiler plate code required to setup the DataSourcingActor and the
    ResamplingActor.
    """

    def __init__(
        self,
        resampler_config: ResamplingActorConfig,
    ) -> None:
        """Create a `DataPipeline` instance.

        Args:
            resampler_config: Config to pass on to the resampler.
        """
        from ..actor import ChannelRegistry

        self._resampler_config: ResamplingActorConfig = resampler_config

        self._channel_registry: ChannelRegistry = ChannelRegistry(
            name="Data Pipeline Registry"
        )

        self._data_sourcing_actor: _ActorInfo | None = None
        self._resampling_actor: _ActorInfo | None = None

        self._battery_status_channel: Broadcast[ComponentPoolStatus] = Broadcast(
            "battery-status", resend_latest=True
        )
        self._power_distribution_requests_channel: Broadcast[Request] = Broadcast(
            "Power Distributing Actor, Requests Broadcast Channel"
        )
        self._power_distribution_results_channel: Broadcast[Result] = Broadcast(
            "Power Distributing Actor, Results Broadcast Channel"
        )

        self._power_management_proposals_channel: Broadcast[
            _power_managing.Proposal
        ] = Broadcast("Power Managing Actor, Requests Broadcast Channel")
        self._power_manager_bounds_subscription_channel: Broadcast[
            _power_managing.ReportRequest
        ] = Broadcast("Power Managing Actor, Bounds Subscription Channel")

        self._power_distributing_actor: PowerDistributingActor | None = None
        self._power_managing_actor: _power_managing.PowerManagingActor | None = None

        self._logical_meter: LogicalMeter | None = None
        self._consumer: Consumer | None = None
        self._producer: Producer | None = None
        self._grid: Grid | None = None
        self._ev_charger_pools: dict[frozenset[int], EVChargerPool] = {}
        self._battery_pools: dict[frozenset[int], BatteryPoolReferenceStore] = {}
        self._frequency_instance: GridFrequency | None = None
        self._voltage_instance: VoltageStreamer | None = None

    def frequency(self) -> GridFrequency:
        """Return the grid frequency measuring point."""
        if self._frequency_instance is None:
            self._frequency_instance = GridFrequency(
                self._data_sourcing_request_sender(),
                self._channel_registry,
            )

        return self._frequency_instance

    def voltage(self) -> VoltageStreamer:
        """Return the 3-phase voltage measuring point."""
        if not self._voltage_instance:
            self._voltage_instance = VoltageStreamer(
                self._resampling_request_sender(),
                self._channel_registry,
            )

        return self._voltage_instance

    def logical_meter(self) -> LogicalMeter:
        """Return the logical meter of the microgrid."""
        from ..timeseries.logical_meter import LogicalMeter

        if self._logical_meter is None:
            self._logical_meter = LogicalMeter(
                channel_registry=self._channel_registry,
                resampler_subscription_sender=self._resampling_request_sender(),
            )
        return self._logical_meter

    def consumer(self) -> Consumer:
        """Return the consumption measuring point of the microgrid."""
        from ..timeseries.consumer import Consumer

        if self._consumer is None:
            self._consumer = Consumer(
                channel_registry=self._channel_registry,
                resampler_subscription_sender=self._resampling_request_sender(),
            )
        return self._consumer

    def producer(self) -> Producer:
        """Return the production measuring point of the microgrid."""
        from ..timeseries.producer import Producer

        if self._producer is None:
            self._producer = Producer(
                channel_registry=self._channel_registry,
                resampler_subscription_sender=self._resampling_request_sender(),
            )
        return self._producer

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

    def grid(self) -> Grid:
        """Return the grid measuring point."""
        if self._grid is None:
            initialize_grid(
                channel_registry=self._channel_registry,
                resampler_subscription_sender=self._resampling_request_sender(),
            )
            self._grid = get_grid()

        return self._grid

    def battery_pool(
        self,
        battery_ids: abc.Set[int] | None = None,
        name: str | None = None,
        priority: int = -sys.maxsize - 1,
    ) -> BatteryPool:
        """Return a new `BatteryPool` instance for the given ids.

        If a `BatteryPoolReferenceStore` instance for the given battery ids doesn't exist,
        a new one is created and used for creating the `BatteryPool`.

        Args:
            battery_ids: Optional set of IDs of batteries to be managed by the
                `BatteryPool`.
            name: An optional name used to identify this instance of the pool or a
                corresponding actor in the logs.
            priority: The priority of the actor making the call.

        Returns:
            A `BatteryPool` instance.
        """
        from ..timeseries.battery_pool import BatteryPool
        from ..timeseries.battery_pool._battery_pool_reference_store import (
            BatteryPoolReferenceStore,
        )

        if not self._power_managing_actor:
            self._start_power_managing_actor()

        # We use frozenset to make a hashable key from the input set.
        key: frozenset[int] = frozenset()
        if battery_ids is not None:
            key = frozenset(battery_ids)

        if key not in self._battery_pools:
            self._battery_pools[key] = BatteryPoolReferenceStore(
                channel_registry=self._channel_registry,
                resampler_subscription_sender=self._resampling_request_sender(),
                batteries_status_receiver=self._battery_status_channel.new_receiver(
                    maxsize=1
                ),
                power_manager_requests_sender=(
                    self._power_management_proposals_channel.new_sender()
                ),
                power_manager_bounds_subscription_sender=(
                    self._power_manager_bounds_subscription_channel.new_sender()
                ),
                min_update_interval=self._resampler_config.resampling_period,
                batteries_id=battery_ids,
            )

        return BatteryPool(self._battery_pools[key], name, priority)

    def _start_power_managing_actor(self) -> None:
        """Start the power managing actor if it is not already running."""
        if self._power_managing_actor:
            return

        component_graph = connection_manager.get().component_graph
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

        self._start_power_distributing_actor()

        from ..actor._power_managing._power_managing_actor import PowerManagingActor

        self._power_managing_actor = PowerManagingActor(
            pool_type=PoolType.BATTERY_POOL,
            proposals_receiver=self._power_management_proposals_channel.new_receiver(),
            bounds_subscription_receiver=(
                self._power_manager_bounds_subscription_channel.new_receiver()
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
            component_pool_status_sender=self._battery_status_channel.new_sender(),
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
        if self._power_managing_actor:
            await self._power_managing_actor.stop()
        for pool in self._battery_pools.values():
            await pool.stop()


_DATA_PIPELINE: _DataPipeline | None = None


async def initialize(resampler_config: ResamplingActorConfig) -> None:
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


def frequency() -> GridFrequency:
    """Return the grid frequency measuring point."""
    return _get().frequency()


def voltage() -> VoltageStreamer:
    """Return the 3-phase voltage measuring point."""
    return _get().voltage()


def logical_meter() -> LogicalMeter:
    """Return the logical meter of the microgrid."""
    return _get().logical_meter()


def consumer() -> Consumer:
    """Return the [`Consumption`][frequenz.sdk.timeseries.consumer.Consumer] measuring point."""
    return _get().consumer()


def producer() -> Producer:
    """Return the [`Production`][frequenz.sdk.timeseries.producer.Producer] measuring point."""
    return _get().producer()


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


def battery_pool(
    battery_ids: abc.Set[int] | None = None,
    name: str | None = None,
    priority: int = -sys.maxsize - 1,
) -> BatteryPool:
    """Return a new `BatteryPool` instance for the given parameters.

    The priority value is used to resolve conflicts when multiple actors are trying to
    propose different power values for the same set of batteries.

    !!! note
        When specifying priority, bigger values indicate higher priority. The default
        priority is the lowest possible value.

    Args:
        battery_ids: Optional set of IDs of batteries to be managed by the `BatteryPool`.
            If not specified, all batteries available in the component graph are used.
        name: An optional name used to identify this instance of the pool or a
            corresponding actor in the logs.
        priority: The priority of the actor making the call.

    Returns:
        A `BatteryPool` instance.
    """
    return _get().battery_pool(battery_ids, name, priority)


def grid() -> Grid:
    """Return the grid measuring point."""
    return _get().grid()


def _get() -> _DataPipeline:
    if _DATA_PIPELINE is None:
        raise RuntimeError(
            "DataPipeline is not initialized. "
            "Call `await microgrid.initialize()` first."
        )
    return _DATA_PIPELINE
