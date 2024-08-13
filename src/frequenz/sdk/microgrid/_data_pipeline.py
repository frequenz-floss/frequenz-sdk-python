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
from datetime import timedelta

from frequenz.channels import Broadcast, Sender
from frequenz.client.microgrid import ComponentCategory, InverterType

from .._internal._channels import ChannelRegistry
from ..actor._actor import Actor
from ..timeseries import ResamplerConfig
from ..timeseries._grid_frequency import GridFrequency
from ..timeseries._voltage_streamer import VoltageStreamer
from ..timeseries.grid import Grid
from ..timeseries.grid import get as get_grid
from ..timeseries.grid import initialize as initialize_grid
from ._data_sourcing import ComponentMetricRequest, DataSourcingActor
from ._power_wrapper import PowerWrapper

# A number of imports had to be done inside functions where they are used, to break
# import cycles.
#
# pylint: disable=import-outside-toplevel
if typing.TYPE_CHECKING:
    from ..timeseries.battery_pool import BatteryPool
    from ..timeseries.battery_pool._battery_pool_reference_store import (
        BatteryPoolReferenceStore,
    )
    from ..timeseries.consumer import Consumer
    from ..timeseries.ev_charger_pool import EVChargerPool
    from ..timeseries.ev_charger_pool._ev_charger_pool_reference_store import (
        EVChargerPoolReferenceStore,
    )
    from ..timeseries.logical_meter import LogicalMeter
    from ..timeseries.producer import Producer
    from ..timeseries.pv_pool import PVPool
    from ..timeseries.pv_pool._pv_pool_reference_store import PVPoolReferenceStore

_logger = logging.getLogger(__name__)


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
        resampler_config: ResamplerConfig,
        api_power_request_timeout: timedelta = timedelta(seconds=5.0),
    ) -> None:
        """Create a `DataPipeline` instance.

        Args:
            resampler_config: Config to pass on to the resampler.
            api_power_request_timeout: Timeout to use when making power requests to
                the microgrid API.
        """
        self._resampler_config: ResamplerConfig = resampler_config

        self._channel_registry: ChannelRegistry = ChannelRegistry(
            name="Data Pipeline Registry"
        )

        self._data_sourcing_actor: _ActorInfo | None = None
        self._resampling_actor: _ActorInfo | None = None

        self._battery_power_wrapper = PowerWrapper(
            self._channel_registry,
            api_power_request_timeout=api_power_request_timeout,
            component_category=ComponentCategory.BATTERY,
        )
        self._ev_power_wrapper = PowerWrapper(
            self._channel_registry,
            api_power_request_timeout=api_power_request_timeout,
            component_category=ComponentCategory.EV_CHARGER,
        )
        self._pv_power_wrapper = PowerWrapper(
            self._channel_registry,
            api_power_request_timeout=api_power_request_timeout,
            component_category=ComponentCategory.INVERTER,
            component_type=InverterType.SOLAR,
        )

        self._logical_meter: LogicalMeter | None = None
        self._consumer: Consumer | None = None
        self._producer: Producer | None = None
        self._grid: Grid | None = None
        self._ev_charger_pool_reference_stores: dict[
            frozenset[int], EVChargerPoolReferenceStore
        ] = {}
        self._battery_pool_reference_stores: dict[
            frozenset[int], BatteryPoolReferenceStore
        ] = {}
        self._pv_pool_reference_stores: dict[frozenset[int], PVPoolReferenceStore] = {}
        self._frequency_instance: GridFrequency | None = None
        self._voltage_instance: VoltageStreamer | None = None

        self._known_pool_keys: set[str] = set()
        """A set of keys for corresponding to created EVChargerPool instances.

        This is used to warn the user if they try to create a new EVChargerPool instance
        for the same set of component IDs, and with the same priority.
        """

    def frequency(self) -> GridFrequency:
        """Return the grid frequency measuring point."""
        if self._frequency_instance is None:
            self._frequency_instance = GridFrequency(
                self._data_sourcing_request_sender(),
                self._channel_registry,
            )

        return self._frequency_instance

    def voltage_per_phase(self) -> VoltageStreamer:
        """Return the per-phase voltage measuring point."""
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

    def grid(self) -> Grid:
        """Return the grid measuring point."""
        if self._grid is None:
            initialize_grid(
                channel_registry=self._channel_registry,
                resampler_subscription_sender=self._resampling_request_sender(),
            )
            self._grid = get_grid()

        return self._grid

    def new_ev_charger_pool(
        self,
        *,
        priority: int,
        component_ids: abc.Set[int] | None = None,
        name: str | None = None,
        set_operating_point: bool = False,
    ) -> EVChargerPool:
        """Return the corresponding EVChargerPool instance for the given ids.

        If an EVChargerPool instance for the given ids doesn't exist, a new one is
        created and returned.

        Args:
            priority: The priority of the actor making the call.
            component_ids: Optional set of IDs of EV Chargers to be managed by the
                EVChargerPool.
            name: An optional name used to identify this instance of the pool or a
                corresponding actor in the logs.
            set_operating_point: Whether this instance sets the operating point power or
                the normal power for the components.

        Returns:
            An EVChargerPool instance.
        """
        from ..timeseries.ev_charger_pool import EVChargerPool
        from ..timeseries.ev_charger_pool._ev_charger_pool_reference_store import (
            EVChargerPoolReferenceStore,
        )

        if not self._ev_power_wrapper.started:
            self._ev_power_wrapper.start()

        # We use frozenset to make a hashable key from the input set.
        ref_store_key: frozenset[int] = frozenset()
        if component_ids is not None:
            ref_store_key = frozenset(component_ids)

        pool_key = f"{ref_store_key}-{priority}"
        if pool_key in self._known_pool_keys:
            _logger.warning(
                "An EVChargerPool instance was already created for ev_charger_ids=%s "
                "and priority=%s using `microgrid.ev_charger_pool(...)`."
                "\n  Hint: If the multiple instances are created from the same actor, "
                "consider reusing the same instance."
                "\n  Hint: If the instances are created from different actors, "
                "consider using different priorities to distinguish them.",
                component_ids,
                priority,
            )
        else:
            self._known_pool_keys.add(pool_key)

        if ref_store_key not in self._ev_charger_pool_reference_stores:
            self._ev_charger_pool_reference_stores[ref_store_key] = (
                EVChargerPoolReferenceStore(
                    channel_registry=self._channel_registry,
                    resampler_subscription_sender=self._resampling_request_sender(),
                    status_receiver=self._ev_power_wrapper.status_channel.new_receiver(
                        limit=1
                    ),
                    power_manager_requests_sender=(
                        self._ev_power_wrapper.proposal_channel.new_sender()
                    ),
                    power_manager_bounds_subs_sender=(
                        self._ev_power_wrapper.bounds_subscription_channel.new_sender()
                    ),
                    power_distribution_results_fetcher=(
                        self._ev_power_wrapper.distribution_results_fetcher()
                    ),
                    component_ids=component_ids,
                )
            )
        return EVChargerPool(
            pool_ref_store=self._ev_charger_pool_reference_stores[ref_store_key],
            name=name,
            priority=priority,
            set_operating_point=set_operating_point,
        )

    def new_pv_pool(
        self,
        *,
        priority: int,
        component_ids: abc.Set[int] | None = None,
        name: str | None = None,
        set_operating_point: bool = False,
    ) -> PVPool:
        """Return a new `PVPool` instance for the given ids.

        If a `PVPoolReferenceStore` instance for the given PV inverter ids doesn't
        exist, a new one is created and used for creating the `PVPool`.

        Args:
            priority: The priority of the actor making the call.
            component_ids: Optional set of IDs of PV inverters to be managed by the
                `PVPool`.
            name: An optional name used to identify this instance of the pool or a
                corresponding actor in the logs.
            set_operating_point: Whether this instance sets the operating point power or
                the normal power for the components.

        Returns:
            A `PVPool` instance.
        """
        from ..timeseries.pv_pool import PVPool
        from ..timeseries.pv_pool._pv_pool_reference_store import PVPoolReferenceStore

        if not self._pv_power_wrapper.started:
            self._pv_power_wrapper.start()

        # We use frozenset to make a hashable key from the input set.
        ref_store_key: frozenset[int] = frozenset()
        if component_ids is not None:
            ref_store_key = frozenset(component_ids)

        pool_key = f"{ref_store_key}-{priority}"
        if pool_key in self._known_pool_keys:
            _logger.warning(
                "A PVPool instance was already created for pv_inverter_ids=%s and "
                "priority=%s using `microgrid.pv_pool(...)`."
                "\n  Hint: If the multiple instances are created from the same actor, "
                "consider reusing the same instance."
                "\n  Hint: If the instances are created from different actors, "
                "consider using different priorities to distinguish them.",
                component_ids,
                priority,
            )
        else:
            self._known_pool_keys.add(pool_key)

        if ref_store_key not in self._pv_pool_reference_stores:
            self._pv_pool_reference_stores[ref_store_key] = PVPoolReferenceStore(
                channel_registry=self._channel_registry,
                resampler_subscription_sender=self._resampling_request_sender(),
                status_receiver=(
                    self._pv_power_wrapper.status_channel.new_receiver(limit=1)
                ),
                power_manager_requests_sender=(
                    self._pv_power_wrapper.proposal_channel.new_sender()
                ),
                power_manager_bounds_subs_sender=(
                    self._pv_power_wrapper.bounds_subscription_channel.new_sender()
                ),
                power_distribution_results_fetcher=(
                    self._pv_power_wrapper.distribution_results_fetcher()
                ),
                component_ids=component_ids,
            )

        return PVPool(
            pool_ref_store=self._pv_pool_reference_stores[ref_store_key],
            name=name,
            priority=priority,
            set_operating_point=set_operating_point,
        )

    def new_battery_pool(
        self,
        *,
        priority: int,
        component_ids: abc.Set[int] | None = None,
        name: str | None = None,
        set_operating_point: bool = False,
    ) -> BatteryPool:
        """Return a new `BatteryPool` instance for the given ids.

        If a `BatteryPoolReferenceStore` instance for the given battery ids doesn't
        exist, a new one is created and used for creating the `BatteryPool`.

        Args:
            priority: The priority of the actor making the call.
            component_ids: Optional set of IDs of batteries to be managed by the
                `BatteryPool`.
            name: An optional name used to identify this instance of the pool or a
                corresponding actor in the logs.
            set_operating_point: Whether this instance sets the operating point power or
                the normal power for the components.

        Returns:
            A `BatteryPool` instance.
        """
        from ..timeseries.battery_pool import BatteryPool
        from ..timeseries.battery_pool._battery_pool_reference_store import (
            BatteryPoolReferenceStore,
        )

        if not self._battery_power_wrapper.started:
            self._battery_power_wrapper.start()

        # We use frozenset to make a hashable key from the input set.
        ref_store_key: frozenset[int] = frozenset()
        if component_ids is not None:
            ref_store_key = frozenset(component_ids)

        pool_key = f"{ref_store_key}-{priority}"
        if pool_key in self._known_pool_keys:
            _logger.warning(
                "A BatteryPool instance was already created for battery_ids=%s and "
                "priority=%s using `microgrid.battery_pool(...)`."
                "\n  Hint: If the multiple instances are created from the same actor, "
                "consider reusing the same instance."
                "\n  Hint: If the instances are created from different actors, "
                "consider using different priorities to distinguish them.",
                component_ids,
                priority,
            )
        else:
            self._known_pool_keys.add(pool_key)

        if ref_store_key not in self._battery_pool_reference_stores:
            self._battery_pool_reference_stores[ref_store_key] = (
                BatteryPoolReferenceStore(
                    channel_registry=self._channel_registry,
                    resampler_subscription_sender=self._resampling_request_sender(),
                    batteries_status_receiver=(
                        self._battery_power_wrapper.status_channel.new_receiver(limit=1)
                    ),
                    power_manager_requests_sender=(
                        self._battery_power_wrapper.proposal_channel.new_sender()
                    ),
                    power_manager_bounds_subscription_sender=(
                        self._battery_power_wrapper.bounds_subscription_channel.new_sender()
                    ),
                    power_distribution_results_fetcher=(
                        self._battery_power_wrapper.distribution_results_fetcher()
                    ),
                    min_update_interval=self._resampler_config.resampling_period,
                    batteries_id=component_ids,
                )
            )

        return BatteryPool(
            pool_ref_store=self._battery_pool_reference_stores[ref_store_key],
            name=name,
            priority=priority,
            set_operating_point=set_operating_point,
        )

    def _data_sourcing_request_sender(self) -> Sender[ComponentMetricRequest]:
        """Return a Sender for sending requests to the data sourcing actor.

        If the data sourcing actor is not already running, this function also starts it.

        Returns:
            A Sender for sending requests to the data sourcing actor.
        """
        if self._data_sourcing_actor is None:
            channel: Broadcast[ComponentMetricRequest] = Broadcast(
                name="Data Pipeline: Data Sourcing Actor Request Channel"
            )
            actor = DataSourcingActor(
                request_receiver=channel.new_receiver(limit=_REQUEST_RECV_BUFFER_SIZE),
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
        from ._resampling import ComponentMetricsResamplingActor

        if self._resampling_actor is None:
            channel: Broadcast[ComponentMetricRequest] = Broadcast(
                name="Data Pipeline: Component Metric Resampling Actor Request Channel"
            )
            actor = ComponentMetricsResamplingActor(
                channel_registry=self._channel_registry,
                data_sourcing_request_sender=self._data_sourcing_request_sender(),
                resampling_request_receiver=channel.new_receiver(
                    limit=_REQUEST_RECV_BUFFER_SIZE
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
        await self._battery_power_wrapper.stop()
        for pool in self._battery_pool_reference_stores.values():
            await pool.stop()


_DATA_PIPELINE: _DataPipeline | None = None


async def initialize(
    resampler_config: ResamplerConfig,
    api_power_request_timeout: timedelta = timedelta(seconds=5.0),
) -> None:
    """Initialize a `DataPipeline` instance.

    Args:
        resampler_config: Config to pass on to the resampler.
        api_power_request_timeout: Timeout to use when making power requests to
            the microgrid API.  When requests to components timeout, they will
            be marked as blocked for a short duration, during which time they
            will be unavailable from the corresponding component pools.

    Raises:
        RuntimeError: if the DataPipeline is already initialized.
    """
    global _DATA_PIPELINE  # pylint: disable=global-statement

    if _DATA_PIPELINE is not None:
        raise RuntimeError("DataPipeline is already initialized.")
    _DATA_PIPELINE = _DataPipeline(resampler_config, api_power_request_timeout)


def frequency() -> GridFrequency:
    """Return the grid frequency measuring point."""
    return _get().frequency()


def voltage_per_phase() -> VoltageStreamer:
    """Return the per-phase voltage measuring point."""
    return _get().voltage_per_phase()


def logical_meter() -> LogicalMeter:
    """Return the logical meter of the microgrid."""
    return _get().logical_meter()


def consumer() -> Consumer:
    """Return the [`Consumption`][frequenz.sdk.timeseries.consumer.Consumer] measuring point."""
    return _get().consumer()


def producer() -> Producer:
    """Return the [`Production`][frequenz.sdk.timeseries.producer.Producer] measuring point."""
    return _get().producer()


def new_ev_charger_pool(
    *,
    priority: int,
    component_ids: abc.Set[int] | None = None,
    name: str | None = None,
    set_operating_point: bool = False,
) -> EVChargerPool:
    """Return a new `EVChargerPool` instance for the given parameters.

    The priority value is used to resolve conflicts when multiple actors are trying to
    propose different power values for the same set of EV chargers.

    !!! note
        When specifying priority, bigger values indicate higher priority.

        It is recommended to reuse the same instance of the `EVChargerPool` within the
        same actor, unless they are managing different sets of EV chargers.

        In deployments with multiple actors managing the same set of EV chargers, it is
        recommended to use different priorities to distinguish between them.  If not,
        a random prioritization will be imposed on them to resolve conflicts, which may
        lead to unexpected behavior like longer duration to converge on the desired
        power.

    Args:
        priority: The priority of the actor making the call.
        component_ids: Optional set of IDs of EV Chargers to be managed by the
            EVChargerPool.  If not specified, all EV Chargers available in the
            component graph are used.
        name: An optional name used to identify this instance of the pool or a
            corresponding actor in the logs.
        set_operating_point: Whether this instance sets the operating point power or the
            normal power for the components.

    Returns:
        An `EVChargerPool` instance.
    """
    return _get().new_ev_charger_pool(
        priority=priority,
        component_ids=component_ids,
        name=name,
        set_operating_point=set_operating_point,
    )


def new_battery_pool(
    *,
    priority: int,
    component_ids: abc.Set[int] | None = None,
    name: str | None = None,
    set_operating_point: bool = False,
) -> BatteryPool:
    """Return a new `BatteryPool` instance for the given parameters.

    The priority value is used to resolve conflicts when multiple actors are trying to
    propose different power values for the same set of batteries.

    !!! note
        When specifying priority, bigger values indicate higher priority.

        It is recommended to reuse the same instance of the `BatteryPool` within the
        same actor, unless they are managing different sets of batteries.

        In deployments with multiple actors managing the same set of batteries, it is
        recommended to use different priorities to distinguish between them.  If not,
        a random prioritization will be imposed on them to resolve conflicts, which may
        lead to unexpected behavior like longer duration to converge on the desired
        power.

    Args:
        priority: The priority of the actor making the call.
        component_ids: Optional set of IDs of batteries to be managed by the
            `BatteryPool`.  If not specified, all batteries available in the component
            graph are used.
        name: An optional name used to identify this instance of the pool or a
            corresponding actor in the logs.
        set_operating_point: Whether this instance sets the operating point power or the
            normal power for the components.

    Returns:
        A `BatteryPool` instance.
    """
    return _get().new_battery_pool(
        priority=priority,
        component_ids=component_ids,
        name=name,
        set_operating_point=set_operating_point,
    )


def new_pv_pool(
    *,
    priority: int,
    component_ids: abc.Set[int] | None = None,
    name: str | None = None,
    set_operating_point: bool = False,
) -> PVPool:
    """Return a new `PVPool` instance for the given parameters.

    The priority value is used to resolve conflicts when multiple actors are trying to
    propose different power values for the same set of PV inverters.

    !!! note
        When specifying priority, bigger values indicate higher priority.

        It is recommended to reuse the same instance of the `PVPool` within the same
        actor, unless they are managing different sets of PV inverters.

        In deployments with multiple actors managing the same set of PV inverters, it is
        recommended to use different priorities to distinguish between them.  If not,
        a random prioritization will be imposed on them to resolve conflicts, which may
        lead to unexpected behavior like longer duration to converge on the desired
        power.

    Args:
        priority: The priority of the actor making the call.
        component_ids: Optional set of IDs of PV inverters to be managed by the
            `PVPool`. If not specified, all PV inverters available in the component
            graph are used.
        name: An optional name used to identify this instance of the pool or a
            corresponding actor in the logs.
        set_operating_point: Whether this instance sets the operating point power or the
            normal power for the components.

    Returns:
        A `PVPool` instance.
    """
    return _get().new_pv_pool(
        priority=priority,
        component_ids=component_ids,
        name=name,
        set_operating_point=set_operating_point,
    )


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
