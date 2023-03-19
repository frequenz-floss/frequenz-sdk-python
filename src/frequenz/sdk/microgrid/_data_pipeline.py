# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Create, connect and own instances of data pipeline components.

Provides SDK users direct access to higher level components of the data pipeline,
eliminating the boiler plate code required to setup the DataSourcingActor and the
ResamplingActor.
"""

from __future__ import annotations

import typing
from dataclasses import dataclass

from frequenz.channels import Broadcast, Sender

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
    from ..timeseries.ev_charger_pool import EVChargerPool
    from ..timeseries.logical_meter import LogicalMeter


@dataclass
class _ActorInfo:
    """Holds instances of core data pipeline actors and their request channels."""

    actor: "DataSourcingActor | ComponentMetricsResamplingActor"
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

        self._data_sourcing_actor: _ActorInfo | None = None
        self._resampling_actor: _ActorInfo | None = None
        self._logical_meter: "LogicalMeter" | None = None
        self._ev_charger_pools: dict[frozenset[int], "EVChargerPool"] = {}

    def logical_meter(self) -> LogicalMeter:
        """Return the logical meter instance.

        If a LogicalMeter instance doesn't exist, a new one is created and returned.

        Returns:
            A logical meter instance.
        """
        from ..microgrid import connection_manager
        from ..timeseries.logical_meter import LogicalMeter

        if self._logical_meter is None:
            self._logical_meter = LogicalMeter(
                channel_registry=self._channel_registry,
                resampler_subscription_sender=self._resampling_request_sender(),
                component_graph=connection_manager.get().component_graph,
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
            ev_charger_ids: Optional set of IDs of EV Charger to be managed by the
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
                request_receiver=channel.new_receiver(), registry=self._channel_registry
            )
            self._data_sourcing_actor = _ActorInfo(actor, channel)
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
                resampling_request_receiver=channel.new_receiver(),
                config=self._resampler_config,
            )
            self._resampling_actor = _ActorInfo(actor, channel)
        return self._resampling_actor.channel.new_sender()

    async def _stop(self) -> None:
        """Stop the data pipeline actors."""
        # pylint: disable=protected-access
        if self._data_sourcing_actor:
            await self._data_sourcing_actor.actor._stop()  # type: ignore
        if self._resampling_actor:
            await self._resampling_actor.actor._stop()  # type: ignore
        # pylint: enable=protected-access


_DATA_PIPELINE: _DataPipeline | None = None


def initialize(resampler_config: ResamplerConfig) -> None:
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
        ev_charger_ids: Optional set of IDs of EV Charger to be managed by the
            EVChargerPool.

    Returns:
        An EVChargerPool instance.
    """
    return _get().ev_charger_pool(ev_charger_ids)


def _get() -> _DataPipeline:
    if _DATA_PIPELINE is None:
        raise RuntimeError(
            "DataPipeline is not initialized. "
            "Call `await microgrid.initialize()` first."
        )
    return _DATA_PIPELINE
