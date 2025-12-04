# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Module to define how to subscribe and fetch component data."""

from __future__ import annotations

import asyncio
import logging
import math
from abc import ABC, abstractmethod
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any, Generic, Self, TypeVar

from frequenz.channels import ChannelClosedError, Receiver
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.component import ComponentCategory
from frequenz.client.microgrid.metrics import Metric
from typing_extensions import override

from ..._internal._asyncio import AsyncConstructible
from ..._internal._constants import MAX_BATTERY_DATA_AGE_SEC
from ...microgrid import connection_manager
from ...microgrid._data_sourcing.microgrid_api_source import (
    _BATTERY_DATA_METHODS,
    _INVERTER_DATA_METHODS,
)
from ...microgrid._old_component_data import (
    BatteryData,
    ComponentData,
    InverterData,
    TransitionalMetric,
)
from ._component_metrics import ComponentMetricsData

_logger = logging.getLogger(__name__)

T = TypeVar("T", bound=ComponentData)
"""Type variable for component data."""


class ComponentMetricFetcher(AsyncConstructible, ABC):
    """Define how to subscribe for and fetch the component metrics data."""

    _component_id: ComponentId
    _metrics: Iterable[Metric | TransitionalMetric]

    @classmethod
    async def async_new(
        cls, component_id: ComponentId, metrics: Iterable[Metric | TransitionalMetric]
    ) -> Self:
        """Create an instance of this class.

        Subscribe for the given component metrics and return them if method
        `fetch_next` is called.

        Args:
            component_id: component id
            metrics: metrics that should be fetched from this component.

        Returns:
            This class instance.
        """
        self: Self = cls.__new__(cls)
        self._component_id = component_id
        self._metrics = metrics
        return self

    @abstractmethod
    async def fetch_next(self) -> ComponentMetricsData | None:
        """Fetch metrics for this component."""

    @abstractmethod
    def stop(self) -> None:
        """Stop the metric fetcher."""


class LatestMetricsFetcher(ComponentMetricFetcher, Generic[T], ABC):
    """Subscribe for the latest component data and extract the needed metrics."""

    _receiver: Receiver[T]
    _max_waiting_time: float

    @classmethod
    @override
    async def async_new(
        cls,
        component_id: ComponentId,
        metrics: Iterable[Metric | TransitionalMetric],
    ) -> Self:
        """Create instance of this class.

        Subscribe for the requested component data and fetch only the latest component
        metrics.

        Args:
            component_id: component id
            metrics: metrics

        Raises:
            ValueError: If any requested metric id is not supported.

        Returns:
            This class instance
        """
        self: Self = await super().async_new(component_id, metrics)

        # pylint: disable=protected-access
        for metric in metrics:
            if metric not in self._supported_metrics():
                category = self._component_category()
                raise ValueError(f"Metric {metric} not supported for {category}")

        self._receiver = self._subscribe()
        self._max_waiting_time = MAX_BATTERY_DATA_AGE_SEC
        # pylint: enable=protected-access
        return self

    @override
    async def fetch_next(self) -> ComponentMetricsData | None:
        """Fetch the latest component metrics.

        Returns:
            Component metrics data.
            None if the channel was closed and fetching next element is impossible.
        """
        try:
            data = await asyncio.wait_for(
                self._receiver.receive(), self._max_waiting_time
            )

        except ChannelClosedError:
            _logger.exception(
                "Channel for component %d was closed.", self._component_id
            )
            return None
        except asyncio.TimeoutError:
            # Next time wait infinitely until we receive any message.
            _logger.debug("Component %d stopped sending data.", self._component_id)
            return ComponentMetricsData(
                self._component_id, datetime.now(tz=timezone.utc), {}
            )

        self._max_waiting_time = MAX_BATTERY_DATA_AGE_SEC
        metrics: dict[Metric | TransitionalMetric, float] = {}
        for metric in self._metrics:
            value = self._extract_metric(data, metric)
            # There is no guarantee that all fields in component message are populated
            if not math.isnan(value):
                metrics[metric] = value

        return ComponentMetricsData(self._component_id, data.timestamp, metrics)

    @override
    def stop(self) -> None:
        """Stop the metric fetcher."""
        self._receiver.close()

    @abstractmethod
    def _extract_metric(
        self, data: T, metric: Metric | TransitionalMetric
    ) -> float: ...

    @abstractmethod
    def _supported_metrics(self) -> set[Metric | TransitionalMetric]: ...

    @abstractmethod
    def _component_category(self) -> ComponentCategory: ...

    @abstractmethod
    def _subscribe(self) -> Receiver[Any]:
        """Subscribe for this component data.

        Size of the receiver buffer should should be 1 to make sure we receive only
        the latest component data.

        Returns:
            Receiver for this component metrics.
        """


class LatestBatteryMetricsFetcher(LatestMetricsFetcher[BatteryData]):
    """Subscribe for the latest battery data using MicrogridApiClient."""

    @classmethod
    @override
    async def async_new(  # noqa: DOC502 (ValueError is raised indirectly super.async_new)
        cls,
        component_id: ComponentId,
        metrics: Iterable[Metric | TransitionalMetric],
    ) -> LatestBatteryMetricsFetcher:
        """Create instance of this class.

        Subscribe for the requested component data and fetch only the latest component
        metrics.

        Args:
            component_id: component id
            metrics: metrics

        Raises:
            ValueError: If any requested metric id is not supported.

        Returns:
            This class instance
        """
        self: LatestBatteryMetricsFetcher = await super().async_new(
            component_id, metrics
        )
        return self

    @override
    def _supported_metrics(self) -> set[Metric | TransitionalMetric]:
        return set(_BATTERY_DATA_METHODS.keys())

    @override
    def _extract_metric(
        self, data: BatteryData, metric: Metric | TransitionalMetric
    ) -> float:
        return _BATTERY_DATA_METHODS[metric](data)

    @override
    def _subscribe(self) -> Receiver[BatteryData]:
        """Subscribe for this component data.

        Size of the receiver buffer should should be 1 to make sure we receive only
        the latest component data.

        Returns:
            Receiver for this component metrics.
        """
        api = connection_manager.get().api_client
        return BatteryData.subscribe(api, self._component_id, buffer_size=1)

    @override
    def _component_category(self) -> ComponentCategory:
        return ComponentCategory.BATTERY


class LatestInverterMetricsFetcher(LatestMetricsFetcher[InverterData]):
    """Subscribe for the latest inverter data using MicrogridApiClient."""

    @classmethod
    @override
    async def async_new(  # noqa: DOC502 (ValueError is raised indirectly by super.async_new)
        cls,
        component_id: ComponentId,
        metrics: Iterable[Metric | TransitionalMetric],
    ) -> LatestInverterMetricsFetcher:
        """Create instance of this class.

        Subscribe for the requested component data and fetch only the latest component
        metrics.

        Args:
            component_id: component id
            metrics: metrics

        Raises:
            ValueError: If any requested metric id is not supported.

        Returns:
            This class instance
        """
        self: LatestInverterMetricsFetcher = await super().async_new(
            component_id, metrics
        )
        return self

    @override
    def _supported_metrics(self) -> set[Metric | TransitionalMetric]:
        return set(_INVERTER_DATA_METHODS.keys())

    @override
    def _extract_metric(
        self, data: InverterData, metric: Metric | TransitionalMetric
    ) -> float:
        return _INVERTER_DATA_METHODS[metric](data)

    @override
    def _subscribe(self) -> Receiver[InverterData]:
        """Subscribe for this component data.

        Size of the receiver buffer should should be 1 to make sure we receive only
        the latest component data.

        Returns:
            Receiver for this component metrics.
        """
        api = connection_manager.get().api_client
        return InverterData.subscribe(api, self._component_id, buffer_size=1)

    @override
    def _component_category(self) -> ComponentCategory:
        return ComponentCategory.INVERTER
