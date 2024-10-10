# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Methods for processing battery-inverter data."""


import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Generic

from frequenz.channels import Broadcast, Receiver

from ..._internal._asyncio import cancel_and_await, run_forever
from ..._internal._constants import RECEIVER_MAX_SIZE, WAIT_FOR_COMPONENT_DATA_SEC
from ...microgrid._power_distributing._component_managers._battery_manager import (
    _get_battery_inverter_mappings,
)
from ._component_metric_fetcher import (
    ComponentMetricFetcher,
    LatestBatteryMetricsFetcher,
    LatestInverterMetricsFetcher,
)
from ._component_metrics import ComponentMetricsData
from ._metric_calculator import MetricCalculator, T

_logger = logging.getLogger(__name__)


class MetricAggregator(Generic[T], ABC):
    """Interface to control how the component data should be aggregated and send."""

    @abstractmethod
    def update_working_batteries(self, new_working_batteries: set[int]) -> None:
        """Update set of the working batteries.

        Args:
            new_working_batteries: Set of the working batteries.
        """

    @abstractmethod
    def new_receiver(self, limit: int | None = RECEIVER_MAX_SIZE) -> Receiver[T]:
        """Return new receiver for the aggregated metric results.

        Args:
            limit: Buffer size of the receiver

        Returns:
            Receiver for the metric results.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop the all pending async task."""

    @classmethod
    @abstractmethod
    def name(cls) -> str:
        """Return name of this method.

        Returns:
            Name of this method.
        """


class SendOnUpdate(MetricAggregator[T]):
    """Wait for the change of the components metrics and send updated result.

    This method will cache the component metrics. When any metric change it will
    recalculate high level metric. If the calculation result change, it will
    send the new value.
    """

    def __init__(
        self,
        working_batteries: set[int],
        metric_calculator: MetricCalculator[T],
        min_update_interval: timedelta,
    ) -> None:
        """Create class instance.

        Args:
            working_batteries: Set of the working batteries
            metric_calculator: Module that tells how to aggregate the component metrics.
            min_update_interval: Minimum frequency for sending update about the change.
        """
        self._metric_calculator: MetricCalculator[T] = metric_calculator
        self._bat_inv_map = _get_battery_inverter_mappings(
            self._metric_calculator.batteries,
            inv_bats=False,
            bat_bats=False,
            inv_invs=False,
        )["bat_invs"]

        self._working_batteries: set[int] = working_batteries.intersection(
            metric_calculator.batteries
        )
        self._result_channel: Broadcast[T] = Broadcast(
            name=SendOnUpdate.name() + "_" + metric_calculator.name(),
            resend_latest=True,
        )

        self._update_event = asyncio.Event()
        self._cached_metrics: dict[int, ComponentMetricsData] = {}

        self._update_task = asyncio.create_task(run_forever(self._update_and_notify))
        self._send_task = asyncio.create_task(
            run_forever(lambda: self._send_on_update(min_update_interval))
        )
        self._pending_data_fetchers: set[asyncio.Task[ComponentMetricsData | None]] = (
            set()
        )

    @classmethod
    def name(cls) -> str:
        """Get name of the method.

        Returns:
            Name of the method.
        """
        return "SendOnUpdate"

    def new_receiver(self, limit: int | None = RECEIVER_MAX_SIZE) -> Receiver[T]:
        """Return new receiver for the aggregated metric results.

        Args:
            limit: Buffer size of the receiver

        Returns:
            Receiver for the metric results.
        """
        if limit is None:
            return self._result_channel.new_receiver()
        return self._result_channel.new_receiver(limit=limit)

    def update_working_batteries(self, new_working_batteries: set[int]) -> None:
        """Update set of the working batteries.

        Recalculate metric if set changed.

        Args:
            new_working_batteries: Set of the working batteries.
        """
        # MetricCalculator can discard some batteries if they doesn't meet requirements.
        # For example batteries without adjacent inverter won't be included
        # int he PowerBounds metrics.
        new_set = new_working_batteries.intersection(self._metric_calculator.batteries)

        stopped_working = self._working_batteries - new_set
        for battery_id in stopped_working:
            # Removed cached metrics for components that stopped working.
            self._cached_metrics.pop(battery_id, None)
            for inv_id in self._bat_inv_map[battery_id]:
                self._cached_metrics.pop(inv_id, None)

        if new_set != self._working_batteries:
            self._working_batteries = new_set
            self._update_event.set()

    async def stop(self) -> None:
        """Stop the all pending async task."""
        await asyncio.gather(
            *[cancel_and_await(task) for task in self._pending_data_fetchers]
        )
        await asyncio.gather(
            *[cancel_and_await(self._send_task), cancel_and_await(self._update_task)]
        )

    async def _create_data_fetchers(self) -> dict[int, ComponentMetricFetcher]:
        fetchers: dict[int, ComponentMetricFetcher] = {
            cid: await LatestBatteryMetricsFetcher.async_new(cid, metrics)
            for cid, metrics in self._metric_calculator.battery_metrics.items()
        }
        inverter_fetchers = {
            cid: await LatestInverterMetricsFetcher.async_new(cid, metrics)
            for cid, metrics in self._metric_calculator.inverter_metrics.items()
        }
        fetchers.update(inverter_fetchers)
        return fetchers

    def _remove_metric_fetcher(
        self, fetchers: dict[int, ComponentMetricFetcher], component_id: int
    ) -> None:
        _logger.error(
            "Removing component %d from the %s formula.",
            component_id,
            self._result_channel._name,  # pylint: disable=protected-access
        )
        fetchers.pop(component_id)

    def _metric_updated(self, new_metrics: ComponentMetricsData) -> bool:
        cid = new_metrics.component_id
        return (
            cid not in self._cached_metrics or new_metrics != self._cached_metrics[cid]
        )

    async def _update_and_notify(self) -> None:
        """Receive component metrics and send notification when they change."""
        fetchers = await self._create_data_fetchers()

        self._pending_data_fetchers = {
            asyncio.create_task(fetcher.fetch_next(), name=str(cid))
            for cid, fetcher in fetchers.items()
        }
        while len(self._pending_data_fetchers) > 0:
            done, self._pending_data_fetchers = await asyncio.wait(
                self._pending_data_fetchers, return_when=asyncio.FIRST_COMPLETED
            )
            for item in done:
                metrics = item.result()
                if metrics is None:
                    self._remove_metric_fetcher(fetchers, int(item.get_name()))
                    continue
                if self._metric_updated(metrics):
                    self._update_event.set()

                cid = metrics.component_id
                # Save metric even if not changed to update its timestamp.
                self._cached_metrics[cid] = metrics
                # Add fetcher back to the processing list.
                self._pending_data_fetchers.add(
                    asyncio.create_task(fetchers[cid].fetch_next(), name=str(cid))
                )

    async def _send_on_update(self, min_update_interval: timedelta) -> None:
        """Wait for an update notification, recalculate metric and send.

        If recalculated metric changed, then send new value.

        Args:
            min_update_interval: Minimum frequency for sending update about the change.
        """
        sender = self._result_channel.new_sender()
        latest_calculation_result: T | None = None
        # Wait for first data to come.
        # In that way the first outputs are reliable.
        await asyncio.sleep(WAIT_FOR_COMPONENT_DATA_SEC)

        while True:
            await self._update_event.wait()
            self._update_event.clear()

            result: T = self._metric_calculator.calculate(
                self._cached_metrics, self._working_batteries
            )
            if result != latest_calculation_result:
                latest_calculation_result = result
                await sender.send(result)

                if result is None:
                    sleep_for = min_update_interval.total_seconds()
                else:
                    # Sleep for the rest of the time.
                    # Then we won't send update more frequently then min_update_interval
                    time_diff = datetime.now(tz=timezone.utc) - result.timestamp
                    sleep_for = (min_update_interval - time_diff).total_seconds()
                await asyncio.sleep(sleep_for)
