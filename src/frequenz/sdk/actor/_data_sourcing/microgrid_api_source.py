# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""The Microgrid API data source for the DataSourcingActor."""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from frequenz.channels import Receiver, Sender

from ... import microgrid
from ...microgrid.component import (
    BatteryData,
    ComponentCategory,
    ComponentMetricId,
    EVChargerData,
    InverterData,
    MeterData,
)
from ...timeseries import Sample
from .._channel_registry import ChannelRegistry


@dataclass
class ComponentMetricRequest:
    """A request object to start streaming a metric for a component."""

    namespace: str
    """The namespace that this request belongs to.

    Metric requests with a shared namespace enable the reuse of channels within
    that namespace.

    If for example, an actor making a multiple requests, uses the name of the
    actor as the namespace, then requests from the actor will get reused when
    possible.
    """

    component_id: int
    """The ID of the requested component."""

    metric_id: ComponentMetricId
    """The ID of the requested component's metric."""

    start_time: Optional[datetime]
    """The start time from which data is required.

    When None, we will stream only live data.
    """

    def get_channel_name(self) -> str:
        """Return a channel name constructed from Self.

        This channel name can be used by the sending side and receiving sides to
        identify the right channel from the ChannelRegistry.

        Returns:
            A string denoting a channel name.
        """
        return f"{self.component_id}::{self.metric_id.name}::{self.start_time}::{self.namespace}"


_MeterDataMethods: Dict[ComponentMetricId, Callable[[MeterData], float]] = {
    ComponentMetricId.ACTIVE_POWER: lambda msg: msg.active_power,
    ComponentMetricId.CURRENT_PHASE_1: lambda msg: msg.current_per_phase[0],
    ComponentMetricId.CURRENT_PHASE_2: lambda msg: msg.current_per_phase[1],
    ComponentMetricId.CURRENT_PHASE_3: lambda msg: msg.current_per_phase[2],
    ComponentMetricId.VOLTAGE_PHASE_1: lambda msg: msg.voltage_per_phase[0],
    ComponentMetricId.VOLTAGE_PHASE_2: lambda msg: msg.voltage_per_phase[1],
    ComponentMetricId.VOLTAGE_PHASE_3: lambda msg: msg.voltage_per_phase[2],
}

_BatteryDataMethods: Dict[ComponentMetricId, Callable[[BatteryData], float]] = {
    ComponentMetricId.SOC: lambda msg: msg.soc,
    ComponentMetricId.SOC_LOWER_BOUND: lambda msg: msg.soc_lower_bound,
    ComponentMetricId.SOC_UPPER_BOUND: lambda msg: msg.soc_upper_bound,
    ComponentMetricId.CAPACITY: lambda msg: msg.capacity,
    ComponentMetricId.POWER_LOWER_BOUND: lambda msg: msg.power_lower_bound,
    ComponentMetricId.POWER_UPPER_BOUND: lambda msg: msg.power_upper_bound,
}

_InverterDataMethods: Dict[ComponentMetricId, Callable[[InverterData], float]] = {
    ComponentMetricId.ACTIVE_POWER: lambda msg: msg.active_power,
    ComponentMetricId.ACTIVE_POWER_LOWER_BOUND: lambda msg: msg.active_power_lower_bound,
    ComponentMetricId.ACTIVE_POWER_UPPER_BOUND: lambda msg: msg.active_power_upper_bound,
}

_EVChargerDataMethods: Dict[ComponentMetricId, Callable[[EVChargerData], float]] = {
    ComponentMetricId.ACTIVE_POWER: lambda msg: msg.active_power,
    ComponentMetricId.CURRENT_PHASE_1: lambda msg: msg.current_per_phase[0],
    ComponentMetricId.CURRENT_PHASE_2: lambda msg: msg.current_per_phase[1],
    ComponentMetricId.CURRENT_PHASE_3: lambda msg: msg.current_per_phase[2],
    ComponentMetricId.VOLTAGE_PHASE_1: lambda msg: msg.voltage_per_phase[0],
    ComponentMetricId.VOLTAGE_PHASE_2: lambda msg: msg.voltage_per_phase[1],
    ComponentMetricId.VOLTAGE_PHASE_3: lambda msg: msg.voltage_per_phase[2],
}


class MicrogridApiSource:
    """Fetches requested metrics from the Microgrid API.

    Used by the DataSourcingActor.
    """

    def __init__(
        self,
        registry: ChannelRegistry,
    ) -> None:
        """Create a `MicrogridApiSource` instance.

        Args:
            registry: A channel registry.  To be replaced by a singleton
                instance.
        """
        self._comp_categories_cache: Dict[int, ComponentCategory] = {}
        self.comp_data_receivers: Dict[int, Receiver[Any]] = {}
        self.comp_data_tasks: Dict[int, asyncio.Task[None]] = {}
        self._registry = registry
        self._req_streaming_metrics: Dict[
            int, Dict[ComponentMetricId, List[ComponentMetricRequest]]
        ] = {}

    async def _get_component_category(
        self, comp_id: int
    ) -> Optional[ComponentCategory]:
        """Get the component category of the given component.

        Args:
            comp_id: Id of the requested component.

        Returns:
            The category of the given component, if it is a valid component, or None
                otherwise.
        """
        if comp_id in self._comp_categories_cache:
            return self._comp_categories_cache[comp_id]

        api = microgrid.get().api_client
        for comp in await api.components():
            self._comp_categories_cache[comp.component_id] = comp.category

        if comp_id in self._comp_categories_cache:
            return self._comp_categories_cache[comp_id]

        return None

    async def _check_battery_request(
        self,
        comp_id: int,
        requests: Dict[ComponentMetricId, List[ComponentMetricRequest]],
    ) -> None:
        """Check if the requests are valid Battery metrics.

        Raises:
            ValueError: if the requested metric is not available for batteries.

        Args:
            comp_id: The id of the requested component.
            requests: A list of metric requests received from external actors
                for the given battery.
        """
        for metric in requests:
            if metric not in _BatteryDataMethods:
                raise ValueError(f"Unknown metric {metric} for Battery id {comp_id}")
        if comp_id not in self.comp_data_receivers:
            self.comp_data_receivers[
                comp_id
            ] = await microgrid.get().api_client.battery_data(comp_id)

    async def _check_ev_charger_request(
        self,
        comp_id: int,
        requests: Dict[ComponentMetricId, List[ComponentMetricRequest]],
    ) -> None:
        """Check if the requests are valid EV Charger metrics.

        Raises:
            ValueError: if the requested metric is not available for ev charger.

        Args:
            comp_id: The id of the requested component.
            requests: A list of metric requests received from external actors
                for the given EV Charger.
        """
        for metric in requests:
            if metric not in _EVChargerDataMethods:
                raise ValueError(f"Unknown metric {metric} for EvCharger id {comp_id}")
        if comp_id not in self.comp_data_receivers:
            self.comp_data_receivers[
                comp_id
            ] = await microgrid.get().api_client.ev_charger_data(comp_id)

    async def _check_inverter_request(
        self,
        comp_id: int,
        requests: Dict[ComponentMetricId, List[ComponentMetricRequest]],
    ) -> None:
        """Check if the requests are valid Inverter metrics.

        Raises:
            ValueError: if the requested metric is not available for inverters.

        Args:
            comp_id: The id of the requested component.
            requests: A list of metric requests received from external actors
                for the given inverter.
        """
        for metric in requests:
            if metric not in _InverterDataMethods:
                raise ValueError(f"Unknown metric {metric} for Inverter id {comp_id}")
        if comp_id not in self.comp_data_receivers:
            self.comp_data_receivers[
                comp_id
            ] = await microgrid.get().api_client.inverter_data(comp_id)

    async def _check_meter_request(
        self,
        comp_id: int,
        requests: Dict[ComponentMetricId, List[ComponentMetricRequest]],
    ) -> None:
        """Check if the requests are valid Meter metrics.

        Raises:
            ValueError: if the requested metric is not available for meters.

        Args:
            comp_id: The id of the requested component.
            requests: A list of metric requests received from external actors
                for the given meter.
        """
        for metric in requests:
            if metric not in _MeterDataMethods:
                raise ValueError(f"Unknown metric {metric} for Meter id {comp_id}")
        if comp_id not in self.comp_data_receivers:
            self.comp_data_receivers[
                comp_id
            ] = await microgrid.get().api_client.meter_data(comp_id)

    async def _check_requested_component_and_metrics(
        self,
        comp_id: int,
        category: ComponentCategory,
        requests: Dict[ComponentMetricId, List[ComponentMetricRequest]],
    ) -> None:
        """Check if the requested component and metrics are valid.

        Raises:
            ValueError: if the category is unknown or if the requested metric
                is unavailable to the given category.

        Args:
            comp_id: The id of the requested component.
            category: The category of the requested component.
            requests: A list of metric requests received from external actors
                for the given component.
        """
        if comp_id in self.comp_data_receivers:
            return

        if category == ComponentCategory.BATTERY:
            await self._check_battery_request(comp_id, requests)
        elif category == ComponentCategory.EV_CHARGER:
            await self._check_ev_charger_request(comp_id, requests)
        elif category == ComponentCategory.INVERTER:
            await self._check_inverter_request(comp_id, requests)
        elif category == ComponentCategory.METER:
            await self._check_meter_request(comp_id, requests)
        else:
            raise ValueError(f"Unknown component category {category}")

    def _get_data_extraction_method(
        self, category: ComponentCategory, metric: ComponentMetricId
    ) -> Callable[[Any], float]:
        """Get the data extraction method for the given metric.

        Raises:
            ValueError: if the category is unknown.

        Args:
            category: The category of the component.
            metric: The metric for which we need an extraction method.

        Returns:
            A method that accepts a `ComponentData` object and returns a float
                representing the given metric.
        """
        if category == ComponentCategory.BATTERY:
            return _BatteryDataMethods[metric]
        if category == ComponentCategory.INVERTER:
            return _InverterDataMethods[metric]
        if category == ComponentCategory.METER:
            return _MeterDataMethods[metric]
        if category == ComponentCategory.EV_CHARGER:
            return _EVChargerDataMethods[metric]
        raise ValueError(f"Unknown component category {category}")

    def _get_metric_senders(
        self,
        category: ComponentCategory,
        requests: Dict[ComponentMetricId, List[ComponentMetricRequest]],
    ) -> List[Tuple[Callable[[Any], float], List[Sender[Sample]]]]:
        """Get channel senders from the channel registry for each requested metric.

        Args:
            category: The category of the component.
            requests: A list of metric requests received from external actors for a
                certain component.

        Returns:
            A dictionary of output metric names to channel senders from the channel
                registry.
        """
        return [
            (
                self._get_data_extraction_method(category, metric),
                [
                    self._registry.new_sender(request.get_channel_name())
                    for request in reqlist
                ],
            )
            for (metric, reqlist) in requests.items()
        ]

    async def _handle_data_stream(
        self,
        comp_id: int,
        category: ComponentCategory,
    ) -> None:
        """Stream component data and send the requested metrics out.

        Args:
            comp_id: Id of the requested component.
            category: The category of the component.
        """
        stream_senders = []
        if comp_id in self._req_streaming_metrics:
            await self._check_requested_component_and_metrics(
                comp_id, category, self._req_streaming_metrics[comp_id]
            )
            stream_senders = self._get_metric_senders(
                category, self._req_streaming_metrics[comp_id]
            )
        api_data_receiver = self.comp_data_receivers[comp_id]

        def process_msg(data: Any) -> None:
            tasks = []
            for (extractor, senders) in stream_senders:
                for sender in senders:
                    tasks.append(sender.send(Sample(data.timestamp, extractor(data))))
            asyncio.gather(*tasks)

        async for data in api_data_receiver:
            process_msg(data)

    async def _update_streams(
        self,
        comp_id: int,
        category: ComponentCategory,
    ) -> None:
        """Update the requested metric streams for the given component.

        Args:
            comp_id: Id of the requested component.
            category: Category of the requested component.
        """
        if comp_id in self.comp_data_tasks:
            self.comp_data_tasks[comp_id].cancel()

        self.comp_data_tasks[comp_id] = asyncio.create_task(
            self._handle_data_stream(comp_id, category)
        )

    async def add_metric(self, request: ComponentMetricRequest) -> None:
        """Add a metric to be streamed from the microgrid API.

        Args:
            request: A request object for a metric, received from a downstream
                actor.
        """
        comp_id = request.component_id

        category = await self._get_component_category(comp_id)

        if category is None:
            logging.error("Unknown component ID: %d in request %s", comp_id, request)
            return

        self._req_streaming_metrics.setdefault(comp_id, {}).setdefault(
            request.metric_id, []
        )

        for existing_request in self._req_streaming_metrics[comp_id][request.metric_id]:
            if existing_request.get_channel_name() == request.get_channel_name():
                # the requested metric is already being handled, so nothing to do.
                return

        self._req_streaming_metrics[comp_id][request.metric_id].append(request)

        await self._update_streams(
            comp_id,
            category,
        )
