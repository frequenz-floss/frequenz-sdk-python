# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""The Microgrid API data source for the DataSourcingActor."""

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from frequenz.channels import Receiver, Sender
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.component import ComponentCategory
from frequenz.client.microgrid.metrics import Metric
from frequenz.quantities import Quantity

from ..._internal._asyncio import run_forever
from ..._internal._channels import ChannelRegistry
from ...microgrid import connection_manager
from ...timeseries import Sample
from .._old_component_data import (
    BatteryData,
    EVChargerData,
    InverterData,
    MeterData,
    TransitionalMetric,
)
from ._component_metric_request import ComponentMetricRequest

_logger = logging.getLogger(__name__)

_METER_DATA_METHODS: dict[Metric | TransitionalMetric, Callable[[MeterData], float]] = {
    Metric.AC_ACTIVE_POWER: lambda msg: msg.active_power,
    Metric.AC_ACTIVE_POWER_PHASE_1: lambda msg: msg.active_power_per_phase[0],
    Metric.AC_ACTIVE_POWER_PHASE_2: lambda msg: msg.active_power_per_phase[1],
    Metric.AC_ACTIVE_POWER_PHASE_3: lambda msg: msg.active_power_per_phase[2],
    Metric.AC_CURRENT_PHASE_1: lambda msg: msg.current_per_phase[0],
    Metric.AC_CURRENT_PHASE_2: lambda msg: msg.current_per_phase[1],
    Metric.AC_CURRENT_PHASE_3: lambda msg: msg.current_per_phase[2],
    Metric.AC_VOLTAGE_PHASE_1_N: lambda msg: msg.voltage_per_phase[0],
    Metric.AC_VOLTAGE_PHASE_2_N: lambda msg: msg.voltage_per_phase[1],
    Metric.AC_VOLTAGE_PHASE_3_N: lambda msg: msg.voltage_per_phase[2],
    Metric.AC_FREQUENCY: lambda msg: msg.frequency,
    Metric.AC_REACTIVE_POWER: lambda msg: msg.reactive_power,
    Metric.AC_REACTIVE_POWER_PHASE_1: lambda msg: msg.reactive_power_per_phase[0],
    Metric.AC_REACTIVE_POWER_PHASE_2: lambda msg: msg.reactive_power_per_phase[1],
    Metric.AC_REACTIVE_POWER_PHASE_3: lambda msg: msg.reactive_power_per_phase[2],
}

_BATTERY_DATA_METHODS: dict[
    Metric | TransitionalMetric, Callable[[BatteryData], float]
] = {
    Metric.BATTERY_SOC_PCT: lambda msg: msg.soc,
    TransitionalMetric.SOC_LOWER_BOUND: lambda msg: msg.soc_lower_bound,
    TransitionalMetric.SOC_UPPER_BOUND: lambda msg: msg.soc_upper_bound,
    Metric.BATTERY_CAPACITY: lambda msg: msg.capacity,
    TransitionalMetric.POWER_INCLUSION_LOWER_BOUND: lambda msg: (
        msg.power_inclusion_lower_bound
    ),
    TransitionalMetric.POWER_EXCLUSION_LOWER_BOUND: lambda msg: (
        msg.power_exclusion_lower_bound
    ),
    TransitionalMetric.POWER_EXCLUSION_UPPER_BOUND: lambda msg: (
        msg.power_exclusion_upper_bound
    ),
    TransitionalMetric.POWER_INCLUSION_UPPER_BOUND: lambda msg: (
        msg.power_inclusion_upper_bound
    ),
    Metric.BATTERY_TEMPERATURE: lambda msg: msg.temperature,
}

_INVERTER_DATA_METHODS: dict[
    Metric | TransitionalMetric, Callable[[InverterData], float]
] = {
    Metric.AC_ACTIVE_POWER: lambda msg: msg.active_power,
    Metric.AC_ACTIVE_POWER_PHASE_1: lambda msg: msg.active_power_per_phase[0],
    Metric.AC_ACTIVE_POWER_PHASE_2: lambda msg: msg.active_power_per_phase[1],
    Metric.AC_ACTIVE_POWER_PHASE_3: lambda msg: msg.active_power_per_phase[2],
    TransitionalMetric.ACTIVE_POWER_INCLUSION_LOWER_BOUND: lambda msg: (
        msg.active_power_inclusion_lower_bound
    ),
    TransitionalMetric.ACTIVE_POWER_EXCLUSION_LOWER_BOUND: lambda msg: (
        msg.active_power_exclusion_lower_bound
    ),
    TransitionalMetric.ACTIVE_POWER_EXCLUSION_UPPER_BOUND: lambda msg: (
        msg.active_power_exclusion_upper_bound
    ),
    TransitionalMetric.ACTIVE_POWER_INCLUSION_UPPER_BOUND: lambda msg: (
        msg.active_power_inclusion_upper_bound
    ),
    Metric.AC_CURRENT_PHASE_1: lambda msg: msg.current_per_phase[0],
    Metric.AC_CURRENT_PHASE_2: lambda msg: msg.current_per_phase[1],
    Metric.AC_CURRENT_PHASE_3: lambda msg: msg.current_per_phase[2],
    Metric.AC_VOLTAGE_PHASE_1_N: lambda msg: msg.voltage_per_phase[0],
    Metric.AC_VOLTAGE_PHASE_2_N: lambda msg: msg.voltage_per_phase[1],
    Metric.AC_VOLTAGE_PHASE_3_N: lambda msg: msg.voltage_per_phase[2],
    Metric.AC_FREQUENCY: lambda msg: msg.frequency,
    Metric.AC_REACTIVE_POWER: lambda msg: msg.reactive_power,
    Metric.AC_REACTIVE_POWER_PHASE_1: lambda msg: msg.reactive_power_per_phase[0],
    Metric.AC_REACTIVE_POWER_PHASE_2: lambda msg: msg.reactive_power_per_phase[1],
    Metric.AC_REACTIVE_POWER_PHASE_3: lambda msg: msg.reactive_power_per_phase[2],
}

_EV_CHARGER_DATA_METHODS: dict[
    Metric | TransitionalMetric, Callable[[EVChargerData], float]
] = {
    Metric.AC_ACTIVE_POWER: lambda msg: msg.active_power,
    Metric.AC_ACTIVE_POWER_PHASE_1: lambda msg: msg.active_power_per_phase[0],
    Metric.AC_ACTIVE_POWER_PHASE_2: lambda msg: msg.active_power_per_phase[1],
    Metric.AC_ACTIVE_POWER_PHASE_3: lambda msg: msg.active_power_per_phase[2],
    Metric.AC_CURRENT_PHASE_1: lambda msg: msg.current_per_phase[0],
    Metric.AC_CURRENT_PHASE_2: lambda msg: msg.current_per_phase[1],
    Metric.AC_CURRENT_PHASE_3: lambda msg: msg.current_per_phase[2],
    Metric.AC_VOLTAGE_PHASE_1_N: lambda msg: msg.voltage_per_phase[0],
    Metric.AC_VOLTAGE_PHASE_2_N: lambda msg: msg.voltage_per_phase[1],
    Metric.AC_VOLTAGE_PHASE_3_N: lambda msg: msg.voltage_per_phase[2],
    Metric.AC_FREQUENCY: lambda msg: msg.frequency,
    Metric.AC_REACTIVE_POWER: lambda msg: msg.reactive_power,
    Metric.AC_REACTIVE_POWER_PHASE_1: lambda msg: msg.reactive_power_per_phase[0],
    Metric.AC_REACTIVE_POWER_PHASE_2: lambda msg: msg.reactive_power_per_phase[1],
    Metric.AC_REACTIVE_POWER_PHASE_3: lambda msg: msg.reactive_power_per_phase[2],
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
        self._comp_categories_cache: dict[ComponentId, ComponentCategory | int] = {}

        self.comp_data_receivers: dict[ComponentId, Receiver[Any]] = {}
        """The dictionary of component IDs to data receivers."""

        self.comp_data_tasks: dict[ComponentId, asyncio.Task[None]] = {}
        """The dictionary of component IDs to asyncio tasks."""

        self._registry = registry
        self._req_streaming_metrics: dict[
            ComponentId, dict[Metric | TransitionalMetric, list[ComponentMetricRequest]]
        ] = {}

    async def _get_component_category(
        self, comp_id: ComponentId
    ) -> ComponentCategory | int | None:
        """Get the component category of the given component.

        Args:
            comp_id: Id of the requested component.

        Returns:
            The category of the given component, if it is a valid component, or None
                otherwise.
        """
        if comp_id in self._comp_categories_cache:
            return self._comp_categories_cache[comp_id]

        api = connection_manager.get().api_client
        for comp in await api.list_components():
            self._comp_categories_cache[comp.id] = comp.category

        if comp_id in self._comp_categories_cache:
            return self._comp_categories_cache[comp_id]

        return None

    async def _check_battery_request(
        self,
        comp_id: ComponentId,
        requests: dict[Metric | TransitionalMetric, list[ComponentMetricRequest]],
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
            if metric not in _BATTERY_DATA_METHODS:
                err = f"Unknown metric {metric} for Battery id {comp_id}"
                _logger.error(err)
                raise ValueError(err)
        if comp_id not in self.comp_data_receivers:
            self.comp_data_receivers[comp_id] = BatteryData.subscribe(
                connection_manager.get().api_client, comp_id
            )

    async def _check_ev_charger_request(
        self,
        comp_id: ComponentId,
        requests: dict[Metric | TransitionalMetric, list[ComponentMetricRequest]],
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
            if metric not in _EV_CHARGER_DATA_METHODS:
                err = f"Unknown metric {metric} for EvCharger id {comp_id}"
                _logger.error(err)
                raise ValueError(err)
        if comp_id not in self.comp_data_receivers:
            self.comp_data_receivers[comp_id] = EVChargerData.subscribe(
                connection_manager.get().api_client, comp_id
            )

    async def _check_inverter_request(
        self,
        comp_id: ComponentId,
        requests: dict[Metric | TransitionalMetric, list[ComponentMetricRequest]],
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
            if metric not in _INVERTER_DATA_METHODS:
                err = f"Unknown metric {metric} for Inverter id {comp_id}"
                _logger.error(err)
                raise ValueError(err)
        if comp_id not in self.comp_data_receivers:
            self.comp_data_receivers[comp_id] = InverterData.subscribe(
                connection_manager.get().api_client, comp_id
            )

    async def _check_meter_request(
        self,
        comp_id: ComponentId,
        requests: dict[Metric | TransitionalMetric, list[ComponentMetricRequest]],
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
            if metric not in _METER_DATA_METHODS:
                err = f"Unknown metric {metric} for Meter id {comp_id}"
                _logger.error(err)
                raise ValueError(err)
        if comp_id not in self.comp_data_receivers:
            self.comp_data_receivers[comp_id] = MeterData.subscribe(
                connection_manager.get().api_client, comp_id
            )

    async def _check_requested_component_and_metrics(
        self,
        comp_id: ComponentId,
        category: ComponentCategory | int,
        requests: dict[Metric | TransitionalMetric, list[ComponentMetricRequest]],
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
            err = f"Unknown component category {category}"
            _logger.error(err)
            raise ValueError(err)

    def _get_data_extraction_method(
        self, category: ComponentCategory | int, metric: Metric | TransitionalMetric
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
            return _BATTERY_DATA_METHODS[metric]
        if category == ComponentCategory.INVERTER:
            return _INVERTER_DATA_METHODS[metric]
        if category == ComponentCategory.METER:
            return _METER_DATA_METHODS[metric]
        if category == ComponentCategory.EV_CHARGER:
            return _EV_CHARGER_DATA_METHODS[metric]
        err = f"Unknown component category {category}"
        _logger.error(err)
        raise ValueError(err)

    def _get_metric_senders(
        self,
        category: ComponentCategory | int,
        requests: dict[Metric | TransitionalMetric, list[ComponentMetricRequest]],
    ) -> list[tuple[Callable[[Any], float], list[Sender[Sample[Quantity]]]]]:
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
                    self._registry.get_or_create(
                        Sample[Quantity], request.get_channel_name()
                    ).new_sender()
                    for request in req_list
                ],
            )
            for (metric, req_list) in requests.items()
        ]

    async def _handle_data_stream(
        self,
        comp_id: ComponentId,
        category: ComponentCategory | int,
    ) -> None:
        """Stream component data and send the requested metrics out.

        Args:
            comp_id: Id of the requested component.
            category: The category of the component.

        Raises:
            Exception: if an error occurs while handling the data stream.
        """
        try:
            stream_senders = []
            if comp_id in self._req_streaming_metrics:
                await self._check_requested_component_and_metrics(
                    comp_id, category, self._req_streaming_metrics[comp_id]
                )
                stream_senders = self._get_metric_senders(
                    category, self._req_streaming_metrics[comp_id]
                )
            api_data_receiver: Receiver[Any] = self.comp_data_receivers[comp_id]

            async def process_msg(data: Any) -> None:
                async with asyncio.TaskGroup() as tg:
                    for extractor, senders in stream_senders:
                        for sender in senders:
                            sample = Sample(data.timestamp, Quantity(extractor(data)))
                            name = f"send:ts={sample.timestamp}:cid={comp_id}"
                            tg.create_task(sender.send(sample), name=name)

            sending_tasks: set[asyncio.Task[None]] = set()

            async def clean_tasks(
                sending_tasks: set[asyncio.Task[None]],
            ) -> set[asyncio.Task[None]]:
                done, pending = await asyncio.wait(sending_tasks, timeout=0)
                for task in done:
                    try:
                        task.result()
                    # pylint: disable-next=broad-except
                    except (asyncio.CancelledError, Exception):
                        _logger.exception(
                            "Error while processing message in task %s",
                            task.get_name(),
                        )
                return pending

            async for data in api_data_receiver:
                name = f"process_msg:cid={comp_id}"
                sending_tasks.add(asyncio.create_task(process_msg(data), name=name))
                sending_tasks = await clean_tasks(sending_tasks)

            await asyncio.gather(*sending_tasks)
            await asyncio.gather(
                *[
                    self._registry.close_and_remove(r.get_channel_name())
                    for requests in self._req_streaming_metrics[comp_id].values()
                    for r in requests
                ]
            )
        except Exception:
            _logger.exception(
                "Unexpected error while handling data stream for component %d (%s), "
                "component data is not being streamed anymore",
                comp_id,
                category,
            )
            raise

    async def _update_streams(
        self,
        comp_id: ComponentId,
        category: ComponentCategory | int,
    ) -> None:
        """Update the requested metric streams for the given component.

        Args:
            comp_id: Id of the requested component.
            category: Category of the requested component.
        """
        if comp_id in self.comp_data_tasks:
            self.comp_data_tasks[comp_id].cancel()

        self.comp_data_tasks[comp_id] = asyncio.create_task(
            run_forever(lambda: self._handle_data_stream(comp_id, category)),
            name=f"{type(self).__name__}._update_stream({comp_id=}, {category})",
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
            _logger.error("Unknown component ID: %d in request %s", comp_id, request)
            return

        self._req_streaming_metrics.setdefault(comp_id, {}).setdefault(
            request.metric, []
        )

        for existing_request in self._req_streaming_metrics[comp_id][request.metric]:
            if existing_request.get_channel_name() == request.get_channel_name():
                # the requested metric is already being handled, so nothing to do.
                return

        self._req_streaming_metrics[comp_id][request.metric].append(request)

        await self._update_streams(
            comp_id,
            category,
        )
