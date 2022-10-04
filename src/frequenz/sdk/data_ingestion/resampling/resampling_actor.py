"""
Actor for combining and resampling stream data from different components.

Copyright
Copyright © 2022 Frequenz Energy-as-a-Service GmbH

License
MIT
"""
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Set

from frequenz.channels import Merge, Receiver, Select, Sender, Timer

from frequenz.sdk.actor.decorator import actor
from frequenz.sdk.data_ingestion.resampling.resampler import Resampler
from frequenz.sdk.data_ingestion.resampling.ring_buffer import DataPoint, RingBuffer
from frequenz.sdk.microgrid import Component, ComponentCategory, InverterData
from frequenz.sdk.microgrid.client import MicrogridApiClient

logger = logging.Logger(__name__)

MAX_BUFFER_SIZE = 1000


async def generate_component_data_receivers(
    microgrid_client: MicrogridApiClient,
) -> Dict[ComponentCategory, Receiver[Dict[str, Any]]]:
    """Generate receivers for all components grouped by component category.

    Group all components by `ComponentCategory` and create a receiver for each group
        to collect data from the components belonging to that group.

    Args:
        microgrid_client: microgrid client

    Returns:
        Receiver of the data per each component category.

    Raises:
        ValueError: if category of any component in the microgrid is not supported
    """
    grouped_components: Dict[ComponentCategory, Set[Component]] = {}
    microgrid_components = await microgrid_client.components()
    for component in microgrid_components:
        if component.category not in grouped_components:
            grouped_components[component.category] = set()
        grouped_components[component.category].add(component)

    receiver_groups: Dict[ComponentCategory, Receiver[Dict[str, Any]]] = {}

    for category, components in grouped_components.items():
        receivers: List[Any] = []
        for component in components:
            component_id = component.component_id
            if category == ComponentCategory.BATTERY:
                battery_receiver = await microgrid_client.battery_data(component_id)
                receivers.append(battery_receiver)
            elif category == ComponentCategory.INVERTER:
                inverter_receiver: Receiver[
                    InverterData
                ] = await microgrid_client.inverter_data(component_id)
                receivers.append(inverter_receiver)
            elif category == ComponentCategory.EV_CHARGER:
                ev_charger_receiver = await microgrid_client.ev_charger_data(
                    component_id
                )
                receivers.append(ev_charger_receiver)
            elif category in {
                ComponentCategory.METER,
                ComponentCategory.CHP,
                ComponentCategory.PV_ARRAY,
            }:
                meter_receiver = await microgrid_client.meter_data(component_id)
                receivers.append(meter_receiver)
            else:
                raise ValueError(f"Unsupported component category: {category}")

        receiver_groups[category] = Merge(*receivers)

    return receiver_groups


@actor
class ResamplingActor:
    """Actor for receiving and resampling component data."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        microgrid_client: MicrogridApiClient,
        components: Set[Component],
        resampling_sink: Sender[DataPoint],
        resampling_frequency: float,
        resampler: Resampler,
        buffer_size: int,
        max_component_data_delay: float = 5.0,
    ) -> None:
        """Initialize ResamplingActor actor.

        Args:
            microgrid_client: microgrid client
            components: set of components in the microgrid
            resampling_sink: sink to be sent the samples
            resampling_frequency: what frequency should the original data points be
                converted to
            resampler: class that implements the `Resampler` interface
            buffer_size: max number of data points to be stored per each component
            max_component_data_delay: data points with a bigger delay shouldn't be
                used to extrapolate values and NaN should be returned instead

        Raises:
            ValueError: if provided `buffer_size` is smaller than 0 or greater than
                `MAX_BUFFER_SIZE`
        """
        self.microgrid_client = microgrid_client
        self.resampling_sink = resampling_sink
        self.resampler = resampler

        if buffer_size < 0 or buffer_size > MAX_BUFFER_SIZE:
            raise ValueError(
                f"Buffer size must be an integer in the (0, {MAX_BUFFER_SIZE}) range."
            )

        self.ring_buffer = RingBuffer(size=buffer_size, components=components)
        self.component_category_mapping: Dict[int, ComponentCategory] = {
            component.component_id: component.category for component in components
        }
        self.resampling_frequency = resampling_frequency
        self.max_component_data_delay = max_component_data_delay

    def resample(self) -> List[DataPoint]:
        """Resample the component data in the ring buffer.

        Returns:
            Resampled data points.

        Raises:
            ValueError: if category of any component in the microgrid is not supported
        """
        samples: List[DataPoint] = []
        now = datetime.utcnow()
        now = now.replace(tzinfo=timezone.utc)
        resampling_window_start = now - timedelta(seconds=self.resampling_frequency)

        for component_id, deque_items in self.ring_buffer.items():
            component_category = self.component_category_mapping.get(component_id)
            if component_category is None:
                raise ValueError(
                    f"Unsupported component category encountered "
                    f"for component: {component_id}"
                )

            self.resampler.resample_component_data(
                component=Component(component_id, component_category),
                data_points=list(deque_items),
                resampling_window_start=resampling_window_start,
                resampling_frequency=self.resampling_frequency,
                max_component_data_delay=self.max_component_data_delay,
            )

        return samples

    async def run(self) -> None:
        """Run the actor."""
        while True:
            receivers = await generate_component_data_receivers(
                microgrid_client=self.microgrid_client
            )

            select = Select(
                component_data_receiver=Merge(*receivers.values()),
                resample_timer=Timer(self.resampling_frequency),
            )
            while await select.ready():
                if msg := select.component_data_receiver:
                    self.ring_buffer.insert(component_data=msg.inner)
                elif _ := select.resample_timer:
                    samples = self.resample()
                    for sample in samples:
                        await self.resampling_sink.send(sample)
