"""
Ring buffer for storing ordered streamed component data for resampling purposes.

Copyright
Copyright © 2022 Frequenz Energy-as-a-Service GmbH

License
MIT
"""
import datetime
import logging
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, ItemsView, Set

from frequenz.sdk.microgrid import Component

logger = logging.Logger(__name__)

MAX_BUFFER_SIZE = 1000


@dataclass
class DataPoint:
    """DataPoint class to be replaced by ComponentData dataclass."""

    component_id: int
    timestamp: datetime.datetime


class RingBuffer:
    """Ring buffer for storing data points with component data."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        size: int,
        components: Set[Component],
    ) -> None:
        """Initialize RingBuffer.

        Args:
            size: max number of data points to be stored per each component
            components: set of components in the microgrid

        Raises:
            ValueError: if provided `buffer_size` is smaller than 0 or greater than
                `MAX_BUFFER_SIZE`
        """
        if size < 0 or size > MAX_BUFFER_SIZE:
            raise ValueError(
                f"Buffer size must be an integer in the (0, {MAX_BUFFER_SIZE}) range."
            )

        self._size = size
        self._buffer: Dict[int, Deque[DataPoint]] = {
            component.component_id: deque(maxlen=size) for component in components
        }

    @staticmethod
    def _find_new_data_point_index(
        data_points: Deque[DataPoint], new_data_point: DataPoint
    ) -> int:
        """Find the appropriate index in the ring buffer to insert a new data point.

        To preserve order in the ring buffer, new data points have to inserted into
            the right position.

        Args:
            data_points: list of existing data points for a single component
            new_data_point: new data point for a single component

        Returns:
            Index where the new data point should be inserted.
        """
        for index, data_point in enumerate(data_points):
            if data_point.timestamp > new_data_point.timestamp:
                return index
        return len(data_points)

    def items(self) -> ItemsView[int, Deque[DataPoint]]:
        """Get all the items in the ring buffer.

        Returns:
            All the items in the ring buffer.
        """
        return self._buffer.items()

    def insert(self, component_data: DataPoint) -> None:
        """Insert the latest data from a single component into the ring buffer.

        The order of data points in the ring buffer per component is preserved after
            inserting the new data point.

        Args:
            component_data: microgrid client

        Raises:
            ValueError: if provided `component_data` is missing component ID
        """
        component_id = component_data.component_id
        if component_id is None:
            raise ValueError(
                f"Component ID missing in component data: {component_data}"
            )

        if len(self._buffer[component_id]) >= self._size:
            self._buffer[component_id].popleft()

        if len(self._buffer[component_id]) == 0:
            self._buffer[component_id].append(component_data)
        else:
            latest_data_point = self._buffer[component_id][-1]

            if component_data.timestamp >= latest_data_point.timestamp:
                self._buffer[component_id].append(component_data)
            else:
                index = RingBuffer._find_new_data_point_index(
                    data_points=self._buffer[component_id],
                    new_data_point=component_data,
                )
                self._buffer[component_id].insert(index, component_data)
