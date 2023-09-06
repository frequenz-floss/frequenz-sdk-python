# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tool for mocking streams of component data."""


import asyncio
from dataclasses import replace
from datetime import datetime, timezone

from frequenz.sdk._internal._asyncio import cancel_and_await
from frequenz.sdk.microgrid.component import ComponentData

from .mock_microgrid_client import MockMicrogridClient


class MockComponentDataStreamer:
    """Mock streams of component data.

    This tool was create to:
    * specify what data should be send with the stream.
    * specify different sampling for each component data.
    * stop and start any stream any time.
    * modify the data that comes with the stream in runtime.
    """

    def __init__(self, mock_microgrid: MockMicrogridClient) -> None:
        """Create class instance.

        Args:
            mock_microgrid: Mock microgrid.
        """
        self._mock_microgrid = mock_microgrid
        self._component_data: dict[int, ComponentData] = {}
        self._streaming_tasks: dict[int, asyncio.Task[None]] = {}

    def start_streaming(
        self, component_data: ComponentData, sampling_rate: float
    ) -> None:
        """Start streaming this component data with given sampling rate.

        Args:
            component_data: component data to be streamed.
            sampling_rate: sampling rate

        Raises:
            RuntimeError: If component is already streaming data.
        """
        component_id = component_data.component_id
        if component_id in self._streaming_tasks:
            raise RuntimeError("Component is already streaming")

        self._component_data[component_id] = component_data
        self._streaming_tasks[component_id] = asyncio.create_task(
            self._stream_data(component_id, sampling_rate)
        )

    def get_current_component_data(self, component_id: int) -> ComponentData:
        """Get component data that are currently streamed or was streaming recently.

        Args:
            component_id: component id

        Raises:
            KeyError: If component never sent any data.

        Returns:
            Component data.
        """
        if component_id not in self._component_data:
            raise KeyError(f"Component {component_id} was never streaming data.")

        return self._component_data[component_id]

    def update_stream(self, new_component_data: ComponentData) -> None:
        """Update component stream to send new data.

        Component id is taken from the given component data/

        Args:
            new_component_data: new component data

        Raises:
            KeyError: If this component is not sending data.
        """
        cid = new_component_data.component_id
        if cid not in self._streaming_tasks:
            raise KeyError(f"Component {cid} is not streaming data")

        self._component_data[cid] = new_component_data

    async def stop_streaming(self, component_id: int) -> None:
        """Stop sending data from this component."""
        if task := self._streaming_tasks.pop(component_id, None):
            await cancel_and_await(task)

    async def stop(self) -> None:
        """Stop sending any data. This will close any pending async tasks."""
        await asyncio.gather(
            *[self.stop_streaming(cid) for cid in self._streaming_tasks]
        )

    async def _stream_data(self, component_id: int, sampling_rate: float) -> None:
        while component_id in self._component_data:
            data = self._component_data[component_id]
            new_data = replace(data, timestamp=datetime.now(tz=timezone.utc))
            await self._mock_microgrid.send(new_data)
            await asyncio.sleep(sampling_rate)
