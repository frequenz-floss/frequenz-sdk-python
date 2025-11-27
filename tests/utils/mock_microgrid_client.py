# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Mock microgrid definition."""

from collections.abc import Iterable
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock

from frequenz.channels import Broadcast, Receiver, Sender
from frequenz.client.common.microgrid import MicrogridId
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid import Location, MicrogridApiClient
from frequenz.client.microgrid.component import (
    Component,
    ComponentConnection,
    ComponentDataSamples,
)
from frequenz.client.microgrid.metrics import Metric
from pytest_mock import MockerFixture

from frequenz.sdk.microgrid.component_graph import (
    ComponentGraph,
    _MicrogridComponentGraph,
)
from frequenz.sdk.microgrid.connection_manager import ConnectionManager


@dataclass(frozen=True)
class ComponentDataReceiverKey:
    """Key for the component data receiver."""

    component_id: ComponentId
    metrics: frozenset[Metric | int]


class MockMicrogridClient:
    """Class that mocks MicrogridClient behavior."""

    def __init__(
        self,
        components: set[Component],
        connections: set[ComponentConnection],
        microgrid_id: MicrogridId = MicrogridId(8),
        location: Location = Location(
            latitude=52.520008, longitude=13.404954, country_code="DE"
        ),
    ):
        """Create mock microgrid with given components and connections.

        This simulates microgrid.
        Every call to `battery_data` and `inverter_data` is mocked to return
        receiver owned by this class.
        User can send data to the receiver using `self.send(..)` method.
        Messages will be send based on component id.

        Args:
            components: List of the microgrid components
            connections: List of the microgrid connections
            microgrid_id: the ID of the microgrid
            location: the location of the microgrid
        """
        self._component_graph = _MicrogridComponentGraph(components, connections)
        self._components = components
        self._connections = connections
        self._component_data_channels: dict[
            ComponentDataReceiverKey, Broadcast[ComponentDataSamples]
        ] = {}
        self._component_data_senders: dict[
            ComponentDataReceiverKey, Sender[ComponentDataSamples]
        ] = {}

        self._mock_microgrid = MagicMock(
            spec=ConnectionManager,
            api_client=self._create_mock_api(),
            component_graph=self._component_graph,
            microgrid_id=microgrid_id,
            location=location,
        )

    def initialize(self, mocker: MockerFixture) -> None:
        """Mock `microgrid.get` call to return this mock_microgrid.

        Args:
            mocker: mocker from the current test
        """
        # Mock _MICROGRID, so `get` method return this mocked microgrid.
        mocker.patch(
            "frequenz.sdk.microgrid.connection_manager._CONNECTION_MANAGER",
            self.mock_microgrid,
        )

    @property
    def mock_microgrid(self) -> ConnectionManager:
        """Return mock microgrid.

        This is needed to patch existing microgrid.get() method.

        Returns:
            Mock microgrid.
        """
        return self._mock_microgrid

    @property
    def component_graph(self) -> ComponentGraph:
        """Return microgrid component graph.

        Component graph is not mocked.

        Returns:
            Mock microgrid.
        """
        return self._component_graph

    async def send(self, data: ComponentDataSamples) -> None:
        """Send component data using channel.

        This simulates component sending data. Right now only battery and inverter
        are supported. More components categories can be added if needed.

        Args:
            data: Data to be sent.
        """
        key = ComponentDataReceiverKey(
            data.component_id, frozenset(s.metric for s in data.metric_samples)
        )
        sender = self._component_data_senders.get(key)

        if sender is None:
            sender = self._get_chan(key).new_sender()
            self._component_data_senders[key] = sender

        await sender.send(data)

    async def close_channels(self, cid: ComponentId) -> None:
        """Close channel for given component id.

        Args:
            cid: Component id
        """
        for key, channel in self._component_data_channels.items():
            if key.component_id == cid:
                await channel.close()

    def _create_mock_api(self) -> MagicMock:
        """Create mock of MicrogridApiClient.

        Returns:
            Magic mock instance of MicrogridApiClient.
        """
        api = MagicMock(spec=MicrogridApiClient)
        api.list_components = AsyncMock(return_value=list(self._components))
        api.list_connections = AsyncMock(return_value=list(self._connections))

        # Replace individual data methods with the new unified stream method
        api.receive_component_data_samples_stream = MagicMock(
            side_effect=self._mock_receiver_component_data_samples_stream
        )

        # Can be overridden in the future
        api.set_component_power_active = AsyncMock(return_value=None)
        return api

    def _mock_receiver_component_data_samples_stream(
        self,
        component: ComponentId | Component,
        metrics: Iterable[Metric | int],
        *,
        buffer_size: int = 50,
    ) -> Receiver[ComponentDataSamples]:
        component_id = component if isinstance(component, ComponentId) else component.id
        if component_id not in map(lambda c: c.id, self._components):
            raise ValueError(f"Unknown {component_id}")

        key = ComponentDataReceiverKey(component_id, frozenset(metrics))
        return self._get_chan(key).new_receiver(limit=buffer_size)

    def _get_chan(
        self, key: ComponentDataReceiverKey
    ) -> Broadcast[ComponentDataSamples]:
        if chan := self._component_data_channels.get(key):
            return chan

        metrics_str = ":".join(
            map(lambda m: m.name if isinstance(m, Metric) else str(m), key.metrics)
        )
        chan = Broadcast[ComponentDataSamples](
            name=f"mock_stream:{key.component_id}:{metrics_str}"
        )
        self._component_data_channels[key] = chan

        return chan
