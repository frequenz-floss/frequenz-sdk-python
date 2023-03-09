# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Mock microgrid definition."""
from functools import partial
from typing import Any, Dict, Set
from unittest.mock import AsyncMock, MagicMock

from frequenz.channels import Broadcast, Receiver
from google.protobuf.empty_pb2 import Empty  # pylint: disable=no-name-in-module
from pytest_mock import MockerFixture

from frequenz.sdk._internal._constants import RECEIVER_MAX_SIZE
from frequenz.sdk.microgrid import ConnectionManager
from frequenz.sdk.microgrid._graph import ComponentGraph, _MicrogridComponentGraph
from frequenz.sdk.microgrid.client import Connection
from frequenz.sdk.microgrid.component import (
    BatteryData,
    Component,
    ComponentCategory,
    ComponentData,
    EVChargerData,
    InverterData,
    MeterData,
)


class MockMicrogridClient:
    """Class that mocks MicrogridClient behavior."""

    def __init__(self, components: Set[Component], connections: Set[Connection]):
        """Create mock microgrid with given components and connections.

        This simulates microgrid.
        Every call to `battery_data` and `inverter_data` is mocked to return
        receiver owned by this class.
        User can send data to the receiver using `self.send(..)` method.
        Messages will be send based on component id.

        Args:
            components: List of the microgrid components
            connections: List of the microgrid connections
        """
        self._component_graph = _MicrogridComponentGraph(components, connections)

        self._components = components

        bat_channels = self._create_battery_channels()
        inv_channels = self._create_inverter_channels()
        meter_channels = self._create_meter_channels()
        ev_charger_channels = self._create_ev_charger_channels()

        self._all_channels: Dict[int, Broadcast[Any]] = {
            **bat_channels,
            **inv_channels,
            **meter_channels,
            **ev_charger_channels,
        }

        mock_api = self._create_mock_api(
            bat_channels, inv_channels, meter_channels, ev_charger_channels
        )
        kwargs: Dict[str, Any] = {
            "api_client": mock_api,
            "component_graph": self._component_graph,
        }

        self._mock_microgrid = MagicMock(spec=ConnectionManager, **kwargs)
        self._battery_data_senders = {
            id: channel.new_sender() for id, channel in bat_channels.items()
        }
        self._inverter_data_senders = {
            id: channel.new_sender() for id, channel in inv_channels.items()
        }
        self._meter_data_senders = {
            id: channel.new_sender() for id, channel in meter_channels.items()
        }
        self._ev_charger_data_senders = {
            id: channel.new_sender() for id, channel in ev_charger_channels.items()
        }

    def initialize(self, mocker: MockerFixture) -> None:
        """Mock `microgrid.get` call to return this mock_microgrid.

        Args:
            mocker: mocker from the current test
        """
        # Mock _MICROGRID, so `get` method return this mocked microgrid.
        mocker.patch(
            "frequenz.sdk.microgrid._microgrid._MICROGRID", self.mock_microgrid
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

    async def send(self, data: ComponentData) -> bool:
        """Send component data using channel.

        This simulates component sending data. Right now only battery and inverter
        are supported. More components categories can be added if needed.

        Args:
            data: Data to be send

        Returns:
            Whether data was send, or not.
        """
        cid = data.component_id
        if isinstance(data, BatteryData):
            return await self._battery_data_senders[cid].send(data)
        if isinstance(data, InverterData):
            return await self._inverter_data_senders[cid].send(data)
        if isinstance(data, MeterData):
            return await self._meter_data_senders[cid].send(data)
        if isinstance(data, EVChargerData):
            return await self._ev_charger_data_senders[cid].send(data)

        raise RuntimeError(f"{type(data)} is not supported in MockMicrogridClient.")

    async def close_channel(self, cid: int) -> None:
        """Close channel for given component id.

        Args:
            cid: Component id
        """
        if cid in self._all_channels:
            await self._all_channels[cid].close()

    def _create_battery_channels(self) -> Dict[int, Broadcast[BatteryData]]:
        """Create channels for the batteries.

        Returns:
            Dictionary where the key is battery id and the value is channel for this
                battery.
        """
        batteries = [
            c.component_id
            for c in self.component_graph.components(
                component_category={ComponentCategory.BATTERY}
            )
        ]

        return {
            bid: Broadcast[BatteryData]("battery_data_" + str(bid)) for bid in batteries
        }

    def _create_meter_channels(self) -> Dict[int, Broadcast[MeterData]]:
        """Create channels for the meters.

        Returns:
            Dictionary where the key is meter id and the value is channel for this
                meter.
        """
        meters = [
            c.component_id
            for c in self.component_graph.components(
                component_category={ComponentCategory.METER}
            )
        ]

        return {cid: Broadcast[MeterData]("meter_data_" + str(cid)) for cid in meters}

    def _create_inverter_channels(self) -> Dict[int, Broadcast[InverterData]]:
        """Create channels for the inverters.

        Returns:
            Dictionary where the key is inverter id and the value is channel for
                this inverter.
        """
        inverters = [
            c.component_id
            for c in self.component_graph.components(
                component_category={ComponentCategory.INVERTER}
            )
        ]

        return {
            cid: Broadcast[InverterData]("inverter_data_" + str(cid))
            for cid in inverters
        }

    def _create_ev_charger_channels(self) -> Dict[int, Broadcast[EVChargerData]]:
        """Create channels for the ev chargers.

        Returns:
            Dictionary where the key is the id of the ev_charger and the value is
                channel for this ev_charger.
        """
        meters = [
            c.component_id
            for c in self.component_graph.components(
                component_category={ComponentCategory.EV_CHARGER}
            )
        ]

        return {
            cid: Broadcast[EVChargerData]("meter_data_" + str(cid)) for cid in meters
        }

    def _create_mock_api(
        self,
        bat_channels: Dict[int, Broadcast[BatteryData]],
        inv_channels: Dict[int, Broadcast[InverterData]],
        meter_channels: Dict[int, Broadcast[MeterData]],
        ev_charger_channels: Dict[int, Broadcast[EVChargerData]],
    ) -> MagicMock:
        """Create mock of MicrogridApiClient.

        Args:
            components: set of components.
            bat_channel: battery channels to be returned from
                MicrogridApiClient.battery_data.
            inv_channel: inverter channels to be returned from
                MicrogridApiClient.inverter_data.

        Returns:
            Magic mock instance of MicrogridApiClient.
        """
        api = MagicMock()
        api.components = AsyncMock(return_value=self._components)
        # NOTE that has to be partial, because battery_data has id argument and takes
        # channel based on the argument.
        api.battery_data = AsyncMock(
            side_effect=partial(self._get_battery_receiver, channels=bat_channels)
        )

        api.inverter_data = AsyncMock(
            side_effect=partial(self._get_inverter_receiver, channels=inv_channels)
        )

        api.meter_data = AsyncMock(
            side_effect=partial(self._get_meter_receiver, channels=meter_channels)
        )

        api.ev_charger_data = AsyncMock(
            side_effect=partial(
                self._get_ev_charger_receiver, channels=ev_charger_channels
            )
        )

        # Can be override in the future
        api.set_power = AsyncMock(return_value=Empty)
        return api

    def _get_battery_receiver(
        self,
        component_id: int,
        channels: Dict[int, Broadcast[BatteryData]],
        maxsize: int = RECEIVER_MAX_SIZE,
    ) -> Receiver[BatteryData]:
        """Return receiver of the broadcast channel for given component_id.

        Args:
            component_id: component_id
            channels: Broadcast channels

        Returns:
            Receiver from the given channels.
        """
        return channels[component_id].new_receiver(
            "component" + str(component_id), maxsize=maxsize
        )

    def _get_meter_receiver(
        self,
        component_id: int,
        channels: Dict[int, Broadcast[MeterData]],
        maxsize: int = RECEIVER_MAX_SIZE,
    ) -> Receiver[MeterData]:
        """Return receiver of the broadcast channel for given component_id.

        Args:
            component_id: component_id
            channels: Broadcast channels

        Returns:
            Receiver from the given channels.
        """
        return channels[component_id].new_receiver(
            "component" + str(component_id), maxsize=maxsize
        )

    def _get_ev_charger_receiver(
        self,
        component_id: int,
        channels: Dict[int, Broadcast[EVChargerData]],
        maxsize: int = RECEIVER_MAX_SIZE,
    ) -> Receiver[EVChargerData]:
        """Return receiver of the broadcast channel for given component_id.

        Args:
            component_id: component_id
            channels: Broadcast channels

        Returns:
            Receiver from the given channels.
        """
        return channels[component_id].new_receiver(
            "component" + str(component_id), maxsize=maxsize
        )

    def _get_inverter_receiver(
        self,
        component_id: int,
        channels: Dict[int, Broadcast[InverterData]],
        maxsize: int = RECEIVER_MAX_SIZE,
    ) -> Receiver[InverterData]:
        """Return receiver of the broadcast channel for given component_id.

        Args:
            component_id: component_id
            channels: Broadcast channels

        Returns:
            Receiver from the given channels.
        """
        return channels[component_id].new_receiver(
            "component" + str(component_id), maxsize=maxsize
        )
