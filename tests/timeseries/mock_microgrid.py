# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""A configurable mock microgrid for testing logical meter formulas."""


import asyncio
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Coroutine

from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk._internal._asyncio import cancel_and_await
from frequenz.sdk.actor import ResamplerConfig
from frequenz.sdk.microgrid import _data_pipeline
from frequenz.sdk.microgrid.client import Connection
from frequenz.sdk.microgrid.component import (
    Component,
    ComponentCategory,
    ComponentData,
    EVChargerCableState,
    EVChargerComponentState,
    GridMetadata,
    InverterType,
)
from frequenz.sdk.microgrid.component_graph import _MicrogridComponentGraph
from frequenz.sdk.timeseries import Current, Fuse

from ..utils import MockMicrogridClient
from ..utils.component_data_wrapper import (
    BatteryDataWrapper,
    EvChargerDataWrapper,
    InverterDataWrapper,
    MeterDataWrapper,
)
from .mock_resampler import MockResampler


class MockMicrogrid:  # pylint: disable=too-many-instance-attributes
    """Setup a MockApi instance with multiple component layouts for tests."""

    grid_id = 1
    _grid_meter_id = 4

    chp_id_suffix = 5
    evc_id_suffix = 6
    meter_id_suffix = 7
    inverter_id_suffix = 8
    battery_id_suffix = 9

    mock_client: MockMicrogridClient
    mock_resampler: MockResampler

    def __init__(  # pylint: disable=too-many-arguments
        self,
        grid_meter: bool | None = None,
        api_client_streaming: bool = False,
        num_values: int = 2000,
        sample_rate_s: float = 0.01,
        num_namespaces: int = 1,
        fuse: Fuse | None = Fuse(Current.from_amperes(10_000.0)),
        graph: _MicrogridComponentGraph | None = None,
        mocker: MockerFixture | None = None,
    ):
        """Create a new instance.

        Args:
            grid_meter: optional, whether there is a meter successor of the GRID component.
            api_client_streaming: whether the mock client should be configured to stream
                raw data from the API client.
            num_values: number of values to generate for each component.
            sample_rate_s: sample rate in seconds.
            num_namespaces: number of namespaces that each metric should be available
                to.  Useful in tests where multiple namespaces (logical_meter,
                battery_pool, etc) are used, and the same metric is used by formulas in
                different namespaces.
            fuse: optional, the fuse to use for the grid connection.
            graph: optional, a graph of components to use instead of the default grid
                layout. If specified, grid_meter must be None.
            mocker: optional, a mocker to pass to the mock client and mock resampler.

        Raises:
            ValueError: if both grid_meter and graph are specified.
        """
        self._mocker = mocker
        if grid_meter is not None and graph is not None:
            raise ValueError("grid_meter and graph are mutually exclusive")

        self._components: set[Component] = (
            {
                Component(1, ComponentCategory.GRID, None, GridMetadata(fuse)),
            }
            if graph is None
            else graph.components()
        )

        self._connections: set[Connection] = (
            set() if graph is None else graph.connections()
        )

        self._id_increment = 0 if graph is None else len(self._components)
        self._api_client_streaming = api_client_streaming
        self._num_values = num_values
        self._sample_rate_s = sample_rate_s
        self._namespaces = num_namespaces

        self._connect_to = self.grid_id

        def filter_comp(category: ComponentCategory) -> list[int]:
            if graph is None:
                return []
            return list(
                map(
                    lambda c: c.component_id,
                    graph.components(component_categories={category}),
                )
            )

        def inverters(comp_type: InverterType) -> list[int]:
            if graph is None:
                return []

            return [
                c.component_id
                for c in graph.components(
                    component_categories={ComponentCategory.INVERTER}
                )
                if c.type == comp_type
            ]

        self.chp_ids: list[int] = filter_comp(ComponentCategory.CHP)
        self.battery_ids: list[int] = filter_comp(ComponentCategory.BATTERY)
        self.evc_ids: list[int] = filter_comp(ComponentCategory.EV_CHARGER)
        self.meter_ids: list[int] = filter_comp(ComponentCategory.METER)

        self.battery_inverter_ids: list[int] = inverters(InverterType.BATTERY)
        self.pv_inverter_ids: list[int] = inverters(InverterType.SOLAR)

        self.bat_inv_map: dict[int, int] = (
            {}
            if graph is None
            else {
                # Hacky, ignores multiple batteries behind one inverter
                list(graph.successors(c.component_id))[0].component_id: c.component_id
                for c in graph.components(
                    component_categories={ComponentCategory.INVERTER}
                )
                if c.type == InverterType.BATTERY
            }
        )

        self.evc_component_states: dict[int, EVChargerComponentState] = {}
        self.evc_cable_states: dict[int, EVChargerCableState] = {}

        self._streaming_coros: list[Coroutine[None, None, None]] = []
        self._streaming_tasks: list[asyncio.Task[None]] = []

        if grid_meter:
            self._connect_to = self._grid_meter_id
            self._connections.add(Connection(self.grid_id, self._grid_meter_id))
            self._components.add(
                Component(self._grid_meter_id, ComponentCategory.METER)
            )
            self.meter_ids.append(self._grid_meter_id)
            self._start_meter_streaming(self._grid_meter_id)

    async def start(self, mocker: MockerFixture | None = None) -> None:
        """Init the mock microgrid client and start the mock resampler."""
        # Return if it is already started
        if hasattr(self, "mock_client") or hasattr(self, "mock_resampler"):
            return

        if mocker is None:
            mocker = self._mocker
        assert mocker is not None, "A mocker must be set at init or start time"

        # This binding to a local is needed because Python uses late binding for
        # closures and `mocker` could be bound to `None` again after the lambda is
        # created. See:
        # https://mypy.readthedocs.io/en/stable/common_issues.html#narrowing-and-inner-functions
        local_mocker = mocker
        self.init_mock_client(lambda mock_client: mock_client.initialize(local_mocker))
        self.mock_resampler = MockResampler(
            mocker,
            ResamplerConfig(timedelta(seconds=self._sample_rate_s)),
            bat_inverter_ids=self.battery_inverter_ids,
            pv_inverter_ids=self.pv_inverter_ids,
            evc_ids=self.evc_ids,
            meter_ids=self.meter_ids,
            chp_ids=self.chp_ids,
            namespaces=self._namespaces,
        )

    def init_mock_client(
        self, initialize_cb: Callable[[MockMicrogridClient], None]
    ) -> None:
        """Set up the mock client. Does not start the streaming tasks."""
        self.mock_client = MockMicrogridClient(self._components, self._connections)
        initialize_cb(self.mock_client)

    def start_mock_client(
        self, initialize_cb: Callable[[MockMicrogridClient], None]
    ) -> MockMicrogridClient:
        """Start the mock client.

        Creates the microgrid mock client, initializes it, and starts the streaming
        tasks.

        For unittests, users should use the `start()` method.

        Args:
            initialize_cb: callback to initialize the mock client.

        Returns:
            A MockMicrogridClient instance.
        """
        self.init_mock_client(initialize_cb)
        self._streaming_tasks = [
            asyncio.create_task(coro) for coro in self._streaming_coros
        ]
        return self.mock_client

    async def _comp_data_send_task(
        self, comp_id: int, make_comp_data: Callable[[int, datetime], ComponentData]
    ) -> None:
        for value in range(1, self._num_values + 1):
            timestamp = datetime.now(tz=timezone.utc)
            val_to_send = value + int(comp_id / 10)
            # for inverters with component_id > 100, send only half the messages.
            if comp_id % 10 == self.inverter_id_suffix:
                if comp_id < 100 or value <= 5:
                    await self.mock_client.send(make_comp_data(val_to_send, timestamp))
            else:
                await self.mock_client.send(make_comp_data(val_to_send, timestamp))
            await asyncio.sleep(self._sample_rate_s)

        await self.mock_client.close_channel(comp_id)

    def _start_meter_streaming(self, meter_id: int) -> None:
        if not self._api_client_streaming:
            return
        self._streaming_coros.append(
            self._comp_data_send_task(
                meter_id,
                lambda value, ts: MeterDataWrapper(
                    component_id=meter_id,
                    timestamp=ts,
                    active_power=value,
                    current_per_phase=(value + 100.0, value + 101.0, value + 102.0),
                    voltage_per_phase=(value + 200.0, value + 199.8, value + 200.2),
                ),
            )
        )

    def _start_battery_streaming(self, bat_id: int) -> None:
        if not self._api_client_streaming:
            return
        self._streaming_coros.append(
            self._comp_data_send_task(
                bat_id,
                lambda value, ts: BatteryDataWrapper(
                    component_id=bat_id, timestamp=ts, soc=value
                ),
            )
        )

    def _start_inverter_streaming(self, inv_id: int) -> None:
        if not self._api_client_streaming:
            return
        self._streaming_coros.append(
            self._comp_data_send_task(
                inv_id,
                lambda value, ts: InverterDataWrapper(
                    component_id=inv_id, timestamp=ts, active_power=value
                ),
            )
        )

    def _start_ev_charger_streaming(self, evc_id: int) -> None:
        if not self._api_client_streaming:
            return
        self._streaming_coros.append(
            self._comp_data_send_task(
                evc_id,
                lambda value, ts: EvChargerDataWrapper(
                    component_id=evc_id,
                    timestamp=ts,
                    active_power=value,
                    current_per_phase=(value + 10.0, value + 11.0, value + 12.0),
                    component_state=self.evc_component_states[evc_id],
                    cable_state=self.evc_cable_states[evc_id],
                ),
            ),
        )

    def add_consumer_meters(self, count: int = 1) -> None:
        """Add consumer meters to the mock microgrid.

        A consumer meter is a meter with unknown successors
        that draw a certain amount of power.

        We use it to calculate the total power consumption
        at the grid connection point.

        Args:
            count: number of consumer meters to add.
        """
        for _ in range(count):
            meter_id = self._id_increment * 10 + self.meter_id_suffix
            self._id_increment += 1
            self.meter_ids.append(meter_id)
            self._components.add(
                Component(
                    meter_id,
                    ComponentCategory.METER,
                )
            )
            self._connections.add(Connection(self._connect_to, meter_id))
            self._start_meter_streaming(meter_id)

    def add_chps(self, count: int, no_meters: bool = False) -> None:
        """Add CHPs with connected meters to the mock microgrid.

        Args:
            count: number of CHPs to add.
            no_meters: if True, do not add a meter for each CHP.
        """
        for _ in range(count):
            meter_id = self._id_increment * 10 + self.meter_id_suffix
            chp_id = self._id_increment * 10 + self.chp_id_suffix
            self._id_increment += 1

            self.meter_ids.append(meter_id)
            self.chp_ids.append(chp_id)

            if not no_meters:
                self._components.add(
                    Component(
                        meter_id,
                        ComponentCategory.METER,
                    )
                )
            self._components.add(
                Component(
                    chp_id,
                    ComponentCategory.CHP,
                )
            )

            self._start_meter_streaming(meter_id)
            if no_meters:
                self._connections.add(Connection(self._connect_to, chp_id))
            else:
                self._connections.add(Connection(self._connect_to, meter_id))
                self._connections.add(Connection(meter_id, chp_id))

    def add_batteries(self, count: int, no_meter: bool = False) -> None:
        """Add batteries with connected inverters and meters to the microgrid.

        Args:
            count: number of battery sets to add.
            no_meter: if True, do not add a meter for each battery set.
        """
        for _ in range(count):
            meter_id = self._id_increment * 10 + self.meter_id_suffix
            inv_id = self._id_increment * 10 + self.inverter_id_suffix
            bat_id = self._id_increment * 10 + self.battery_id_suffix
            self._id_increment += 1

            self.battery_inverter_ids.append(inv_id)
            self.battery_ids.append(bat_id)
            self.bat_inv_map[bat_id] = inv_id

            self._components.add(
                Component(inv_id, ComponentCategory.INVERTER, InverterType.BATTERY)
            )
            self._components.add(
                Component(
                    bat_id,
                    ComponentCategory.BATTERY,
                )
            )
            self._start_battery_streaming(bat_id)
            self._start_inverter_streaming(inv_id)

            if no_meter:
                self._connections.add(Connection(self._connect_to, inv_id))
            else:
                self.meter_ids.append(meter_id)
                self._components.add(
                    Component(
                        meter_id,
                        ComponentCategory.METER,
                    )
                )
                self._start_meter_streaming(meter_id)
                self._connections.add(Connection(self._connect_to, meter_id))
                self._connections.add(Connection(meter_id, inv_id))
            self._connections.add(Connection(inv_id, bat_id))

    def add_solar_inverters(self, count: int, no_meter: bool = False) -> None:
        """Add pv inverters and connected pv meters to the microgrid.

        Args:
            count: number of inverters to add to the microgrid.
            no_meter: if True, do not add a meter for each inverter.
        """
        for _ in range(count):
            meter_id = self._id_increment * 10 + self.meter_id_suffix
            inv_id = self._id_increment * 10 + self.inverter_id_suffix
            self._id_increment += 1

            self.pv_inverter_ids.append(inv_id)

            self._components.add(
                Component(
                    inv_id,
                    ComponentCategory.INVERTER,
                    InverterType.SOLAR,
                )
            )
            self._start_inverter_streaming(inv_id)

            if no_meter:
                self._connections.add(Connection(self._connect_to, inv_id))
            else:
                self.meter_ids.append(meter_id)
                self._components.add(
                    Component(
                        meter_id,
                        ComponentCategory.METER,
                    )
                )
                self._start_meter_streaming(meter_id)
                self._connections.add(Connection(self._connect_to, meter_id))
                self._connections.add(Connection(meter_id, inv_id))

    def add_ev_chargers(self, count: int) -> None:
        """Add EV Chargers to the microgrid.

        Args:
            count: Number of ev chargers to add to the microgrid.
        """
        for _ in range(count):
            evc_id = self._id_increment * 10 + self.evc_id_suffix
            self._id_increment += 1

            self.evc_ids.append(evc_id)
            self.evc_component_states[evc_id] = EVChargerComponentState.READY
            self.evc_cable_states[evc_id] = EVChargerCableState.UNPLUGGED

            self._components.add(
                Component(
                    evc_id,
                    ComponentCategory.EV_CHARGER,
                )
            )
            self._start_ev_charger_streaming(evc_id)
            self._connections.add(Connection(self._connect_to, evc_id))

    async def send_meter_data(self, values: list[float]) -> None:
        """Send raw meter data from the mock microgrid.

        Args:
            values: list of active power values for each meter.
        """
        assert len(values) == len(self.meter_ids)
        timestamp = datetime.now(tz=timezone.utc)
        for comp_id, value in zip(self.meter_ids, values):
            await self.mock_client.send(
                MeterDataWrapper(
                    component_id=comp_id,
                    timestamp=timestamp,
                    active_power=value,
                    current_per_phase=(
                        value + 100.0,
                        value + 101.0,
                        value + 102.0,
                    ),
                    voltage_per_phase=(
                        value + 200.0,
                        value + 199.8,
                        value + 200.2,
                    ),
                )
            )

    async def send_battery_data(self, socs: list[float]) -> None:
        """Send raw battery data from the mock microgrid.

        Args:
            socs: list of soc values for each battery.
        """
        assert len(socs) == len(self.battery_ids)
        timestamp = datetime.now(tz=timezone.utc)
        for comp_id, value in zip(self.battery_ids, socs):
            await self.mock_client.send(
                BatteryDataWrapper(component_id=comp_id, timestamp=timestamp, soc=value)
            )

    async def send_battery_inverter_data(self, values: list[float]) -> None:
        """Send raw battery inverter data from the mock microgrid.

        Args:
            values: list of active power values for each battery inverter.
        """
        assert len(values) == len(self.battery_inverter_ids)
        timestamp = datetime.now(tz=timezone.utc)
        for comp_id, value in zip(self.battery_inverter_ids, values):
            await self.mock_client.send(
                InverterDataWrapper(
                    component_id=comp_id, timestamp=timestamp, active_power=value
                )
            )

    async def send_pv_inverter_data(self, values: list[float]) -> None:
        """Send raw pv inverter data from the mock microgrid.

        Args:
            values: list of active power values for each pv inverter.
        """
        assert len(values) == len(self.pv_inverter_ids)
        timestamp = datetime.now(tz=timezone.utc)
        for comp_id, value in zip(self.pv_inverter_ids, values):
            await self.mock_client.send(
                InverterDataWrapper(
                    component_id=comp_id, timestamp=timestamp, active_power=value
                )
            )

    async def send_ev_charger_data(self, values: list[float]) -> None:
        """Send raw ev charger data from the mock microgrid.

        Args:
            values: list of active power values for each ev charger.
        """
        assert len(values) == len(self.evc_ids)
        timestamp = datetime.now(tz=timezone.utc)
        for comp_id, value in zip(self.evc_ids, values):
            await self.mock_client.send(
                EvChargerDataWrapper(
                    component_id=comp_id,
                    timestamp=timestamp,
                    active_power=value,
                    current_per_phase=(
                        value + 100.0,
                        value + 101.0,
                        value + 102.0,
                    ),
                    component_state=self.evc_component_states[comp_id],
                    cable_state=self.evc_cable_states[comp_id],
                )
            )

    async def cleanup(self) -> None:
        """Clean up after a test."""
        # pylint: disable=protected-access
        if _data_pipeline._DATA_PIPELINE:
            await _data_pipeline._DATA_PIPELINE._stop()

        await self.mock_resampler._stop()

        for coro in self._streaming_coros:
            coro.close()

        for task in self._streaming_tasks:
            await cancel_and_await(task)
        microgrid.connection_manager._CONNECTION_MANAGER = None
        # pylint: enable=protected-access
