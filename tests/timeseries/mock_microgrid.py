# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""A configurable mock microgrid for testing logical meter formulas."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from types import TracebackType
from typing import Coroutine

from frequenz.client.common.microgrid import MicrogridId
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.component import (
    AcEvCharger,
    Battery,
    BatteryInverter,
    Chp,
    Component,
    ComponentConnection,
    ComponentStateCode,
    EvCharger,
    GridConnectionPoint,
    Inverter,
    LiIonBattery,
    Meter,
    SolarInverter,
)
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk._internal._asyncio import cancel_and_await
from frequenz.sdk.microgrid import _data_pipeline
from frequenz.sdk.microgrid._old_component_data import ComponentData
from frequenz.sdk.microgrid.component_graph import _MicrogridComponentGraph
from frequenz.sdk.timeseries import ResamplerConfig2

from ..utils import MockMicrogridClient
from ..utils.component_data_wrapper import (
    BatteryDataWrapper,
    EvChargerDataWrapper,
    InverterDataWrapper,
    MeterDataWrapper,
)
from .mock_resampler import MockResampler

_MICROGRID_ID = MicrogridId(1)


class MockMicrogrid:  # pylint: disable=too-many-instance-attributes
    """Setup a MockApi instance with multiple component layouts for tests."""

    grid_id = ComponentId(1)
    _grid_meter_id = ComponentId(4)

    chp_id_suffix = 5
    evc_id_suffix = 6
    meter_id_suffix = 7
    inverter_id_suffix = 8
    battery_id_suffix = 9

    mock_client: MockMicrogridClient
    mock_resampler: MockResampler

    def __init__(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        grid_meter: bool | None = None,
        api_client_streaming: bool = False,
        num_values: int = 2000,
        sample_rate_s: float = 0.01,
        num_namespaces: int = 1,
        rated_fuse_current: int = 10_000,
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
            rated_fuse_current: optional, the rated current of the fuse for the grid connection.
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
                GridConnectionPoint(
                    id=ComponentId(1),
                    microgrid_id=_MICROGRID_ID,
                    rated_fuse_current=rated_fuse_current,
                ),
            }
            if graph is None
            else graph.components()
        )

        self._connections: set[ComponentConnection] = (
            set() if graph is None else graph.connections()
        )

        self._id_increment = 0 if graph is None else len(self._components)
        self._api_client_streaming = api_client_streaming
        self._num_values = num_values
        self._sample_rate_s = sample_rate_s
        self._namespaces = num_namespaces

        self._connect_to = self.grid_id

        def filter_comp(component_type: type[Component]) -> list[ComponentId]:
            if graph is None:
                return []
            components = graph.components(matching_types=component_type)
            return sorted(map(lambda c: c.id, components), key=int)

        def inverters(component_type: type[Inverter]) -> list[ComponentId]:
            if graph is None:
                return []
            components = graph.components(matching_types=component_type)
            return sorted(map(lambda c: c.id, components), key=int)

        self.chp_ids: list[ComponentId] = filter_comp(Chp)
        self.battery_ids: list[ComponentId] = filter_comp(Battery)
        self.evc_ids: list[ComponentId] = filter_comp(EvCharger)
        self.meter_ids: list[ComponentId] = filter_comp(Meter)

        self.battery_inverter_ids: list[ComponentId] = inverters(BatteryInverter)
        self.pv_inverter_ids: list[ComponentId] = inverters(SolarInverter)

        self.bat_inv_map: dict[ComponentId, ComponentId] = (
            {}
            if graph is None
            else {
                # Hacky, ignores multiple batteries behind one inverter
                list(graph.successors(c.id))[0].id: c.id
                for c in graph.components(matching_types=BatteryInverter)
            }
        )

        self.evc_states: dict[ComponentId, set[ComponentStateCode]] = {}

        self._streaming_coros: list[tuple[ComponentId, Coroutine[None, None, None]]] = (
            []
        )
        """The streaming coroutines for each component.

        The tuple stores the component id we are streaming for as the first item and the
        coroutine as the second item.
        """

        self._streaming_tasks: dict[ComponentId, asyncio.Task[None]] = {}
        """The streaming tasks for each component.

        The key is the component id we are streaming for in this task.
        """

        if grid_meter:
            self._connect_to = self._grid_meter_id
            self._connections.add(
                ComponentConnection(
                    source=self.grid_id, destination=self._grid_meter_id
                )
            )
            self._components.add(
                Meter(
                    id=self._grid_meter_id,
                    microgrid_id=MicrogridId(1),
                )
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
            ResamplerConfig2(timedelta(seconds=self._sample_rate_s)),
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

        def _done_callback(task: asyncio.Task[None]) -> None:
            try:
                task.result()
            except (asyncio.CancelledError, Exception) as exc:
                raise SystemExit(
                    f"Streaming task {task.get_name()!r} failed: {exc}"
                ) from exc

        for component_id, coro in self._streaming_coros:
            task = asyncio.create_task(coro, name=f"component-id:{component_id}")
            self._streaming_tasks[component_id] = task
            task.add_done_callback(_done_callback)

        return self.mock_client

    async def _comp_data_send_task(
        self,
        comp_id: ComponentId,
        make_comp_data: Callable[[int, datetime], ComponentData],
    ) -> None:
        for value in range(1, self._num_values + 1):
            timestamp = datetime.now(tz=timezone.utc)
            val_to_send = value + int(comp_id) // 10
            # for inverters with component_id > 100, send only half the messages.
            if int(comp_id) % 10 == self.inverter_id_suffix:
                if int(comp_id) < 100 or value <= 5:
                    await self.mock_client.send(
                        make_comp_data(val_to_send, timestamp).to_samples()
                    )
            else:
                await self.mock_client.send(
                    make_comp_data(val_to_send, timestamp).to_samples()
                )
            await asyncio.sleep(self._sample_rate_s)

        await self.mock_client.close_channels(comp_id)

    def _start_meter_streaming(self, meter_id: ComponentId) -> None:
        if not self._api_client_streaming:
            return
        self._streaming_coros.append(
            (
                meter_id,
                self._comp_data_send_task(
                    meter_id,
                    lambda value, ts: MeterDataWrapper(
                        component_id=meter_id,
                        timestamp=ts,
                        reactive_power=2 * value,
                        active_power=value,
                        current_per_phase=(value + 100.0, value + 101.0, value + 102.0),
                        voltage_per_phase=(value + 200.0, value + 199.8, value + 200.2),
                    ),
                ),
            )
        )

    def _start_battery_streaming(self, bat_id: ComponentId) -> None:
        if not self._api_client_streaming:
            return
        self._streaming_coros.append(
            (
                bat_id,
                self._comp_data_send_task(
                    bat_id,
                    lambda value, ts: BatteryDataWrapper(
                        component_id=bat_id, timestamp=ts, soc=value
                    ),
                ),
            )
        )

    def _start_inverter_streaming(self, inv_id: ComponentId) -> None:
        if not self._api_client_streaming:
            return
        self._streaming_coros.append(
            (
                inv_id,
                self._comp_data_send_task(
                    inv_id,
                    lambda value, ts: InverterDataWrapper(
                        component_id=inv_id,
                        timestamp=ts,
                        active_power=value,
                        reactive_power=2 * value,
                    ),
                ),
            )
        )

    def _start_ev_charger_streaming(self, evc_id: ComponentId) -> None:
        if not self._api_client_streaming:
            return
        self._streaming_coros.append(
            (
                evc_id,
                self._comp_data_send_task(
                    evc_id,
                    lambda value, ts: EvChargerDataWrapper(
                        component_id=evc_id,
                        timestamp=ts,
                        active_power=value,
                        reactive_power=2 * value,
                        current_per_phase=(value + 10.0, value + 11.0, value + 12.0),
                        states=self.evc_states[evc_id],
                    ),
                ),
            )
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
            meter_id = ComponentId(self._id_increment * 10 + self.meter_id_suffix)
            self._id_increment += 1
            self.meter_ids.append(meter_id)
            self._components.add(Meter(id=meter_id, microgrid_id=_MICROGRID_ID))
            self._connections.add(
                ComponentConnection(source=self._connect_to, destination=meter_id)
            )
            self._start_meter_streaming(meter_id)

    def add_chps(self, count: int, no_meters: bool = False) -> None:
        """Add CHPs with connected meters to the mock microgrid.

        Args:
            count: number of CHPs to add.
            no_meters: if True, do not add a meter for each CHP.
        """
        for _ in range(count):
            chp_id = ComponentId(self._id_increment * 10 + self.chp_id_suffix)
            self.chp_ids.append(chp_id)
            self._components.add(Chp(id=chp_id, microgrid_id=_MICROGRID_ID))
            if no_meters:
                self._connections.add(
                    ComponentConnection(source=self._connect_to, destination=chp_id)
                )
            else:
                meter_id = ComponentId(self._id_increment * 10 + self.meter_id_suffix)
                self.meter_ids.append(meter_id)
                self._components.add(Meter(id=meter_id, microgrid_id=_MICROGRID_ID))
                self._start_meter_streaming(meter_id)
                self._connections.add(
                    ComponentConnection(source=self._connect_to, destination=meter_id)
                )
                self._connections.add(
                    ComponentConnection(source=meter_id, destination=chp_id)
                )

            self._id_increment += 1

    def add_batteries(self, count: int, no_meter: bool = False) -> None:
        """Add batteries with connected inverters and meters to the microgrid.

        Args:
            count: number of battery sets to add.
            no_meter: if True, do not add a meter for each battery set.
        """
        for _ in range(count):
            meter_id = ComponentId(self._id_increment * 10 + self.meter_id_suffix)
            inv_id = ComponentId(self._id_increment * 10 + self.inverter_id_suffix)
            bat_id = ComponentId(self._id_increment * 10 + self.battery_id_suffix)
            self._id_increment += 1

            self.battery_inverter_ids.append(inv_id)
            self.battery_ids.append(bat_id)
            self.bat_inv_map[bat_id] = inv_id

            self._components.add(BatteryInverter(id=inv_id, microgrid_id=_MICROGRID_ID))
            self._components.add(LiIonBattery(id=bat_id, microgrid_id=_MICROGRID_ID))
            self._start_battery_streaming(bat_id)
            self._start_inverter_streaming(inv_id)

            if no_meter:
                self._connections.add(
                    ComponentConnection(source=self._connect_to, destination=inv_id)
                )
            else:
                self.meter_ids.append(meter_id)
                self._components.add(Meter(id=meter_id, microgrid_id=_MICROGRID_ID))
                self._start_meter_streaming(meter_id)
                self._connections.add(
                    ComponentConnection(source=self._connect_to, destination=meter_id)
                )
                self._connections.add(
                    ComponentConnection(source=meter_id, destination=inv_id)
                )
            self._connections.add(
                ComponentConnection(source=inv_id, destination=bat_id)
            )

    def add_solar_inverters(self, count: int, no_meter: bool = False) -> None:
        """Add pv inverters and connected pv meters to the microgrid.

        Args:
            count: number of inverters to add to the microgrid.
            no_meter: if True, do not add a meter for each inverter.
        """
        for _ in range(count):
            meter_id = ComponentId(self._id_increment * 10 + self.meter_id_suffix)
            inv_id = ComponentId(self._id_increment * 10 + self.inverter_id_suffix)
            self._id_increment += 1

            self.pv_inverter_ids.append(inv_id)

            self._components.add(SolarInverter(id=inv_id, microgrid_id=_MICROGRID_ID))
            self._start_inverter_streaming(inv_id)

            if no_meter:
                self._connections.add(
                    ComponentConnection(source=self._connect_to, destination=inv_id)
                )
            else:
                self.meter_ids.append(meter_id)
                self._components.add(Meter(id=meter_id, microgrid_id=_MICROGRID_ID))
                self._start_meter_streaming(meter_id)
                self._connections.add(
                    ComponentConnection(source=self._connect_to, destination=meter_id)
                )
                self._connections.add(
                    ComponentConnection(source=meter_id, destination=inv_id)
                )

    def add_ev_chargers(self, count: int) -> None:
        """Add EV Chargers to the microgrid.

        Args:
            count: Number of ev chargers to add to the microgrid.
        """
        for _ in range(count):
            evc_id = ComponentId(self._id_increment * 10 + self.evc_id_suffix)
            self._id_increment += 1

            self.evc_ids.append(evc_id)
            self.evc_states[evc_id] = {
                ComponentStateCode.READY,
                ComponentStateCode.EV_CHARGING_CABLE_UNPLUGGED,
            }

            self._components.add(AcEvCharger(id=evc_id, microgrid_id=_MICROGRID_ID))
            self._start_ev_charger_streaming(evc_id)
            self._connections.add(
                ComponentConnection(source=self._connect_to, destination=evc_id)
            )

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
                ).to_samples()
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
                BatteryDataWrapper(
                    component_id=comp_id, timestamp=timestamp, soc=value
                ).to_samples()
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
                ).to_samples()
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
                ).to_samples()
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
                    states=self.evc_states[comp_id],
                ).to_samples()
            )

    async def cleanup(self) -> None:
        """Clean up after a test."""
        # pylint: disable=protected-access
        if _data_pipeline._DATA_PIPELINE:
            await _data_pipeline._DATA_PIPELINE._stop()

        await self.mock_resampler._stop()

        for _, coro in self._streaming_coros:
            coro.close()

        for task in self._streaming_tasks.values():
            await cancel_and_await(task)
        microgrid.connection_manager._CONNECTION_MANAGER = None
        # pylint: enable=protected-access

    async def __aenter__(self) -> MockMicrogrid:
        """Enter context manager."""
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> None:
        """Exit context manager."""
        await self.cleanup()
