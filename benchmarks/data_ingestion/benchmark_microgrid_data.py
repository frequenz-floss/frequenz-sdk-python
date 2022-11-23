# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Benchmark for microgrid data."""

import asyncio
import time
from typing import Any, Dict, Optional, Set, Tuple
from unittest.mock import AsyncMock

import frequenz.api.microgrid.microgrid_pb2 as microgrid_pb
from frequenz.api.microgrid.battery_pb2 import Battery
from frequenz.api.microgrid.battery_pb2 import Data as PbBatteryData
from frequenz.api.microgrid.battery_pb2 import Properties as BatteryProperties
from frequenz.api.microgrid.common_pb2 import AC, DC, Bounds, Metric, MetricAggregation
from frequenz.api.microgrid.inverter_pb2 import Data as PbInverterData
from frequenz.api.microgrid.inverter_pb2 import Inverter
from frequenz.api.microgrid.meter_pb2 import Data as PbMeterData
from frequenz.api.microgrid.meter_pb2 import Meter
from frequenz.channels import Broadcast, Receiver, Sender
from frequenz.channels.util import MergeNamed, Select
from google.protobuf.timestamp_pb2 import Timestamp  # pylint:disable=no-name-in-module

import frequenz.sdk.microgrid.graph as gr
from frequenz.sdk._data_handling.time_series import TimeSeriesEntry, TimeSeriesFormula
from frequenz.sdk._data_ingestion.formula_calculator import FormulaCalculator
from frequenz.sdk._data_ingestion.microgrid_data import MicrogridData
from frequenz.sdk.microgrid import BatteryData, InverterData, MeterData
from frequenz.sdk.microgrid.client import MicrogridGrpcClient
from frequenz.sdk.microgrid.component import Component, ComponentCategory
from frequenz.sdk.microgrid.connection import Connection


def gen_market_data(component_id: int) -> MeterData:
    """Generate meter data.

    Args:
        component_id: component ID

    Returns:
        Generated meter data
    """
    timestamp = Timestamp()
    timestamp.GetCurrentTime()
    return MeterData.from_proto(
        microgrid_pb.ComponentData(
            id=component_id,
            ts=timestamp,
            meter=Meter(
                data=PbMeterData(
                    ac=AC(power_active=Metric(value=100.0)),
                )
            ),
        )
    )


def gen_pv_data(component_id: int) -> MeterData:
    """Generate PV array data.

    Args:
        component_id: component ID

    Returns:
        Generated PV array data
    """
    timestamp = Timestamp()
    timestamp.GetCurrentTime()
    return MeterData.from_proto(
        microgrid_pb.ComponentData(
            id=component_id,
            ts=timestamp,
            meter=Meter(
                data=PbMeterData(
                    ac=AC(power_active=Metric(value=-500.0)),
                )
            ),
        )
    )


def gen_battery_data(component_id: int) -> BatteryData:
    """Generate battery data.

    Args:
        component_id: component ID

    Returns:
        Generated battery data
    """
    timestamp = Timestamp()
    timestamp.GetCurrentTime()
    return BatteryData.from_proto(
        microgrid_pb.ComponentData(
            id=component_id,
            ts=timestamp,
            battery=Battery(
                data=PbBatteryData(
                    soc=MetricAggregation(avg=float(component_id % 100)),
                    dc=DC(
                        power=Metric(
                            value=100.0, system_bounds=Bounds(lower=0.0, upper=1000.0)
                        )
                    ),
                ),
                properties=BatteryProperties(capacity=float(component_id * 10)),
            ),
        )
    )


def gen_inverter_data(component_id: int) -> InverterData:
    """Generate inverter data.

    Args:
        component_id: component ID

    Returns:
        Generated inverter data
    """
    timestamp = Timestamp()
    timestamp.GetCurrentTime()
    return InverterData.from_proto(
        microgrid_pb.ComponentData(
            id=component_id,
            ts=timestamp,
            inverter=Inverter(
                data=PbInverterData(
                    ac=AC(
                        power_active=Metric(
                            value=100.0, system_bounds=Bounds(lower=0.0, upper=1000.0)
                        )
                    ),
                )
            ),
        )
    )


async def market_data_sender(
    component_id: int,
    chan: Sender[MeterData],
) -> None:
    """Send a message to the channel every 0.2 seconds."""
    while True:
        await chan.send(gen_market_data(component_id))
        await asyncio.sleep(0.2)


async def pv_data_sender(
    component_id: int,
    chan: Sender[MeterData],
) -> None:
    """Send a message to the channel every 0.2 seconds."""
    while True:
        await chan.send(gen_pv_data(component_id))
        await asyncio.sleep(0.2)


async def battery_data_sender(
    component_id: int,
    chan: Sender[BatteryData],
) -> None:
    """Send a message to the channel every 0.2 seconds."""
    while True:
        await chan.send(gen_battery_data(component_id))
        await asyncio.sleep(0.2)


async def inverter_data_sender(
    component_id: int,
    chan: Sender[InverterData],
) -> None:
    """Send a message to the channel every 0.2 seconds."""
    while True:
        await chan.send(gen_inverter_data(component_id))
        await asyncio.sleep(0.2)


class MockClient(MicrogridGrpcClient):
    """Mock used to replace MicrogridApiClient for testing purposes."""

    def __init__(self) -> None:
        """Instantiate a mocked MicrogridApiClient."""
        super().__init__(AsyncMock(), "[::]:11111")

    def component_data_peekable(self) -> None:
        """Mock component_data_peekable method on MicrogridApiClient."""

    async def components(self) -> Set[Component]:
        """Mock components method on MicrogridApiClient."""
        return set()

    async def connections(
        self,
        starts: Optional[Set[int]] = None,
        ends: Optional[Set[int]] = None,
    ) -> Set[Connection]:
        """Mock connections method on MicrogridApiClient."""
        return set()

    def categories_from_id(
        self, component_id: int
    ) -> Tuple[ComponentCategory, Optional[ComponentCategory]]:
        """Derive component categories from component ids.

        Args:
            component_id: component ID

        Returns:
            Pair of component category and optional type of its meter connection
        """
        if component_id < 10000:
            component_category = ComponentCategory.METER
            if component_id % 5 == 0:
                meter_connection = ComponentCategory.PV_ARRAY
            else:
                meter_connection = None
        elif component_id < 20000:
            meter_connection = None
            if ((component_id - 10008) % 2) == 0:
                component_category = ComponentCategory.BATTERY
            else:
                component_category = ComponentCategory.INVERTER
        return component_category, meter_connection

    async def meter_data(
        self,
        component_id: int,
    ) -> Receiver[MeterData]:
        """Mock meter_data method on MicrogridApiClient.

        Args:
            component_id: id of the meter to get data for.

        Returns:
            Receiver[MeterData]: A channel receiver that provides realtime meter
                data.
        """
        if component_id in self._component_streams:
            return self._component_streams[component_id].new_receiver()
        chan: Broadcast[MeterData] = Broadcast(f"{component_id=}")
        component_category, meter_connection = self.categories_from_id(component_id)
        if component_category == ComponentCategory.METER:
            if meter_connection is None:
                asyncio.create_task(market_data_sender(component_id, chan.new_sender()))
            if meter_connection == ComponentCategory.PV_ARRAY:
                asyncio.create_task(pv_data_sender(component_id, chan.new_sender()))
        return chan.new_receiver()

    async def battery_data(
        self,
        component_id: int,
    ) -> Receiver[BatteryData]:
        """Mock battery_data method on MicrogridApiClient.

        Args:
            component_id: id of the inverter to get data for.

        Returns:
            Receiver[BatteryData]: A channel receiver that provides realtime
                battery data.
        """
        if component_id in self._component_streams:
            return self._component_streams[component_id].new_receiver()
        chan: Broadcast[BatteryData] = Broadcast(f"{component_id=}")
        asyncio.create_task(battery_data_sender(component_id, chan.new_sender()))
        return chan.new_receiver()

    async def inverter_data(
        self,
        component_id: int,
    ) -> Receiver[InverterData]:
        """Mock inverter_data method on MicrogridApiClient.

        Args:
            component_id: id of the inverter to get data for.

        Returns:
            Receiver[InverterData]: A channel receiver that provides realtime
                inverter data.
        """
        if component_id in self._component_streams:
            return self._component_streams[component_id].new_receiver()
        chan: Broadcast[InverterData] = Broadcast(f"{component_id=}")
        asyncio.create_task(inverter_data_sender(component_id, chan.new_sender()))
        return chan.new_receiver()


# pylint: disable=too-many-locals
async def benchmark_multiple_batteries(n_bat: int = 10, n_msg: int = 100) -> None:
    """Run benchmark for a scenario with multiple batteries."""
    components = set(
        [
            Component(1, ComponentCategory.GRID),
            Component(3, ComponentCategory.JUNCTION),
            Component(4, ComponentCategory.METER),
            Component(5, ComponentCategory.METER),
            Component(20007, ComponentCategory.PV_ARRAY),
        ]
        + [Component(10008 + bi * 2, ComponentCategory.BATTERY) for bi in range(n_bat)]
        + [Component(10009 + bi * 2, ComponentCategory.INVERTER) for bi in range(n_bat)]
    )
    connections = set(
        [
            Connection(1, 3),
            Connection(3, 4),
            Connection(3, 5),
            Connection(5, 20007),
        ]
        + [Connection(10009 + bi * 2, 10008 + bi * 2) for bi in range(n_bat)]
        + [Connection(3, 10009 + bi * 2) for bi in range(n_bat)]
    )
    # pylint: disable=protected-access
    component_graph = gr._MicrogridComponentGraph(components, connections)

    # pylint: disable=abstract-class-instantiated
    microgrid_client = MockClient()
    client_load_chan = Broadcast[TimeSeriesEntry[float]]("ClientLoad")
    grid_load_chan = Broadcast[TimeSeriesEntry[float]]("GridLoad")
    pv_prod_chan = Broadcast[TimeSeriesEntry[float]]("PVProd")
    ev_chargers_consumption_chan = Broadcast[TimeSeriesEntry[float]](
        "EVChargersConsumption"
    )
    batteries_remaining_energy_chan = Broadcast[TimeSeriesEntry[float]](
        "BatteriesRemainingEnergy"
    )
    batteries_active_power_chan = Broadcast[TimeSeriesEntry[float]](
        "BatteriesActivePower"
    )
    batteries_active_power_bounds_chan = Broadcast[
        TimeSeriesEntry[Tuple[float, float]]
    ]("BatteriesActivePowerBounds")

    n_batteries_remaining_energy_msg = 0
    n_batteries_active_power_msg = 0
    n_batteries_active_power_bounds_msg = 0
    formula_calculator = FormulaCalculator(component_graph)
    _microgrid_actor = MicrogridData(
        microgrid_client,
        component_graph,
        {
            "client_load": client_load_chan.new_sender(),
            "grid_load": grid_load_chan.new_sender(),
            "pv_prod": pv_prod_chan.new_sender(),
            "ev_chargers_consumption": ev_chargers_consumption_chan.new_sender(),
            "batteries_remaining_energy": batteries_remaining_energy_chan.new_sender(),
            "batteries_active_power": batteries_active_power_chan.new_sender(),
            "batteries_active_power_bounds": batteries_active_power_bounds_chan.new_sender(),
        },
        formula_calculator,
    )
    select = Select(
        batteries_remaining_energy=batteries_remaining_energy_chan.new_receiver(),
        batteries_active_power=batteries_active_power_chan.new_receiver(),
        batteries_active_power_bounds=batteries_active_power_bounds_chan.new_receiver(),
    )
    start = time.time()
    while await select.ready():
        if select.batteries_remaining_energy:
            n_batteries_remaining_energy_msg = n_batteries_remaining_energy_msg + 1
        elif select.batteries_active_power:
            n_batteries_active_power_msg = n_batteries_active_power_msg + 1
        elif select.batteries_active_power_bounds:
            n_batteries_active_power_bounds_msg = (
                n_batteries_active_power_bounds_msg + 1
            )
        print(
            time.time() - start,
            {
                "n_batteries_remaining_energy_msg": n_batteries_remaining_energy_msg,
                "n_batteries_active_power_msg": n_batteries_active_power_msg,
            },
        )
        if (
            (n_batteries_remaining_energy_msg >= n_msg)
            & (n_batteries_active_power_msg >= n_msg)
            & (n_batteries_active_power_bounds_msg >= n_msg)
        ):
            print(
                f"total time taken for {n_msg} messages from {n_bat} batteries:",
                time.time() - start,
            )
            break


# pylint: disable=too-many-locals
async def benchmark_multiple_meters(n_meter: int = 5, n_msg: int = 100) -> None:
    """Run benchmark for a scenario with multiple meters."""
    components = set(
        [
            Component(1, ComponentCategory.GRID),
            Component(3, ComponentCategory.JUNCTION),
            Component(4, ComponentCategory.METER),
            Component(10008, ComponentCategory.BATTERY),
            Component(10009, ComponentCategory.INVERTER),
        ]
        + [Component(5 + 5 * mi, ComponentCategory.METER) for mi in range(n_meter)]
        + [Component(20000 + mi, ComponentCategory.PV_ARRAY) for mi in range(n_meter)]
    )
    connections = set(
        [
            Connection(1, 3),
            Connection(3, 4),
            Connection(3, 10009),
            Connection(10009, 10008),
        ]
        + [Connection(5 + 5 * mi, 20000 + mi) for mi in range(n_meter)]
        + [Connection(3, 5 + 5 * mi) for mi in range(n_meter)]
    )
    # pylint: disable=protected-access
    component_graph = gr._MicrogridComponentGraph(components, connections)
    # pylint: disable=abstract-class-instantiated
    microgrid_client = MockClient()
    client_load_chan = Broadcast[TimeSeriesEntry[float]]("ClientLoad")
    grid_load_chan = Broadcast[TimeSeriesEntry[float]]("GridLoad")
    pv_prod_chan = Broadcast[TimeSeriesEntry[float]]("PVProd")
    ev_chargers_consumption_chan = Broadcast[TimeSeriesEntry[float]](
        "EVChargersConsumption"
    )
    batteries_remaining_energy_chan = Broadcast[TimeSeriesEntry[float]](
        "BatteriesRemainingEnergy"
    )
    batteries_active_power_chan = Broadcast[TimeSeriesEntry[float]](
        "BatteriesActivePower"
    )
    batteries_active_power_bounds_chan = Broadcast[
        TimeSeriesEntry[Tuple[float, float]]
    ]("BatteriesActivePowerBounds")
    formula_calculator = FormulaCalculator(component_graph)
    _microgrid_actor = MicrogridData(
        microgrid_client,
        component_graph,
        {
            "client_load": client_load_chan.new_sender(),
            "grid_load": grid_load_chan.new_sender(),
            "pv_prod": pv_prod_chan.new_sender(),
            "ev_chargers_consumption": ev_chargers_consumption_chan.new_sender(),
            "batteries_remaining_energy": batteries_remaining_energy_chan.new_sender(),
            "batteries_active_power": batteries_active_power_chan.new_sender(),
            "batteries_active_power_bounds": batteries_active_power_bounds_chan.new_sender(),
        },
        formula_calculator,
    )
    n_client_load_msg = 0
    n_pv_prod_msg = 0
    select = Select(
        client_load=client_load_chan.new_receiver(),
        pv_prod=pv_prod_chan.new_receiver(),
    )
    start = time.time()
    while await select.ready():
        if _msg := select.client_load:
            n_client_load_msg = n_client_load_msg + 1
        elif _msg := select.pv_prod:
            n_pv_prod_msg = n_pv_prod_msg + 1
        print(
            time.time() - start,
            {"n_client_load_msg": n_client_load_msg, "n_pv_prod_msg": n_pv_prod_msg},
        )
        if (n_client_load_msg >= n_msg) & (n_pv_prod_msg >= n_msg):
            print(
                f"total time taken for {n_msg} messages from {n_meter} meters:",
                time.time() - start,
            )
            break


# pylint: disable=too-many-locals
async def benchmark_multiple_formulas(n_formula: int = 100, n_msg: int = 100) -> None:
    """Run benchmark for a scenario with multiple formulas.

    component_infos = [
        ComponentInfo(4, ComponentCategory.METER, MeterType.MARKET),
        ComponentInfo(5, ComponentCategory.METER, MeterType.PV),
        ComponentInfo(10008, ComponentCategory.BATTERY, None),
        ComponentInfo(10009, ComponentCategory.INVERTER, None),
        ComponentInfo(10010, ComponentCategory.BATTERY, None),
        ComponentInfo(10011, ComponentCategory.INVERTER, None),
    ]
    bat_inv_mappings = {10008: 10009, 10010: 10011}
    """
    components = {
        Component(1, ComponentCategory.GRID),
        Component(3, ComponentCategory.JUNCTION),
        Component(4, ComponentCategory.METER),
        Component(5, ComponentCategory.METER),
        Component(20000, ComponentCategory.PV_ARRAY),
        Component(10008, ComponentCategory.BATTERY),
        Component(10009, ComponentCategory.INVERTER),
        Component(10010, ComponentCategory.BATTERY),
        Component(10011, ComponentCategory.INVERTER),
    }

    connections = {
        Connection(1, 3),
        Connection(3, 4),
        Connection(3, 10009),
        Connection(10009, 10008),
        Connection(3, 10011),
        Connection(10011, 10010),
        Connection(3, 5),
        Connection(5, 20000),
    }
    # pylint: disable=protected-access
    component_graph = gr._MicrogridComponentGraph(components, connections)

    # pylint: disable=abstract-class-instantiated
    microgrid_client = MockClient()
    client_load_chan = Broadcast[TimeSeriesEntry[float]]("ClientLoad")
    grid_load_chan = Broadcast[TimeSeriesEntry[float]]("GridLoad")
    pv_prod_chan = Broadcast[TimeSeriesEntry[float]]("PVProd")
    ev_chargers_consumption_chan = Broadcast[TimeSeriesEntry[float]](
        "EVChargersConsumption"
    )
    batteries_remaining_energy_chan = Broadcast[TimeSeriesEntry[float]](
        "BatteriesRemainingEnergy"
    )
    batteries_active_power_chan = Broadcast[TimeSeriesEntry[float]](
        "BatteriesActivePower"
    )
    batteries_active_power_bounds_chan = Broadcast[
        TimeSeriesEntry[Tuple[float, float]]
    ]("BatteriesActivePowerBounds")
    microgrid_formulas: Dict[str, TimeSeriesFormula[float]] = {
        "f"
        + str(index): TimeSeriesFormula(
            str(index + 1) + " * inverter_10009_active_power"
        )
        for index in range(n_formula)
    }
    senders: Dict[str, Sender[TimeSeriesEntry[Any]]] = {
        "client_load": client_load_chan.new_sender(),
        "grid_load": grid_load_chan.new_sender(),
        "pv_prod": pv_prod_chan.new_sender(),
        "ev_chargers_consumption": ev_chargers_consumption_chan.new_sender(),
        "batteries_remaining_energy": batteries_remaining_energy_chan.new_sender(),
        "batteries_active_power": batteries_active_power_chan.new_sender(),
        "batteries_active_power_bounds": batteries_active_power_bounds_chan.new_sender(),
    }
    for index in range(n_formula):
        locals()["f" + str(index) + "_chan"] = Broadcast[TimeSeriesEntry[Any]](
            "f" + str(index)
        )
        senders["f" + str(index)] = locals()["f" + str(index) + "_chan"].new_sender()
    formula_calculator = FormulaCalculator(
        component_graph, additional_formulas=microgrid_formulas
    )
    _microgrid_actor = MicrogridData(
        microgrid_client, component_graph, senders, formula_calculator
    )
    recv = {}
    for index in range(n_formula):
        recv["f" + str(index)] = locals()["f" + str(index) + "_chan"].new_receiver()

    recv_merged = MergeNamed(**recv)
    n_formula_msg = {"f" + str(fi): 0 for fi in range(n_formula)}
    start = time.time()
    async for msg in recv_merged:
        n_formula_msg[msg[0]] = n_formula_msg[msg[0]] + 1
        print(time.time() - start, n_formula_msg)
        if min(list(n_formula_msg.values())) >= n_msg:
            print(
                f"total time taken for {n_msg} messages from {n_formula} formulas:",
                time.time() - start,
            )
            break


if __name__ == "__main__":
    # asyncio.run(benchmark_multiple_meters())
    # asyncio.run(benchmark_multiple_formulas())
    asyncio.run(benchmark_multiple_batteries())
