# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for battery pool."""
from __future__ import annotations

import asyncio
import dataclasses
import logging
from dataclasses import dataclass, is_dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Generic, Iterator, TypeVar

import async_solipsism
import pytest
from frequenz.channels import Receiver, Sender
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk._internal._constants import (
    MAX_BATTERY_DATA_AGE_SEC,
    WAIT_FOR_COMPONENT_DATA_SEC,
)
from frequenz.sdk.actor import ResamplerConfig
from frequenz.sdk.actor.power_distributing import BatteryStatus
from frequenz.sdk.microgrid.component import ComponentCategory
from frequenz.sdk.timeseries import Energy, Power, Quantity, Sample
from frequenz.sdk.timeseries.battery_pool import BatteryPool, Bound, PowerMetrics
from frequenz.sdk.timeseries.battery_pool._metric_calculator import (
    battery_inverter_mapping,
)

from ...timeseries.mock_microgrid import MockMicrogrid
from ...utils.component_data_streamer import MockComponentDataStreamer
from ...utils.component_data_wrapper import BatteryDataWrapper, InverterDataWrapper
from ...utils.component_graph_utils import (
    ComponentGraphConfig,
    create_component_graph_structure,
)
from ...utils.mock_microgrid_client import MockMicrogridClient

_logger = logging.getLogger(__name__)

# pylint doesn't understand fixtures. It thinks it is redefined name.
# pylint: disable=redefined-outer-name


@pytest.fixture()
def event_loop() -> Iterator[async_solipsism.EventLoop]:
    """Replace the loop with one that doesn't interact with the outside world."""
    loop = async_solipsism.EventLoop()
    yield loop
    loop.close()


def get_components(
    mock_microgrid: MockMicrogridClient, component_category: ComponentCategory
) -> set[int]:
    """Get components of given category from mock microgrid

    Args:
        mock_microgrid: mock microgrid
        component_category: components category

    Returns:
        Components of this category.
    """
    return {
        component.component_id
        for component in mock_microgrid.component_graph.components(
            component_category={component_category}
        )
    }


@dataclass
class SetupArgs:
    """Setup arguments needed to run tests."""

    battery_pool: BatteryPool
    """Battery pool that should be tested."""

    min_update_interval: float
    """Minimal time set in battery_pool."""

    mock_microgrid: MockMicrogridClient
    """Mock microgrid client."""

    streamer: MockComponentDataStreamer
    """Tool for streaming mock component data."""

    battery_status_sender: Sender[BatteryStatus]
    """Channel for sending status of the batteries."""


def create_mock_microgrid(
    mocker: MockerFixture, config: ComponentGraphConfig
) -> MockMicrogridClient:
    """Create mock microgrid and initialize it.

    Args:
        mocker: mocker
        config: required component graph config.

    Returns:
        mock microgrid
    """
    components, connections = create_component_graph_structure(config)
    mock_microgrid = MockMicrogridClient(components, connections)
    mock_microgrid.initialize(mocker)
    return mock_microgrid


@pytest.fixture
async def setup_all_batteries(mocker: MockerFixture) -> AsyncIterator[SetupArgs]:
    """Create battery pool for all batteries in microgrid.

    Also create all needed async tasks and stop them when test ends.
    This will stop tasks even if test fail.

    Args:
        mocker: pytest mocker

    Yields:
        Arguments that are needed in test.
    """
    mock_microgrid = create_mock_microgrid(
        mocker, ComponentGraphConfig(batteries_num=2)
    )
    min_update_interval: float = 0.2
    # pylint: disable=protected-access
    microgrid._data_pipeline._DATA_PIPELINE = None
    microgrid._data_pipeline.initialize(
        ResamplerConfig(resampling_period=timedelta(seconds=min_update_interval))
    )
    streamer = MockComponentDataStreamer(mock_microgrid)

    # We don't use status channel from the sdk interface to limit
    # the scope of this tests. This tests should cover BatteryPool only.
    # We use our own battery status channel, where we easily control set of working
    # batteries.
    battery_pool = microgrid.battery_pool()

    assert microgrid._data_pipeline._DATA_PIPELINE is not None

    args = SetupArgs(
        battery_pool,
        min_update_interval,
        mock_microgrid,
        streamer,
        microgrid._data_pipeline._DATA_PIPELINE._battery_status_channel.new_sender(),
    )

    yield args
    await asyncio.gather(
        *[
            microgrid._data_pipeline._DATA_PIPELINE._stop(),
            battery_pool.stop(),
            streamer.stop(),
        ]
    )
    # pylint: enable=protected-access


@pytest.fixture
async def setup_batteries_pool(mocker: MockerFixture) -> AsyncIterator[SetupArgs]:
    """Create battery pool for subset of microgrid batteries.

    Also create all needed async tasks and stop them when test ends.
    This will stop tasks even if test fail.

    Args:
        mocker: pytest mocker

    Yields:
        Arguments that are needed in test.
    """
    mock_microgrid = create_mock_microgrid(
        mocker, ComponentGraphConfig(batteries_num=4)
    )
    streamer = MockComponentDataStreamer(mock_microgrid)
    min_update_interval: float = 0.2
    # pylint: disable=protected-access
    microgrid._data_pipeline._DATA_PIPELINE = None
    microgrid._data_pipeline.initialize(
        ResamplerConfig(resampling_period=timedelta(seconds=min_update_interval))
    )

    # We don't use status channel from the sdk interface to limit
    # the scope of this tests. This tests should cover BatteryPool only.
    # We use our own battery status channel, where we easily control set of working
    # batteries.
    all_batteries = list(get_components(mock_microgrid, ComponentCategory.BATTERY))

    battery_pool = microgrid.battery_pool(set(all_batteries[:2]))

    assert microgrid._data_pipeline._DATA_PIPELINE is not None

    args = SetupArgs(
        battery_pool,
        min_update_interval,
        mock_microgrid,
        streamer,
        microgrid._data_pipeline._DATA_PIPELINE._battery_status_channel.new_sender(),
    )

    yield args

    await asyncio.gather(
        *[
            microgrid._data_pipeline._DATA_PIPELINE._stop(),
            battery_pool.stop(),
            streamer.stop(),
        ]
    )
    # pylint: enable=protected-access


T = TypeVar("T")


@dataclass
class Scenario(Generic[T]):
    """Single test scenario."""

    component_id: int
    """Which component should send new metrics."""

    new_metrics: dict[str, Any]
    """New metrics for this component."""

    expected_result: T | None
    """Expected aggregated metric update (from the battery pool)."""

    wait_for_result: bool = True
    """Time to wait for the metric update."""


async def run_scenarios(
    scenarios: list[Scenario[T]],
    streamer: MockComponentDataStreamer,
    receiver: Receiver[T | None],
    waiting_time_sec: float,
) -> None:
    """Run all scenarios in the list.

    Args:
        scenarios: List of the scenarios
        streamer: Tool for streaming component metric.
        receiver: Channel for receiving metrics updated form battery pool.
        waiting_time_sec: Time to wait for the metric update.

    Raises:
        TimeoutError: If metric update was not received.
        AssertError: If received metric is not as expected.
    """
    for idx, scenario in enumerate(scenarios):
        # Update data stream
        old_data = streamer.get_current_component_data(scenario.component_id)
        new_data = replace(old_data, **scenario.new_metrics)
        streamer.update_stream(new_data)

        if scenario.wait_for_result is False:
            continue

        # try-except to identify failing test scenario.
        # Wait for result and check if received expected message
        try:
            msg = await asyncio.wait_for(receiver.receive(), timeout=waiting_time_sec)
        except TimeoutError as err:
            _logger.error("Test scenario %d failed with timeout error.", idx)
            raise err

        if scenario.expected_result is None:
            assert msg is None
            continue

        try:
            compare_messages(msg, scenario.expected_result, waiting_time_sec)
        except AssertionError as err:
            _logger.error("Test scenario: %d failed.", idx)
            raise err


async def test_all_batteries_capacity(setup_all_batteries: SetupArgs) -> None:
    """Test capacity metric for battery pool with all components in the microgrid.

    Args:
        setup_all_batteries: Fixture that creates needed microgrid tools.
    """
    await run_capacity_test(setup_all_batteries)


async def test_battery_pool_capacity(setup_batteries_pool: SetupArgs) -> None:
    """Test capacity metric for battery pool with subset of components in the microgrid.

    Args:
        setup_all_batteries: Fixture that creates needed microgrid tools.
    """
    await run_capacity_test(setup_batteries_pool)


async def test_all_batteries_soc(setup_all_batteries: SetupArgs) -> None:
    """Test soc metric for battery pool with all components in the microgrid.

    Args:
        setup_all_batteries: Fixture that creates needed microgrid tools.
    """
    await run_soc_test(setup_all_batteries)


async def test_battery_pool_soc(setup_batteries_pool: SetupArgs) -> None:
    """Test soc metric for battery pool with subset of components in the microgrid.

    Args:
        setup_all_batteries: Fixture that creates needed microgrid tools.
    """
    await run_soc_test(setup_batteries_pool)


async def test_all_batteries_power_bounds(setup_all_batteries: SetupArgs) -> None:
    """Test power bounds metric for battery pool with all components in the microgrid.

    Args:
        setup_all_batteries: Fixture that creates needed microgrid tools.
    """
    await run_power_bounds_test(setup_all_batteries)


async def test_battery_pool_power_bounds(setup_batteries_pool: SetupArgs) -> None:
    """Test power bounds metric for battery pool with subset of components in the microgrid.

    Args:
        setup_all_batteries: Fixture that creates needed microgrid tools.
    """
    await run_power_bounds_test(setup_batteries_pool)


def assert_dataclass(arg: Any) -> None:
    """Raise assert error if argument is not dataclass.

    Args:
        arg: argument to check
    """
    assert is_dataclass(arg), f"Expected dataclass, received {type(arg)}, {arg}"


def compare_messages(msg: Any, expected_msg: Any, time_diff: float) -> None:
    """Compare two dataclass arguments.

    Compare if both are dataclass.
    Compare if all its arguments except `timestamp` are equal.
    Check if timestamp of the msg is not older then `time_diff`.

    Args:
        msg: dataclass to compare
        expected_msg: expected dataclass
        time_diff: maximum time difference between now and the `msg`
    """
    assert_dataclass(msg)
    assert_dataclass(expected_msg)

    msg_dict = dataclasses.asdict(msg)
    expected_dict = dataclasses.asdict(expected_msg)

    assert "timestamp" in msg_dict
    assert "timestamp" in expected_dict

    msg_timestamp = msg_dict.pop("timestamp")
    expected_dict.pop("timestamp")

    assert msg_dict == expected_dict

    diff = datetime.now(tz=timezone.utc) - msg_timestamp
    assert diff.total_seconds() < time_diff


async def run_test_battery_status_channel(  # pylint: disable=too-many-arguments
    battery_status_sender: Sender[BatteryStatus],
    battery_pool_metric_receiver: Receiver[T],
    all_batteries: set[int],
    batteries_in_pool: list[int],
    waiting_time_sec: float,
    all_pool_result: T,
    only_first_battery_result: T,
) -> None:
    """Change status of the batteries and check how battery pool is reacting.

    Args:
        battery_status_sender: Sender to send status of the batteries.
        battery_pool_metric_receiver: receiver to receive metric updates
        all_batteries: list of all batteries in the microgrid
        batteries_in_pool: list of batteries in the pool
        waiting_time_sec: how long wait for the result
        all_pool_result: result metric if all batteries in pool are working
        only_first_battery_result: result metric if only first battery in pool is
            working
    """
    assert len(batteries_in_pool) == 2

    # Second battery stopped working.
    working = all_batteries - {batteries_in_pool[1]}
    await battery_status_sender.send(
        BatteryStatus(working=working, uncertain={batteries_in_pool[1]})
    )
    msg = await asyncio.wait_for(
        battery_pool_metric_receiver.receive(), timeout=waiting_time_sec
    )
    compare_messages(msg, only_first_battery_result, waiting_time_sec)

    # All batteries stopped working data
    working -= {batteries_in_pool[0]}
    await battery_status_sender.send(BatteryStatus(working=working, uncertain=set()))
    msg = await asyncio.wait_for(
        battery_pool_metric_receiver.receive(), timeout=waiting_time_sec
    )
    assert msg is None

    # One battery in uncertain state.
    await battery_status_sender.send(
        BatteryStatus(working=working, uncertain={batteries_in_pool[0]})
    )
    msg = await asyncio.wait_for(
        battery_pool_metric_receiver.receive(), timeout=waiting_time_sec
    )
    compare_messages(msg, only_first_battery_result, waiting_time_sec)

    # All batteries are working again
    await battery_status_sender.send(
        BatteryStatus(working=set(all_batteries), uncertain=set())
    )
    msg = await asyncio.wait_for(
        battery_pool_metric_receiver.receive(), timeout=waiting_time_sec
    )
    compare_messages(msg, all_pool_result, waiting_time_sec)


async def test_battery_pool_power(mocker: MockerFixture) -> None:
    """Test `BatteryPool.{,production,consumption}_power` methods."""
    mockgrid = MockMicrogrid(grid_side_meter=True)
    mockgrid.add_batteries(2)
    await mockgrid.start_mock_datapipeline(mocker)

    battery_pool = microgrid.battery_pool()
    power_receiver = battery_pool.power.new_receiver()
    consumption_receiver = battery_pool.consumption_power.new_receiver()
    production_receiver = battery_pool.production_power.new_receiver()

    await mockgrid.mock_data.send_bat_inverter_power([2.0, 3.0])
    assert (await power_receiver.receive()).value == Power(5.0)
    assert (await consumption_receiver.receive()).value == Power(5.0)
    assert (await production_receiver.receive()).value == Power(0.0)

    await mockgrid.mock_data.send_bat_inverter_power([-2.0, -5.0])
    assert (await power_receiver.receive()).value == Power(-7.0)
    assert (await consumption_receiver.receive()).value == Power(0.0)
    assert (await production_receiver.receive()).value == Power(7.0)

    await mockgrid.mock_data.send_bat_inverter_power([2.0, -5.0])
    assert (await power_receiver.receive()).value == Power(-3.0)
    assert (await consumption_receiver.receive()).value == Power(0.0)
    assert (await production_receiver.receive()).value == Power(3.0)

    await mockgrid.cleanup()


async def run_capacity_test(setup_args: SetupArgs) -> None:
    """Test if capacity metric is working as expected.

    Args:
        setup_args: Needed sdk tools and tools for mocking microgrid.
    """
    battery_pool = setup_args.battery_pool
    mock_microgrid = setup_args.mock_microgrid
    streamer = setup_args.streamer
    battery_status_sender = setup_args.battery_status_sender

    # All batteries are working and sending data. Not just the ones in the
    # battery pool.
    all_batteries = get_components(mock_microgrid, ComponentCategory.BATTERY)
    await battery_status_sender.send(
        BatteryStatus(working=all_batteries, uncertain=set())
    )

    for battery_id in all_batteries:
        # Sampling rate choose to reflect real application.
        streamer.start_streaming(
            BatteryDataWrapper(
                component_id=battery_id,
                timestamp=datetime.now(tz=timezone.utc),
                capacity=50,
                soc_lower_bound=25,
                soc_upper_bound=75,
            ),
            sampling_rate=0.05,
        )

    capacity_receiver = battery_pool.capacity.new_receiver(maxsize=50)

    # First metrics delivers slower because of the startup delay in the pool.
    msg = await asyncio.wait_for(
        capacity_receiver.receive(), timeout=WAIT_FOR_COMPONENT_DATA_SEC + 0.2
    )
    now = datetime.now(tz=timezone.utc)
    expected = Sample[Energy](
        timestamp=now,
        value=Energy.from_watt_hours(
            50.0
        ),  # 50% of 50 kWh + 50% of 50 kWh = 25 + 25 = 50 kWh
    )
    compare_messages(msg, expected, WAIT_FOR_COMPONENT_DATA_SEC + 0.2)

    batteries_in_pool = list(battery_pool.battery_ids)
    scenarios: list[Scenario[Sample[Energy]]] = [
        Scenario(
            batteries_in_pool[0],
            {"capacity": 90.0},
            Sample(
                now,
                Energy.from_watt_hours(
                    70.0
                ),  # 50% of 90 kWh + 50% of 50 kWh = 45 + 25 = 70 kWh
            ),
        ),
        Scenario(
            batteries_in_pool[1],
            {"soc_lower_bound": 0.0, "soc_upper_bound": 90.0},
            Sample(
                now,
                Energy.from_watt_hours(
                    90.0
                ),  # 50% of 90 kWh + 90% of 50 kWh = 45 + 45 = 90 kWh
            ),
        ),
        Scenario(
            batteries_in_pool[0],
            {"capacity": 0.0, "soc_lower_bound": 0.0},
            Sample(
                now,
                Energy.from_watt_hours(
                    45.0
                ),  # 75% of 0 kWh + 90% of 50 kWh = 0 + 45 = 45 kWh
            ),
        ),
        # Test zero division error
        Scenario(
            batteries_in_pool[1],
            {"capacity": 0.0},
            Sample(
                now,
                Energy.from_watt_hours(
                    0.0
                ),  # 75% of 0 kWh + 90% of 0 kWh = 0 + 0 = 0 kWh
            ),
        ),
        Scenario(
            batteries_in_pool[1],
            {"capacity": 50.0},
            Sample(
                now,
                Energy.from_watt_hours(
                    45.0
                ),  # 75% of 0 kWh + 90% of 50 kWh = 0 + 45 = 45 kWh
            ),
        ),
        Scenario(
            batteries_in_pool[1],
            {"soc_upper_bound": float("NaN")},
            Sample(
                now,
                Energy.from_watt_hours(
                    0.0
                ),  # 75% of 0 kWh + 90% of 0 kWh = 0 + 0 = 0 kWh
            ),
        ),
        Scenario(
            batteries_in_pool[0],
            {"capacity": 30.0, "soc_lower_bound": 20.0, "soc_upper_bound": 90.0},
            Sample(
                now,
                Energy.from_watt_hours(
                    21.0
                ),  # 70% of 30 kWh + 90% of 0 kWh = 21 + 0 = 21 kWh
            ),
        ),
        Scenario(
            batteries_in_pool[1],
            {"capacity": 200.0, "soc_lower_bound": 20.0, "soc_upper_bound": 90.0},
            Sample(
                now,
                Energy.from_watt_hours(
                    161.0
                ),  # 70% of 30 kWh + 70% of 200 kWh = 21 + 140 = 161 kWh
            ),
        ),
        Scenario(
            batteries_in_pool[1],
            {"capacity": float("NaN")},
            Sample(
                now,
                Energy.from_watt_hours(
                    21.0
                ),  # 70% of 30 kWh + 70% of 0 kWh = 21 + 0 = 21 kWh
            ),
        ),
        Scenario(
            batteries_in_pool[1],
            {"capacity": 200.0},
            Sample(
                now,
                Energy.from_watt_hours(
                    161.0
                ),  # 70% of 30 kWh + 70% of 200 kWh = 21 + 140 = 161 kWh
            ),
        ),
    ]

    waiting_time_sec = setup_args.min_update_interval + 0.02
    await run_scenarios(scenarios, streamer, capacity_receiver, waiting_time_sec)

    await run_test_battery_status_channel(
        battery_status_sender=battery_status_sender,
        battery_pool_metric_receiver=capacity_receiver,
        all_batteries=all_batteries,
        batteries_in_pool=batteries_in_pool,
        waiting_time_sec=waiting_time_sec,
        all_pool_result=Sample(now, Energy.from_watt_hours(161.0)),
        only_first_battery_result=Sample(now, Energy.from_watt_hours(21.0)),
    )

    # One battery stopped sending data.
    await streamer.stop_streaming(batteries_in_pool[1])
    await asyncio.sleep(MAX_BATTERY_DATA_AGE_SEC + 0.2)
    msg = await asyncio.wait_for(capacity_receiver.receive(), timeout=waiting_time_sec)
    compare_messages(msg, Sample(now, Energy.from_watt_hours(21.0)), 0.2)

    # All batteries stopped sending data.
    await streamer.stop_streaming(batteries_in_pool[0])
    await asyncio.sleep(MAX_BATTERY_DATA_AGE_SEC + 0.2)
    msg = await asyncio.wait_for(capacity_receiver.receive(), timeout=waiting_time_sec)
    assert msg is None

    # One battery started sending data.
    latest_data = streamer.get_current_component_data(batteries_in_pool[0])
    streamer.start_streaming(latest_data, sampling_rate=0.1)
    msg = await asyncio.wait_for(capacity_receiver.receive(), timeout=waiting_time_sec)
    compare_messages(msg, Sample(now, Energy.from_watt_hours(21.0)), 0.2)


async def run_soc_test(setup_args: SetupArgs) -> None:
    """Test if soc metric is working as expected.

    Args:
        setup_args: Needed sdk tools and tools for mocking microgrid.
    """
    battery_pool = setup_args.battery_pool
    mock_microgrid = setup_args.mock_microgrid
    streamer = setup_args.streamer
    battery_status_sender = setup_args.battery_status_sender

    # All batteries are working and sending data. Not just the ones in the
    # battery pool.
    all_batteries = get_components(mock_microgrid, ComponentCategory.BATTERY)
    await battery_status_sender.send(
        BatteryStatus(working=all_batteries, uncertain=set())
    )

    for battery_id in all_batteries:
        # Sampling rate choose to reflect real application.
        streamer.start_streaming(
            BatteryDataWrapper(
                component_id=battery_id,
                timestamp=datetime.now(tz=timezone.utc),
                capacity=50,
                soc=30,
                soc_lower_bound=25,
                soc_upper_bound=75,
            ),
            sampling_rate=0.05,
        )

    receiver = battery_pool.soc.new_receiver(maxsize=50)

    # First metrics delivers slower because of the startup delay in the pool.
    msg = await asyncio.wait_for(
        receiver.receive(), timeout=WAIT_FOR_COMPONENT_DATA_SEC + 0.2
    )
    now = datetime.now(tz=timezone.utc)
    expected = Sample(
        timestamp=now,
        value=Quantity(10.0),
    )
    compare_messages(msg, expected, WAIT_FOR_COMPONENT_DATA_SEC + 0.2)

    batteries_in_pool = list(battery_pool.battery_ids)
    scenarios: list[Scenario[Sample[Quantity]]] = [
        Scenario(
            batteries_in_pool[0],
            {"capacity": 150, "soc": 10},
            Sample(now, Quantity(2.5)),
        ),
        Scenario(
            batteries_in_pool[0],
            {
                "soc_lower_bound": 0.0,
            },
            Sample(now, Quantity(12.727272727272727)),
        ),
        # If NaN, then not include that battery in the metric.
        Scenario(
            batteries_in_pool[0],
            {"soc_upper_bound": float("NaN")},
            Sample(now, Quantity(10.0)),
        ),
        # All batteries are sending NaN, can't calculate SoC so we should send None
        Scenario(
            batteries_in_pool[1],
            {"soc": float("NaN")},
            None,
        ),
        Scenario(
            batteries_in_pool[1],
            {"soc": 30},
            Sample(now, Quantity(10.0)),
        ),
        # Final metric didn't change, so nothing should be received.
        Scenario(
            batteries_in_pool[0],
            {"capacity": 0, "soc_lower_bound": 10.0, "soc_upper_bound": 100.0},
            None,
            wait_for_result=False,
        ),
        # Test zero division error
        Scenario(
            batteries_in_pool[1],
            {"capacity": 0},
            Sample(now, Quantity(0.0)),
        ),
        Scenario(
            batteries_in_pool[0],
            {"capacity": 50, "soc": 55.0},
            Sample(now, Quantity(50.0)),
        ),
        Scenario(
            batteries_in_pool[1],
            {"capacity": 150},
            Sample(now, Quantity(25.0)),
        ),
    ]

    waiting_time_sec = setup_args.min_update_interval + 0.2
    await run_scenarios(scenarios, streamer, receiver, waiting_time_sec)

    await run_test_battery_status_channel(
        battery_status_sender=battery_status_sender,
        battery_pool_metric_receiver=receiver,
        all_batteries=all_batteries,
        batteries_in_pool=batteries_in_pool,
        waiting_time_sec=waiting_time_sec,
        all_pool_result=Sample(now, Quantity(25.0)),
        only_first_battery_result=Sample(now, Quantity(50.0)),
    )

    # One battery stopped sending data.
    await streamer.stop_streaming(batteries_in_pool[1])
    await asyncio.sleep(MAX_BATTERY_DATA_AGE_SEC + 0.2)
    msg = await asyncio.wait_for(receiver.receive(), timeout=waiting_time_sec)
    compare_messages(msg, Sample(now, Quantity(50.0)), 0.2)

    # All batteries stopped sending data.
    await streamer.stop_streaming(batteries_in_pool[0])
    await asyncio.sleep(MAX_BATTERY_DATA_AGE_SEC + 0.2)
    msg = await asyncio.wait_for(receiver.receive(), timeout=waiting_time_sec)
    assert msg is None

    # One battery started sending data.
    latest_data = streamer.get_current_component_data(batteries_in_pool[0])
    streamer.start_streaming(latest_data, sampling_rate=0.1)
    msg = await asyncio.wait_for(receiver.receive(), timeout=waiting_time_sec)
    compare_messages(msg, Sample(now, Quantity(50.0)), 0.2)


async def run_power_bounds_test(  # pylint: disable=too-many-locals
    setup_args: SetupArgs,
) -> None:
    """Test if power bounds metric is working as expected.

    Args:
        setup_args: Needed sdk tools and tools for mocking microgrid.
    """
    battery_pool = setup_args.battery_pool
    mock_microgrid = setup_args.mock_microgrid
    streamer = setup_args.streamer
    battery_status_sender = setup_args.battery_status_sender

    # All batteries are working and sending data. Not just the ones in the
    # battery pool.
    all_batteries = get_components(mock_microgrid, ComponentCategory.BATTERY)
    await battery_status_sender.send(
        BatteryStatus(working=all_batteries, uncertain=set())
    )
    bat_inv_map = battery_inverter_mapping(all_batteries)

    for battery_id, inverter_id in bat_inv_map.items():
        # Sampling rate choose to reflect real application.
        streamer.start_streaming(
            BatteryDataWrapper(
                component_id=battery_id,
                timestamp=datetime.now(tz=timezone.utc),
                power_lower_bound=-1000,
                power_upper_bound=5000,
            ),
            sampling_rate=0.05,
        )
        streamer.start_streaming(
            InverterDataWrapper(
                component_id=inverter_id,
                timestamp=datetime.now(tz=timezone.utc),
                active_power_lower_bound=-900,
                active_power_upper_bound=6000,
            ),
            sampling_rate=0.1,
        )

    receiver = battery_pool.power_bounds.new_receiver(maxsize=50)

    # First metrics delivers slower because of the startup delay in the pool.
    msg = await asyncio.wait_for(
        receiver.receive(), timeout=WAIT_FOR_COMPONENT_DATA_SEC + 0.2
    )
    now = datetime.now(tz=timezone.utc)
    expected = PowerMetrics(
        timestamp=now,
        supply_bound=Bound(-1800, 0),
        consume_bound=Bound(0, 10000),
    )
    compare_messages(msg, expected, WAIT_FOR_COMPONENT_DATA_SEC + 0.2)

    batteries_in_pool = list(battery_pool.battery_ids)
    scenarios: list[Scenario[PowerMetrics]] = [
        Scenario(
            bat_inv_map[batteries_in_pool[0]],
            {"active_power_lower_bound": -100},
            PowerMetrics(now, Bound(-1000, 0), Bound(0, 10000)),
        ),
        # Inverter bound changed, but metric result should not change.
        Scenario(
            component_id=bat_inv_map[batteries_in_pool[0]],
            new_metrics={"active_power_upper_bound": 9000},
            expected_result=None,
            wait_for_result=False,
        ),
        Scenario(
            batteries_in_pool[0],
            {"power_lower_bound": 0, "power_upper_bound": 4000},
            PowerMetrics(now, Bound(-900, 0), Bound(0, 9000)),
        ),
        Scenario(
            batteries_in_pool[1],
            {"power_lower_bound": -10, "power_upper_bound": 200},
            PowerMetrics(now, Bound(-10, 0), Bound(0, 4200)),
        ),
        # Test 2 things:
        # 1. Battery is sending upper bounds=NaN, use only inverter upper bounds
        # 2. Upper and lower bounds should be independent.
        # Setting upper bound to NaN should not influence lower bound
        Scenario(
            batteries_in_pool[0],
            {"power_lower_bound": -50, "power_upper_bound": float("NaN")},
            PowerMetrics(now, Bound(-60, 0), Bound(0, 9200)),
        ),
        Scenario(
            bat_inv_map[batteries_in_pool[0]],
            {
                "active_power_lower_bound": float("NaN"),
                "active_power_upper_bound": float("NaN"),
            },
            PowerMetrics(now, Bound(-60, 0), Bound(0, 200)),
        ),
        Scenario(
            batteries_in_pool[0],
            {"power_lower_bound": float("NaN")},
            PowerMetrics(now, Bound(-10, 0), Bound(0, 200)),
        ),
        Scenario(
            batteries_in_pool[1],
            {
                "power_lower_bound": -100,
                "power_upper_bound": float("NaN"),
            },
            PowerMetrics(now, Bound(-100, 0), Bound(0, 6000)),
        ),
        Scenario(
            bat_inv_map[batteries_in_pool[1]],
            {
                "active_power_lower_bound": float("NaN"),
                "active_power_upper_bound": float("NaN"),
            },
            PowerMetrics(now, Bound(-100, 0), Bound(0, 0)),
        ),
        # All components are sending NaN, can't calculate bounds
        Scenario(
            batteries_in_pool[1],
            {
                "power_lower_bound": float("NaN"),
                "power_upper_bound": float("NaN"),
            },
            None,
        ),
        Scenario(
            batteries_in_pool[0],
            {"power_lower_bound": -100, "power_upper_bound": 100},
            PowerMetrics(now, Bound(-100, 0), Bound(0, 100)),
        ),
        Scenario(
            bat_inv_map[batteries_in_pool[1]],
            {
                "active_power_lower_bound": -400,
                "active_power_upper_bound": 400,
            },
            PowerMetrics(now, Bound(-500, 0), Bound(0, 500)),
        ),
        Scenario(
            batteries_in_pool[1],
            {
                "power_lower_bound": -300,
                "power_upper_bound": 700,
            },
            PowerMetrics(now, Bound(-400, 0), Bound(0, 500)),
        ),
        Scenario(
            bat_inv_map[batteries_in_pool[0]],
            {
                "active_power_lower_bound": -200,
                "active_power_upper_bound": 50,
            },
            PowerMetrics(now, Bound(-400, 0), Bound(0, 450)),
        ),
    ]

    waiting_time_sec = setup_args.min_update_interval + 0.02
    await run_scenarios(scenarios, streamer, receiver, waiting_time_sec)

    await run_test_battery_status_channel(
        battery_status_sender=battery_status_sender,
        battery_pool_metric_receiver=receiver,
        all_batteries=all_batteries,
        batteries_in_pool=batteries_in_pool,
        waiting_time_sec=waiting_time_sec,
        all_pool_result=PowerMetrics(now, Bound(-400, 0), Bound(0, 450)),
        only_first_battery_result=PowerMetrics(now, Bound(-100, 0), Bound(0, 50)),
    )

    # One battery stopped sending data, inverter data should be used.
    await streamer.stop_streaming(batteries_in_pool[1])
    await asyncio.sleep(MAX_BATTERY_DATA_AGE_SEC + 0.2)
    msg = await asyncio.wait_for(receiver.receive(), timeout=waiting_time_sec)
    compare_messages(msg, PowerMetrics(now, Bound(-500, 0), Bound(0, 450)), 0.2)

    # All batteries stopped sending data, use inverters only.
    await streamer.stop_streaming(batteries_in_pool[0])
    await asyncio.sleep(MAX_BATTERY_DATA_AGE_SEC + 0.2)
    msg = await asyncio.wait_for(receiver.receive(), timeout=waiting_time_sec)
    compare_messages(msg, PowerMetrics(now, Bound(-600, 0), Bound(0, 450)), 0.2)

    # One inverter stopped sending data, use one remaining inverter
    await streamer.stop_streaming(bat_inv_map[batteries_in_pool[0]])
    await asyncio.sleep(MAX_BATTERY_DATA_AGE_SEC + 0.2)
    msg = await asyncio.wait_for(receiver.receive(), timeout=waiting_time_sec)
    compare_messages(msg, PowerMetrics(now, Bound(-400, 0), Bound(0, 400)), 0.2)

    # All components stopped sending data, we can assume that power bounds are 0
    await streamer.stop_streaming(bat_inv_map[batteries_in_pool[1]])
    await asyncio.sleep(MAX_BATTERY_DATA_AGE_SEC + 0.2)
    msg = await asyncio.wait_for(receiver.receive(), timeout=waiting_time_sec)
    assert msg is None

    # One battery started sending data.
    latest_data = streamer.get_current_component_data(batteries_in_pool[0])
    streamer.start_streaming(latest_data, sampling_rate=0.1)
    msg = await asyncio.wait_for(receiver.receive(), timeout=waiting_time_sec)
    compare_messages(msg, PowerMetrics(now, Bound(-100, 0), Bound(0, 100)), 0.2)
