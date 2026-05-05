# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for battery pool."""

# pylint: disable=too-many-lines


import asyncio
import dataclasses
import logging
import math
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack
from dataclasses import dataclass, is_dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Any, Generic, TypeVar
from unittest.mock import MagicMock

import async_solipsism
import pytest
import time_machine
from frequenz.channels import Broadcast, Receiver, Sender
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.component import Battery, Component
from frequenz.quantities import Energy, Percentage, Power, Temperature
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid, timeseries
from frequenz.sdk._internal._channels import ChannelRegistry
from frequenz.sdk._internal._constants import (
    MAX_BATTERY_DATA_AGE_SEC,
    WAIT_FOR_COMPONENT_DATA_SEC,
)
from frequenz.sdk.microgrid._power_distributing import ComponentPoolStatus
from frequenz.sdk.microgrid._power_distributing._component_managers._battery_manager import (
    _get_battery_inverter_mappings,
)
from frequenz.sdk.microgrid._power_managing import ReportRequest, _Report
from frequenz.sdk.timeseries import Bounds, ResamplerConfig2, Sample
from frequenz.sdk.timeseries._base_types import SystemBounds
from frequenz.sdk.timeseries.battery_pool import BatteryPool
from frequenz.sdk.timeseries.battery_pool._battery_pool_reference_store import (
    BatteryPoolReferenceStore,
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


def _new_power_status_report(target_power_watts: float) -> _Report:
    """Create a distinct report for power status assertions."""
    target_power = Power.from_watts(target_power_watts)
    return _Report(
        target_power=target_power,
        _inclusion_bounds=timeseries.Bounds(target_power, target_power),
        _exclusion_bounds=None,
    )


@pytest.fixture(autouse=True)
def event_loop_policy() -> async_solipsism.EventLoopPolicy:
    """Return an event loop policy that uses the async solipsism event loop."""
    return async_solipsism.EventLoopPolicy()


def get_components(
    mock_microgrid: MockMicrogridClient, component_type: type[Component]
) -> set[ComponentId]:
    """Get components of given type from mock microgrid.

    Args:
        mock_microgrid: mock microgrid
        component_type: components type

    Returns:
        Components of this type.
    """
    return {
        component.id
        for component in mock_microgrid.component_graph.components(
            matching_types=[component_type]
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

    battery_status_sender: Sender[ComponentPoolStatus]
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
    await microgrid._data_pipeline.initialize(
        ResamplerConfig2(resampling_period=timedelta(seconds=min_update_interval))
    )
    streamer = MockComponentDataStreamer(mock_microgrid)

    # We don't use status channel from the sdk interface to limit
    # the scope of this tests. This tests should cover BatteryPool only.
    # We use our own battery status channel, where we easily control set of working
    # batteries.
    battery_pool = microgrid.new_battery_pool(priority=5)

    dp = microgrid._data_pipeline._DATA_PIPELINE
    assert dp is not None

    args = SetupArgs(
        battery_pool,
        min_update_interval,
        mock_microgrid,
        streamer,
        dp._battery_power_wrapper.status_channel.new_sender(),
    )

    yield args
    await asyncio.gather(
        *[
            dp._stop(),
            battery_pool._pool_ref_store.stop(),
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
    await microgrid._data_pipeline.initialize(
        ResamplerConfig2(resampling_period=timedelta(seconds=min_update_interval))
    )

    # We don't use status channel from the sdk interface to limit
    # the scope of this tests. This tests should cover BatteryPool only.
    # We use our own battery status channel, where we easily control set of working
    # batteries.
    all_batteries = get_components(mock_microgrid, Battery)

    # This is a hack because these tests used to rely on the order in which components
    # are returned from the component graph using `all_batteries[:2]` to get the first 2
    # batteries. But the component graph returns a set, which doesn't have any ordering
    # guarantees, so if the Python implementation changes, the hashing can change and
    # this resulting order could be different, breaking the tests. This is why we are
    # now specifying IDs to use explicitly (these are the IDs that were used when the
    # tests were written). This can also change in the future if generator of mock
    # components changes, as it might produce different IDs, so this solution is also
    # not bullet-proof.
    assert ComponentId(8) in all_batteries
    assert ComponentId(11) in all_batteries

    battery_pool = microgrid.new_battery_pool(
        priority=5, component_ids=set([ComponentId(8), ComponentId(11)])
    )

    dp = microgrid._data_pipeline._DATA_PIPELINE
    assert dp is not None

    args = SetupArgs(
        battery_pool,
        min_update_interval,
        mock_microgrid,
        streamer,
        dp._battery_power_wrapper.status_channel.new_sender(),
    )

    yield args

    await asyncio.gather(
        *[
            dp._stop(),
            battery_pool._pool_ref_store.stop(),
            streamer.stop(),
        ]
    )
    # pylint: enable=protected-access


T = TypeVar("T")


@dataclass
class Scenario(Generic[T]):
    """Single test scenario."""

    component_id: ComponentId
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
    receiver: Receiver[T],
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
        AssertionError: If received metric is not as expected.
    """
    for idx, scenario in enumerate(scenarios):
        _logger.info("Testing scenario: %d", idx)
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
        except TimeoutError:
            _logger.error("Test scenario %d failed with timeout error.", idx)
            raise

        if scenario.expected_result is None:
            assert msg is None
            continue

        try:
            compare_messages(msg, scenario.expected_result)
        except AssertionError:
            _logger.error("Test scenario: %d failed.", idx)
            raise


async def test_all_batteries_capacity(
    fake_time: time_machine.Coordinates, setup_all_batteries: SetupArgs
) -> None:
    """Test capacity metric for battery pool with all components in the microgrid.

    Args:
        fake_time: The time machine fake time argument.
        setup_all_batteries: Fixture that creates needed microgrid tools.
    """
    await run_capacity_test(fake_time, setup_all_batteries)


async def test_battery_pool_capacity(
    fake_time: time_machine.Coordinates, setup_batteries_pool: SetupArgs
) -> None:
    """Test capacity metric for battery pool with subset of components in the microgrid.

    Args:
        fake_time: The time machine fake time argument.
        setup_batteries_pool: Fixture that creates needed microgrid tools.
    """
    await run_capacity_test(fake_time, setup_batteries_pool)


async def test_all_batteries_soc(setup_all_batteries: SetupArgs) -> None:
    """Test soc metric for battery pool with all components in the microgrid.

    Args:
        setup_all_batteries: Fixture that creates needed microgrid tools.
    """
    await run_soc_test(setup_all_batteries)


async def test_battery_pool_soc(setup_batteries_pool: SetupArgs) -> None:
    """Test soc metric for battery pool with subset of components in the microgrid.

    Args:
        setup_batteries_pool: Fixture that creates needed microgrid tools.
    """
    await run_soc_test(setup_batteries_pool)


@pytest.mark.skip(
    reason="Bounds are not calculated properly, see "
    "https://github.com/frequenz-floss/frequenz-sdk-python/issues/1180"
)
async def test_all_batteries_power_bounds(setup_all_batteries: SetupArgs) -> None:
    """Test power bounds metric for battery pool with all components in the microgrid.

    Args:
        setup_all_batteries: Fixture that creates needed microgrid tools.
    """
    await run_power_bounds_test(setup_all_batteries)


@pytest.mark.skip(
    reason="Bounds are not calculated properly, see "
    "https://github.com/frequenz-floss/frequenz-sdk-python/issues/1180"
)
async def test_battery_pool_power_bounds(setup_batteries_pool: SetupArgs) -> None:
    """Test power bounds metric for battery pool with subset of components in the microgrid.

    Args:
        setup_batteries_pool: Fixture that creates needed microgrid tools.
    """
    await run_power_bounds_test(setup_batteries_pool)


async def test_all_batteries_temperature(setup_all_batteries: SetupArgs) -> None:
    """Test temperature for battery pool with all components in the microgrid.

    Args:
        setup_all_batteries: Fixture that creates needed microgrid tools.
    """
    await run_temperature_test(setup_all_batteries)


async def test_battery_pool_temperature(setup_batteries_pool: SetupArgs) -> None:
    """Test temperature for battery pool with subset of components in the microgrid.

    Args:
        setup_batteries_pool: Fixture that creates needed microgrid tools.
    """
    await run_temperature_test(setup_batteries_pool)


def assert_dataclass(arg: Any) -> None:
    """Raise assert error if argument is not dataclass.

    Args:
        arg: argument to check
    """
    assert is_dataclass(arg), f"Expected dataclass, received {type(arg)}, {arg}"


def compare_messages(msg: Any, expected_msg: Any) -> None:
    """Compare two dataclass arguments.

    Compare if both are dataclass.
    Compare if all its arguments except `timestamp` are equal.

    Args:
        msg: dataclass to compare
        expected_msg: expected dataclass
    """
    assert_dataclass(msg)
    assert_dataclass(expected_msg)

    msg_dict = dataclasses.asdict(msg)
    expected_dict = dataclasses.asdict(expected_msg)

    assert "timestamp" in msg_dict
    assert "timestamp" in expected_dict

    msg_dict.pop("timestamp")
    expected_dict.pop("timestamp")
    assert msg_dict == expected_dict


# pylint: disable-next=too-many-arguments,too-many-positional-arguments
async def run_test_battery_status_channel(
    battery_status_sender: Sender[ComponentPoolStatus],
    battery_pool_metric_receiver: Receiver[T],
    all_batteries: set[ComponentId],
    batteries_in_pool: list[ComponentId],
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

    Raises:
        ValueError: If the received message is not an instance of PowerMetrics or Sample.
    """
    assert len(batteries_in_pool) == 2

    # Second battery stopped working.
    working = all_batteries - {batteries_in_pool[1]}
    await battery_status_sender.send(
        ComponentPoolStatus(working=working, uncertain={batteries_in_pool[1]})
    )
    msg = await asyncio.wait_for(
        battery_pool_metric_receiver.receive(), timeout=waiting_time_sec
    )
    compare_messages(msg, only_first_battery_result)

    # All batteries stopped working data
    working -= {batteries_in_pool[0]}
    await battery_status_sender.send(
        ComponentPoolStatus(working=working, uncertain=set())
    )
    msg = await asyncio.wait_for(
        battery_pool_metric_receiver.receive(), timeout=waiting_time_sec
    )
    if isinstance(msg, SystemBounds):
        assert msg.inclusion_bounds is None
        assert msg.exclusion_bounds is None
    elif isinstance(msg, Sample):
        assert msg.value is None
    else:
        raise ValueError("Expected an instance of PowerMetrics or Sample")

    # One battery in uncertain state.
    await battery_status_sender.send(
        ComponentPoolStatus(working=working, uncertain={batteries_in_pool[0]})
    )
    msg = await asyncio.wait_for(
        battery_pool_metric_receiver.receive(), timeout=waiting_time_sec
    )
    compare_messages(msg, only_first_battery_result)

    # All batteries are working again
    await battery_status_sender.send(
        ComponentPoolStatus(working=set(all_batteries), uncertain=set())
    )
    msg = await asyncio.wait_for(
        battery_pool_metric_receiver.receive(), timeout=waiting_time_sec
    )
    compare_messages(msg, all_pool_result)


async def test_battery_pool_power(mocker: MockerFixture) -> None:
    """Test `BatteryPool.power` method."""
    mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
    mockgrid.add_batteries(2)

    async with mockgrid, AsyncExitStack() as stack:
        battery_pool = microgrid.new_battery_pool(priority=5)
        stack.push_async_callback(battery_pool.stop)
        power_receiver = battery_pool.power.new_receiver()

        # send meter power [grid_meter, battery1_meter, battery2_meter]
        await mockgrid.mock_resampler.send_meter_power([100.0, 20.0, 30.0])
        assert (await power_receiver.receive()).value == Power.from_watts(50.0)


async def run_capacity_test(  # pylint: disable=too-many-locals
    fake_time: time_machine.Coordinates, setup_args: SetupArgs
) -> None:
    """Test if capacity metric is working as expected.

    Args:
        fake_time: The time machine fake time argument.
        setup_args: Needed sdk tools and tools for mocking microgrid.
    """
    battery_pool = setup_args.battery_pool
    mock_microgrid = setup_args.mock_microgrid
    streamer = setup_args.streamer
    battery_status_sender = setup_args.battery_status_sender

    # All batteries are working and sending data. Not just the ones in the
    # battery pool.
    all_batteries = get_components(mock_microgrid, Battery)
    await battery_status_sender.send(
        ComponentPoolStatus(working=all_batteries, uncertain=set())
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

    capacity_receiver = battery_pool.capacity.new_receiver(limit=50)

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
    compare_messages(msg, expected)

    batteries_in_pool = list(battery_pool.component_ids)
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
            {"soc_upper_bound": math.nan},
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
            {"capacity": math.nan},
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
    fake_time.shift(MAX_BATTERY_DATA_AGE_SEC + 0.2)
    msg = await asyncio.wait_for(capacity_receiver.receive(), timeout=waiting_time_sec)
    # the msg time difference shouldn't be bigger then the shifted time + 0.2 sec tolerance
    compare_messages(msg, Sample(now, Energy.from_watt_hours(21.0)))

    # All batteries stopped sending data.
    await streamer.stop_streaming(batteries_in_pool[0])
    await asyncio.sleep(MAX_BATTERY_DATA_AGE_SEC + 0.2)
    fake_time.shift(MAX_BATTERY_DATA_AGE_SEC + 0.2)
    msg = await asyncio.wait_for(capacity_receiver.receive(), timeout=waiting_time_sec)
    assert isinstance(msg, Sample) and msg.value is None

    # One battery started sending data.
    latest_data = streamer.get_current_component_data(batteries_in_pool[0])
    streamer.start_streaming(latest_data, sampling_rate=0.1)
    msg = await asyncio.wait_for(capacity_receiver.receive(), timeout=waiting_time_sec)
    # the msg time difference shouldn't be bigger then the shifted time + 0.2 sec tolerance
    compare_messages(
        msg,
        Sample(datetime.now(tz=timezone.utc), Energy.from_watt_hours(21.0)),
    )


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
    all_batteries = get_components(mock_microgrid, Battery)
    await battery_status_sender.send(
        ComponentPoolStatus(working=all_batteries, uncertain=set())
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

    receiver = battery_pool.soc.new_receiver(limit=50)

    # First metrics delivers slower because of the startup delay in the pool.
    msg = await asyncio.wait_for(
        receiver.receive(), timeout=WAIT_FOR_COMPONENT_DATA_SEC + 0.2
    )
    now = datetime.now(tz=timezone.utc)
    expected = Sample(
        timestamp=now,
        value=Percentage.from_percent(10.0),
    )
    compare_messages(msg, expected)

    batteries_in_pool = list(battery_pool.component_ids)
    scenarios: list[Scenario[Sample[Percentage]]] = [
        Scenario(
            batteries_in_pool[0],
            {"capacity": 150, "soc": 10},
            Sample(now, Percentage.from_percent(2.5)),
        ),
        Scenario(
            batteries_in_pool[0],
            {
                "soc_lower_bound": 0.0,
            },
            Sample(now, Percentage.from_percent(12.727272727272727)),
        ),
        # If NaN, then not include that battery in the metric.
        Scenario(
            batteries_in_pool[0],
            {"soc_upper_bound": math.nan},
            Sample(now, Percentage.from_percent(10.0)),
        ),
        # All batteries are sending NaN, can't calculate SoC so we should send None
        Scenario(
            batteries_in_pool[1],
            {"soc": math.nan},
            Sample(now, None),
        ),
        Scenario(
            batteries_in_pool[1],
            {"soc": 30},
            Sample(now, Percentage.from_percent(10.0)),
        ),
        # Final metric didn't change, so nothing should be received.
        Scenario(
            batteries_in_pool[0],
            {"capacity": 0, "soc_lower_bound": 10.0, "soc_upper_bound": 100.0},
            Sample(now, None),
            wait_for_result=False,
        ),
        # Test zero division error
        Scenario(
            batteries_in_pool[1],
            {"capacity": 0},
            Sample(now, Percentage.from_percent(0.0)),
        ),
        Scenario(
            batteries_in_pool[0],
            {"capacity": 50, "soc": 55.0},
            Sample(now, Percentage.from_percent(50.0)),
        ),
        Scenario(
            batteries_in_pool[1],
            {"capacity": 150},
            Sample(now, Percentage.from_percent(25.0)),
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
        all_pool_result=Sample(now, Percentage.from_percent(25.0)),
        only_first_battery_result=Sample(now, Percentage.from_percent(50.0)),
    )

    # One battery stopped sending data.
    await streamer.stop_streaming(batteries_in_pool[1])
    await asyncio.sleep(MAX_BATTERY_DATA_AGE_SEC + 0.2)
    msg = await asyncio.wait_for(receiver.receive(), timeout=waiting_time_sec)
    compare_messages(msg, Sample(now, Percentage.from_percent(50.0)))

    # All batteries stopped sending data.
    await streamer.stop_streaming(batteries_in_pool[0])
    await asyncio.sleep(MAX_BATTERY_DATA_AGE_SEC + 0.2)
    msg = await asyncio.wait_for(receiver.receive(), timeout=waiting_time_sec)
    assert isinstance(msg, Sample) and msg.value is None

    # One battery started sending data.
    latest_data = streamer.get_current_component_data(batteries_in_pool[0])
    streamer.start_streaming(latest_data, sampling_rate=0.1)
    msg = await asyncio.wait_for(receiver.receive(), timeout=waiting_time_sec)
    compare_messages(msg, Sample(now, Percentage.from_percent(50.0)))


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
    all_batteries = get_components(mock_microgrid, Battery)
    await battery_status_sender.send(
        ComponentPoolStatus(working=all_batteries, uncertain=set())
    )
    bat_invs_map = _get_battery_inverter_mappings(
        all_batteries,
        inv_bats=False,
        bat_bats=False,
        inv_invs=False,
    )["bat_invs"]

    for battery_id, inverter_ids in bat_invs_map.items():
        # Sampling rate choose to reflect real application.
        streamer.start_streaming(
            BatteryDataWrapper(
                component_id=battery_id,
                timestamp=datetime.now(tz=timezone.utc),
                power_inclusion_lower_bound=-1000,
                power_inclusion_upper_bound=5000,
                power_exclusion_lower_bound=-300,
                power_exclusion_upper_bound=300,
            ),
            sampling_rate=0.05,
        )
        for inverter_id in inverter_ids:
            streamer.start_streaming(
                InverterDataWrapper(
                    component_id=inverter_id,
                    timestamp=datetime.now(tz=timezone.utc),
                    active_power_inclusion_lower_bound=-900,
                    active_power_inclusion_upper_bound=6000,
                    active_power_exclusion_lower_bound=-200,
                    active_power_exclusion_upper_bound=200,
                ),
                sampling_rate=0.1,
            )

    receiver = battery_pool.system_power_bounds.new_receiver(limit=50)

    # First metrics delivers slower because of the startup delay in the pool.
    msg = await asyncio.wait_for(
        receiver.receive(), timeout=WAIT_FOR_COMPONENT_DATA_SEC + 0.2
    )
    now = datetime.now(tz=timezone.utc)
    expected = SystemBounds(
        timestamp=now,
        inclusion_bounds=Bounds(Power.from_watts(-1800), Power.from_watts(10000)),
        exclusion_bounds=Bounds(Power.from_watts(-600), Power.from_watts(600)),
    )
    compare_messages(msg, expected)

    batteries_in_pool = list(sorted(battery_pool.component_ids))
    scenarios: list[Scenario[SystemBounds]] = [
        Scenario(
            next(iter(bat_invs_map[batteries_in_pool[0]])),
            {
                "active_power_inclusion_lower_bound": -100,
                "active_power_exclusion_lower_bound": -400,
            },
            SystemBounds(
                timestamp=now,
                inclusion_bounds=Bounds(
                    Power.from_watts(-1000), Power.from_watts(10000)
                ),
                exclusion_bounds=Bounds(Power.from_watts(-700), Power.from_watts(600)),
            ),
        ),
        # Inverter bound changed, but metric result should not change.
        Scenario(
            component_id=next(iter(bat_invs_map[batteries_in_pool[0]])),
            new_metrics={
                "active_power_inclusion_upper_bound": 9000,
                "active_power_exclusion_upper_bound": 250,
            },
            expected_result=SystemBounds(
                timestamp=now,
                inclusion_bounds=None,
                exclusion_bounds=None,
            ),
            wait_for_result=False,
        ),
        Scenario(
            batteries_in_pool[0],
            {
                "power_inclusion_lower_bound": 0,
                "power_inclusion_upper_bound": 4000,
                "power_exclusion_lower_bound": 0,
                "power_exclusion_upper_bound": 100,
            },
            SystemBounds(
                timestamp=now,
                inclusion_bounds=Bounds(Power.from_watts(-900), Power.from_watts(9000)),
                exclusion_bounds=Bounds(Power.from_watts(-700), Power.from_watts(550)),
            ),
        ),
        Scenario(
            batteries_in_pool[1],
            {
                "power_inclusion_lower_bound": -10,
                "power_inclusion_upper_bound": 200,
                "power_exclusion_lower_bound": -5,
                "power_exclusion_upper_bound": 5,
            },
            SystemBounds(
                timestamp=now,
                inclusion_bounds=Bounds(Power.from_watts(-10), Power.from_watts(4200)),
                exclusion_bounds=Bounds(Power.from_watts(-600), Power.from_watts(450)),
            ),
        ),
        Scenario(
            next(iter(bat_invs_map[batteries_in_pool[0]])),
            {
                "active_power_inclusion_lower_bound": math.nan,
                "active_power_inclusion_upper_bound": math.nan,
                "active_power_exclusion_lower_bound": math.nan,
                "active_power_exclusion_upper_bound": math.nan,
            },
            SystemBounds(
                timestamp=now,
                inclusion_bounds=Bounds(Power.from_watts(-10), Power.from_watts(200)),
                exclusion_bounds=Bounds(Power.from_watts(-200), Power.from_watts(200)),
            ),
        ),
        Scenario(
            batteries_in_pool[1],
            {
                "power_inclusion_lower_bound": -100,
                "power_inclusion_upper_bound": math.nan,
                "power_exclusion_lower_bound": -50,
                "power_exclusion_upper_bound": 50,
            },
            SystemBounds(
                timestamp=now,
                inclusion_bounds=None,
                exclusion_bounds=None,
            ),
        ),
        Scenario(
            batteries_in_pool[0],
            {
                "power_inclusion_lower_bound": -100,
                "power_inclusion_upper_bound": 100,
                "power_exclusion_lower_bound": -20,
                "power_exclusion_upper_bound": 20,
            },
            SystemBounds(
                timestamp=now,
                inclusion_bounds=Bounds(Power.from_watts(-100), Power.from_watts(100)),
                exclusion_bounds=Bounds(Power.from_watts(-70), Power.from_watts(70)),
            ),
            wait_for_result=False,
        ),
        Scenario(
            next(iter(bat_invs_map[batteries_in_pool[1]])),
            {
                "active_power_inclusion_lower_bound": -400,
                "active_power_inclusion_upper_bound": 400,
                "active_power_exclusion_lower_bound": -100,
                "active_power_exclusion_upper_bound": 100,
            },
            SystemBounds(
                timestamp=now,
                inclusion_bounds=Bounds(Power.from_watts(-500), Power.from_watts(500)),
                exclusion_bounds=Bounds(Power.from_watts(-120), Power.from_watts(120)),
            ),
            wait_for_result=False,
        ),
        Scenario(
            batteries_in_pool[1],
            {
                "power_inclusion_lower_bound": -300,
                "power_inclusion_upper_bound": 700,
                "power_exclusion_lower_bound": -130,
                "power_exclusion_upper_bound": 130,
            },
            SystemBounds(
                timestamp=now,
                inclusion_bounds=Bounds(Power.from_watts(-300), Power.from_watts(400)),
                exclusion_bounds=Bounds(Power.from_watts(-130), Power.from_watts(130)),
            ),
        ),
        Scenario(
            next(iter(bat_invs_map[batteries_in_pool[0]])),
            {
                "active_power_inclusion_lower_bound": -200,
                "active_power_inclusion_upper_bound": 50,
                "active_power_exclusion_lower_bound": -80,
                "active_power_exclusion_upper_bound": 80,
            },
            SystemBounds(
                timestamp=now,
                inclusion_bounds=Bounds(Power.from_watts(-400), Power.from_watts(450)),
                exclusion_bounds=Bounds(Power.from_watts(-210), Power.from_watts(210)),
            ),
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
        all_pool_result=SystemBounds(
            timestamp=now,
            inclusion_bounds=Bounds(Power.from_watts(-400), Power.from_watts(450)),
            exclusion_bounds=Bounds(Power.from_watts(-210), Power.from_watts(210)),
        ),
        only_first_battery_result=SystemBounds(
            timestamp=now,
            inclusion_bounds=Bounds(Power.from_watts(-100), Power.from_watts(50)),
            exclusion_bounds=Bounds(Power.from_watts(-80), Power.from_watts(80)),
        ),
    )

    # One inverter stopped sending data, use one remaining inverter
    await streamer.stop_streaming(next(iter(bat_invs_map[batteries_in_pool[0]])))
    await asyncio.sleep(MAX_BATTERY_DATA_AGE_SEC + 0.1)
    msg = await asyncio.wait_for(receiver.receive(), timeout=waiting_time_sec)
    compare_messages(
        msg,
        SystemBounds(
            timestamp=now,
            inclusion_bounds=Bounds(Power.from_watts(-300), Power.from_watts(400)),
            exclusion_bounds=Bounds(Power.from_watts(-130), Power.from_watts(130)),
        ),
    )

    # All components stopped sending data, we can assume that power bounds are 0
    await streamer.stop_streaming(next(iter(bat_invs_map[batteries_in_pool[1]])))
    await asyncio.sleep(MAX_BATTERY_DATA_AGE_SEC + 0.2)
    msg = await asyncio.wait_for(receiver.receive(), timeout=waiting_time_sec)
    assert (
        isinstance(msg, SystemBounds)
        and msg.inclusion_bounds is None
        and msg.exclusion_bounds is None
    )


async def run_temperature_test(  # pylint: disable=too-many-locals
    setup_args: SetupArgs,
) -> None:
    """Test if temperature metric is working as expected."""
    battery_pool = setup_args.battery_pool
    mock_microgrid = setup_args.mock_microgrid
    streamer = setup_args.streamer
    battery_status_sender = setup_args.battery_status_sender

    all_batteries = get_components(mock_microgrid, Battery)
    await battery_status_sender.send(
        ComponentPoolStatus(working=all_batteries, uncertain=set())
    )
    bat_invs_map = _get_battery_inverter_mappings(all_batteries)["bat_invs"]

    for battery_id, inverter_ids in bat_invs_map.items():
        # Sampling rate choose to reflect real application.
        streamer.start_streaming(
            BatteryDataWrapper(
                component_id=battery_id,
                timestamp=datetime.now(tz=timezone.utc),
                temperature=25.0,
            ),
            sampling_rate=0.05,
        )
        for inverter_id in inverter_ids:
            streamer.start_streaming(
                InverterDataWrapper(
                    component_id=inverter_id,
                    timestamp=datetime.now(tz=timezone.utc),
                ),
                sampling_rate=0.1,
            )

    receiver = battery_pool.temperature.new_receiver()

    msg = await asyncio.wait_for(
        receiver.receive(), timeout=WAIT_FOR_COMPONENT_DATA_SEC + 0.2
    )
    now = datetime.now(tz=timezone.utc)
    expected = Sample(now, value=Temperature.from_celsius(25.0))
    compare_messages(msg, expected)

    batteries_in_pool = list(battery_pool.component_ids)
    bat_0, bat_1 = batteries_in_pool
    scenarios: list[Scenario[Sample[Temperature]]] = [
        Scenario(
            bat_0,
            {"temperature": 30.0},
            Sample(now, value=Temperature.from_celsius(27.5)),
        ),
        Scenario(
            bat_1,
            {"temperature": 20.0},
            Sample(now, value=Temperature.from_celsius(25.0)),
        ),
        Scenario(
            bat_0,
            {"temperature": math.nan},
            Sample(now, value=Temperature.from_celsius(20.0)),
        ),
        Scenario(
            bat_1,
            {"temperature": math.nan},
            Sample(now, None),
        ),
        Scenario(
            bat_0,
            {"temperature": 30.0},
            Sample(now, value=Temperature.from_celsius(30.0)),
        ),
        Scenario(
            bat_1,
            {"temperature": 15.0},
            Sample(now, value=Temperature.from_celsius(22.5)),
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
        all_pool_result=Sample(now, Temperature.from_celsius(22.5)),
        only_first_battery_result=Sample(now, Temperature.from_celsius(30.0)),
    )

    # one battery stops sending data.
    await streamer.stop_streaming(bat_1)
    await asyncio.sleep(MAX_BATTERY_DATA_AGE_SEC + 0.2)
    msg = await asyncio.wait_for(receiver.receive(), timeout=waiting_time_sec)
    compare_messages(msg, Sample(now, Temperature.from_celsius(30.0)))

    # All batteries stopped sending data.
    await streamer.stop_streaming(bat_0)
    await asyncio.sleep(MAX_BATTERY_DATA_AGE_SEC + 0.2)
    msg = await asyncio.wait_for(receiver.receive(), timeout=waiting_time_sec)
    assert isinstance(msg, Sample) and msg.value is None

    # one battery started sending data.
    latest_data = streamer.get_current_component_data(bat_1)
    streamer.start_streaming(latest_data, sampling_rate=0.1)
    msg = await asyncio.wait_for(receiver.receive(), timeout=waiting_time_sec)
    compare_messages(msg, Sample(now, Temperature.from_celsius(15.0)))


async def test_power_status_same_instance_subscriptions_work(
    mocker: MockerFixture,
) -> None:
    """Ensure same-instance power_status subscriptions share the same channel."""
    mock_cm = MagicMock()
    mock_graph = MagicMock()
    mock_graph.components.return_value = [
        MagicMock(id=ComponentId(8)),
        MagicMock(id=ComponentId(18)),
    ]
    mock_cm.component_graph = mock_graph
    mocker.patch(
        "frequenz.sdk.microgrid.connection_manager._CONNECTION_MANAGER",
        mock_cm,
    )
    mocker.patch("frequenz.sdk.microgrid.connection_manager.get", return_value=mock_cm)

    registry = ChannelRegistry(name="battery-pool-test")
    requests_channel = Broadcast[ReportRequest](name="battery-pool-requests")
    requests_rx = requests_channel.new_receiver()
    component_ids = frozenset({ComponentId(8), ComponentId(18)})
    pool = BatteryPool(
        pool_ref_store=BatteryPoolReferenceStore(
            channel_registry=registry,
            resampler_subscription_sender=MagicMock(),
            batteries_status_receiver=MagicMock(),
            power_manager_requests_sender=MagicMock(),
            power_manager_bounds_subscription_sender=requests_channel.new_sender(),
            power_distribution_results_fetcher=MagicMock(),
            min_update_interval=timedelta(seconds=1),
            component_ids=component_ids,
        ),
        name="battery-pool",
        priority=5,
    )

    first_status_rx = pool.power_status.new_receiver()
    second_status_rx = pool.power_status.new_receiver()

    await asyncio.sleep(0)

    first_request = await asyncio.wait_for(requests_rx.receive(), timeout=1.0)
    second_request = await asyncio.wait_for(requests_rx.receive(), timeout=1.0)
    assert second_request.get_channel_name() == first_request.get_channel_name()

    await registry.get_or_create(
        _Report, first_request.get_channel_name()
    ).new_sender().send(_new_power_status_report(123.0))

    first_report = await asyncio.wait_for(first_status_rx.receive(), timeout=1.0)
    second_report = await asyncio.wait_for(second_status_rx.receive(), timeout=1.0)
    assert first_report.target_power == Power.from_watts(123.0)
    assert second_report.target_power == Power.from_watts(123.0)
