# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH
"""Tests for BatteryStatusTracker."""

import asyncio
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Generic, Iterable, List, Optional, Set, Tuple, TypeVar

import pytest
import time_machine
from frequenz.api.microgrid.battery_pb2 import ComponentState as BatteryState
from frequenz.api.microgrid.battery_pb2 import Error as BatteryError
from frequenz.api.microgrid.battery_pb2 import ErrorCode as BatteryErrorCode
from frequenz.api.microgrid.battery_pb2 import RelayState as BatteryRelayState
from frequenz.api.microgrid.common_pb2 import ErrorLevel
from frequenz.api.microgrid.inverter_pb2 import ComponentState as InverterState
from frequenz.api.microgrid.inverter_pb2 import Error as InverterError
from frequenz.api.microgrid.inverter_pb2 import ErrorCode as InverterErrorCode
from frequenz.channels import Broadcast
from pytest_mock import MockerFixture

from frequenz.sdk.actor.power_distributing._battery_status import (
    BatteryStatusTracker,
    SetPowerResult,
    Status,
)
from frequenz.sdk.microgrid.client import Connection
from frequenz.sdk.microgrid.component import (
    BatteryData,
    Component,
    ComponentCategory,
    InverterData,
)

from ..utils.component_data_wrapper import BatteryDataWrapper, InverterDataWrapper
from ..utils.mock_microgrid_client import MockMicrogridClient


def battery_data(  # pylint: disable=too-many-arguments
    component_id: int,
    timestamp: Optional[datetime] = None,
    relay_state: BatteryRelayState.ValueType = BatteryRelayState.RELAY_STATE_CLOSED,
    component_state: BatteryState.ValueType = BatteryState.COMPONENT_STATE_CHARGING,
    errors: Optional[Iterable[BatteryError]] = None,
    capacity: float = 0,
) -> BatteryData:
    """Create BatteryData with given arguments.

    By default function creates BatteryData correct for BatteryPoolStatus with specified
        default arguments.
    If other arguments are given, then it creates BatteryData with that arguments.

    Args:
        component_id: component id
        timestamp: Timestamp of the component message.
            Defaults to datetime.now(tz=timezone.utc).
        relay_state: Battery relay state.
            Defaults to BatteryRelayState.RELAY_STATE_CLOSED.
        component_state: Component state.
            Defaults to BatteryState.COMPONENT_STATE_CHARGING.
        errors: List of the components error. By default empty list will be created.

    Returns:
        BatteryData with given arguments.
    """

    return BatteryDataWrapper(
        component_id=component_id,
        capacity=capacity,
        timestamp=datetime.now(tz=timezone.utc) if timestamp is None else timestamp,
        _relay_state=relay_state,
        _component_state=component_state,
        _errors=list(errors) if errors is not None else [],
    )


def inverter_data(
    component_id: int,
    timestamp: Optional[datetime] = None,
    component_state: InverterState.ValueType = InverterState.COMPONENT_STATE_CHARGING,
    errors: Optional[List[InverterError]] = None,
) -> InverterData:
    """Create InverterData with given arguments.

    By default function creates BatteryData correct for BatteryPoolStatus with specified
        default arguments.
    If other arguments are given, then it creates BatteryData with that arguments.

    Args:
        component_id: component id
        timestamp: Timestamp of the component message.
            Defaults to datetime.now(tz=timezone.utc).
        component_state: Component state.
            Defaults to InverterState.CHARGING.
        errors: List of the components error. By default empty list will be created.

    Returns:
        InverterData with given arguments.
    """

    return InverterDataWrapper(
        component_id=component_id,
        timestamp=datetime.now(tz=timezone.utc) if timestamp is None else timestamp,
        _component_state=component_state,
        _errors=list(errors) if errors is not None else [],
    )


def component_graph() -> Tuple[Set[Component], Set[Connection]]:
    """Creates components and connections for the microgrid component graph.

    Returns:
        Tuple with set of components and set of connections.
    """
    components = {
        Component(1, ComponentCategory.GRID),
        Component(2, ComponentCategory.METER),
        Component(104, ComponentCategory.METER),
        Component(105, ComponentCategory.INVERTER),
        Component(106, ComponentCategory.BATTERY),
        Component(204, ComponentCategory.METER),
        Component(205, ComponentCategory.INVERTER),
        Component(206, ComponentCategory.BATTERY),
        Component(304, ComponentCategory.METER),
        Component(305, ComponentCategory.INVERTER),
        Component(306, ComponentCategory.BATTERY),
    }

    connections = {
        Connection(1, 2),
        Connection(2, 104),
        Connection(104, 105),
        Connection(105, 106),
        Connection(2, 204),
        Connection(204, 205),
        Connection(205, 206),
        Connection(2, 304),
        Connection(304, 305),
        Connection(305, 306),
    }

    return components, connections


T = TypeVar("T")


@dataclass
class Message(Generic[T]):
    """Helper class to store FakeSelect data in the `inner` attribute."""

    inner: T


class FakeSelect:
    """Helper class to mock Select object used in BatteryStatusTracker"""

    # This is just number of Select instance attributes.
    def __init__(  # pylint: disable=too-many-arguments
        self,
        battery: Optional[BatteryData] = None,
        inverter: Optional[InverterData] = None,
        set_power_result: Optional[SetPowerResult] = None,
        battery_timer_flag: bool = False,
        inverter_timer_flag: bool = False,
    ) -> None:
        """Create FakeSelect instance

        Args:
            battery: Expected battery message. Defaults to None.
            inverter: Expected inverter message. Defaults to None.
            battery_timer_flag: If true the battery data timer will be set to indicate
                that no messages have been received for a while.
            inverter_timer_flag: If true the inverter data timer will be set to indicate
                that no messages have been received for a while.
            set_power_result: Expected SetPowerResult message. Defaults to None.
        """
        self.battery = None if battery is None else Message(battery)
        self.inverter = None if inverter is None else Message(inverter)
        self.inverter_timer = inverter_timer_flag
        self.battery_timer = battery_timer_flag
        self.set_power_result = (
            None if set_power_result is None else Message(set_power_result)
        )


BATTERY_ID = 106
INVERTER_ID = 105


# pylint: disable=protected-access, unused-argument
class TestBatteryStatus:
    """Tests BatteryStatusTracker."""

    @pytest.fixture
    async def mock_microgrid(self, mocker: MockerFixture) -> MockMicrogridClient:
        """Create and initialize mock microgrid

        Args:
            mocker: pytest mocker

        Returns:
            MockMicrogridClient
        """
        components, connections = component_graph()
        microgrid = MockMicrogridClient(components, connections)
        microgrid.initialize(mocker)
        return microgrid

    @time_machine.travel("2022-01-01 00:00 UTC", tick=False)
    async def test_sync_update_status_with_messages(
        self, mock_microgrid: MockMicrogridClient
    ) -> None:
        """Test if messages changes battery status/

        Tests uses FakeSelect to test status in sync way.
        Otherwise we would have lots of async calls and waiting.

        Args:
            mock_microgrid: mock_microgrid fixture
        """
        status_channel = Broadcast[Status]("battery_status")
        set_power_result_channel = Broadcast[SetPowerResult]("set_power_result")

        tracker = BatteryStatusTracker(
            BATTERY_ID,
            max_data_age_sec=5,
            max_blocking_duration_sec=30,
            status_sender=status_channel.new_sender(),
            set_power_result_receiver=set_power_result_channel.new_receiver(),
        )

        assert tracker.battery_id == BATTERY_ID
        assert tracker._last_status == Status.NOT_WORKING

        tracker._handle_status_inverter(inverter_data(component_id=INVERTER_ID))
        assert tracker._check_status() is None

        tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))
        assert tracker._check_status() is Status.WORKING

        # --- Send correct message once again, status should not change ---
        tracker._handle_status_inverter(inverter_data(component_id=INVERTER_ID))
        assert tracker._check_status() is None

        tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))
        assert tracker._check_status() is None

        # --- Send outdated message ---
        tracker._handle_status_inverter(
            inverter_data(
                component_id=INVERTER_ID,
                timestamp=datetime.now(tz=timezone.utc) - timedelta(seconds=31),
            )
        )
        assert tracker._check_status() is Status.NOT_WORKING

        # --- BatteryRelayState is invalid. ---
        tracker._handle_status_battery(
            battery_data(
                component_id=BATTERY_ID,
                relay_state=BatteryRelayState.RELAY_STATE_OPENED,
            )
        )
        assert tracker._check_status() is None

        # --- Inverter started sending data, but battery relays state are still invalid ---
        tracker._handle_status_inverter(inverter_data(component_id=INVERTER_ID))
        assert tracker._check_status() is None

        tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))
        assert tracker._check_status() is Status.WORKING

        # --- Inverter started sending data, but battery relays state are still invalid ---
        tracker._handle_status_inverter(
            inverter_data(
                component_id=INVERTER_ID,
                component_state=InverterState.COMPONENT_STATE_SWITCHING_OFF,
            )
        )
        assert tracker._check_status() is Status.NOT_WORKING

        inverter_critical_error = InverterError(
            code=InverterErrorCode.ERROR_CODE_UNSPECIFIED,
            level=ErrorLevel.ERROR_LEVEL_CRITICAL,
            msg="",
        )

        inverter_warning_error = InverterError(
            code=InverterErrorCode.ERROR_CODE_UNSPECIFIED,
            level=ErrorLevel.ERROR_LEVEL_WARN,
            msg="",
        )

        tracker._handle_status_inverter(
            inverter_data(
                component_id=INVERTER_ID,
                component_state=InverterState.COMPONENT_STATE_SWITCHING_OFF,
                errors=[inverter_critical_error, inverter_warning_error],
            )
        )

        assert tracker._check_status() is None

        tracker._handle_status_inverter(
            inverter_data(
                component_id=INVERTER_ID,
                errors=[inverter_critical_error, inverter_warning_error],
            )
        )

        assert tracker._check_status() is None

        tracker._handle_status_inverter(
            inverter_data(component_id=INVERTER_ID, errors=[inverter_warning_error])
        )

        assert tracker._check_status() is Status.WORKING

        battery_critical_error = BatteryError(
            code=BatteryErrorCode.ERROR_CODE_UNSPECIFIED,
            level=ErrorLevel.ERROR_LEVEL_CRITICAL,
            msg="",
        )

        battery_warning_error = BatteryError(
            code=BatteryErrorCode.ERROR_CODE_UNSPECIFIED,
            level=ErrorLevel.ERROR_LEVEL_WARN,
            msg="",
        )

        tracker._handle_status_battery(
            battery_data(component_id=BATTERY_ID, errors=[battery_warning_error])
        )

        assert tracker._check_status() is None

        tracker._handle_status_battery(
            battery_data(
                component_id=BATTERY_ID,
                errors=[battery_warning_error, battery_critical_error],
            )
        )

        assert tracker._check_status() is Status.NOT_WORKING

        tracker._handle_status_battery(
            battery_data(
                component_id=BATTERY_ID,
                component_state=BatteryState.COMPONENT_STATE_ERROR,
                errors=[battery_warning_error, battery_critical_error],
            )
        )

        assert tracker._check_status() is None

        # Check if NaN capacity changes the battery status.
        tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))

        assert tracker._check_status() is Status.WORKING

        tracker._handle_status_battery(
            battery_data(component_id=BATTERY_ID, capacity=math.nan)
        )

        assert tracker._check_status() is Status.NOT_WORKING

        await tracker.stop()

    async def test_sync_blocking_feature(
        self, mock_microgrid: MockMicrogridClient
    ) -> None:
        """Test if status changes when SetPowerResult message is received.

        Tests uses FakeSelect to test status in sync way.
        Otherwise we would have lots of async calls and waiting.

        Args:
            mock_microgrid: mock_microgrid fixture
        """

        status_channel = Broadcast[Status]("battery_status")
        set_power_result_channel = Broadcast[SetPowerResult]("set_power_result")

        # increase max_data_age_sec for blocking tests.
        # Otherwise it will block blocking.
        tracker = BatteryStatusTracker(
            BATTERY_ID,
            max_data_age_sec=500,
            max_blocking_duration_sec=30,
            status_sender=status_channel.new_sender(),
            set_power_result_receiver=set_power_result_channel.new_receiver(),
        )

        with time_machine.travel("2022-01-01 00:00 UTC", tick=False) as time:
            tracker._handle_status_inverter(inverter_data(component_id=INVERTER_ID))

            assert tracker._check_status() is None

            tracker._handle_status_battery(
                battery_data(
                    component_id=BATTERY_ID,
                    component_state=BatteryState.COMPONENT_STATE_ERROR,
                )
            )

            assert tracker._check_status() is None

            # message is not correct, component should not block.
            tracker._handle_status_set_power_result(
                SetPowerResult(succeed={1}, failed={106})
            )

            assert tracker._check_status() is None

            tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))

            assert tracker._check_status() is Status.WORKING

            expected_blocking_timeout = [1, 2, 4, 8, 16, 30, 30]

            for timeout in expected_blocking_timeout:
                # message is not correct, component should not block.
                tracker._handle_status_set_power_result(
                    SetPowerResult(succeed={1}, failed={106})
                )

                assert tracker._check_status() is Status.UNCERTAIN

                # Battery should be still blocked, nothing should happen
                time.shift(timeout - 1)
                tracker._handle_status_set_power_result(
                    SetPowerResult(succeed={1}, failed={106})
                )

                assert tracker._check_status() is None

                tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))

                assert tracker._check_status() is None

                time.shift(1)
                tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))

                assert tracker._check_status() is Status.WORKING

            # should block for 30 sec
            tracker._handle_status_set_power_result(
                SetPowerResult(succeed={1}, failed={106})
            )

            assert tracker._check_status() is Status.UNCERTAIN
            time.shift(28)

            tracker._handle_status_battery(
                battery_data(
                    component_id=BATTERY_ID,
                    component_state=BatteryState.COMPONENT_STATE_ERROR,
                )
            )

            assert tracker._check_status() is Status.NOT_WORKING

            # Message that changed status to correct should unblock the battery.
            tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))
            assert tracker._check_status() is Status.WORKING

            # should block for 30 sec
            tracker._handle_status_set_power_result(
                SetPowerResult(succeed={1}, failed={106})
            )
            assert tracker._check_status() is Status.UNCERTAIN
            time.shift(28)

            # If battery succeed, then it should unblock.
            tracker._handle_status_set_power_result(
                SetPowerResult(succeed={106}, failed={206})
            )
            assert tracker._check_status() is Status.WORKING

        await tracker.stop()

    async def test_sync_blocking_interrupted_with_with_max_data(
        self, mock_microgrid: MockMicrogridClient
    ) -> None:
        """Test if status changes when SetPowerResult message is received.

        Tests uses FakeSelect to test status in sync way.
        Otherwise we would have lots of async calls and waiting.

        Args:
            mock_microgrid: mock_microgrid fixture
        """

        status_channel = Broadcast[Status]("battery_status")
        set_power_result_channel = Broadcast[SetPowerResult]("set_power_result")

        tracker = BatteryStatusTracker(
            BATTERY_ID,
            max_data_age_sec=5,
            max_blocking_duration_sec=30,
            status_sender=status_channel.new_sender(),
            set_power_result_receiver=set_power_result_channel.new_receiver(),
        )

        with time_machine.travel("2022-01-01 00:00 UTC", tick=False) as time:
            tracker._handle_status_inverter(inverter_data(component_id=INVERTER_ID))
            assert tracker._check_status() is None

            tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))
            assert tracker._check_status() is Status.WORKING

            tracker._handle_status_set_power_result(
                SetPowerResult(succeed={1}, failed={106})
            )
            assert tracker._check_status() is Status.UNCERTAIN

            expected_blocking_timeout = [1, 2, 4]
            for timeout in expected_blocking_timeout:
                # message is not correct, component should not block.
                tracker._handle_status_set_power_result(
                    SetPowerResult(succeed={1}, failed={106})
                )
                assert tracker._check_status() is None
                time.shift(timeout)

            await tracker.stop()

    @time_machine.travel("2022-01-01 00:00 UTC", tick=False)
    async def test_sync_blocking_interrupted_with_invalid_message(
        self, mock_microgrid: MockMicrogridClient
    ) -> None:
        """Test if status changes when SetPowerResult message is received.

        Tests uses FakeSelect to test status in sync way.
        Otherwise we would have lots of async calls and waiting.

        Args:
            mock_microgrid: mock_microgrid fixture
        """

        status_channel = Broadcast[Status]("battery_status")
        set_power_result_channel = Broadcast[SetPowerResult]("set_power_result")

        tracker = BatteryStatusTracker(
            BATTERY_ID,
            max_data_age_sec=5,
            max_blocking_duration_sec=30,
            status_sender=status_channel.new_sender(),
            set_power_result_receiver=set_power_result_channel.new_receiver(),
        )

        tracker._handle_status_inverter(inverter_data(component_id=INVERTER_ID))
        assert tracker._check_status() is None

        tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))
        assert tracker._check_status() is Status.WORKING

        tracker._handle_status_set_power_result(
            SetPowerResult(succeed={1}, failed={106})
        )
        assert tracker._check_status() is Status.UNCERTAIN

        tracker._handle_status_inverter(
            inverter_data(
                component_id=INVERTER_ID,
                component_state=InverterState.COMPONENT_STATE_ERROR,
            )
        )
        assert tracker._check_status() is Status.NOT_WORKING

        tracker._handle_status_set_power_result(
            SetPowerResult(succeed={1}, failed={106})
        )
        assert tracker._check_status() is None

        tracker._handle_status_set_power_result(
            SetPowerResult(succeed={106}, failed={})
        )
        assert tracker._check_status() is None

        tracker._handle_status_inverter(inverter_data(component_id=INVERTER_ID))
        assert tracker._check_status() is Status.WORKING

        await tracker.stop()

    @time_machine.travel("2022-01-01 00:00 UTC", tick=False)
    async def test_timers(
        self, mock_microgrid: MockMicrogridClient, mocker: MockerFixture
    ) -> None:
        """Test if messages changes battery status/

        Tests uses FakeSelect to test status in sync way.
        Otherwise we would have lots of async calls and waiting.

        Args:
            mock_microgrid: mock_microgrid fixture
            mocker: pytest mocker instance
        """
        status_channel = Broadcast[Status]("battery_status")
        set_power_result_channel = Broadcast[SetPowerResult]("set_power_result")

        tracker = BatteryStatusTracker(
            BATTERY_ID,
            max_data_age_sec=5,
            max_blocking_duration_sec=30,
            status_sender=status_channel.new_sender(),
            set_power_result_receiver=set_power_result_channel.new_receiver(),
        )

        battery_timer_spy = mocker.spy(tracker._battery.data_recv_timer, "reset")
        inverter_timer_spy = mocker.spy(tracker._inverter.data_recv_timer, "reset")

        assert tracker.battery_id == BATTERY_ID
        assert tracker._last_status == Status.NOT_WORKING

        tracker._handle_status_inverter(inverter_data(component_id=INVERTER_ID))
        assert tracker._check_status() is None

        tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))
        assert tracker._check_status() is Status.WORKING

        assert battery_timer_spy.call_count == 1

        tracker._handle_status_battery_timer(timedelta(seconds=0))
        assert tracker._check_status() is Status.NOT_WORKING

        assert battery_timer_spy.call_count == 1

        tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))
        assert tracker._check_status() is Status.WORKING

        assert battery_timer_spy.call_count == 2

        tracker._handle_status_inverter_timer(timedelta(seconds=0))
        assert tracker._check_status() is Status.NOT_WORKING

        tracker._handle_status_battery_timer(timedelta(seconds=0))
        assert tracker._check_status() is None

        tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))
        assert tracker._check_status() is None

        tracker._handle_status_inverter(inverter_data(component_id=INVERTER_ID))
        assert tracker._check_status() is Status.WORKING

        assert inverter_timer_spy.call_count == 2
        await tracker.stop()

    @time_machine.travel("2022-01-01 00:00 UTC", tick=False)
    async def test_async_battery_status(
        self, mock_microgrid: MockMicrogridClient
    ) -> None:
        """Test if status changes.

        Args:
            mock_microgrid: mock_microgrid fixture
        """

        status_channel = Broadcast[Status]("battery_status")
        set_power_result_channel = Broadcast[SetPowerResult]("set_power_result")

        status_receiver = status_channel.new_receiver()
        set_power_result_sender = set_power_result_channel.new_sender()

        tracker = BatteryStatusTracker(
            BATTERY_ID,
            max_data_age_sec=5,
            max_blocking_duration_sec=30,
            status_sender=status_channel.new_sender(),
            set_power_result_receiver=set_power_result_channel.new_receiver(),
        )
        await asyncio.sleep(0.01)

        with time_machine.travel("2022-01-01 00:00 UTC", tick=False) as time:
            await mock_microgrid.send(inverter_data(component_id=INVERTER_ID))
            await mock_microgrid.send(battery_data(component_id=BATTERY_ID))
            status = await asyncio.wait_for(status_receiver.receive(), timeout=0.1)
            assert status is Status.WORKING

            await set_power_result_sender.send(
                SetPowerResult(succeed={}, failed={BATTERY_ID})
            )
            status = await asyncio.wait_for(status_receiver.receive(), timeout=0.1)
            assert status is Status.UNCERTAIN

            time.shift(2)

            await mock_microgrid.send(battery_data(component_id=BATTERY_ID))
            status = await asyncio.wait_for(status_receiver.receive(), timeout=0.1)
            assert status is Status.WORKING

            await mock_microgrid.send(
                inverter_data(
                    component_id=INVERTER_ID,
                    timestamp=datetime.now(tz=timezone.utc) - timedelta(seconds=7),
                )
            )
            status = await asyncio.wait_for(status_receiver.receive(), timeout=0.1)
            assert status is Status.NOT_WORKING

            await set_power_result_sender.send(
                SetPowerResult(succeed={}, failed={BATTERY_ID})
            )
            await asyncio.sleep(0.3)
            assert len(status_receiver) == 0

            await mock_microgrid.send(inverter_data(component_id=INVERTER_ID))
            status = await asyncio.wait_for(status_receiver.receive(), timeout=0.1)
            assert status is Status.WORKING

        await tracker.stop()
