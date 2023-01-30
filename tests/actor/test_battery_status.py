# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH
"""Tests for BatteryStatusTracker."""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Generic, Iterable, List, Optional, Set, Tuple, TypeVar

import frequenz.api.microgrid.microgrid_pb2 as microgrid_pb
import pytest
import pytz
import time_machine
from frequenz.api.microgrid.battery_pb2 import Battery as PbBattery
from frequenz.api.microgrid.battery_pb2 import ComponentState as BatteryState
from frequenz.api.microgrid.battery_pb2 import Error as BatteryError
from frequenz.api.microgrid.battery_pb2 import ErrorCode as BatteryErrorCode
from frequenz.api.microgrid.battery_pb2 import RelayState as BatteryRelayState
from frequenz.api.microgrid.battery_pb2 import State as PbBatteryState
from frequenz.api.microgrid.common_pb2 import ErrorLevel
from frequenz.api.microgrid.inverter_pb2 import ComponentState as InverterState
from frequenz.api.microgrid.inverter_pb2 import Error as InverterError
from frequenz.api.microgrid.inverter_pb2 import ErrorCode as InverterErrorCode
from frequenz.api.microgrid.inverter_pb2 import Inverter as PbInverter
from frequenz.api.microgrid.inverter_pb2 import State as PbInverterState
from frequenz.channels import Broadcast
from google.protobuf.timestamp_pb2 import Timestamp  # pylint: disable=no-name-in-module
from pytest_mock import MockerFixture

from frequenz.sdk.actor.power_distributing._battery_status import (
    BatteryStatus,
    BatteryStatusTracker,
    SetPowerResult,
)
from frequenz.sdk.microgrid.client import Connection
from frequenz.sdk.microgrid.component import (
    BatteryData,
    Component,
    ComponentCategory,
    InverterData,
)

from ..utils.mock_microgrid import MockMicrogridClient


def battery_data(
    component_id: int,
    timestamp: Optional[datetime] = None,
    relay_state: BatteryRelayState.ValueType = BatteryRelayState.RELAY_STATE_CLOSED,
    component_state: BatteryState.ValueType = BatteryState.COMPONENT_STATE_CHARGING,
    errors: Optional[Iterable[BatteryError]] = None,
) -> BatteryData:
    """Create BatteryData with given arguments.

    By default function creates BatteryData correct for BatteryPoolStatus with specified
        default arguments.
    If other arguments are given, then it creates BatteryData with that arguments.

    Args:
        component_id: component id
        timestamp: Timestamp of the component message.
            Defaults to datetime.now(tz=pytz.utc).
        relay_state: Battery relay state.
            Defaults to BatteryRelayState.RELAY_STATE_CLOSED.
        component_state: Component state.
            Defaults to BatteryState.COMPONENT_STATE_CHARGING.
        errors: List of the components error. By default empty list will be created.

    Returns:
        BatteryData with given arguments.
    """
    if timestamp is None:
        timestamp = datetime.now(tz=pytz.utc)

    # Create protobuf message first. Thanks to that I don't have to specify
    # all class variables.
    pb_timestamp = Timestamp()
    pb_timestamp.FromDatetime(timestamp)

    pb_data = microgrid_pb.ComponentData(
        id=component_id,
        ts=pb_timestamp,
        battery=PbBattery(
            state=PbBatteryState(
                component_state=component_state,
                relay_state=relay_state,
            ),
            # We don't need this method in public api. But we need it in unit tests.
            # pylint: disable=protected-access
            errors=[] if errors is None else errors,
        ),
    )

    return BatteryData.from_proto(pb_data)


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
            Defaults to datetime.now(tz=pytz.utc).
        component_state: Component state.
            Defaults to InverterState.CHARGING.
        errors: List of the components error. By default empty list will be created.

    Returns:
        InverterData with given arguments.
    """
    if timestamp is None:
        timestamp = datetime.now(tz=pytz.utc)

    # Create protobuf message first. Thanks to that I don't have to specify
    # all InverterData class variable.
    pb_timestamp = Timestamp()
    pb_timestamp.FromDatetime(timestamp)

    pb_data = microgrid_pb.ComponentData(
        id=component_id,
        ts=pb_timestamp,
        inverter=PbInverter(
            state=PbInverterState(
                component_state=component_state,
            ),
            errors=[] if errors is None else errors,
        ),
    )

    return InverterData.from_proto(pb_data)


def component_graph() -> Tuple[Set[Component], Set[Connection]]:
    """Creates components and connections for the microgrid component graph.

    Returns:
        Tuple with set of components and set of connections.
    """
    components = {
        Component(1, ComponentCategory.GRID),
        Component(2, ComponentCategory.METER),
        Component(3, ComponentCategory.JUNCTION),
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
        Connection(2, 3),
        Connection(3, 104),
        Connection(104, 105),
        Connection(105, 106),
        Connection(3, 204),
        Connection(204, 205),
        Connection(205, 206),
        Connection(3, 304),
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

    def __init__(
        self,
        battery: Optional[BatteryData] = None,
        inverter: Optional[InverterData] = None,
        request_result: Optional[SetPowerResult] = None,
    ) -> None:
        """Create FakeSelect instance

        Args:
            battery: Expected battery message. Defaults to None.
            inverter: Expected inverter message. Defaults to None.
            request_result: Expected SetPowerResult message. Defaults to None.
        """
        self.battery = None if battery is None else Message(battery)
        self.inverter = None if inverter is None else Message(inverter)
        self.request_result = (
            None if request_result is None else Message(request_result)
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
        status_channel = Broadcast[BatteryStatus]("battery_status")
        request_result_channel = Broadcast[SetPowerResult]("request_result")

        tracker = BatteryStatusTracker(
            BATTERY_ID,
            max_data_age_sec=5,
            max_blocking_duration_sec=30,
            status_sender=status_channel.new_sender(),
            request_result_receiver=request_result_channel.new_receiver(),
        )

        assert tracker.battery_id == BATTERY_ID
        assert tracker._last_status == BatteryStatus.NOT_WORKING

        select = FakeSelect(inverter=inverter_data(component_id=INVERTER_ID))
        assert tracker._update_status(select) is None  # type: ignore[arg-type]

        select = FakeSelect(battery=battery_data(component_id=BATTERY_ID))
        assert tracker._update_status(select) is BatteryStatus.WORKING  # type: ignore[arg-type]

        # --- Send correct message once again, status should not change ---
        select = FakeSelect(inverter=inverter_data(component_id=INVERTER_ID))
        assert tracker._update_status(select) is None  # type: ignore[arg-type]

        select = FakeSelect(battery=battery_data(component_id=BATTERY_ID))
        assert tracker._update_status(select) is None  # type: ignore[arg-type]

        # --- Send outdated message ---
        select = FakeSelect(
            inverter=inverter_data(
                component_id=INVERTER_ID,
                timestamp=datetime.now(tz=timezone.utc) - timedelta(seconds=31),
            )
        )
        assert tracker._update_status(select) is BatteryStatus.NOT_WORKING  # type: ignore[arg-type]

        # --- BatteryRelayState is invalid. ---
        select = FakeSelect(
            battery=battery_data(
                component_id=BATTERY_ID,
                relay_state=BatteryRelayState.RELAY_STATE_OPENED,
            )
        )
        assert tracker._update_status(select) is None  # type: ignore[arg-type]

        # --- Inverter started sending data, but battery relays state are still invalid ---
        select = FakeSelect(inverter=inverter_data(component_id=INVERTER_ID))
        assert tracker._update_status(select) is None  # type: ignore[arg-type]

        select = FakeSelect(battery=battery_data(component_id=BATTERY_ID))
        assert tracker._update_status(select) is BatteryStatus.WORKING  # type: ignore[arg-type]

        # --- Inverter started sending data, but battery relays state are still invalid ---
        select = FakeSelect(
            inverter=inverter_data(
                component_id=INVERTER_ID,
                component_state=InverterState.COMPONENT_STATE_SWITCHING_OFF,
            )
        )
        assert tracker._update_status(select) is BatteryStatus.NOT_WORKING  # type: ignore[arg-type]

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

        select = FakeSelect(
            inverter=inverter_data(
                component_id=INVERTER_ID,
                component_state=InverterState.COMPONENT_STATE_SWITCHING_OFF,
                errors=[inverter_critical_error, inverter_warning_error],
            )
        )
        assert tracker._update_status(select) is None  # type: ignore[arg-type]

        select = FakeSelect(
            inverter=inverter_data(
                component_id=INVERTER_ID,
                errors=[inverter_critical_error, inverter_warning_error],
            )
        )
        assert tracker._update_status(select) is None  # type: ignore[arg-type]

        select = FakeSelect(
            inverter=inverter_data(
                component_id=INVERTER_ID, errors=[inverter_warning_error]
            )
        )
        assert tracker._update_status(select) is BatteryStatus.WORKING  # type: ignore[arg-type]

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

        select = FakeSelect(
            battery=battery_data(
                component_id=BATTERY_ID, errors=[battery_warning_error]
            )
        )
        assert tracker._update_status(select) is None  # type: ignore[arg-type]

        select = FakeSelect(
            battery=battery_data(
                component_id=BATTERY_ID,
                errors=[battery_warning_error, battery_critical_error],
            )
        )
        assert tracker._update_status(select) is BatteryStatus.NOT_WORKING  # type: ignore[arg-type]

        select = FakeSelect(
            battery=battery_data(
                component_id=BATTERY_ID,
                component_state=BatteryState.COMPONENT_STATE_ERROR,
                errors=[battery_warning_error, battery_critical_error],
            )
        )
        assert tracker._update_status(select) is None  # type: ignore[arg-type]

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

        status_channel = Broadcast[BatteryStatus]("battery_status")
        request_result_channel = Broadcast[SetPowerResult]("request_result")

        # increase max_data_age_sec for blocking tests.
        # Otherwise it will block blocking.
        tracker = BatteryStatusTracker(
            BATTERY_ID,
            max_data_age_sec=500,
            max_blocking_duration_sec=30,
            status_sender=status_channel.new_sender(),
            request_result_receiver=request_result_channel.new_receiver(),
        )

        with time_machine.travel("2022-01-01 00:00 UTC", tick=False) as time:
            select = FakeSelect(inverter=inverter_data(component_id=INVERTER_ID))
            assert tracker._update_status(select) is None  # type: ignore[arg-type]

            select = FakeSelect(
                battery=battery_data(
                    component_id=BATTERY_ID,
                    component_state=BatteryState.COMPONENT_STATE_ERROR,
                )
            )
            assert tracker._update_status(select) is None  # type: ignore[arg-type]

            # message is not correct, component should not block.
            select = FakeSelect(
                request_result=SetPowerResult(succeed={1}, failed={106})
            )
            assert tracker._update_status(select) is None  # type: ignore[arg-type]

            select = FakeSelect(battery=battery_data(component_id=BATTERY_ID))
            assert tracker._update_status(select) is BatteryStatus.WORKING  # type: ignore[arg-type]

            expected_blocking_timeout = [1, 2, 4, 8, 16, 30, 30]

            for timeout in expected_blocking_timeout:
                # message is not correct, component should not block.
                select = FakeSelect(
                    request_result=SetPowerResult(succeed={1}, failed={106})
                )
                status = tracker._update_status(select)  # type: ignore[arg-type]
                assert status is BatteryStatus.UNCERTAIN

                # Battery should be still blocked, nothing should happen
                time.shift(timeout - 1)
                select = FakeSelect(
                    request_result=SetPowerResult(succeed={1}, failed={106})
                )
                assert tracker._update_status(select) is None  # type: ignore[arg-type]

                select = FakeSelect(battery=battery_data(component_id=BATTERY_ID))
                assert tracker._update_status(select) is None  # type: ignore[arg-type]

                time.shift(1)
                select = FakeSelect(battery=battery_data(component_id=BATTERY_ID))
                status = tracker._update_status(select)  # type: ignore[arg-type]
                assert status is BatteryStatus.WORKING

            # should block for 30 sec
            select = FakeSelect(
                request_result=SetPowerResult(succeed={1}, failed={106})
            )
            status = tracker._update_status(select)  # type: ignore[arg-type]
            assert status is BatteryStatus.UNCERTAIN
            time.shift(28)

            select = FakeSelect(
                battery=battery_data(
                    component_id=BATTERY_ID,
                    component_state=BatteryState.COMPONENT_STATE_ERROR,
                )
            )
            status = tracker._update_status(select)  # type: ignore[arg-type]
            assert status is BatteryStatus.NOT_WORKING

            # Message that changed status to correct should unblock the battery.
            select = FakeSelect(battery=battery_data(component_id=BATTERY_ID))
            status = tracker._update_status(select)  # type: ignore[arg-type]
            assert status is BatteryStatus.WORKING

            # should block for 30 sec
            select = FakeSelect(
                request_result=SetPowerResult(succeed={1}, failed={106})
            )
            status = tracker._update_status(select)  # type: ignore[arg-type]
            assert status is BatteryStatus.UNCERTAIN
            time.shift(28)

            # If battery succeed, then it should unblock.
            select = FakeSelect(
                request_result=SetPowerResult(succeed={106}, failed={206})
            )
            status = tracker._update_status(select)  # type: ignore[arg-type]
            assert status is BatteryStatus.WORKING

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

        status_channel = Broadcast[BatteryStatus]("battery_status")
        request_result_channel = Broadcast[SetPowerResult]("request_result")

        tracker = BatteryStatusTracker(
            BATTERY_ID,
            max_data_age_sec=5,
            max_blocking_duration_sec=30,
            status_sender=status_channel.new_sender(),
            request_result_receiver=request_result_channel.new_receiver(),
        )

        with time_machine.travel("2022-01-01 00:00 UTC", tick=False) as time:
            select = FakeSelect(inverter=inverter_data(component_id=INVERTER_ID))
            assert tracker._update_status(select) is None  # type: ignore[arg-type]

            select = FakeSelect(battery=battery_data(component_id=BATTERY_ID))
            status = tracker._update_status(select)  # type: ignore[arg-type]
            assert status is BatteryStatus.WORKING

            select = FakeSelect(
                request_result=SetPowerResult(succeed={1}, failed={106})
            )
            status = tracker._update_status(select)  # type: ignore[arg-type]
            assert status is BatteryStatus.UNCERTAIN

            expected_blocking_timeout = [1, 2, 4]
            for timeout in expected_blocking_timeout:
                # message is not correct, component should not block.
                select = FakeSelect(
                    request_result=SetPowerResult(succeed={1}, failed={106})
                )
                assert tracker._update_status(select) is None  # type: ignore[arg-type]
                time.shift(timeout)

            # Battery message should be to old after 5 seconds.
            select = FakeSelect(
                request_result=SetPowerResult(succeed={1}, failed={106})
            )
            status = tracker._update_status(select)  # type: ignore[arg-type]
            assert status is BatteryStatus.NOT_WORKING

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

        status_channel = Broadcast[BatteryStatus]("battery_status")
        request_result_channel = Broadcast[SetPowerResult]("request_result")

        tracker = BatteryStatusTracker(
            BATTERY_ID,
            max_data_age_sec=5,
            max_blocking_duration_sec=30,
            status_sender=status_channel.new_sender(),
            request_result_receiver=request_result_channel.new_receiver(),
        )

        select = FakeSelect(inverter=inverter_data(component_id=INVERTER_ID))
        assert tracker._update_status(select) is None  # type: ignore[arg-type]

        select = FakeSelect(battery=battery_data(component_id=BATTERY_ID))
        assert tracker._update_status(select) is BatteryStatus.WORKING  # type: ignore[arg-type]

        select = FakeSelect(request_result=SetPowerResult(succeed={1}, failed={106}))
        assert tracker._update_status(select) is BatteryStatus.UNCERTAIN  # type: ignore[arg-type]

        select = FakeSelect(
            inverter=inverter_data(
                component_id=INVERTER_ID,
                component_state=InverterState.COMPONENT_STATE_ERROR,
            )
        )
        assert tracker._update_status(select) is BatteryStatus.NOT_WORKING  # type: ignore[arg-type]

        select = FakeSelect(request_result=SetPowerResult(succeed={1}, failed={106}))
        assert tracker._update_status(select) is None  # type: ignore[arg-type]

        select = FakeSelect(request_result=SetPowerResult(succeed={106}, failed={}))
        assert tracker._update_status(select) is None  # type: ignore[arg-type]

        select = FakeSelect(inverter=inverter_data(component_id=INVERTER_ID))
        assert tracker._update_status(select) is BatteryStatus.WORKING  # type: ignore[arg-type]

        await tracker.stop()

    @time_machine.travel("2022-01-01 00:00 UTC", tick=False)
    async def test_async_battery_status(
        self, mock_microgrid: MockMicrogridClient
    ) -> None:
        """Test if status changes.

        Args:
            mock_microgrid: mock_microgrid fixture
        """

        status_channel = Broadcast[BatteryStatus]("battery_status")
        request_result_channel = Broadcast[SetPowerResult]("request_result")

        status_receiver = status_channel.new_receiver()
        request_result_sender = request_result_channel.new_sender()

        tracker = BatteryStatusTracker(
            BATTERY_ID,
            max_data_age_sec=5,
            max_blocking_duration_sec=30,
            status_sender=status_channel.new_sender(),
            request_result_receiver=request_result_channel.new_receiver(),
        )
        await asyncio.sleep(0.01)

        with time_machine.travel("2022-01-01 00:00 UTC", tick=False) as time:
            assert await mock_microgrid.send(inverter_data(component_id=INVERTER_ID))
            assert await mock_microgrid.send(battery_data(component_id=BATTERY_ID))
            status = await asyncio.wait_for(status_receiver.receive(), timeout=0.1)
            assert status is BatteryStatus.WORKING

            assert await request_result_sender.send(
                SetPowerResult(succeed={}, failed={BATTERY_ID})
            )
            status = await asyncio.wait_for(status_receiver.receive(), timeout=0.1)
            assert status is BatteryStatus.UNCERTAIN

            time.shift(2)

            assert await mock_microgrid.send(battery_data(component_id=BATTERY_ID))
            status = await asyncio.wait_for(status_receiver.receive(), timeout=0.1)
            assert status is BatteryStatus.WORKING

            assert await mock_microgrid.send(
                inverter_data(
                    component_id=INVERTER_ID,
                    timestamp=datetime.now(tz=timezone.utc) - timedelta(seconds=7),
                )
            )
            status = await asyncio.wait_for(status_receiver.receive(), timeout=0.1)
            assert status is BatteryStatus.NOT_WORKING

            assert await request_result_sender.send(
                SetPowerResult(succeed={}, failed={BATTERY_ID})
            )
            await asyncio.sleep(0.3)
            assert len(status_receiver) == 0

            assert await mock_microgrid.send(inverter_data(component_id=INVERTER_ID))
            status = await asyncio.wait_for(status_receiver.receive(), timeout=0.1)
            assert status is BatteryStatus.WORKING

        await tracker.stop()
