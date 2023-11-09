# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH
"""Tests for BatteryStatusTracker."""

import asyncio
import math
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Generic, TypeVar

import pytest
import time_machine

# pylint: disable=no-name-in-module
from frequenz.api.microgrid.battery_pb2 import ComponentState as BatteryState
from frequenz.api.microgrid.battery_pb2 import Error as BatteryError
from frequenz.api.microgrid.battery_pb2 import ErrorCode as BatteryErrorCode
from frequenz.api.microgrid.battery_pb2 import RelayState as BatteryRelayState
from frequenz.api.microgrid.common_pb2 import ErrorLevel
from frequenz.api.microgrid.inverter_pb2 import ComponentState as InverterState
from frequenz.api.microgrid.inverter_pb2 import Error as InverterError
from frequenz.api.microgrid.inverter_pb2 import ErrorCode as InverterErrorCode

# pylint: enable=no-name-in-module
from frequenz.channels import Broadcast, Receiver
from pytest_mock import MockerFixture

from frequenz.sdk.actor.power_distributing._battery_status import (
    BatteryStatusTracker,
    SetPowerResult,
)
from frequenz.sdk.actor.power_distributing._component_status import ComponentStatusEnum
from frequenz.sdk.microgrid.component import BatteryData, InverterData
from tests.timeseries.mock_microgrid import MockMicrogrid

from ..utils.component_data_wrapper import BatteryDataWrapper, InverterDataWrapper


def battery_data(  # pylint: disable=too-many-arguments
    component_id: int,
    timestamp: datetime | None = None,
    relay_state: BatteryRelayState.ValueType = BatteryRelayState.RELAY_STATE_CLOSED,
    component_state: BatteryState.ValueType = BatteryState.COMPONENT_STATE_CHARGING,
    errors: Iterable[BatteryError] | None = None,
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
        capacity: Battery capacity.

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
    timestamp: datetime | None = None,
    component_state: InverterState.ValueType = InverterState.COMPONENT_STATE_CHARGING,
    errors: list[InverterError] | None = None,
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


T = TypeVar("T")


@dataclass
class Message(Generic[T]):
    """Helper class to store FakeSelect data in the `inner` attribute."""

    inner: T


BATTERY_ID = 9
INVERTER_ID = 8


class _Timeout:
    """Sentinel for timeout."""


async def recv_timeout(recv: Receiver[T], timeout: float = 0.1) -> T | type[_Timeout]:
    """Receive message from receiver with timeout.

    Args:
        recv: Receiver to receive message from.
        timeout: Timeout in seconds.

    Returns:
        Received message or _Timeout if timeout is reached.
    """
    try:
        return await asyncio.wait_for(recv.receive(), timeout=timeout)
    except asyncio.TimeoutError:
        return _Timeout


# pylint: disable=protected-access, unused-argument
class TestBatteryStatus:
    """Tests BatteryStatusTracker."""

    @time_machine.travel("2022-01-01 00:00 UTC", tick=False)
    async def test_sync_update_status_with_messages(
        self, mocker: MockerFixture
    ) -> None:
        """Test if messages changes battery status.

        Tests uses FakeSelect to test status in sync way.
        Otherwise we would have lots of async calls and waiting.

        Args:
            mocker: Pytest mocker fixture.
        """
        mock_microgrid = MockMicrogrid(grid_meter=True)
        mock_microgrid.add_batteries(3)
        await mock_microgrid.start(mocker)

        status_channel = Broadcast[ComponentStatusEnum]("battery_status")
        set_power_result_channel = Broadcast[SetPowerResult]("set_power_result")

        tracker = BatteryStatusTracker(
            BATTERY_ID,
            max_data_age_sec=5,
            max_blocking_duration_sec=30,
            status_sender=status_channel.new_sender(),
            set_power_result_receiver=set_power_result_channel.new_receiver(),
        )

        assert tracker.battery_id == BATTERY_ID
        assert tracker._last_status == ComponentStatusEnum.NOT_WORKING

        tracker._handle_status_inverter(inverter_data(component_id=INVERTER_ID))
        assert tracker._get_new_status_if_changed() is None

        tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))
        assert tracker._get_new_status_if_changed() is ComponentStatusEnum.WORKING

        # --- Send correct message once again, status should not change ---
        tracker._handle_status_inverter(inverter_data(component_id=INVERTER_ID))
        assert tracker._get_new_status_if_changed() is None

        tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))
        assert tracker._get_new_status_if_changed() is None

        # --- Send outdated message ---
        tracker._handle_status_inverter(
            inverter_data(
                component_id=INVERTER_ID,
                timestamp=datetime.now(tz=timezone.utc) - timedelta(seconds=31),
            )
        )
        assert tracker._get_new_status_if_changed() is ComponentStatusEnum.NOT_WORKING

        # --- BatteryRelayState is invalid. ---
        tracker._handle_status_battery(
            battery_data(
                component_id=BATTERY_ID,
                relay_state=BatteryRelayState.RELAY_STATE_OPENED,
            )
        )
        assert tracker._get_new_status_if_changed() is None

        # --- Inverter started sending data, but battery relays state are still invalid ---
        tracker._handle_status_inverter(inverter_data(component_id=INVERTER_ID))
        assert tracker._get_new_status_if_changed() is None

        tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))
        assert tracker._get_new_status_if_changed() is ComponentStatusEnum.WORKING

        # --- Inverter started sending data, but battery relays state are still invalid ---
        tracker._handle_status_inverter(
            inverter_data(
                component_id=INVERTER_ID,
                component_state=InverterState.COMPONENT_STATE_SWITCHING_OFF,
            )
        )
        assert tracker._get_new_status_if_changed() is ComponentStatusEnum.NOT_WORKING

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

        assert tracker._get_new_status_if_changed() is None

        tracker._handle_status_inverter(
            inverter_data(
                component_id=INVERTER_ID,
                errors=[inverter_critical_error, inverter_warning_error],
            )
        )

        assert tracker._get_new_status_if_changed() is None

        tracker._handle_status_inverter(
            inverter_data(component_id=INVERTER_ID, errors=[inverter_warning_error])
        )

        assert tracker._get_new_status_if_changed() is ComponentStatusEnum.WORKING

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

        assert tracker._get_new_status_if_changed() is None

        tracker._handle_status_battery(
            battery_data(
                component_id=BATTERY_ID,
                errors=[battery_warning_error, battery_critical_error],
            )
        )

        assert tracker._get_new_status_if_changed() is ComponentStatusEnum.NOT_WORKING

        tracker._handle_status_battery(
            battery_data(
                component_id=BATTERY_ID,
                component_state=BatteryState.COMPONENT_STATE_ERROR,
                errors=[battery_warning_error, battery_critical_error],
            )
        )

        assert tracker._get_new_status_if_changed() is None

        # Check if NaN capacity changes the battery status.
        tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))

        assert tracker._get_new_status_if_changed() is ComponentStatusEnum.WORKING

        tracker._handle_status_battery(
            battery_data(component_id=BATTERY_ID, capacity=math.nan)
        )

        assert tracker._get_new_status_if_changed() is ComponentStatusEnum.NOT_WORKING

        await tracker.stop()
        await mock_microgrid.cleanup()

    async def test_sync_blocking_feature(self, mocker: MockerFixture) -> None:
        """Test if status changes when SetPowerResult message is received.

        Tests uses FakeSelect to test status in sync way.
        Otherwise we would have lots of async calls and waiting.

        Args:
            mocker: Pytest mocker fixture.
        """
        mock_microgrid = MockMicrogrid(grid_meter=True)
        mock_microgrid.add_batteries(3)
        await mock_microgrid.start(mocker)

        status_channel = Broadcast[ComponentStatusEnum]("battery_status")
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

            assert tracker._get_new_status_if_changed() is None

            tracker._handle_status_battery(
                battery_data(
                    component_id=BATTERY_ID,
                    component_state=BatteryState.COMPONENT_STATE_ERROR,
                )
            )

            assert tracker._get_new_status_if_changed() is None

            # message is not correct, component should not block.
            tracker._handle_status_set_power_result(
                SetPowerResult(succeed={1}, failed={BATTERY_ID})
            )

            assert tracker._get_new_status_if_changed() is None

            tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))

            assert tracker._get_new_status_if_changed() is ComponentStatusEnum.WORKING

            expected_blocking_timeout = [1, 2, 4, 8, 16, 30, 30]

            for timeout in expected_blocking_timeout:
                # message is not correct, component should not block.
                tracker._handle_status_set_power_result(
                    SetPowerResult(succeed={1}, failed={BATTERY_ID})
                )

                assert (
                    tracker._get_new_status_if_changed()
                    is ComponentStatusEnum.UNCERTAIN
                )

                # Battery should be still blocked, nothing should happen
                time.shift(timeout - 1)
                tracker._handle_status_set_power_result(
                    SetPowerResult(succeed={1}, failed={BATTERY_ID})
                )

                assert tracker._get_new_status_if_changed() is None

                tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))

                assert tracker._get_new_status_if_changed() is None

                time.shift(1)
                tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))

                assert (
                    tracker._get_new_status_if_changed() is ComponentStatusEnum.WORKING
                )

            # should block for 30 sec
            tracker._handle_status_set_power_result(
                SetPowerResult(succeed={1}, failed={BATTERY_ID})
            )

            assert tracker._get_new_status_if_changed() is ComponentStatusEnum.UNCERTAIN
            time.shift(28)

            tracker._handle_status_battery(
                battery_data(
                    component_id=BATTERY_ID,
                    component_state=BatteryState.COMPONENT_STATE_ERROR,
                )
            )

            assert (
                tracker._get_new_status_if_changed() is ComponentStatusEnum.NOT_WORKING
            )

            # Message that changed status to correct should unblock the battery.
            tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))
            assert tracker._get_new_status_if_changed() is ComponentStatusEnum.WORKING

            # should block for 30 sec
            tracker._handle_status_set_power_result(
                SetPowerResult(succeed={1}, failed={BATTERY_ID})
            )
            assert tracker._get_new_status_if_changed() is ComponentStatusEnum.UNCERTAIN
            time.shift(28)

            # If battery succeed, then it should unblock.
            tracker._handle_status_set_power_result(
                SetPowerResult(succeed={BATTERY_ID}, failed={19})
            )
            assert tracker._get_new_status_if_changed() is ComponentStatusEnum.WORKING

        await tracker.stop()
        await mock_microgrid.cleanup()

    async def test_sync_blocking_interrupted_with_with_max_data(
        self, mocker: MockerFixture
    ) -> None:
        """Test if status changes when SetPowerResult message is received.

        Tests uses FakeSelect to test status in sync way.
        Otherwise we would have lots of async calls and waiting.

        Args:
            mocker: Pytest mocker fixture.
        """
        mock_microgrid = MockMicrogrid(grid_meter=True)
        mock_microgrid.add_batteries(3)
        await mock_microgrid.start(mocker)

        status_channel = Broadcast[ComponentStatusEnum]("battery_status")
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
            assert tracker._get_new_status_if_changed() is None

            tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))
            assert tracker._get_new_status_if_changed() is ComponentStatusEnum.WORKING

            tracker._handle_status_set_power_result(
                SetPowerResult(succeed={1}, failed={BATTERY_ID})
            )
            assert tracker._get_new_status_if_changed() is ComponentStatusEnum.UNCERTAIN

            expected_blocking_timeout = [1, 2, 4]
            for timeout in expected_blocking_timeout:
                # message is not correct, component should not block.
                tracker._handle_status_set_power_result(
                    SetPowerResult(succeed={1}, failed={BATTERY_ID})
                )
                assert tracker._get_new_status_if_changed() is None
                time.shift(timeout)

        await tracker.stop()
        await mock_microgrid.cleanup()

    @time_machine.travel("2022-01-01 00:00 UTC", tick=False)
    async def test_sync_blocking_interrupted_with_invalid_message(
        self, mocker: MockerFixture
    ) -> None:
        """Test if status changes when SetPowerResult message is received.

        Tests uses FakeSelect to test status in sync way.
        Otherwise we would have lots of async calls and waiting.

        Args:
            mocker: Pytest mocker fixture.
        """
        mock_microgrid = MockMicrogrid(grid_meter=True)
        mock_microgrid.add_batteries(3)
        await mock_microgrid.start(mocker)

        status_channel = Broadcast[ComponentStatusEnum]("battery_status")
        set_power_result_channel = Broadcast[SetPowerResult]("set_power_result")

        tracker = BatteryStatusTracker(
            BATTERY_ID,
            max_data_age_sec=5,
            max_blocking_duration_sec=30,
            status_sender=status_channel.new_sender(),
            set_power_result_receiver=set_power_result_channel.new_receiver(),
        )

        tracker._handle_status_inverter(inverter_data(component_id=INVERTER_ID))
        assert tracker._get_new_status_if_changed() is None

        tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))
        assert tracker._get_new_status_if_changed() is ComponentStatusEnum.WORKING

        tracker._handle_status_set_power_result(
            SetPowerResult(succeed={1}, failed={BATTERY_ID})
        )
        assert tracker._get_new_status_if_changed() is ComponentStatusEnum.UNCERTAIN

        tracker._handle_status_inverter(
            inverter_data(
                component_id=INVERTER_ID,
                component_state=InverterState.COMPONENT_STATE_ERROR,
            )
        )
        assert tracker._get_new_status_if_changed() is ComponentStatusEnum.NOT_WORKING

        tracker._handle_status_set_power_result(
            SetPowerResult(succeed={1}, failed={BATTERY_ID})
        )
        assert tracker._get_new_status_if_changed() is None

        tracker._handle_status_set_power_result(
            SetPowerResult(succeed={BATTERY_ID}, failed={})
        )
        assert tracker._get_new_status_if_changed() is None

        tracker._handle_status_inverter(inverter_data(component_id=INVERTER_ID))
        assert tracker._get_new_status_if_changed() is ComponentStatusEnum.WORKING

        await tracker.stop()
        await mock_microgrid.cleanup()

    @time_machine.travel("2022-01-01 00:00 UTC", tick=False)
    async def test_timers(self, mocker: MockerFixture) -> None:
        """Test if messages changes battery status.

        Tests uses FakeSelect to test status in sync way.
        Otherwise we would have lots of async calls and waiting.

        Args:
            mocker: Pytest mocker fixture.
        """
        mock_microgrid = MockMicrogrid(grid_meter=True)
        mock_microgrid.add_batteries(3)
        await mock_microgrid.start(mocker)

        status_channel = Broadcast[ComponentStatusEnum]("battery_status")
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
        assert tracker._last_status == ComponentStatusEnum.NOT_WORKING

        tracker._handle_status_inverter(inverter_data(component_id=INVERTER_ID))
        assert tracker._get_new_status_if_changed() is None

        tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))
        assert tracker._get_new_status_if_changed() is ComponentStatusEnum.WORKING

        assert battery_timer_spy.call_count == 1

        tracker._handle_status_battery_timer()
        assert tracker._get_new_status_if_changed() is ComponentStatusEnum.NOT_WORKING

        assert battery_timer_spy.call_count == 1

        tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))
        assert tracker._get_new_status_if_changed() is ComponentStatusEnum.WORKING

        assert battery_timer_spy.call_count == 2

        tracker._handle_status_inverter_timer()
        assert tracker._get_new_status_if_changed() is ComponentStatusEnum.NOT_WORKING

        tracker._handle_status_battery_timer()
        assert tracker._get_new_status_if_changed() is None

        tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))
        assert tracker._get_new_status_if_changed() is None

        tracker._handle_status_inverter(inverter_data(component_id=INVERTER_ID))
        assert tracker._get_new_status_if_changed() is ComponentStatusEnum.WORKING

        assert inverter_timer_spy.call_count == 2
        await tracker.stop()
        await mock_microgrid.cleanup()

    @time_machine.travel("2022-01-01 00:00 UTC", tick=False)
    async def test_async_battery_status(self, mocker: MockerFixture) -> None:
        """Test if status changes.

        Args:
            mocker: Pytest mocker fixture.
        """
        mock_microgrid = MockMicrogrid(grid_meter=True)
        mock_microgrid.add_batteries(3)
        await mock_microgrid.start(mocker)

        status_channel = Broadcast[ComponentStatusEnum]("battery_status")
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
            await mock_microgrid.mock_client.send(
                inverter_data(component_id=INVERTER_ID)
            )
            await mock_microgrid.mock_client.send(battery_data(component_id=BATTERY_ID))
            status = await asyncio.wait_for(status_receiver.receive(), timeout=0.1)
            assert status is ComponentStatusEnum.WORKING

            await set_power_result_sender.send(
                SetPowerResult(succeed={}, failed={BATTERY_ID})
            )
            status = await asyncio.wait_for(status_receiver.receive(), timeout=0.1)
            assert status is ComponentStatusEnum.UNCERTAIN

            time.shift(2)

            await mock_microgrid.mock_client.send(battery_data(component_id=BATTERY_ID))
            status = await asyncio.wait_for(status_receiver.receive(), timeout=0.1)
            assert status is ComponentStatusEnum.WORKING

            await mock_microgrid.mock_client.send(
                inverter_data(
                    component_id=INVERTER_ID,
                    timestamp=datetime.now(tz=timezone.utc) - timedelta(seconds=7),
                )
            )
            status = await asyncio.wait_for(status_receiver.receive(), timeout=0.1)
            assert status is ComponentStatusEnum.NOT_WORKING

            await set_power_result_sender.send(
                SetPowerResult(succeed={}, failed={BATTERY_ID})
            )
            await asyncio.sleep(0.3)
            assert len(status_receiver) == 0

            await mock_microgrid.mock_client.send(
                inverter_data(component_id=INVERTER_ID)
            )
            status = await asyncio.wait_for(status_receiver.receive(), timeout=0.1)
            assert status is ComponentStatusEnum.WORKING

        await tracker.stop()
        await mock_microgrid.cleanup()


class TestBatteryStatusRecovery:
    """Test battery status recovery.

    The following cases are tested:

    - battery/inverter data missing
    - battery/inverter bad state
    - battery/inverter warning/critical error
    - battery capacity missing
    - received stale battery/inverter data
    """

    @pytest.fixture
    async def setup_tracker(
        self, mocker: MockerFixture
    ) -> AsyncIterator[tuple[MockMicrogrid, Receiver[ComponentStatusEnum]]]:
        """Set a BatteryStatusTracker instance up to run tests with."""
        mock_microgrid = MockMicrogrid(grid_meter=True)
        mock_microgrid.add_batteries(1)
        await mock_microgrid.start(mocker)

        status_channel = Broadcast[ComponentStatusEnum]("battery_status")
        set_power_result_channel = Broadcast[SetPowerResult]("set_power_result")

        status_receiver = status_channel.new_receiver()

        _tracker = BatteryStatusTracker(
            BATTERY_ID,
            max_data_age_sec=0.1,
            max_blocking_duration_sec=1,
            status_sender=status_channel.new_sender(),
            set_power_result_receiver=set_power_result_channel.new_receiver(),
        )

        await asyncio.sleep(0.05)

        yield (mock_microgrid, status_receiver)

        await _tracker.stop()
        await mock_microgrid.cleanup()

    async def _send_healthy_battery(
        self, mock_microgrid: MockMicrogrid, timestamp: datetime | None = None
    ) -> None:
        await mock_microgrid.mock_client.send(
            battery_data(
                timestamp=timestamp,
                component_id=BATTERY_ID,
                component_state=BatteryState.COMPONENT_STATE_IDLE,
                relay_state=BatteryRelayState.RELAY_STATE_CLOSED,
            )
        )

    async def _send_battery_missing_capacity(
        self, mock_microgrid: MockMicrogrid
    ) -> None:
        await mock_microgrid.mock_client.send(
            battery_data(
                component_id=BATTERY_ID,
                component_state=BatteryState.COMPONENT_STATE_IDLE,
                relay_state=BatteryRelayState.RELAY_STATE_CLOSED,
                capacity=math.nan,
            )
        )

    async def _send_healthy_inverter(
        self, mock_microgrid: MockMicrogrid, timestamp: datetime | None = None
    ) -> None:
        await mock_microgrid.mock_client.send(
            inverter_data(
                timestamp=timestamp,
                component_id=INVERTER_ID,
                component_state=InverterState.COMPONENT_STATE_IDLE,
            )
        )

    async def _send_bad_state_battery(self, mock_microgrid: MockMicrogrid) -> None:
        await mock_microgrid.mock_client.send(
            battery_data(
                component_id=BATTERY_ID,
                component_state=BatteryState.COMPONENT_STATE_ERROR,
                relay_state=BatteryRelayState.RELAY_STATE_CLOSED,
            )
        )

    async def _send_bad_state_inverter(self, mock_microgrid: MockMicrogrid) -> None:
        await mock_microgrid.mock_client.send(
            inverter_data(
                component_id=INVERTER_ID,
                component_state=InverterState.COMPONENT_STATE_ERROR,
            )
        )

    async def _send_critical_error_battery(self, mock_microgrid: MockMicrogrid) -> None:
        battery_critical_error = BatteryError(
            code=BatteryErrorCode.ERROR_CODE_BLOCK_ERROR,
            level=ErrorLevel.ERROR_LEVEL_CRITICAL,
            msg="",
        )
        await mock_microgrid.mock_client.send(
            battery_data(
                component_id=BATTERY_ID,
                component_state=BatteryState.COMPONENT_STATE_IDLE,
                relay_state=BatteryRelayState.RELAY_STATE_CLOSED,
                errors=[battery_critical_error],
            )
        )

    async def _send_warning_error_battery(self, mock_microgrid: MockMicrogrid) -> None:
        battery_warning_error = BatteryError(
            code=BatteryErrorCode.ERROR_CODE_HIGH_HUMIDITY,
            level=ErrorLevel.ERROR_LEVEL_WARN,
            msg="",
        )
        await mock_microgrid.mock_client.send(
            battery_data(
                component_id=BATTERY_ID,
                component_state=BatteryState.COMPONENT_STATE_IDLE,
                relay_state=BatteryRelayState.RELAY_STATE_CLOSED,
                errors=[battery_warning_error],
            )
        )

    async def _send_critical_error_inverter(
        self, mock_microgrid: MockMicrogrid
    ) -> None:
        inverter_critical_error = InverterError(
            code=InverterErrorCode.ERROR_CODE_UNSPECIFIED,
            level=ErrorLevel.ERROR_LEVEL_CRITICAL,
            msg="",
        )
        await mock_microgrid.mock_client.send(
            inverter_data(
                component_id=INVERTER_ID,
                component_state=InverterState.COMPONENT_STATE_IDLE,
                errors=[inverter_critical_error],
            )
        )

    async def _send_warning_error_inverter(self, mock_microgrid: MockMicrogrid) -> None:
        inverter_warning_error = InverterError(
            code=InverterErrorCode.ERROR_CODE_UNSPECIFIED,
            level=ErrorLevel.ERROR_LEVEL_WARN,
            msg="",
        )
        await mock_microgrid.mock_client.send(
            inverter_data(
                component_id=INVERTER_ID,
                component_state=InverterState.COMPONENT_STATE_IDLE,
                errors=[inverter_warning_error],
            )
        )

    async def test_missing_data(
        self,
        setup_tracker: tuple[MockMicrogrid, Receiver[ComponentStatusEnum]],
    ) -> None:
        """Test recovery after missing data."""
        mock_microgrid, status_receiver = setup_tracker

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid)
        assert await status_receiver.receive() is ComponentStatusEnum.WORKING

        # --- missing battery data ---
        await self._send_healthy_inverter(mock_microgrid)
        assert await status_receiver.receive() is ComponentStatusEnum.NOT_WORKING

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid)
        assert await status_receiver.receive() is ComponentStatusEnum.WORKING

        # --- missing inverter data ---
        await self._send_healthy_battery(mock_microgrid)
        assert await status_receiver.receive() is ComponentStatusEnum.NOT_WORKING

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid)
        assert await status_receiver.receive() is ComponentStatusEnum.WORKING

    async def test_bad_state(
        self,
        setup_tracker: tuple[MockMicrogrid, Receiver[ComponentStatusEnum]],
    ) -> None:
        """Test recovery after bad component state."""
        mock_microgrid, status_receiver = setup_tracker

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid)
        assert await status_receiver.receive() is ComponentStatusEnum.WORKING

        # --- bad battery state ---
        await self._send_healthy_inverter(mock_microgrid)
        await self._send_bad_state_battery(mock_microgrid)
        assert await status_receiver.receive() is ComponentStatusEnum.NOT_WORKING

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid)
        assert await status_receiver.receive() is ComponentStatusEnum.WORKING

        # --- bad inverter state ---
        await self._send_bad_state_inverter(mock_microgrid)
        await self._send_healthy_battery(mock_microgrid)
        assert await status_receiver.receive() is ComponentStatusEnum.NOT_WORKING

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid)
        assert await status_receiver.receive() is ComponentStatusEnum.WORKING

    async def test_critical_error(
        self,
        setup_tracker: tuple[MockMicrogrid, Receiver[ComponentStatusEnum]],
    ) -> None:
        """Test recovery after critical error."""
        mock_microgrid, status_receiver = setup_tracker

        await self._send_healthy_inverter(mock_microgrid)
        await self._send_healthy_battery(mock_microgrid)
        assert await status_receiver.receive() is ComponentStatusEnum.WORKING

        # --- battery warning error (keeps working) ---
        await self._send_healthy_inverter(mock_microgrid)
        await self._send_warning_error_battery(mock_microgrid)
        assert await recv_timeout(status_receiver, timeout=0.1) is _Timeout

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid)

        # --- battery critical error ---
        await self._send_healthy_inverter(mock_microgrid)
        await self._send_critical_error_battery(mock_microgrid)
        assert await status_receiver.receive() is ComponentStatusEnum.NOT_WORKING

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid)
        assert await status_receiver.receive() is ComponentStatusEnum.WORKING

        # --- inverter warning error (keeps working) ---
        await self._send_healthy_battery(mock_microgrid)
        await self._send_warning_error_inverter(mock_microgrid)
        assert await recv_timeout(status_receiver, timeout=0.1) is _Timeout

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid)

        # --- inverter critical error ---
        await self._send_healthy_battery(mock_microgrid)
        await self._send_critical_error_inverter(mock_microgrid)
        assert await status_receiver.receive() is ComponentStatusEnum.NOT_WORKING

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid)
        assert await status_receiver.receive() is ComponentStatusEnum.WORKING

    async def test_missing_capacity(
        self,
        setup_tracker: tuple[MockMicrogrid, Receiver[ComponentStatusEnum]],
    ) -> None:
        """Test recovery after missing capacity."""
        mock_microgrid, status_receiver = setup_tracker

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid)
        assert await status_receiver.receive() is ComponentStatusEnum.WORKING

        await self._send_healthy_inverter(mock_microgrid)
        await self._send_battery_missing_capacity(mock_microgrid)
        assert await status_receiver.receive() is ComponentStatusEnum.NOT_WORKING

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid)
        assert await status_receiver.receive() is ComponentStatusEnum.WORKING

    async def test_stale_data(
        self,
        setup_tracker: tuple[MockMicrogrid, Receiver[ComponentStatusEnum]],
    ) -> None:
        """Test recovery after stale data."""
        mock_microgrid, status_receiver = setup_tracker

        timestamp = datetime.now(timezone.utc)
        await self._send_healthy_battery(mock_microgrid, timestamp)
        await self._send_healthy_inverter(mock_microgrid)
        assert await status_receiver.receive() is ComponentStatusEnum.WORKING

        # --- stale battery data ---
        await self._send_healthy_inverter(mock_microgrid)
        await self._send_healthy_battery(mock_microgrid, timestamp)
        assert await recv_timeout(status_receiver) is _Timeout

        await self._send_healthy_inverter(mock_microgrid)
        await self._send_healthy_battery(mock_microgrid, timestamp)
        assert await recv_timeout(status_receiver) is ComponentStatusEnum.NOT_WORKING

        timestamp = datetime.now(timezone.utc)
        await self._send_healthy_battery(mock_microgrid, timestamp)
        await self._send_healthy_inverter(mock_microgrid, timestamp)
        assert await status_receiver.receive() is ComponentStatusEnum.WORKING

        # --- stale inverter data ---
        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid, timestamp)
        assert await recv_timeout(status_receiver) is _Timeout

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid, timestamp)
        assert await recv_timeout(status_receiver) is ComponentStatusEnum.NOT_WORKING

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid)
        assert await status_receiver.receive() is ComponentStatusEnum.WORKING
