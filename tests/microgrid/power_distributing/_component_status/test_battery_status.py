# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for BatteryStatusTracker."""

# pylint: disable=too-many-lines

import asyncio
import math
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Generic, TypeVar

import pytest
from frequenz.channels import Broadcast, Receiver
from frequenz.client.microgrid import (
    BatteryComponentState,
    BatteryData,
    BatteryError,
    BatteryErrorCode,
    BatteryRelayState,
    ErrorLevel,
    InverterComponentState,
    InverterData,
    InverterError,
    InverterErrorCode,
)
from pytest_mock import MockerFixture
from time_machine import TimeMachineFixture

from frequenz.sdk.microgrid._power_distributing._component_status import (
    BatteryStatusTracker,
    ComponentStatus,
    ComponentStatusEnum,
    SetPowerResult,
)
from tests.timeseries.mock_microgrid import MockMicrogrid

from ....utils.component_data_wrapper import BatteryDataWrapper, InverterDataWrapper
from ....utils.receive_timeout import Timeout, receive_timeout


def battery_data(  # pylint: disable=too-many-arguments,too-many-positional-arguments
    component_id: int,
    timestamp: datetime | None = None,
    relay_state: BatteryRelayState = BatteryRelayState.CLOSED,
    component_state: BatteryComponentState = BatteryComponentState.CHARGING,
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
            Defaults to BatteryRelayState.CLOSED.
        component_state: Component state.
            Defaults to BatteryComponentState.CHARGING.
        errors: List of the components error. By default empty list will be created.
        capacity: Battery capacity.

    Returns:
        BatteryData with given arguments.
    """
    return BatteryDataWrapper(
        component_id=component_id,
        capacity=capacity,
        timestamp=datetime.now(tz=timezone.utc) if timestamp is None else timestamp,
        relay_state=relay_state,
        component_state=component_state,
        errors=list(errors) if errors is not None else [],
    )


def inverter_data(
    component_id: int,
    timestamp: datetime | None = None,
    component_state: InverterComponentState = InverterComponentState.CHARGING,
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
            Defaults to InverterComponentState.CHARGING.
        errors: List of the components error. By default empty list will be created.

    Returns:
        InverterData with given arguments.
    """
    return InverterDataWrapper(
        component_id=component_id,
        timestamp=datetime.now(tz=timezone.utc) if timestamp is None else timestamp,
        component_state=component_state,
        errors=errors,
    )


T = TypeVar("T")


@dataclass
class Message(Generic[T]):
    """Helper class to store FakeSelect data in the `inner` attribute."""

    inner: T


BATTERY_ID = 9
INVERTER_ID = 8


# pylint: disable=protected-access, unused-argument
class TestBatteryStatus:
    """Tests BatteryStatusTracker."""

    async def test_sync_update_status_with_messages(
        self, mocker: MockerFixture, time_machine: TimeMachineFixture
    ) -> None:
        """Test if messages changes battery status.

        Tests uses FakeSelect to test status in sync way.
        Otherwise we would have lots of async calls and waiting.

        Args:
            mocker: Pytest mocker fixture.
            time_machine: Pytest time_machine fixture.
        """
        mock_microgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mock_microgrid.add_batteries(3)

        status_channel = Broadcast[ComponentStatus](name="battery_status")
        set_power_result_channel = Broadcast[SetPowerResult](name="set_power_result")

        async with (
            mock_microgrid,
            BatteryStatusTracker(
                component_id=BATTERY_ID,
                max_data_age=timedelta(seconds=5),
                max_blocking_duration=timedelta(seconds=30),
                status_sender=status_channel.new_sender(),
                set_power_result_receiver=set_power_result_channel.new_receiver(),
            ) as tracker,
        ):
            time_machine.move_to("2022-01-01 00:00 UTC", tick=False)
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
            assert (
                tracker._get_new_status_if_changed() is ComponentStatusEnum.NOT_WORKING
            )

            # --- BatteryRelayState is invalid. ---
            tracker._handle_status_battery(
                battery_data(
                    component_id=BATTERY_ID,
                    relay_state=BatteryRelayState.OPENED,
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
                    component_state=InverterComponentState.SWITCHING_OFF,
                )
            )
            assert (
                tracker._get_new_status_if_changed() is ComponentStatusEnum.NOT_WORKING
            )

            inverter_critical_error = InverterError(
                code=InverterErrorCode.UNSPECIFIED,
                level=ErrorLevel.CRITICAL,
                message="",
            )

            inverter_warning_error = InverterError(
                code=InverterErrorCode.UNSPECIFIED,
                level=ErrorLevel.WARN,
                message="",
            )

            tracker._handle_status_inverter(
                inverter_data(
                    component_id=INVERTER_ID,
                    component_state=InverterComponentState.SWITCHING_OFF,
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
                code=BatteryErrorCode.UNSPECIFIED,
                level=ErrorLevel.CRITICAL,
                message="",
            )

            battery_warning_error = BatteryError(
                code=BatteryErrorCode.UNSPECIFIED,
                level=ErrorLevel.WARN,
                message="",
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

            assert (
                tracker._get_new_status_if_changed() is ComponentStatusEnum.NOT_WORKING
            )

            tracker._handle_status_battery(
                battery_data(
                    component_id=BATTERY_ID,
                    component_state=BatteryComponentState.ERROR,
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

            assert (
                tracker._get_new_status_if_changed() is ComponentStatusEnum.NOT_WORKING
            )

    async def test_sync_blocking_feature(self, mocker: MockerFixture) -> None:
        """Test if status changes when SetPowerResult message is received.

        Tests uses FakeSelect to test status in sync way.
        Otherwise we would have lots of async calls and waiting.

        Args:
            mocker: Pytest mocker fixture.
        """
        import time_machine  # pylint: disable=import-outside-toplevel

        mock_microgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mock_microgrid.add_batteries(3)

        status_channel = Broadcast[ComponentStatus](name="battery_status")
        set_power_result_channel = Broadcast[SetPowerResult](name="set_power_result")

        async with (
            mock_microgrid,
            BatteryStatusTracker(
                # increase max_data_age_sec for blocking tests.
                # Otherwise it will block blocking.
                component_id=BATTERY_ID,
                max_data_age=timedelta(seconds=500),
                max_blocking_duration=timedelta(seconds=30),
                status_sender=status_channel.new_sender(),
                set_power_result_receiver=set_power_result_channel.new_receiver(),
            ) as tracker,
        ):
            with time_machine.travel("2022-01-01 00:00 UTC", tick=False) as time:
                tracker._handle_status_inverter(inverter_data(component_id=INVERTER_ID))

                assert tracker._get_new_status_if_changed() is None

                tracker._handle_status_battery(
                    battery_data(
                        component_id=BATTERY_ID,
                        component_state=BatteryComponentState.ERROR,
                    )
                )

                assert tracker._get_new_status_if_changed() is None

                # message is not correct, component should not block.
                tracker._handle_status_set_power_result(
                    SetPowerResult(succeeded={1}, failed={BATTERY_ID})
                )

                assert tracker._get_new_status_if_changed() is None

                tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))

                assert (
                    tracker._get_new_status_if_changed() is ComponentStatusEnum.WORKING
                )

                expected_blocking_timeout = [1, 2, 4, 8, 16, 30, 30]

                for timeout in expected_blocking_timeout:
                    # message is not correct, component should not block.
                    tracker._handle_status_set_power_result(
                        SetPowerResult(succeeded={1}, failed={BATTERY_ID})
                    )

                    assert (
                        tracker._get_new_status_if_changed()
                        is ComponentStatusEnum.UNCERTAIN
                    )

                    # Battery should be still blocked, nothing should happen
                    time.shift(timeout - 1)
                    tracker._handle_status_set_power_result(
                        SetPowerResult(succeeded={1}, failed={BATTERY_ID})
                    )

                    assert tracker._get_new_status_if_changed() is None

                    tracker._handle_status_battery(
                        battery_data(component_id=BATTERY_ID)
                    )

                    assert tracker._get_new_status_if_changed() is None

                    time.shift(1)
                    tracker._handle_status_battery(
                        battery_data(component_id=BATTERY_ID)
                    )

                    assert (
                        tracker._get_new_status_if_changed()
                        is ComponentStatusEnum.WORKING
                    )

                # should block for 30 sec
                tracker._handle_status_set_power_result(
                    SetPowerResult(succeeded={1}, failed={BATTERY_ID})
                )

                assert (
                    tracker._get_new_status_if_changed()
                    is ComponentStatusEnum.UNCERTAIN
                )
                time.shift(28)

                tracker._handle_status_battery(
                    battery_data(
                        component_id=BATTERY_ID,
                        component_state=BatteryComponentState.ERROR,
                    )
                )

                assert (
                    tracker._get_new_status_if_changed()
                    is ComponentStatusEnum.NOT_WORKING
                )

                # Message that changed status to correct should unblock the battery.
                tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))
                assert (
                    tracker._get_new_status_if_changed() is ComponentStatusEnum.WORKING
                )

                # should block for 30 sec
                tracker._handle_status_set_power_result(
                    SetPowerResult(succeeded={1}, failed={BATTERY_ID})
                )
                assert (
                    tracker._get_new_status_if_changed()
                    is ComponentStatusEnum.UNCERTAIN
                )
                time.shift(28)

                # If battery succeed, then it should unblock.
                tracker._handle_status_set_power_result(
                    SetPowerResult(succeeded={BATTERY_ID}, failed={19})
                )
                assert (
                    tracker._get_new_status_if_changed() is ComponentStatusEnum.WORKING
                )

    async def test_sync_blocking_interrupted_with_with_max_data(
        self, mocker: MockerFixture, time_machine: TimeMachineFixture
    ) -> None:
        """Test if status changes when SetPowerResult message is received.

        Tests uses FakeSelect to test status in sync way.
        Otherwise we would have lots of async calls and waiting.

        Args:
            mocker: Pytest mocker fixture.
            time_machine: Pytest time_machine fixture.
        """
        mock_microgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mock_microgrid.add_batteries(3)

        status_channel = Broadcast[ComponentStatus](name="battery_status")
        set_power_result_channel = Broadcast[SetPowerResult](name="set_power_result")

        async with (
            mock_microgrid,
            BatteryStatusTracker(
                component_id=BATTERY_ID,
                max_data_age=timedelta(seconds=5),
                max_blocking_duration=timedelta(seconds=30),
                status_sender=status_channel.new_sender(),
                set_power_result_receiver=set_power_result_channel.new_receiver(),
            ) as tracker,
        ):
            start = datetime(2022, 1, 1, tzinfo=timezone.utc)
            time_machine.move_to(start, tick=False)

            tracker._handle_status_inverter(inverter_data(component_id=INVERTER_ID))
            assert tracker._get_new_status_if_changed() is None

            tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))
            assert tracker._get_new_status_if_changed() is ComponentStatusEnum.WORKING

            tracker._handle_status_set_power_result(
                SetPowerResult(succeeded={1}, failed={BATTERY_ID})
            )
            assert tracker._get_new_status_if_changed() is ComponentStatusEnum.UNCERTAIN

            expected_blocking_timeout = [1, 2, 4]
            for timeout in expected_blocking_timeout:
                # message is not correct, component should not block.
                tracker._handle_status_set_power_result(
                    SetPowerResult(succeeded={1}, failed={BATTERY_ID})
                )
                assert tracker._get_new_status_if_changed() is None
                time_machine.move_to(start + timedelta(seconds=timeout))

    async def test_sync_blocking_interrupted_with_invalid_message(
        self, mocker: MockerFixture, time_machine: TimeMachineFixture
    ) -> None:
        """Test if status changes when SetPowerResult message is received.

        Tests uses FakeSelect to test status in sync way.
        Otherwise we would have lots of async calls and waiting.

        Args:
            mocker: Pytest mocker fixture.
            time_machine: Pytest time_machine fixture.
        """
        mock_microgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mock_microgrid.add_batteries(3)

        status_channel = Broadcast[ComponentStatus](name="battery_status")
        set_power_result_channel = Broadcast[SetPowerResult](name="set_power_result")

        async with (
            mock_microgrid,
            BatteryStatusTracker(
                component_id=BATTERY_ID,
                max_data_age=timedelta(seconds=5),
                max_blocking_duration=timedelta(seconds=30),
                status_sender=status_channel.new_sender(),
                set_power_result_receiver=set_power_result_channel.new_receiver(),
            ) as tracker,
        ):
            time_machine.move_to("2022-01-01 00:00 UTC", tick=False)

            tracker._handle_status_inverter(inverter_data(component_id=INVERTER_ID))
            assert tracker._get_new_status_if_changed() is None

            tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))
            assert tracker._get_new_status_if_changed() is ComponentStatusEnum.WORKING

            tracker._handle_status_set_power_result(
                SetPowerResult(succeeded={1}, failed={BATTERY_ID})
            )
            assert tracker._get_new_status_if_changed() is ComponentStatusEnum.UNCERTAIN

            tracker._handle_status_inverter(
                inverter_data(
                    component_id=INVERTER_ID,
                    component_state=InverterComponentState.ERROR,
                )
            )
            assert (
                tracker._get_new_status_if_changed() is ComponentStatusEnum.NOT_WORKING
            )

            tracker._handle_status_set_power_result(
                SetPowerResult(succeeded={1}, failed={BATTERY_ID})
            )
            assert tracker._get_new_status_if_changed() is None

            tracker._handle_status_set_power_result(
                SetPowerResult(succeeded={BATTERY_ID}, failed=set())
            )
            assert tracker._get_new_status_if_changed() is None

            tracker._handle_status_inverter(inverter_data(component_id=INVERTER_ID))
            assert tracker._get_new_status_if_changed() is ComponentStatusEnum.WORKING

    async def test_timers(
        self, mocker: MockerFixture, time_machine: TimeMachineFixture
    ) -> None:
        """Test if messages changes battery status.

        Tests uses FakeSelect to test status in sync way.
        Otherwise we would have lots of async calls and waiting.

        Args:
            mocker: Pytest mocker fixture.
            time_machine: Pytest time_machine fixture.
        """
        mock_microgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mock_microgrid.add_batteries(3)

        status_channel = Broadcast[ComponentStatus](name="battery_status")
        set_power_result_channel = Broadcast[SetPowerResult](name="set_power_result")

        async with (
            mock_microgrid,
            BatteryStatusTracker(
                component_id=BATTERY_ID,
                max_data_age=timedelta(seconds=5),
                max_blocking_duration=timedelta(seconds=30),
                status_sender=status_channel.new_sender(),
                set_power_result_receiver=set_power_result_channel.new_receiver(),
            ) as tracker,
        ):
            time_machine.move_to("2022-01-01 00:00 UTC", tick=False)

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
            assert (
                tracker._get_new_status_if_changed() is ComponentStatusEnum.NOT_WORKING
            )

            assert battery_timer_spy.call_count == 1

            tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))
            assert tracker._get_new_status_if_changed() is ComponentStatusEnum.WORKING

            assert battery_timer_spy.call_count == 2

            tracker._handle_status_inverter_timer()
            assert (
                tracker._get_new_status_if_changed() is ComponentStatusEnum.NOT_WORKING
            )

            tracker._handle_status_battery_timer()
            assert tracker._get_new_status_if_changed() is None

            tracker._handle_status_battery(battery_data(component_id=BATTERY_ID))
            assert tracker._get_new_status_if_changed() is None

            tracker._handle_status_inverter(inverter_data(component_id=INVERTER_ID))
            assert tracker._get_new_status_if_changed() is ComponentStatusEnum.WORKING

            assert inverter_timer_spy.call_count == 2

    async def test_async_battery_status(self, mocker: MockerFixture) -> None:
        """Test if status changes.

        Args:
            mocker: Pytest mocker fixture.
        """
        mock_microgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mock_microgrid.add_batteries(3)

        status_channel = Broadcast[ComponentStatus](name="battery_status")
        set_power_result_channel = Broadcast[SetPowerResult](name="set_power_result")

        status_receiver = status_channel.new_receiver()
        set_power_result_sender = set_power_result_channel.new_sender()

        async with (
            mock_microgrid,
            BatteryStatusTracker(
                component_id=BATTERY_ID,
                max_data_age=timedelta(seconds=5),
                max_blocking_duration=timedelta(seconds=30),
                status_sender=status_channel.new_sender(),
                set_power_result_receiver=set_power_result_channel.new_receiver(),
            ),
        ):
            import time_machine  # pylint: disable=import-outside-toplevel

            await asyncio.sleep(0.01)

            with time_machine.travel("2022-01-01 00:00 UTC", tick=False) as time:
                await mock_microgrid.mock_client.send(
                    inverter_data(component_id=INVERTER_ID)
                )
                await mock_microgrid.mock_client.send(
                    battery_data(component_id=BATTERY_ID)
                )
                status = await asyncio.wait_for(status_receiver.receive(), timeout=0.1)
                assert status.value is ComponentStatusEnum.WORKING

                await set_power_result_sender.send(
                    SetPowerResult(succeeded=set(), failed={BATTERY_ID})
                )
                status = await asyncio.wait_for(status_receiver.receive(), timeout=0.1)
                assert status.value is ComponentStatusEnum.UNCERTAIN

                time.shift(2)

                await mock_microgrid.mock_client.send(
                    battery_data(component_id=BATTERY_ID)
                )
                status = await asyncio.wait_for(status_receiver.receive(), timeout=0.1)
                assert status.value is ComponentStatusEnum.WORKING

                await mock_microgrid.mock_client.send(
                    inverter_data(
                        component_id=INVERTER_ID,
                        timestamp=datetime.now(tz=timezone.utc) - timedelta(seconds=7),
                    )
                )
                status = await asyncio.wait_for(status_receiver.receive(), timeout=0.1)
                assert status.value is ComponentStatusEnum.NOT_WORKING

                await set_power_result_sender.send(
                    SetPowerResult(succeeded=set(), failed={BATTERY_ID})
                )
                time.shift(10)
                await asyncio.sleep(0.3)
                with pytest.raises(asyncio.TimeoutError):
                    await asyncio.wait_for(status_receiver.receive(), timeout=0.1)

                await mock_microgrid.mock_client.send(
                    inverter_data(component_id=INVERTER_ID)
                )
                status = await asyncio.wait_for(status_receiver.receive(), timeout=0.1)
                assert status.value is ComponentStatusEnum.WORKING


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
        self,
        mocker: MockerFixture,
    ) -> AsyncIterator[tuple[MockMicrogrid, Receiver[ComponentStatus]]]:
        """Set a BatteryStatusTracker instance up to run tests with."""
        mock_microgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mock_microgrid.add_batteries(1)

        status_channel = Broadcast[ComponentStatus](name="battery_status")
        set_power_result_channel = Broadcast[SetPowerResult](name="set_power_result")

        status_receiver = status_channel.new_receiver()

        async with (
            mock_microgrid,
            BatteryStatusTracker(
                component_id=BATTERY_ID,
                max_data_age=timedelta(seconds=0.2),
                max_blocking_duration=timedelta(seconds=1),
                status_sender=status_channel.new_sender(),
                set_power_result_receiver=set_power_result_channel.new_receiver(),
            ),
        ):
            await asyncio.sleep(0.05)
            yield (mock_microgrid, status_receiver)

    async def _send_healthy_battery(
        self, mock_microgrid: MockMicrogrid, timestamp: datetime | None = None
    ) -> None:
        await mock_microgrid.mock_client.send(
            battery_data(
                timestamp=timestamp,
                component_id=BATTERY_ID,
                component_state=BatteryComponentState.IDLE,
                relay_state=BatteryRelayState.CLOSED,
            )
        )

    async def _send_battery_missing_capacity(
        self, mock_microgrid: MockMicrogrid
    ) -> None:
        await mock_microgrid.mock_client.send(
            battery_data(
                component_id=BATTERY_ID,
                component_state=BatteryComponentState.IDLE,
                relay_state=BatteryRelayState.CLOSED,
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
                component_state=InverterComponentState.IDLE,
            )
        )

    async def _send_bad_state_battery(self, mock_microgrid: MockMicrogrid) -> None:
        await mock_microgrid.mock_client.send(
            battery_data(
                component_id=BATTERY_ID,
                component_state=BatteryComponentState.ERROR,
                relay_state=BatteryRelayState.CLOSED,
            )
        )

    async def _send_bad_state_inverter(self, mock_microgrid: MockMicrogrid) -> None:
        await mock_microgrid.mock_client.send(
            inverter_data(
                component_id=INVERTER_ID,
                component_state=InverterComponentState.ERROR,
            )
        )

    async def _send_critical_error_battery(self, mock_microgrid: MockMicrogrid) -> None:
        battery_critical_error = BatteryError(
            code=BatteryErrorCode.BLOCK_ERROR,
            level=ErrorLevel.CRITICAL,
            message="",
        )
        await mock_microgrid.mock_client.send(
            battery_data(
                component_id=BATTERY_ID,
                component_state=BatteryComponentState.IDLE,
                relay_state=BatteryRelayState.CLOSED,
                errors=[battery_critical_error],
            )
        )

    async def _send_warning_error_battery(self, mock_microgrid: MockMicrogrid) -> None:
        battery_warning_error = BatteryError(
            code=BatteryErrorCode.HIGH_HUMIDITY,
            level=ErrorLevel.WARN,
            message="",
        )
        await mock_microgrid.mock_client.send(
            battery_data(
                component_id=BATTERY_ID,
                component_state=BatteryComponentState.IDLE,
                relay_state=BatteryRelayState.CLOSED,
                errors=[battery_warning_error],
            )
        )

    async def _send_critical_error_inverter(
        self, mock_microgrid: MockMicrogrid
    ) -> None:
        inverter_critical_error = InverterError(
            code=InverterErrorCode.UNSPECIFIED,
            level=ErrorLevel.CRITICAL,
            message="",
        )
        await mock_microgrid.mock_client.send(
            inverter_data(
                component_id=INVERTER_ID,
                component_state=InverterComponentState.IDLE,
                errors=[inverter_critical_error],
            )
        )

    async def _send_warning_error_inverter(self, mock_microgrid: MockMicrogrid) -> None:
        inverter_warning_error = InverterError(
            code=InverterErrorCode.UNSPECIFIED,
            level=ErrorLevel.WARN,
            message="",
        )
        await mock_microgrid.mock_client.send(
            inverter_data(
                component_id=INVERTER_ID,
                component_state=InverterComponentState.IDLE,
                errors=[inverter_warning_error],
            )
        )

    async def test_missing_data(
        self,
        setup_tracker: tuple[MockMicrogrid, Receiver[ComponentStatus]],
    ) -> None:
        """Test recovery after missing data."""
        mock_microgrid, status_receiver = setup_tracker

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid)
        assert (await status_receiver.receive()).value is ComponentStatusEnum.WORKING

        # --- missing battery data ---
        await self._send_healthy_inverter(mock_microgrid)
        assert (
            await status_receiver.receive()
        ).value is ComponentStatusEnum.NOT_WORKING

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid)
        assert (await status_receiver.receive()).value is ComponentStatusEnum.WORKING

        # --- missing inverter data ---
        await self._send_healthy_battery(mock_microgrid)
        assert (
            await status_receiver.receive()
        ).value is ComponentStatusEnum.NOT_WORKING

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid)
        assert (await status_receiver.receive()).value is ComponentStatusEnum.WORKING

    async def test_bad_state(
        self,
        setup_tracker: tuple[MockMicrogrid, Receiver[ComponentStatus]],
    ) -> None:
        """Test recovery after bad component state."""
        mock_microgrid, status_receiver = setup_tracker

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid)
        assert (await status_receiver.receive()).value is ComponentStatusEnum.WORKING

        # --- bad battery state ---
        await self._send_healthy_inverter(mock_microgrid)
        await self._send_bad_state_battery(mock_microgrid)
        assert (
            await status_receiver.receive()
        ).value is ComponentStatusEnum.NOT_WORKING

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid)
        assert (await status_receiver.receive()).value is ComponentStatusEnum.WORKING

        # --- bad inverter state ---
        await self._send_bad_state_inverter(mock_microgrid)
        await self._send_healthy_battery(mock_microgrid)
        assert (
            await status_receiver.receive()
        ).value is ComponentStatusEnum.NOT_WORKING

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid)
        assert (await status_receiver.receive()).value is ComponentStatusEnum.WORKING

    async def test_critical_error(
        self,
        setup_tracker: tuple[MockMicrogrid, Receiver[ComponentStatus]],
    ) -> None:
        """Test recovery after critical error."""
        mock_microgrid, status_receiver = setup_tracker

        await self._send_healthy_inverter(mock_microgrid)
        await self._send_healthy_battery(mock_microgrid)
        assert (await status_receiver.receive()).value is ComponentStatusEnum.WORKING

        # --- battery warning error (keeps working) ---
        await self._send_healthy_inverter(mock_microgrid)
        await self._send_warning_error_battery(mock_microgrid)
        assert await receive_timeout(status_receiver, timeout=0.1) is Timeout

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid)

        # --- battery critical error ---
        await self._send_healthy_inverter(mock_microgrid)
        await self._send_critical_error_battery(mock_microgrid)
        assert (
            await status_receiver.receive()
        ).value is ComponentStatusEnum.NOT_WORKING

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid)
        assert (await status_receiver.receive()).value is ComponentStatusEnum.WORKING

        # --- inverter warning error (keeps working) ---
        await self._send_healthy_battery(mock_microgrid)
        await self._send_warning_error_inverter(mock_microgrid)
        assert await receive_timeout(status_receiver, timeout=0.1) is Timeout

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid)

        # --- inverter critical error ---
        await self._send_healthy_battery(mock_microgrid)
        await self._send_critical_error_inverter(mock_microgrid)
        assert (
            await status_receiver.receive()
        ).value is ComponentStatusEnum.NOT_WORKING

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid)
        assert (await status_receiver.receive()).value is ComponentStatusEnum.WORKING

    async def test_missing_capacity(
        self,
        setup_tracker: tuple[MockMicrogrid, Receiver[ComponentStatus]],
    ) -> None:
        """Test recovery after missing capacity."""
        mock_microgrid, status_receiver = setup_tracker

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid)
        assert (await status_receiver.receive()).value is ComponentStatusEnum.WORKING

        await self._send_healthy_inverter(mock_microgrid)
        await self._send_battery_missing_capacity(mock_microgrid)
        assert (
            await status_receiver.receive()
        ).value is ComponentStatusEnum.NOT_WORKING

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid)
        assert (await status_receiver.receive()).value is ComponentStatusEnum.WORKING

    async def test_stale_data(
        self,
        setup_tracker: tuple[MockMicrogrid, Receiver[ComponentStatus]],
    ) -> None:
        """Test recovery after stale data."""
        mock_microgrid, status_receiver = setup_tracker

        timestamp = datetime.now(timezone.utc)
        await self._send_healthy_battery(mock_microgrid, timestamp)
        await self._send_healthy_inverter(mock_microgrid)
        assert (await status_receiver.receive()).value is ComponentStatusEnum.WORKING

        # --- stale battery data ---
        await self._send_healthy_inverter(mock_microgrid)
        await self._send_healthy_battery(mock_microgrid, timestamp)
        assert await receive_timeout(status_receiver) is Timeout

        await self._send_healthy_inverter(mock_microgrid)
        await self._send_healthy_battery(mock_microgrid, timestamp)
        assert await receive_timeout(status_receiver, 0.3) == ComponentStatus(
            BATTERY_ID, ComponentStatusEnum.NOT_WORKING
        )

        timestamp = datetime.now(timezone.utc)
        await self._send_healthy_battery(mock_microgrid, timestamp)
        await self._send_healthy_inverter(mock_microgrid, timestamp)
        assert (await status_receiver.receive()).value is ComponentStatusEnum.WORKING

        # --- stale inverter data ---
        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid, timestamp)
        assert await receive_timeout(status_receiver) is Timeout

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid, timestamp)
        assert await receive_timeout(status_receiver, 0.3) == ComponentStatus(
            BATTERY_ID, ComponentStatusEnum.NOT_WORKING
        )

        await self._send_healthy_battery(mock_microgrid)
        await self._send_healthy_inverter(mock_microgrid)
        assert (await status_receiver.receive()).value is ComponentStatusEnum.WORKING
