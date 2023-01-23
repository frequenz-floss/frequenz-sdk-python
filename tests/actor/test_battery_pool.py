# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH
"""Tests for BatteryPoolStatus module."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Iterable, List, Optional, Set

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
from google.protobuf.timestamp_pb2 import Timestamp  # pylint: disable=no-name-in-module
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk._internal.asyncio import NotSyncConstructible
from frequenz.sdk.actor.power_distributing import PartialFailure, Request, Success
from frequenz.sdk.actor.power_distributing._battery_pool_status import BatteryPoolStatus
from frequenz.sdk.microgrid.component import BatteryData, InverterData
from tests.microgrid import mock_api


@dataclass
class BatInvIdsPair:
    """Tuple with ids of battery and adjacent inverter."""

    bat_id: int
    inv_id: int


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


# pylint: disable=too-many-public-methods
class TestBatteryPoolStatus:
    """Test BatteryPoolStatus class."""

    async def my_setup(
        self, mocker: MockerFixture, batteries_num: int
    ) -> List[BatInvIdsPair]:
        """Initialize a mock microgrid api for a test.

        Because we can't create a __init__ or multiple instances of the Test class, we
        use the `setup` method as a constructor, and call it once before each test.
        """
        microgrid._microgrid._MICROGRID = None  # pylint: disable=protected-access
        servicer = mock_api.MockMicrogridServicer()

        servicer.add_component(
            1, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_GRID
        )
        servicer.add_component(
            3, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_JUNCTION
        )
        servicer.add_component(
            4, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_METER
        )
        servicer.add_connection(1, 3)
        servicer.add_connection(3, 4)

        pairs: List[BatInvIdsPair] = []
        for idx in range(10, 10 + batteries_num):
            servicer.add_component(
                3 * idx, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_METER
            )
            servicer.add_component(
                3 * idx + 1, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_INVERTER
            )
            servicer.add_component(
                3 * idx + 2, microgrid_pb.ComponentCategory.COMPONENT_CATEGORY_BATTERY
            )

            servicer.add_connection(3, 3 * idx)
            servicer.add_connection(3 * idx, 3 * idx + 1)
            servicer.add_connection(3 * idx + 1, 3 * idx + 2)
            pairs.append(BatInvIdsPair(3 * idx + 2, 3 * idx + 1))

        # pylint: disable=attribute-defined-outside-init
        self.server = mock_api.MockGrpcServer(servicer, port=57891)
        # pylint: enable=attribute-defined-outside-init
        await self.server.start()

        await microgrid.initialize("[::1]", 57891)

        api_client = microgrid.get().api_client
        mocker.patch.object(
            api_client,
            "battery_data",
            # create new mock with every call
            mocker.AsyncMock(side_effect=lambda _: mocker.MagicMock()),
        )

        # pylint: disable=no-member
        mock = api_client.battery_data.into_peekable  # type: ignore[attr-defined]
        mock.return_value = mocker.MagicMock()

        mocker.patch.object(
            api_client,
            "inverter_data",
            # create new mock with every call
            mocker.AsyncMock(side_effect=lambda _: mocker.MagicMock()),
        )

        # pylint: disable=no-member
        mock = api_client.inverter_data.into_peekable  # type: ignore[attr-defined]
        mock.return_value = mocker.MagicMock()
        return pairs

    def set_battery_message(
        self,
        battery_pool: BatteryPoolStatus,
        bat_inv_pairs: List[BatInvIdsPair],
        create_msg: Callable[[int], Optional[BatteryData]],
    ) -> None:
        """Set mock receiver to return component returned by create_msg.

        Set mock receivers only for batteries given in bat_inv_pairs.
        Note that receiver will return given data with every call, until it will
            be set once again.

        Args:
            battery_pool: BatteryPoolStatus instance
            bat_inv_pairs: Pairs with ids of battery and adjacent inverter,
            create_msg: how to create BatteryData that should be returned
                from the receiver
        """
        for pair in bat_inv_pairs:
            bat_id = pair.bat_id

            # Use protected access to set expected output from the `peek`.
            # pylint: disable=protected-access
            recv = battery_pool._batteries[bat_id]._battery_receiver.receiver
            recv.peek.return_value = create_msg(bat_id)  # type: ignore[attr-defined]

    def set_inverter_message(
        self,
        battery_pool: BatteryPoolStatus,
        bat_inv_pairs: List[BatInvIdsPair],
        create_msg: Callable[[int], Optional[InverterData]],
    ) -> None:
        """Set mock receiver to return component returned by create_msg.

        Set mock receivers only for batteries given in bat_inv_pairs.
        Note that receiver will return given data with every call, until it will
            be set once again.

        Args:
            battery_pool: BatteryPoolStatus instance
            bat_inv_pairs: Pairs with ids of battery and adjacent inverter,
            create_msg: how to create BatteryData that should be returned
                from the receiver
        """
        for pair in bat_inv_pairs:
            bat_id = pair.bat_id
            inv_id = pair.inv_id

            # Use protected access to set expected output from the `peek`.
            # pylint: disable=protected-access
            inv_receiver = battery_pool._batteries[bat_id]._inverter_receiver.receiver
            inv_receiver.peek.return_value = create_msg(  # type: ignore[attr-defined]
                inv_id
            )

    async def cleanup(self) -> None:
        """Clean up after a test."""
        await self.server.graceful_shutdown()
        microgrid._microgrid._MICROGRID = None  # pylint: disable=protected-access

    def scenario_all_msg_correct(
        self, battery_pool: BatteryPoolStatus, pairs: List[BatInvIdsPair]
    ) -> None:
        """Set all receivers to return correct ComponentData.

        Args:
            battery_pool: BatteryPoolStatus instance
            bat_inv_pairs: Pairs with ids of battery and adjacent inverter.
        """

        self.set_battery_message(battery_pool, pairs, battery_data)
        self.set_inverter_message(battery_pool, pairs, inverter_data)
        batteries = {pair.bat_id for pair in pairs}
        assert battery_pool.get_working_batteries(batteries) == batteries

    def scenario_no_message(
        self, battery_pool: BatteryPoolStatus, pairs: List[BatInvIdsPair]
    ) -> None:
        """Set all receivers to return None instead of ComponentData.

        Args:
            battery_pool: BatteryPoolStatus instance
            bat_inv_pairs: Pairs with ids of battery and adjacent inverter.
        """
        self.set_battery_message(battery_pool, pairs, lambda _: None)
        self.set_inverter_message(battery_pool, pairs, lambda _: None)
        batteries = {pair.bat_id for pair in pairs}
        assert battery_pool.get_working_batteries(batteries) == set()

    def set_battery_outdated(
        self, battery_pool: BatteryPoolStatus, pairs: List[BatInvIdsPair]
    ) -> None:
        """Set outdated message for the given batteries.

        Args:
            battery_pool: Created battery pool
            pairs: pairs with batteries that should has this message
        """

        # Battery didn't send data for long time (last message is outdated)
        old_timestamp = datetime.now(tz=pytz.UTC) - timedelta(seconds=35)
        self.set_battery_message(
            battery_pool,
            pairs,
            lambda id: battery_data(id, timestamp=old_timestamp),
        )

    def set_inverter_outdated(
        self, battery_pool: BatteryPoolStatus, pairs: List[BatInvIdsPair]
    ) -> None:
        """Set outdated message for the given inverters.

        Args:
            battery_pool: Created battery pool
            pairs: pairs with inverters that should has this message
        """

        # Battery didn't send data for long time (last message is outdated)
        old_timestamp = datetime.now(tz=pytz.UTC) - timedelta(seconds=35)
        self.set_inverter_message(
            battery_pool,
            pairs,
            lambda id: inverter_data(id, timestamp=old_timestamp),
        )

    def set_battery_state(
        self,
        battery_pool: BatteryPoolStatus,
        pairs: List[BatInvIdsPair],
        state: BatteryState.ValueType,
    ) -> None:
        """Set state for the battery.

        Args:
            battery_pool: Created battery pool
            pairs: pairs with batteries that should has this state
            state: state that should be set
        """

        self.set_battery_message(
            battery_pool,
            pairs,
            lambda id: battery_data(id, component_state=state),
        )

    def set_inverter_state(
        self,
        battery_pool: BatteryPoolStatus,
        pairs: List[BatInvIdsPair],
        state: InverterState.ValueType,
    ) -> None:
        """Set state for the inverter.

        Args:
            battery_pool: Created battery pool
            pairs: pairs with inverter that should has this state
            state: state that should be set
        """

        self.set_inverter_message(
            battery_pool,
            pairs,
            lambda id: inverter_data(id, component_state=state),
        )

    def set_battery_relay_state(
        self,
        battery_pool: BatteryPoolStatus,
        pairs: List[BatInvIdsPair],
        state: BatteryRelayState.ValueType,
    ) -> None:
        """Set relay state for the battery.

        Args:
            battery_pool: Created battery pool
            pairs: pairs with batteries that should has this state
            battery_pool: state that should be set
        """

        self.set_battery_message(
            battery_pool,
            pairs,
            lambda id: battery_data(id, relay_state=state),
        )

    def set_inverter_error(
        self,
        battery_pool: BatteryPoolStatus,
        pairs: List[BatInvIdsPair],
        errors: List[ErrorLevel.ValueType],
    ) -> None:
        """Create inverter message with given errors.

        Args:
            battery_pool: Created battery pool
            pairs: pairs with batteries that should has this state
            errors: errors that battery should return
        """
        self.set_inverter_message(
            battery_pool,
            pairs,
            lambda id: inverter_data(
                id,
                errors=[
                    InverterError(
                        code=InverterErrorCode.ERROR_CODE_UNSPECIFIED,
                        level=err_lvl,
                        msg="",
                    )
                    for err_lvl in errors
                ],
            ),
        )

    def set_battery_error(
        self,
        battery_pool: BatteryPoolStatus,
        pairs: List[BatInvIdsPair],
        errors: List[ErrorLevel.ValueType],
    ) -> None:
        """Create battery message with given errors.

        Args:
            battery_pool: Created battery pool
            pairs: pairs with batteries that should has this state
            errors: errors that battery should return
        """
        self.set_battery_message(
            battery_pool,
            pairs,
            lambda id: battery_data(
                id,
                errors=[
                    BatteryError(
                        code=BatteryErrorCode.ERROR_CODE_UNSPECIFIED,
                        level=err_lvl,
                        msg="",
                    )
                    for err_lvl in errors
                ],
            ),
        )

    def test_create_sync_pool(self) -> None:
        """Test if error is raised after calling default constructor."""
        with pytest.raises(NotSyncConstructible):
            BatteryPoolStatus()

    async def test_scenario_one_battery(self, mocker: MockerFixture) -> None:
        """Test scenario with one battery.

        Args:
            mocker: pytest mocker.
        """
        pairs = await self.my_setup(mocker=mocker, batteries_num=1)

        batteries = {pair.bat_id for pair in pairs}
        battery_pool = await BatteryPoolStatus.async_new(
            batteries, max_data_age_sec=30, max_blocking_duration_sec=30
        )

        self.scenario_all_msg_correct(battery_pool, pairs)
        self.scenario_no_message(battery_pool, pairs)
        self.scenario_all_msg_correct(battery_pool, pairs)

        self.set_battery_outdated(battery_pool, [pairs[0]])
        assert battery_pool.get_working_batteries(batteries) == set()

        await self.cleanup()

    async def test_scenario_five_batteries(self, mocker: MockerFixture) -> None:
        """Tests with 5 battery-inverter pairs.

        Args:
            mocker: pytest mocker.
        """
        pairs = await self.my_setup(mocker=mocker, batteries_num=5)
        batteries = {pair.bat_id for pair in pairs}
        battery_pool = await BatteryPoolStatus.async_new(
            batteries, max_data_age_sec=30, max_blocking_duration_sec=30
        )

        # Starting server takes long time, so it is better to start it once and
        # then run tests.
        self.scenario_batteries_become_broken(battery_pool, pairs)
        self.scenario_invalid_state(battery_pool, pairs)
        self.scenario_error(battery_pool, pairs)
        self.scenario_block_component(battery_pool, pairs)
        self.scenario_blocking_timeout_increases(battery_pool, pairs)
        self.scenario_state_changes_for_blocked_component(battery_pool, pairs)
        self.scenario_unknown_batteries_are_used(battery_pool, pairs)
        await self.cleanup()

    def scenario_batteries_become_broken(
        self, battery_pool: BatteryPoolStatus, pairs: List[BatInvIdsPair]
    ) -> None:
        """Test what happens if all batteries are broken.

        Args:
            battery_pool: Created battery pool
            pairs: All battery and inverter pairs in the pool
        """
        print(f"{__name__}::scenario_batteries_become_broken")
        self.scenario_all_msg_correct(battery_pool, pairs)
        batteries = {pair.bat_id for pair in pairs}
        expected = batteries

        self.set_battery_message(battery_pool, [pairs[0]], lambda _: None)
        expected = expected - {pairs[0].bat_id}
        assert battery_pool.get_working_batteries(batteries) == expected

        self.set_inverter_message(battery_pool, [pairs[2]], lambda _: None)
        expected = expected - {pairs[2].bat_id}
        assert battery_pool.get_working_batteries(batteries) == expected

        self.set_battery_outdated(battery_pool, [pairs[1]])
        expected = expected - {pairs[1].bat_id}
        assert battery_pool.get_working_batteries(batteries) == expected

        self.set_inverter_outdated(battery_pool, [pairs[3]])
        expected = expected - {pairs[3].bat_id}
        assert battery_pool.get_working_batteries(batteries) == expected

        self.set_battery_state(
            battery_pool, [pairs[0]], BatteryState.COMPONENT_STATE_ERROR
        )
        expected = expected - {pairs[0].bat_id}
        assert battery_pool.get_working_batteries(batteries) == expected

        self.set_battery_message(battery_pool, [pairs[0]], battery_data)
        expected = expected | {pairs[0].bat_id}
        assert battery_pool.get_working_batteries(batteries) == expected

    def scenario_invalid_state(
        self, battery_pool: BatteryPoolStatus, pairs: List[BatInvIdsPair]
    ) -> None:
        """Test all the scenarios where components have invalid state.

        Args:
            battery_pool: Created battery pool
            pairs: All battery and inverter pairs in the pool
        """
        print(f"{__name__}::scenario_invalid_state")
        self.scenario_all_msg_correct(battery_pool, pairs)
        batteries = {pair.bat_id for pair in pairs}
        expected = batteries

        self.set_battery_relay_state(
            battery_pool, [pairs[2]], BatteryRelayState.RELAY_STATE_OPENED
        )
        expected = expected - {pairs[2].bat_id}
        assert battery_pool.get_working_batteries(batteries) == expected

        self.set_inverter_state(
            battery_pool, [pairs[2]], InverterState.COMPONENT_STATE_UNAVAILABLE
        )
        expected = expected - {pairs[2].bat_id}
        assert battery_pool.get_working_batteries(batteries) == expected

        # Battery is in correct state, but adjacent inverter is still in invalid state.
        self.set_battery_message(battery_pool, [pairs[2]], battery_data)
        assert battery_pool.get_working_batteries(batteries) == expected

        # Inverter changed state, but also invalid
        self.set_inverter_state(
            battery_pool, [pairs[2]], InverterState.COMPONENT_STATE_UNSPECIFIED
        )
        assert battery_pool.get_working_batteries(batteries) == expected

        # Inverter has is in correct state
        self.set_inverter_message(battery_pool, [pairs[2]], inverter_data)
        expected = expected | {pairs[2].bat_id}
        assert battery_pool.get_working_batteries(batteries) == expected

    def scenario_error(
        self, battery_pool: BatteryPoolStatus, pairs: List[BatInvIdsPair]
    ) -> None:
        """Test all the scenarios where components have errors.

        Args:
            battery_pool: Created battery pool
            pairs: All battery and inverter pairs in the pool
        """
        print(f"{__name__}::scenario_error")
        self.scenario_all_msg_correct(battery_pool, pairs)
        batteries = {pair.bat_id for pair in pairs}
        expected = batteries

        # Inverter 7 has critical error
        errors = [ErrorLevel.ERROR_LEVEL_WARN, ErrorLevel.ERROR_LEVEL_CRITICAL]
        self.set_inverter_error(battery_pool, [pairs[3]], errors)
        expected = expected - {pairs[3].bat_id}
        assert battery_pool.get_working_batteries(batteries) == expected

        # Inverter 9 has warning error. This should not exclude it from set of working
        # batteries.
        errors = [ErrorLevel.ERROR_LEVEL_WARN]
        self.set_inverter_error(battery_pool, [pairs[4]], errors)
        assert battery_pool.get_working_batteries(batteries) == expected

        # Battery 0 has critical error.
        errors = [ErrorLevel.ERROR_LEVEL_CRITICAL]
        self.set_battery_error(battery_pool, [pairs[0]], errors)
        expected = expected - {pairs[0].bat_id}
        assert battery_pool.get_working_batteries(batteries) == expected

    # Name is invalid but it is more readable like this.
    # pylint: disable=invalid-name
    def fake_PartialFailure(
        self,
        requested_batteries: Set[int],
        failed_batteries: Set[int],
        succeed_batteries: Set[int],
    ) -> PartialFailure:
        """Create fake PartialFailure returned by PowerDistributingActor.

        Args:
            requested_batteries: Set of batteries that were requested. This message
                respond for that request.
            failed_batteries: Subset of the requested batteries for which the request
                failed.
            succeed_batteries: Subset of the requested batteries for which the request
                succeed.

        Returns:
            Fake PartialFailure.
        """
        return PartialFailure(
            request=Request(power=0, batteries=requested_batteries),
            succeed_power=0,
            succeed_batteries=succeed_batteries,
            failed_power=0,
            failed_batteries=failed_batteries,
            excess_power=0,
        )

    # Name is invalid but it is more readable like this.
    # pylint: disable=invalid-name
    def fake_Success(
        self, requested_batteries: Set[int], used_batteries: Set[int]
    ) -> Success:
        """Create fake Success returned by PowerDistributingActor.

        Args:
            requested_batteries: Set of batteries that were requested. This message
                respond for that request.
            used_batteries: Subset of the requested batteries for which the request
                succeed.

        Returns:
            Fake Success message.
        """
        return Success(
            request=Request(power=0, batteries=requested_batteries),
            succeed_power=0,
            used_batteries=used_batteries,
            excess_power=0,
        )

    def scenario_block_component(
        self, battery_pool: BatteryPoolStatus, pairs: List[BatInvIdsPair]
    ) -> None:
        """Test all the scenarios where components are blocked.

        Args:
            battery_pool: Created battery pool
            pairs: All battery and inverter pairs in the pool
        """

        print(f"{__name__}::scenario_block_component")
        with time_machine.travel("2022-01-01 00:00 UTC", tick=False) as time:
            self.scenario_all_msg_correct(battery_pool, pairs)
            batteries = {pair.bat_id for pair in pairs}
            expected = batteries

            failed_batteries = {pairs[2].bat_id, pairs[4].bat_id}
            # Notify that batteries 4 and 6 failed in last request.
            resp = self.fake_PartialFailure(
                requested_batteries=batteries,
                failed_batteries=failed_batteries,
                succeed_batteries=batteries - failed_batteries,
            )
            battery_pool.update_last_request_status(resp)
            expected = expected - failed_batteries
            working_batteries = battery_pool.get_working_batteries(batteries)
            assert working_batteries == expected

            # Previous components should still be blocked.
            time.shift(timedelta(seconds=0.5))
            working_batteries = battery_pool.get_working_batteries(batteries)
            assert working_batteries == expected

            failed_batteries = {pairs[0].bat_id}
            # Notify that another battery failed in last request. Now we have three
            # batteries blocked.
            resp = self.fake_PartialFailure(
                requested_batteries=batteries,
                failed_batteries=failed_batteries,
                succeed_batteries=working_batteries - failed_batteries,
            )
            battery_pool.update_last_request_status(resp)
            expected = expected - failed_batteries
            working_batteries = battery_pool.get_working_batteries(batteries)
            assert working_batteries == expected

            # Components from first request should be unblocked.
            time.shift(timedelta(seconds=0.5))
            expected = expected | {pairs[2].bat_id, pairs[4].bat_id}
            working_batteries = battery_pool.get_working_batteries(batteries)
            assert working_batteries == expected

            # Components from second request should be unblocked.
            time.shift(timedelta(seconds=0.5))
            expected = expected | {pairs[0].bat_id}
            working_batteries = battery_pool.get_working_batteries(batteries)
            assert working_batteries == expected

            success_resp: Success = self.fake_Success(
                requested_batteries=batteries, used_batteries=batteries
            )
            battery_pool.update_last_request_status(success_resp)
            assert battery_pool.get_working_batteries(batteries) == batteries

    def scenario_blocking_timeout_increases(
        self, battery_pool: BatteryPoolStatus, pairs: List[BatInvIdsPair]
    ) -> None:
        """Test if blocking components timeout increases.

        Args:
            battery_pool: Created battery pool
            pairs: All battery and inverter pairs in the pool
        """

        print(f"{__name__}::scenario_blocking_timeout_increases")
        # timeout should stop at 30, because in init_battery_pool
        # max_blocking_timeout_s is 30 seconds.
        expected_blocking_timeout = [1, 2, 4, 8, 16, 30, 30]
        with time_machine.travel("2022-01-01 00:00 UTC", tick=False) as time:
            self.scenario_all_msg_correct(battery_pool, pairs)
            batteries = {pair.bat_id for pair in pairs}
            expected = batteries

            for timeout in expected_blocking_timeout:
                # Notify that batteries failed in last request.
                failed_batteries = {pairs[idx].bat_id for idx in [2, 4, 0]}
                resp = self.fake_PartialFailure(
                    requested_batteries=batteries,
                    failed_batteries=failed_batteries,
                    succeed_batteries=expected - failed_batteries,
                )

                battery_pool.update_last_request_status(resp)
                expected = expected - failed_batteries
                assert battery_pool.get_working_batteries(batteries) == expected

                # After that timeout set of working batteries should not change
                time.shift(timedelta(seconds=timeout - 1))
                # Set all messages once again to update timeout for the messages
                self.set_battery_message(battery_pool, pairs, battery_data)
                self.set_inverter_message(battery_pool, pairs, inverter_data)
                assert battery_pool.get_working_batteries(batteries) == expected

                # Shift one more seconds so the timeout should expire
                time.shift(timedelta(seconds=1))
                working_batteries = battery_pool.get_working_batteries(batteries)
                assert working_batteries == batteries

            self.scenario_all_msg_correct(battery_pool, pairs)
            # Now we would like to check if timeout resets if the battery succeed.
            # Now the next timeout should be 30 (Tested in loop above).
            # Notify that battery all batteries, except 4 succeed.
            failed_batteries = {pairs[4].bat_id}
            resp = self.fake_PartialFailure(
                requested_batteries=batteries,
                failed_batteries=failed_batteries,
                succeed_batteries=batteries - failed_batteries,
            )
            battery_pool.update_last_request_status(resp)
            expected = batteries - failed_batteries
            assert battery_pool.get_working_batteries(batteries) == expected

            # Battery 4 should has timeout 30 sec.
            time.shift(timedelta(seconds=10))
            # Set all messages once again to update timeout for the messages
            self.set_battery_message(battery_pool, pairs, battery_data)
            self.set_inverter_message(battery_pool, pairs, inverter_data)
            assert battery_pool.get_working_batteries(batteries) == expected

            # Notify that batteries 4,6 failed.
            # Battery 1 should has timeout 2 sec (it reset)
            # Battery 4 should not reset its timeout (30 sec since last fail response.)
            failed_batteries = {pairs[4].bat_id, pairs[1].bat_id}
            resp = self.fake_PartialFailure(
                requested_batteries=batteries,
                failed_batteries=failed_batteries,
                succeed_batteries=batteries - failed_batteries,
            )
            battery_pool.update_last_request_status(resp)
            working_batteries = battery_pool.get_working_batteries(batteries)
            # Battery 4 should be blocked since last call
            expected = expected - {pairs[1].bat_id}
            assert working_batteries == expected

            # Check if battery 1 timeout expired
            time.shift(timedelta(seconds=2))
            self.set_battery_message(battery_pool, pairs, battery_data)
            self.set_inverter_message(battery_pool, pairs, inverter_data)
            expected = expected | {pairs[1].bat_id}
            working_batteries = battery_pool.get_working_batteries(batteries)
            assert working_batteries == expected

            # Check if battery 4 timeout expired
            time.shift(timedelta(seconds=18))
            self.set_battery_message(battery_pool, pairs, battery_data)
            self.set_inverter_message(battery_pool, pairs, inverter_data)
            expected = expected | {pairs[4].bat_id}
            assert battery_pool.get_working_batteries(batteries) == expected

            success_resp: Success = self.fake_Success(
                requested_batteries=batteries, used_batteries=batteries
            )
            battery_pool.update_last_request_status(success_resp)
            assert battery_pool.get_working_batteries(batteries) == batteries

    def scenario_state_changes_for_blocked_component(
        self, battery_pool: BatteryPoolStatus, pairs: List[BatInvIdsPair]
    ) -> None:
        """Test if component is unblocked if component state changes to Success.

        Args:
            battery_pool: Created battery pool
            pairs: All battery and inverter pairs in the pool
        """
        print(f"{__name__}::scenario_state_changes_for_blocked_component")
        with time_machine.travel("2022-01-01 00:00 UTC", tick=False):
            self.scenario_all_msg_correct(battery_pool, pairs)
            batteries = {pair.bat_id for pair in pairs}
            expected = batteries

            failed = {pairs[idx].bat_id for idx in range(3)}
            # Notify that batteries batteries: 0..2 failed in last request.
            resp = self.fake_PartialFailure(
                requested_batteries=batteries,
                failed_batteries=failed,
                succeed_batteries=batteries - failed,
            )
            battery_pool.update_last_request_status(resp)
            expected = batteries - failed
            assert battery_pool.get_working_batteries(batteries) == expected

            self.set_battery_relay_state(
                battery_pool, [pairs[3]], BatteryRelayState.RELAY_STATE_OPENED
            )
            expected = expected - {pairs[3].bat_id}
            assert battery_pool.get_working_batteries(batteries) == expected

            self.set_inverter_state(
                battery_pool, [pairs[2]], InverterState.COMPONENT_STATE_UNAVAILABLE
            )
            expected = expected - {pairs[2].bat_id}
            assert battery_pool.get_working_batteries(batteries) == expected

            # Inverter 2 is in correct state.
            # Because state changed from to correct, then adjacent battery should be
            # unblocked.
            self.set_inverter_message(battery_pool, [pairs[2]], inverter_data)
            expected = expected = expected | {pairs[2].bat_id}
            assert battery_pool.get_working_batteries(batteries) == expected

            # Battery 3 is in correct relay state
            # Because state changed from to correct, then battery should be
            # unblocked.
            self.set_battery_message(battery_pool, [pairs[3]], battery_data)
            expected = expected = expected | {pairs[3].bat_id}
            assert battery_pool.get_working_batteries(batteries) == expected

            success_resp: Success = self.fake_Success(
                requested_batteries=batteries, used_batteries=batteries
            )
            battery_pool.update_last_request_status(success_resp)
            assert battery_pool.get_working_batteries(batteries) == batteries

    def scenario_unknown_batteries_are_used(
        self, battery_pool: BatteryPoolStatus, pairs: List[BatInvIdsPair]
    ) -> None:
        """If there are no working batteries, then unknown batteries should be returned.

        Args:
            battery_pool: Created battery pool
            pairs: All battery and inverter pairs in the pool
        """
        print(f"{__name__}::scenario_unknown_batteries_are_used")
        with time_machine.travel("2022-01-01 00:00 UTC", tick=False):
            self.scenario_all_msg_correct(battery_pool, pairs)
            batteries = {pair.bat_id for pair in pairs}
            expected = batteries
            assert battery_pool.get_working_batteries(batteries) == expected

            # Notify that battery 0 failed in last request.
            failed = {pairs[0].bat_id}
            resp = self.fake_PartialFailure(
                requested_batteries=batteries,
                failed_batteries=failed,
                succeed_batteries=batteries - failed,
            )
            battery_pool.update_last_request_status(resp)
            expected = expected - failed
            assert battery_pool.get_working_batteries(batteries) == expected

            self.set_battery_outdated(battery_pool, [pairs[1]])
            expected = expected - {pairs[1].bat_id}
            assert battery_pool.get_working_batteries(batteries) == expected

            error = [ErrorLevel.ERROR_LEVEL_CRITICAL]
            self.set_battery_error(battery_pool, [pairs[2]], error)
            expected = expected - {pairs[2].bat_id}
            assert battery_pool.get_working_batteries(batteries) == expected

            self.set_battery_relay_state(
                battery_pool, [pairs[3]], BatteryRelayState.RELAY_STATE_OPENED
            )
            expected = expected - {pairs[3].bat_id}
            assert battery_pool.get_working_batteries(batteries) == expected

            self.set_inverter_message(battery_pool, [pairs[4]], lambda _: None)

            # Since no battery is working, Unknown batteries should be returned.
            assert battery_pool.get_working_batteries(batteries) == {pairs[0].bat_id}
