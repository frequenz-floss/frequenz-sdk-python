# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Tests for the `SteamBoilerPool`."""

import asyncio
from unittest.mock import MagicMock

import pytest
from frequenz.channels import Broadcast, ReceiverStoppedError
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.quantities import Power
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid, timeseries
from frequenz.sdk._internal._asyncio import cancel_and_await
from frequenz.sdk._internal._channels import ChannelRegistry
from frequenz.sdk.microgrid._old_component_data import SteamBoilerData
from frequenz.sdk.microgrid._power_distributing import ComponentPoolStatus
from frequenz.sdk.microgrid._power_managing import ReportRequest, _Report
from frequenz.sdk.timeseries.steam_boiler_pool import (
    SteamBoilerPool,
    SteamBoilerPoolError,
)
from frequenz.sdk.timeseries.steam_boiler_pool._steam_boiler_pool_reference_store import (
    SteamBoilerPoolReferenceStore,
)
from frequenz.sdk.timeseries.steam_boiler_pool._system_bounds_tracker import (
    SteamBoilerSystemBoundsTracker,
)
from tests.timeseries.mock_microgrid import MockMicrogrid
from tests.utils.unreachable_power import UnreachablePowerFeeder


def _new_power_status_report(target_power_watts: float) -> _Report:
    """Create a distinct report for power status assertions."""
    target_power = Power.from_watts(target_power_watts)
    return _Report(
        target_power=target_power,
        _inclusion_bounds=timeseries.Bounds(target_power, target_power),
        _exclusion_bounds=None,
    )


class TestSteamBoilerPool:
    """Tests for the `SteamBoilerPool`."""

    async def test_power(  # pylint: disable=too-many-locals
        self,
        mocker: MockerFixture,
    ) -> None:
        """Test the power formula."""
        mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mockgrid.add_steam_boilers(3)

        async with mockgrid:
            steam_boiler_pool = microgrid.new_steam_boiler_pool(priority=5)
            power_receiver = steam_boiler_pool.power.new_receiver()

            # The boiler values sum to 15 W, not the 16 W on the meter, to
            # check that the boilers are the primary source.
            await mockgrid.mock_resampler.send_meter_power([16.0])
            await mockgrid.mock_resampler.send_steam_boiler_power([2.0, 6.0, 7.0])
            assert (await power_receiver.receive()).value == Power.from_watts(15.0)

            # When the boilers have no value, the formula falls back to the
            # meter.  The fallback subscription starts after the first `None`,
            # so send twice and skip one output.
            await mockgrid.mock_resampler.send_meter_power([16.0])
            await mockgrid.mock_resampler.send_steam_boiler_power([None, None, None])
            await mockgrid.mock_resampler.send_meter_power([16.0])
            await mockgrid.mock_resampler.send_steam_boiler_power([None, None, None])
            await power_receiver.receive()
            assert (await power_receiver.receive()).value == Power.from_watts(16.0)

    async def test_power_without_meters(
        self,
        mocker: MockerFixture,
    ) -> None:
        """Test the power formula when the boilers have no meter of their own.

        Without an upstream meter the formula reads each steam boiler component
        directly, which exercises the steam boiler path in the data sourcing actor.
        """
        mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
        mockgrid.add_steam_boilers(3)

        async with mockgrid:
            steam_boiler_pool = microgrid.new_steam_boiler_pool(priority=5)
            power_receiver = steam_boiler_pool.power.new_receiver()

            await mockgrid.mock_resampler.send_steam_boiler_power([2.0, 3.0, 4.0])
            assert (await power_receiver.receive()).value == Power.from_watts(9.0)

    async def test_propose_discharge_power_is_rejected(
        self,
        mocker: MockerFixture,
    ) -> None:
        """Proposing a discharge (negative) power for steam boilers is rejected."""
        mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mockgrid.add_steam_boilers(1)

        async with mockgrid:
            steam_boiler_pool = microgrid.new_steam_boiler_pool(priority=5)
            with pytest.raises(SteamBoilerPoolError):
                await steam_boiler_pool.propose_power(Power.from_watts(-5.0))


async def test_power_status_same_instance_subscriptions_work(
    mocker: MockerFixture,
) -> None:
    """Ensure same-instance power_status subscriptions share the same channel."""
    mock_cm = MagicMock()
    mock_graph = MagicMock()
    mock_graph.components.return_value = [
        MagicMock(id=ComponentId(12)),
        MagicMock(id=ComponentId(22)),
    ]
    mock_cm.component_graph = mock_graph
    mocker.patch(
        "frequenz.sdk.microgrid.connection_manager._CONNECTION_MANAGER",
        mock_cm,
    )
    mocker.patch("frequenz.sdk.microgrid.connection_manager.get", return_value=mock_cm)

    registry = ChannelRegistry(name="steam_boiler-pool-test")
    requests_channel = Broadcast[ReportRequest](name="steam_boiler-pool-requests")
    requests_rx = requests_channel.new_receiver()
    component_ids = frozenset({ComponentId(12), ComponentId(22)})
    pool = SteamBoilerPool(
        pool_ref_store=SteamBoilerPoolReferenceStore(
            channel_registry=registry,
            resampler_subscription_sender=MagicMock(),
            status_receiver=MagicMock(),
            power_manager_requests_sender=MagicMock(),
            power_manager_bounds_subs_sender=requests_channel.new_sender(),
            power_distribution_results_fetcher=MagicMock(),
            component_ids=component_ids,
        ),
        name="steam_boiler-pool",
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


async def test_empty_component_ids_means_all(mocker: MockerFixture) -> None:
    """An empty `component_ids` set must behave like `None`: use all components.

    Regression test for
    https://github.com/frequenz-floss/frequenz-sdk-python/issues/1474
    """
    mock_cm = MagicMock()
    mock_graph = MagicMock()
    all_ids = [ComponentId(12), ComponentId(22)]
    mock_graph.components.return_value = [MagicMock(id=i) for i in all_ids]
    mock_cm.component_graph = mock_graph
    mocker.patch(
        "frequenz.sdk.microgrid.connection_manager._CONNECTION_MANAGER",
        mock_cm,
    )
    mocker.patch("frequenz.sdk.microgrid.connection_manager.get", return_value=mock_cm)

    registry = ChannelRegistry(name="steam_boiler-pool-empty-test")
    requests_channel = Broadcast[ReportRequest](name="steam_boiler-pool-empty-requests")

    def new_store(
        component_ids: frozenset[ComponentId] | None,
    ) -> SteamBoilerPoolReferenceStore:
        return SteamBoilerPoolReferenceStore(
            channel_registry=registry,
            resampler_subscription_sender=MagicMock(),
            status_receiver=MagicMock(),
            power_manager_requests_sender=MagicMock(),
            power_manager_bounds_subs_sender=requests_channel.new_sender(),
            power_distribution_results_fetcher=MagicMock(),
            component_ids=component_ids,
        )

    none_store = new_store(None)
    empty_store = new_store(frozenset())

    assert none_store.component_ids == frozenset(all_ids)
    assert empty_store.component_ids == frozenset(all_ids)


async def test_bounds_tracker_tracks_unreachable_power_again_on_restart(
    mocker: MockerFixture,
) -> None:
    """Test that a restarted bounds tracker tracks the unreachable power again.

    A restart after an error stops tracking unreachable power, and the status isn't
    sent again until it changes, so the tracker must use the status it kept.
    """
    mocker.patch("frequenz.sdk.microgrid.connection_manager.get")
    feeder = UnreachablePowerFeeder(mocker, -500.0)
    data_channel = Broadcast[SteamBoilerData](name="data")
    mocker.patch.object(
        SteamBoilerData, "subscribe", side_effect=lambda *_: data_channel.new_receiver()
    )
    status_channel = Broadcast[ComponentPoolStatus](name="status")
    tracker = SteamBoilerSystemBoundsTracker(
        {ComponentId(12), ComponentId(22)}, status_channel.new_receiver(), MagicMock()
    )
    # pylint: disable=protected-access
    run = asyncio.create_task(tracker._run())
    try:
        await asyncio.sleep(0.01)
        await status_channel.new_sender().send(
            ComponentPoolStatus(working={ComponentId(12)}, uncertain=set())
        )
        await feeder.wait_for_formulas(1)

        # An error ends the run, the way `run_forever` sees it.
        await data_channel.close()
        with pytest.raises(ReceiverStoppedError):
            await run
        data_channel = Broadcast[SteamBoilerData](name="data")

        run = asyncio.create_task(tracker._run())
        # The kept status is tracked again, without a new status message.
        await feeder.wait_for_formulas(2)
    finally:
        await cancel_and_await(run)
