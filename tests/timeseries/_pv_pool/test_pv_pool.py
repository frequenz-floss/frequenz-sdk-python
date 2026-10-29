# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Tests for the `PVPool`."""

import asyncio
from unittest.mock import MagicMock

import pytest
from frequenz.channels import Broadcast, ReceiverStoppedError
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.quantities import Power
from pytest_mock import MockerFixture

from frequenz.sdk import timeseries
from frequenz.sdk._internal._asyncio import cancel_and_await
from frequenz.sdk._internal._channels import ChannelRegistry
from frequenz.sdk.microgrid._old_component_data import InverterData
from frequenz.sdk.microgrid._power_distributing import ComponentPoolStatus
from frequenz.sdk.microgrid._power_managing import ReportRequest, _Report
from frequenz.sdk.timeseries.pv_pool import PVPool
from frequenz.sdk.timeseries.pv_pool._pv_pool_reference_store import (
    PVPoolReferenceStore,
)
from frequenz.sdk.timeseries.pv_pool._system_bounds_tracker import (
    PVSystemBoundsTracker,
)

from ...utils.unreachable_power import UnreachablePowerFeeder


def _new_power_status_report(target_power_watts: float) -> _Report:
    """Create a distinct report for power status assertions."""
    target_power = Power.from_watts(target_power_watts)
    return _Report(
        target_power=target_power,
        _inclusion_bounds=timeseries.Bounds(target_power, target_power),
        _exclusion_bounds=None,
    )


async def test_power_status_same_instance_subscriptions_work(
    mocker: MockerFixture,
) -> None:
    """Ensure same-instance power_status subscriptions share the same channel."""
    mock_cm = MagicMock()
    mock_graph = MagicMock()
    mock_graph.components.return_value = [
        MagicMock(id=ComponentId(28)),
        MagicMock(id=ComponentId(38)),
    ]
    mock_cm.component_graph = mock_graph
    mocker.patch(
        "frequenz.sdk.microgrid.connection_manager._CONNECTION_MANAGER",
        mock_cm,
    )
    mocker.patch("frequenz.sdk.microgrid.connection_manager.get", return_value=mock_cm)

    registry = ChannelRegistry(name="pv-pool-test")
    requests_channel = Broadcast[ReportRequest](name="pv-pool-requests")
    requests_rx = requests_channel.new_receiver()
    component_ids = frozenset({ComponentId(28), ComponentId(38)})
    pool = PVPool(
        pool_ref_store=PVPoolReferenceStore(
            channel_registry=registry,
            resampler_subscription_sender=MagicMock(),
            status_receiver=MagicMock(),
            power_manager_requests_sender=MagicMock(),
            power_manager_bounds_subs_sender=requests_channel.new_sender(),
            power_distribution_results_fetcher=MagicMock(),
            component_ids=component_ids,
        ),
        name="pv-pool",
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
    all_ids = [ComponentId(28), ComponentId(38)]
    mock_graph.components.return_value = [MagicMock(id=i) for i in all_ids]
    mock_cm.component_graph = mock_graph
    mocker.patch(
        "frequenz.sdk.microgrid.connection_manager._CONNECTION_MANAGER",
        mock_cm,
    )
    mocker.patch("frequenz.sdk.microgrid.connection_manager.get", return_value=mock_cm)

    registry = ChannelRegistry(name="pv-pool-empty-test")
    requests_channel = Broadcast[ReportRequest](name="pv-pool-empty-requests")

    def new_store(
        component_ids: frozenset[ComponentId] | None,
    ) -> PVPoolReferenceStore:
        return PVPoolReferenceStore(
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
    data_channel = Broadcast[InverterData](name="data")
    mocker.patch.object(
        InverterData, "subscribe", side_effect=lambda *_: data_channel.new_receiver()
    )
    status_channel = Broadcast[ComponentPoolStatus](name="status")
    tracker = PVSystemBoundsTracker(
        {ComponentId(8), ComponentId(18)}, status_channel.new_receiver(), MagicMock()
    )
    # pylint: disable=protected-access
    run = asyncio.create_task(tracker._run())
    try:
        await asyncio.sleep(0.01)
        await status_channel.new_sender().send(
            ComponentPoolStatus(working={ComponentId(8)}, uncertain=set())
        )
        await feeder.wait_for_formulas(1)

        # An error ends the run, the way `run_forever` sees it.
        await data_channel.close()
        with pytest.raises(ReceiverStoppedError):
            await run
        data_channel = Broadcast[InverterData](name="data")

        run = asyncio.create_task(tracker._run())
        # The kept status is tracked again, without a new status message.
        await feeder.wait_for_formulas(2)
    finally:
        await cancel_and_await(run)
