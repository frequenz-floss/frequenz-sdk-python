# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the `EVChargerPool`."""

import asyncio
from unittest.mock import MagicMock

from frequenz.channels import Broadcast
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.quantities import Power
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid, timeseries
from frequenz.sdk._internal._channels import ChannelRegistry
from frequenz.sdk.microgrid._power_managing import ReportRequest, _Report
from frequenz.sdk.timeseries.ev_charger_pool import EVChargerPool
from frequenz.sdk.timeseries.ev_charger_pool._ev_charger_pool_reference_store import (
    EVChargerPoolReferenceStore,
)
from tests.timeseries.mock_microgrid import MockMicrogrid


def _new_power_status_report(target_power_watts: float) -> _Report:
    """Create a distinct report for power status assertions."""
    target_power = Power.from_watts(target_power_watts)
    return _Report(
        target_power=target_power,
        _inclusion_bounds=timeseries.Bounds(target_power, target_power),
        _exclusion_bounds=None,
    )


class TestEVChargerPool:
    """Tests for the `EVChargerPool`."""

    async def test_ev_power(  # pylint: disable=too-many-locals
        self,
        mocker: MockerFixture,
    ) -> None:
        """Test the ev power formula."""
        mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mockgrid.add_ev_chargers(3)

        async with mockgrid:
            ev_pool = microgrid.new_ev_charger_pool(priority=5)
            power_receiver = ev_pool.power.new_receiver()

            # The charger values sum to 15 W, not the 16 W on the meter, to
            # check that the chargers are the primary source.
            await mockgrid.mock_resampler.send_meter_power([16.0])
            await mockgrid.mock_resampler.send_evc_power([2.0, 6.0, 7.0])
            assert (await power_receiver.receive()).value == Power.from_watts(15.0)


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

    registry = ChannelRegistry(name="ev-pool-test")
    requests_channel = Broadcast[ReportRequest](name="ev-pool-requests")
    requests_rx = requests_channel.new_receiver()
    component_ids = frozenset({ComponentId(12), ComponentId(22)})
    pool = EVChargerPool(
        pool_ref_store=EVChargerPoolReferenceStore(
            channel_registry=registry,
            resampler_subscription_sender=MagicMock(),
            status_receiver=MagicMock(),
            power_manager_requests_sender=MagicMock(),
            power_manager_bounds_subs_sender=requests_channel.new_sender(),
            power_distribution_results_fetcher=MagicMock(),
            component_ids=component_ids,
        ),
        name="ev-pool",
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
