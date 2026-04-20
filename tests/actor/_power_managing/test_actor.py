# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the power managing actor."""

import asyncio
from unittest.mock import Mock, call

import pytest
from frequenz.channels import BroadcastChannel, OneshotChannel
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.component import Battery

from frequenz.sdk.microgrid import _power_distributing
from frequenz.sdk.microgrid._power_managing import PowerManagingActor, ReportRequest
from frequenz.sdk.microgrid._power_managing._base_classes import (
    DefaultPower,
    PowerManagerAlgorithm,
    Proposal,
)


# pylint: disable=too-many-locals,protected-access
async def test_bounds_subscription_handles_closed_oneshot_sender(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure closed oneshot sender is ignored and processing continues."""
    _, proposals_receiver = BroadcastChannel[Proposal](name="proposals")
    bounds_subscription_sender, bounds_subscription_receiver = BroadcastChannel[
        ReportRequest
    ](name="bounds-subscriptions")
    distributing_requests_sender, _ = BroadcastChannel[_power_distributing.Request](
        name="power-distributing"
    )
    _, distributing_results_receiver = BroadcastChannel[_power_distributing.Result](
        name="power-distributing-results"
    )

    actor = PowerManagingActor(
        proposals_receiver=proposals_receiver,
        bounds_subscription_receiver=bounds_subscription_receiver,
        power_distributing_requests_sender=distributing_requests_sender,
        power_distributing_results_receiver=distributing_results_receiver,
        algorithm=PowerManagerAlgorithm.MATRYOSHKA,
        default_power=DefaultPower.ZERO,
        component_class=Battery,
    )

    def _add_tracker_task(component_ids: frozenset[ComponentId]) -> None:
        actor._bound_tracker_tasks[component_ids] = object()  # type: ignore[assignment]

    mock_add_system_bounds = Mock(side_effect=_add_tracker_task)
    monkeypatch.setattr(actor, "_add_system_bounds_tracker", mock_add_system_bounds)

    component_ids = frozenset({ComponentId(11), ComponentId(13)})

    async with actor:
        sender, receiver = OneshotChannel()

        first_request = ReportRequest(
            source_id="source-1",
            component_ids=component_ids,
            priority=1,
            report_stream_sender=sender,
        )
        await bounds_subscription_sender.send(first_request)
        async with asyncio.timeout(1.0):
            _ = await receiver.receive()

        second_request = ReportRequest(
            source_id="source-2",
            component_ids=component_ids,
            priority=2,
            report_stream_sender=sender,
        )
        await bounds_subscription_sender.send(second_request)

        sender3, receiver3 = OneshotChannel()
        third_request = ReportRequest(
            source_id="source-3",
            component_ids=component_ids,
            priority=3,
            report_stream_sender=sender3,
        )
        await bounds_subscription_sender.send(third_request)
        async with asyncio.timeout(1.0):
            _ = await receiver3.receive()
        await asyncio.sleep(0)

    assert set(actor._subscriptions[component_ids]) == {1, 2, 3}
    assert mock_add_system_bounds.call_args_list == [call(component_ids)]


# pylint: enable=too-many-locals,protected-access
