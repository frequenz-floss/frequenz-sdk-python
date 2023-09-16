# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the power managing actor."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock

from frequenz.channels import Broadcast
from pytest_mock import MockerFixture

from frequenz.sdk.actor._power_managing._base_classes import Proposal
from frequenz.sdk.actor._power_managing._power_managing_actor import PowerManagingActor
from frequenz.sdk.actor.power_distributing.request import Request
from frequenz.sdk.timeseries import Power, battery_pool


async def test_power_managing_actor_matryoshka(mocker: MockerFixture) -> None:
    """Tests for the power managing actor."""
    input_channel = Broadcast[Proposal]("power managing proposals")
    output_channel = Broadcast[Request]("Power managing outputs")
    input_tx = input_channel.new_sender()
    output_rx = output_channel.new_receiver()

    batteries = frozenset({2, 5})

    mocker.patch(
        "frequenz.sdk.actor._power_managing._power_managing_actor"
        ".PowerManagingActor._add_bounds_tracker",
        new_callable=AsyncMock,
    )

    async def case(
        *,
        priority: int,
        power: float,
        bounds: tuple[float, float],
        expected: float,
    ) -> None:
        await input_tx.send(
            Proposal(
                battery_ids=batteries,
                source_id=f"actor-{priority}",
                preferred_power=Power.from_watts(power),
                bounds=(Power.from_watts(bounds[0]), Power.from_watts(bounds[1])),
                priority=priority,
            )
        )
        assert (await output_rx.receive()).power.as_watts() == expected

    async with PowerManagingActor(
        input_channel.new_receiver(), output_channel.new_sender()
    ) as powmgract:
        powmgract._system_bounds[  # pylint: disable=protected-access
            batteries
        ] = battery_pool.PowerMetrics(
            timestamp=datetime.now(tz=timezone.utc),
            inclusion_bounds=battery_pool.Bounds(
                lower=Power.from_watts(-200.0), upper=Power.from_watts(200.0)
            ),
            exclusion_bounds=battery_pool.Bounds(
                lower=Power.zero(), upper=Power.zero()
            ),
        )

        await case(priority=2, power=25.0, bounds=(25.0, 50.0), expected=25.0)
        await case(priority=1, power=20.0, bounds=(20.0, 50.0), expected=25.0)
        await case(priority=3, power=10.0, bounds=(10.0, 15.0), expected=10.0)
        await case(priority=3, power=10.0, bounds=(10.0, 22.0), expected=20.0)
        await case(priority=1, power=30.0, bounds=(20.0, 50.0), expected=10.0)
        await case(priority=3, power=10.0, bounds=(10.0, 50.0), expected=30.0)
        await case(priority=2, power=40.0, bounds=(40.0, 50.0), expected=40.0)
        await case(priority=2, power=0.0, bounds=(-200.0, 200.0), expected=30.0)
        await case(priority=4, power=-50.0, bounds=(-200.0, -50.0), expected=-50.0)
        await case(priority=3, power=-0.0, bounds=(-200.0, 200.0), expected=-50.0)
        await case(priority=1, power=-150.0, bounds=(-200.0, -150.0), expected=-150.0)
        await case(priority=4, power=-180.0, bounds=(-200.0, -50.0), expected=-150.0)
        await case(priority=4, power=50.0, bounds=(50.0, 200.0), expected=50.0)
