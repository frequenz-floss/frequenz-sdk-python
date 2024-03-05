# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the `EVChargerPool`."""


from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.timeseries._quantities import Power
from tests.timeseries.mock_microgrid import MockMicrogrid


class TestEVChargerPool:
    """Tests for the `EVChargerPool`."""

    async def test_ev_power(  # pylint: disable=too-many-locals
        self,
        mocker: MockerFixture,
    ) -> None:
        """Test the ev power formula."""
        mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
        mockgrid.add_ev_chargers(3)

        async with mockgrid:
            ev_pool = microgrid.ev_charger_pool()
            power_receiver = ev_pool.power.new_receiver()

            await mockgrid.mock_resampler.send_evc_power([2.0, 4.0, 10.0])
            assert (await power_receiver.receive()).value == Power.from_watts(16.0)

            await mockgrid.mock_resampler.send_evc_power([2.0, 4.0, -10.0])
            assert (await power_receiver.receive()).value == Power.from_watts(-4.0)
