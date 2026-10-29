# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""A formula pool stub for feeding the power of unreachable components."""

import asyncio
from datetime import datetime, timezone

from frequenz.channels import Broadcast, Receiver
from frequenz.client.microgrid.metrics import Metric
from frequenz.quantities import Quantity
from pytest_mock import MockerFixture

from frequenz.sdk.timeseries import Sample


class _Formula:
    """A formula stub whose output is fed by the test."""

    def __init__(self) -> None:
        self.channel = Broadcast[Sample[Quantity]](name="unreachable-power-stub")
        self.sender = self.channel.new_sender()

    def new_receiver(self) -> Receiver[Sample[Quantity]]:
        """Return a receiver for the formula's output."""
        return self.channel.new_receiver()


class UnreachablePowerFeeder:
    """Replace the unreachable-power formulas with ones that output a fixed power.

    The test fixtures don't feed the formulas, so the power distributor and the
    pools would never get a value for the power of the components that aren't
    working.
    """

    def __init__(self, mocker: MockerFixture, power_watts: float) -> None:
        """Patch the formula pools of the unreachable-power trackers.

        Args:
            mocker: The mocker to patch with.
            power_watts: The power every unreachable-power formula outputs.
        """
        self._power = Quantity(power_watts)
        self._formulas: list[_Formula] = []
        self._task: asyncio.Task[None] | None = None
        mocker.patch(
            "frequenz.sdk.microgrid._data_pipeline._new_formula_pool",
            side_effect=lambda _name: self,
        )

    def from_string(self, formula_str: str, metric: Metric) -> _Formula:
        """Return a new formula stub."""
        # pylint: disable=unused-argument
        formula = _Formula()
        self._formulas.append(formula)
        return formula

    async def stop_string_formula(self, formula_str: str, metric: Metric) -> None:
        """Do nothing; the stubs don't run anything."""
        # pylint: disable=unused-argument

    async def stop(self) -> None:
        """Do nothing; the stubs don't run anything."""

    def start(self, interval: float = 0.1) -> None:
        """Send the power from every formula created so far, at the given interval.

        Args:
            interval: The time between values, in seconds.
        """

        async def feed() -> None:
            while True:
                sample = Sample(datetime.now(tz=timezone.utc), self._power)
                for formula in self._formulas:
                    await formula.sender.send(sample)
                await asyncio.sleep(interval)

        self._task = asyncio.create_task(feed())

    async def wait_for_formulas(self, count: int) -> None:
        """Wait until `count` formulas have been created, and check no more were.

        Args:
            count: The number of formulas to wait for.
        """
        async with asyncio.timeout(1.0):
            while len(self._formulas) < count:
                await asyncio.sleep(0)
        assert len(self._formulas) == count

    def cancel(self) -> None:
        """Stop sending values."""
        if self._task is not None:
            self._task.cancel()
