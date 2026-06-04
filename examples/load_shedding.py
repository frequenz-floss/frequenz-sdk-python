# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Load shedding actor."""

import asyncio
import random
import sys
import termios
import tty
from dataclasses import dataclass
from datetime import datetime, timezone
from heapq import heappop, heappush
from typing import AsyncGenerator
from unittest.mock import patch

from frequenz.channels import Broadcast, Receiver, Sender
from frequenz.quantities import Percentage, Power

from frequenz.sdk import microgrid
from frequenz.sdk.actor import Actor, run
from frequenz.sdk.timeseries import Sample

# Mock configuration
CONF_STATE: dict[str, float] = {}


def mock_set_consumer(name: str, power: float) -> None:
    """Mock setting consumer power by storing the state in a dictionary.

    Args:
        name: Consumer name.
        power: Power value to set.
    """
    CONF_STATE[name] = power


def _log(msg: str) -> None:
    print(msg, end="\n\r")


class PowerMockActor(Actor):
    """Power Mock Actor.

    Asynchronously listens to user key presses 'm' and 'n' to increase and decrease power of a
    static consumer.
    """

    def __init__(self, consumer_name: str) -> None:
        """Initialize the actor."""
        super().__init__()
        self.consumer_name = consumer_name
        self.power_step = Power.from_kilowatts(1.0)

    async def _run(self) -> None:
        _log("Press 'm' to increase power or 'n' to decrease power for the consumer.")

        while True:
            # Call _read_key in a thread to avoid blocking the event loop
            key = await asyncio.to_thread(self._read_key)
            if key == "m":
                CONF_STATE[self.consumer_name] = (
                    CONF_STATE.get(self.consumer_name, 0) + self.power_step.as_watts()
                )
                _log(
                    f"Increased {self.consumer_name} power to "
                    f"{CONF_STATE[self.consumer_name]/1000.0} kW"
                )
            elif key == "n":
                CONF_STATE[self.consumer_name] = max(
                    0,
                    CONF_STATE.get(self.consumer_name, 0) - self.power_step.as_watts(),
                )
                _log(
                    f"Decreased {self.consumer_name} power to "
                    f"{CONF_STATE[self.consumer_name]/1000.0} kW"
                )
            elif key == "q":
                sys.exit()
            else:
                _log("Invalid key. Use 'm' or 'n'.")

    def _read_key(self) -> str:
        """Read a single key press without waiting for Enter."""
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setraw(fd)
            key = sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        return key


@dataclass(order=True)
class Consumer:
    """Consumer dataclass."""

    priority: int
    name: str
    power: Power
    enabled: bool = False


class LoadSheddingActor(Actor):
    """Simple load shedding actor."""

    def __init__(
        self,
        max_peak: Power,
        consumers: list[Consumer],
        grid_meter_receiver: Receiver[Sample[Power]],
    ):
        """Initialize the actor."""
        super().__init__()
        self.max_peak = max_peak
        self.disable_tolerance = self.max_peak * 0.9
        self.enable_tolerance = self.max_peak * 0.8
        self.grid_meter_receiver = grid_meter_receiver

        self.enabled_consumers: list[Consumer] = []
        self.disabled_consumers: list[Consumer] = []

        for c in consumers:
            heappush(self.disabled_consumers, c)

    async def _enable_consumer(self, consumer: Consumer) -> None:
        if not consumer.enabled:
            consumer.enabled = True
            heappush(self.enabled_consumers, consumer)
            _log(f"+++{consumer.name}, +{consumer.power}")
            # This is a mock function to set the consumer power,
            # in a real system this would be replaced with the actual implementation
            mock_set_consumer(consumer.name, consumer.power.as_watts())

    async def _disable_consumer(self, consumer: Consumer) -> None:
        if consumer.enabled:
            consumer.enabled = False
            heappush(self.disabled_consumers, consumer)
            _log(f"---{consumer.name}, -{consumer.power}")
            # This is a mock function to set the consumer power,
            # in a real system this would be replaced with the actual implementation
            mock_set_consumer(consumer.name, 0)

    async def _adjust_loads(self, current_load: Power) -> None:
        while current_load > self.disable_tolerance and self.enabled_consumers:
            enabled_consumer: Consumer = heappop(self.enabled_consumers)
            await self._disable_consumer(enabled_consumer)
            current_load -= enabled_consumer.power

        temp_disabled: list[Consumer] = []
        while self.disabled_consumers:
            disabled_consumer: Consumer = heappop(self.disabled_consumers)
            if current_load + disabled_consumer.power <= self.enable_tolerance:
                await self._enable_consumer(disabled_consumer)
                current_load += disabled_consumer.power
            else:
                heappush(temp_disabled, disabled_consumer)
                break

        while temp_disabled:
            heappush(self.disabled_consumers, heappop(temp_disabled))

    async def _run(self) -> None:
        async for power_sample in self.grid_meter_receiver:
            if power_sample.value:
                _log(
                    f"Power: {power_sample.value}, "
                    f"Peak: {self.max_peak} ({self.disable_tolerance} / {self.enable_tolerance})"
                    f", Enabled: {', '.join(c.name for c in self.enabled_consumers)}\r"
                )
                await self._adjust_loads(power_sample.value)


async def mock_sender(
    sender: Sender[Sample[Power]],
) -> AsyncGenerator[Sample[Power], None]:
    """Mock implementation of a grid meter receiver.

    It sends power values every second.
    """
    current_load = Power.from_kilowatts(0.0)

    def compute_power() -> Power:
        """Compute current grid power based on mock state."""
        return Power.from_watts(sum(CONF_STATE.values()))

    while True:
        current_load = compute_power()
        # Add +- 8% noise to the current load
        current_load += current_load * Percentage.from_fraction(
            random.uniform(-0.08, 0.08)
        )
        await sender.send(
            Sample(timestamp=datetime.now(tz=timezone.utc), value=current_load)
        )
        await asyncio.sleep(1)


async def main() -> None:
    """Program entry point."""
    consumers = [
        Consumer(priority=1, name="Fan2", power=Power.from_kilowatts(2.5)),
        Consumer(priority=2, name="Drier1", power=Power.from_kilowatts(3.0)),
        Consumer(priority=2, name="Drier2", power=Power.from_kilowatts(2.0)),
        Consumer(priority=3, name="Conveyor1", power=Power.from_kilowatts(1.5)),
        Consumer(priority=3, name="Conveyor2", power=Power.from_kilowatts(1.0)),
        Consumer(priority=4, name="Auger", power=Power.from_kilowatts(2.0)),
        Consumer(priority=4, name="HopperMixer", power=Power.from_kilowatts(2.5)),
        Consumer(priority=5, name="SiloVentilation", power=Power.from_kilowatts(1.0)),
        Consumer(priority=5, name="LoaderArm", power=Power.from_kilowatts(3.0)),
        Consumer(priority=6, name="SeedCleaner", power=Power.from_kilowatts(2.5)),
        Consumer(priority=6, name="Sprayer", power=Power.from_kilowatts(2.0)),
        Consumer(priority=7, name="Grinder", power=Power.from_kilowatts(3.0)),
        Consumer(priority=7, name="Shaker", power=Power.from_kilowatts(1.5)),
        Consumer(priority=8, name="Sorter", power=Power.from_kilowatts(2.0)),
    ]

    for consumer in consumers:
        mock_set_consumer(consumer.name, 0)

    grid_meter_receiver = microgrid.grid().power.new_receiver()

    actor_instance = LoadSheddingActor(
        max_peak=Power.from_kilowatts(30),
        consumers=consumers,
        grid_meter_receiver=grid_meter_receiver,
    )

    user_input_actor = PowerMockActor(consumer_name="static_consumer")

    await run(actor_instance, user_input_actor)


if __name__ == "__main__":
    with patch("frequenz.sdk.microgrid.grid") as mock_grid:
        chan = Broadcast[Sample[Power]](name="grid_power")
        mock_grid.return_value.power.new_receiver = chan.new_receiver

        async def begin() -> None:
            """Start main & mock sender."""
            await asyncio.gather(
                main(),
                mock_sender(chan.new_sender()),
            )

        asyncio.run(begin())
