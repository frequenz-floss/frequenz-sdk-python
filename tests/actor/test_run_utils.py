# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Simple tests for the actor runner."""

import asyncio
import time
from typing import Iterator

import async_solipsism
import pytest
import time_machine

from frequenz.sdk.actor import Actor, run


# Setting 'autouse' has no effect as this method replaces the event loop for all tests in the file.
@pytest.fixture()
def event_loop() -> Iterator[async_solipsism.EventLoop]:
    """Replace the loop with one that doesn't interact with the outside world."""
    loop = async_solipsism.EventLoop()
    yield loop
    loop.close()


@pytest.fixture
def fake_time() -> Iterator[time_machine.Coordinates]:
    """Replace real time with a time machine that doesn't automatically tick."""
    with time_machine.travel(0, tick=False) as traveller:
        yield traveller


class FaultyActor(Actor):
    """A test faulty actor."""

    def __init__(self, name: str) -> None:
        """Initialize the faulty actor.

        Args:
            name: the name of the faulty actor.
        """
        super().__init__(name=name)
        self.is_cancelled = False

    async def _run(self) -> None:
        """Run the faulty actor.

        Raises:
            CancelledError: the exception causes the actor to be cancelled
        """
        self.is_cancelled = True
        raise asyncio.CancelledError(f"Faulty Actor {self.name} failed")


class SleepyActor(Actor):
    """A test actor that sleeps a short time."""

    def __init__(self, name: str, sleep_duration: float) -> None:
        """Initialize the sleepy actor.

        Args:
            name: the name of the sleepy actor.
            sleep_duration: the virtual duration to sleep while running.
        """
        super().__init__(name=name)
        self.sleep_duration = sleep_duration
        self.is_joined = False

    async def _run(self) -> None:
        """Run the sleepy actor."""
        while time.time() < self.sleep_duration:
            await asyncio.sleep(0.1)

        self.is_joined = True


# pylint: disable=redefined-outer-name
async def test_all_actors_done(fake_time: time_machine.Coordinates) -> None:
    """Test the completion of all actors."""

    sleepy_actor_1 = SleepyActor("sleepy_actor_1", sleep_duration=1.0)
    sleepy_actor_2 = SleepyActor("sleepy_actor_2", sleep_duration=2.0)

    test_task = asyncio.create_task(run(sleepy_actor_1, sleepy_actor_2))

    sleep_duration = time.time()

    assert sleep_duration == 0
    assert sleepy_actor_1.is_joined is False
    assert sleepy_actor_2.is_joined is False

    while not test_task.done():
        if sleep_duration < 1:
            assert sleepy_actor_1.is_joined is False
            assert sleepy_actor_2.is_joined is False
        elif sleep_duration < 2:
            assert sleepy_actor_1.is_joined is True
            assert sleepy_actor_2.is_joined is False
        elif sleep_duration == 2:
            assert sleepy_actor_1.is_joined is True
            assert sleepy_actor_2.is_joined is True

        fake_time.shift(0.5)
        sleep_duration = time.time()
        await asyncio.sleep(1)

    assert sleepy_actor_1.is_joined
    assert sleepy_actor_2.is_joined


async def test_actors_cancelled() -> None:
    """Test the completion of actors being cancelled."""

    faulty_actors = [FaultyActor(f"faulty_actor_{idx}") for idx in range(5)]

    await asyncio.wait_for(run(*faulty_actors), timeout=1.0)

    for faulty_actor in faulty_actors:
        assert faulty_actor.is_cancelled
