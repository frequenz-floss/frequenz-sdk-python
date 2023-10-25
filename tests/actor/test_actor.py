# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Simple test for the BaseActor."""

import asyncio
from datetime import timedelta

import pytest
from frequenz.channels import Broadcast, Receiver, Sender
from frequenz.channels.util import select, selected_from

from frequenz.sdk.actor import Actor, run

from ..conftest import actor_restart_limit


class MyBaseException(BaseException):
    """A base exception for testing purposes."""


class BaseTestActor(Actor):
    """A base actor for testing purposes."""

    restart_count: int = -1

    def inc_restart_count(self) -> None:
        """Increment the restart count."""
        BaseTestActor.restart_count += 1

    @classmethod
    def reset_restart_count(cls) -> None:
        """Reset the restart count."""
        cls.restart_count = -1


@pytest.fixture(autouse=True)
def reset_restart_count() -> None:
    """Reset the restart count before each test."""
    BaseTestActor.reset_restart_count()


class NopActor(BaseTestActor):
    """An actor that does nothing."""

    def __init__(self) -> None:
        """Create an instance."""
        super().__init__(name="test")

    async def _run(self) -> None:
        """Start the actor and crash upon receiving a message."""
        print(f"{self} started")
        self.inc_restart_count()
        print(f"{self} done")


class RaiseExceptionActor(BaseTestActor):
    """A faulty actor that raises an Exception as soon as it receives a message."""

    def __init__(
        self,
        recv: Receiver[int],
    ) -> None:
        """Create an instance.

        Args:
            recv: A channel receiver for int data.
        """
        super().__init__(name="test")
        self._recv = recv

    async def _run(self) -> None:
        """Start the actor and crash upon receiving a message."""
        print(f"{self} started")
        self.inc_restart_count()
        async for msg in self._recv:
            print(f"{self} is about to crash")
            _ = msg / 0
        print(f"{self} done (should not happen)")


class RaiseBaseExceptionActor(BaseTestActor):
    """A faulty actor that raises a BaseException as soon as it receives a message."""

    def __init__(
        self,
        recv: Receiver[int],
    ) -> None:
        """Create an instance.

        Args:
            recv: A channel receiver for int data.
        """
        super().__init__(name="test")
        self._recv = recv

    async def _run(self) -> None:
        """Start the actor and crash upon receiving a message."""
        print(f"{self} started")
        self.inc_restart_count()
        async for _ in self._recv:
            print(f"{self} is about to crash")
            raise MyBaseException("This is a test")
        print(f"{self} done (should not happen)")


ACTOR_INFO = ("frequenz.sdk.actor._actor", 20)
ACTOR_ERROR = ("frequenz.sdk.actor._actor", 40)
RUN_INFO = ("frequenz.sdk.actor._run_utils", 20)
RUN_ERROR = ("frequenz.sdk.actor._run_utils", 40)


class EchoActor(BaseTestActor):
    """An echo actor that whatever it receives into the output channel."""

    def __init__(
        self,
        name: str,
        recv1: Receiver[bool],
        recv2: Receiver[bool],
        output: Sender[bool],
    ) -> None:
        """Create an `EchoActor` instance.

        Args:
            name: Name of the actor.
            recv1: A channel receiver for test boolean data.
            recv2: A channel receiver for test boolean data.
            output: A channel sender for output test boolean data.
        """
        super().__init__(name=name)
        self._recv1 = recv1
        self._recv2 = recv2
        self._output = output

    async def _run(self) -> None:
        """Do computations depending on the selected input message."""
        print(f"{self} started")
        self.inc_restart_count()

        channel_1 = self._recv1
        channel_2 = self._recv2

        async for selected in select(channel_1, channel_2):
            print(f"{self} received message {selected.value!r}")
            if selected_from(selected, channel_1):
                print(f"{self} sending message received from channel_1")
                await self._output.send(selected.value)
            elif selected_from(selected, channel_2):
                print(f"{self} sending message received from channel_2")
                await self._output.send(selected.value)

        print(f"{self} done (should not happen)")


async def test_basic_actor(caplog: pytest.LogCaptureFixture) -> None:
    """Initialize the TestActor send a message and wait for the response."""
    caplog.set_level("DEBUG", logger="frequenz.sdk.actor._actor")

    input_chan_1: Broadcast[bool] = Broadcast("TestChannel1")
    input_chan_2: Broadcast[bool] = Broadcast("TestChannel2")

    echo_chan: Broadcast[bool] = Broadcast("echo output")
    echo_rx = echo_chan.new_receiver()

    async with EchoActor(
        "EchoActor",
        input_chan_1.new_receiver(),
        input_chan_2.new_receiver(),
        echo_chan.new_sender(),
    ) as actor:
        assert actor.is_running is True
        original_tasks = set(actor.tasks)

        # Start is a no-op if already started
        actor.start()
        assert actor.is_running is True
        assert original_tasks == set(actor.tasks)

        await input_chan_1.new_sender().send(True)
        msg = await echo_rx.receive()
        assert msg is True

        await input_chan_2.new_sender().send(False)
        msg = await echo_rx.receive()
        assert msg is False

        assert actor.is_running is True

    assert actor.is_running is False
    assert BaseTestActor.restart_count == 0
    assert caplog.record_tuples == [
        (*ACTOR_INFO, "Actor EchoActor[EchoActor]: Started."),
        (*ACTOR_INFO, "Actor EchoActor[EchoActor]: Cancelled."),
    ]


@pytest.mark.parametrize("restart_limit", [0, 1, 2, 10])
async def test_restart_on_unhandled_exception(
    restart_limit: int, caplog: pytest.LogCaptureFixture
) -> None:
    """Create a faulty actor and expect it to restart because it raises an exception.

    Also test this works with different restart limits.

    Args:
        restart_limit: The restart limit to use.
        caplog: The log capture fixture.
    """
    caplog.set_level("DEBUG", logger="frequenz.sdk.actor._actor")
    caplog.set_level("DEBUG", logger="frequenz.sdk.actor._run_utils")

    channel: Broadcast[int] = Broadcast("channel")

    # NB: We're adding 1.0s to the timeout to account for the time it takes to
    # run, crash, and restart the actor.
    expected_wait_time = timedelta(
        seconds=restart_limit * RaiseExceptionActor.RESTART_DELAY.total_seconds() + 1.0
    )
    async with asyncio.timeout(expected_wait_time.total_seconds()):
        with actor_restart_limit(restart_limit):
            actor = RaiseExceptionActor(
                channel.new_receiver(),
            )
            for i in range(restart_limit + 1):
                await channel.new_sender().send(i)

            await run(actor)
            await actor.wait()

    assert actor.is_running is False
    assert BaseTestActor.restart_count == restart_limit
    expected_log = [
        (*RUN_INFO, "Starting 1 actor(s)..."),
        (*RUN_INFO, "Actor RaiseExceptionActor[test]: Starting..."),
        (*ACTOR_INFO, "Actor RaiseExceptionActor[test]: Started."),
    ]
    restart_delay = Actor.RESTART_DELAY.total_seconds()
    for i in range(restart_limit):
        expected_log.extend(
            [
                (
                    *ACTOR_ERROR,
                    "Actor RaiseExceptionActor[test]: Raised an unhandled exception.",
                ),
                (
                    *ACTOR_INFO,
                    f"Actor test: Restarting ({i}/{restart_limit})...",
                ),
                (
                    *ACTOR_INFO,
                    f"Actor RaiseExceptionActor[test]: Waiting {restart_delay} seconds...",
                ),
            ]
        )
    expected_log.extend(
        [
            (
                *ACTOR_ERROR,
                "Actor RaiseExceptionActor[test]: Raised an unhandled exception.",
            ),
            (
                *ACTOR_INFO,
                "Actor RaiseExceptionActor[test]: Maximum restarts attempted "
                f"({restart_limit}/{restart_limit}), bailing out...",
            ),
            (
                *RUN_ERROR,
                "Actor RaiseExceptionActor[test]: Raised an exception while running.",
            ),
            (*RUN_INFO, "All 1 actor(s) finished."),
        ]
    )
    print("expected_log:", expected_log)
    print("caplog.record_tuples:", caplog.record_tuples)
    assert caplog.record_tuples == expected_log


async def test_does_not_restart_on_normal_exit(
    actor_auto_restart_once: None,  # pylint: disable=unused-argument
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Create an actor that exists normally and expect it to not be restarted."""
    caplog.set_level("DEBUG", logger="frequenz.sdk.actor._actor")
    caplog.set_level("DEBUG", logger="frequenz.sdk.actor._run_utils")

    channel: Broadcast[int] = Broadcast("channel")

    actor = NopActor()

    async with asyncio.timeout(1.0):
        await channel.new_sender().send(1)
        await run(actor)

    assert BaseTestActor.restart_count == 0
    assert caplog.record_tuples == [
        (*RUN_INFO, "Starting 1 actor(s)..."),
        (*RUN_INFO, "Actor NopActor[test]: Starting..."),
        (*ACTOR_INFO, "Actor NopActor[test]: Started."),
        (*ACTOR_INFO, "Actor NopActor[test]: _run() returned without error."),
        (*ACTOR_INFO, "Actor NopActor[test]: Stopped."),
        (*RUN_INFO, "Actor NopActor[test]: Finished normally."),
        (*RUN_INFO, "All 1 actor(s) finished."),
    ]


async def test_does_not_restart_on_base_exception(
    actor_auto_restart_once: None,  # pylint: disable=unused-argument
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Create a faulty actor and expect it not to restart because it raises a base exception."""
    caplog.set_level("DEBUG", logger="frequenz.sdk.actor._actor")
    caplog.set_level("DEBUG", logger="frequenz.sdk.actor._run_utils")

    channel: Broadcast[int] = Broadcast("channel")

    actor = RaiseBaseExceptionActor(channel.new_receiver())

    async with asyncio.timeout(1.0):
        await channel.new_sender().send(1)
        # We can't use pytest.raises() here because known BaseExceptions are handled
        # specially by pytest.
        try:
            await run(actor)
        except MyBaseException as error:
            assert str(error) == "This is a test"

    assert BaseTestActor.restart_count == 0
    assert caplog.record_tuples == [
        (*RUN_INFO, "Starting 1 actor(s)..."),
        (*RUN_INFO, "Actor RaiseBaseExceptionActor[test]: Starting..."),
        (*ACTOR_INFO, "Actor RaiseBaseExceptionActor[test]: Started."),
        (*ACTOR_ERROR, "Actor RaiseBaseExceptionActor[test]: Raised a BaseException."),
        (
            *RUN_ERROR,
            "Actor RaiseBaseExceptionActor[test]: Raised an exception while running.",
        ),
        (*RUN_INFO, "All 1 actor(s) finished."),
    ]


async def test_does_not_restart_if_cancelled(
    actor_auto_restart_once: None,  # pylint: disable=unused-argument
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Create a faulty actor and expect it not to restart when cancelled."""
    caplog.set_level("DEBUG", logger="frequenz.sdk.actor._actor")
    caplog.set_level("DEBUG", logger="frequenz.sdk.actor._run_utils")

    input_chan_1: Broadcast[bool] = Broadcast("TestChannel1")
    input_chan_2: Broadcast[bool] = Broadcast("TestChannel2")

    echo_chan: Broadcast[bool] = Broadcast("echo output")
    echo_rx = echo_chan.new_receiver()

    actor = EchoActor(
        "EchoActor",
        input_chan_1.new_receiver(),
        input_chan_2.new_receiver(),
        echo_chan.new_sender(),
    )

    async def cancel_actor() -> None:
        """Cancel the actor after a short delay."""
        await input_chan_1.new_sender().send(True)
        msg = await echo_rx.receive()
        assert msg is True
        assert actor.is_running is True

        await input_chan_2.new_sender().send(False)
        msg = await echo_rx.receive()
        assert msg is False

        actor.cancel()

    async with asyncio.timeout(1.0):
        async with asyncio.TaskGroup() as group:
            group.create_task(cancel_actor(), name="cancel")
            await run(actor)

    assert actor.is_running is False
    assert BaseTestActor.restart_count == 0
    assert caplog.record_tuples == [
        (*RUN_INFO, "Starting 1 actor(s)..."),
        (*RUN_INFO, "Actor EchoActor[EchoActor]: Starting..."),
        (*ACTOR_INFO, "Actor EchoActor[EchoActor]: Started."),
        (*ACTOR_INFO, "Actor EchoActor[EchoActor]: Cancelled."),
        (*RUN_ERROR, "Actor EchoActor[EchoActor]: Raised an exception while running."),
        (*RUN_INFO, "All 1 actor(s) finished."),
    ]
