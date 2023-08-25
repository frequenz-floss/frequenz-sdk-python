# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Actor model implementation."""

import abc
import asyncio
import logging

from ._background_service import BackgroundService

_logger = logging.getLogger(__name__)


class Actor(BackgroundService, abc.ABC):
    """A primitive unit of computation that runs autonomously.

    From [Wikipedia](https://en.wikipedia.org/wiki/Actor_model), an actor is:

    > [...] the basic building block of concurrent computation. In response to
    > a message it receives, an actor can: make local decisions, create more actors,
    > send more messages, and determine how to respond to the next message received.
    > Actors may modify their own private state, but can only affect each other
    > indirectly through messaging (removing the need for lock-based synchronization).

    [Channels](https://github.com/frequenz-floss/frequenz-channels-python/) can be used
    to implement communication between actors, as shown in the examples below.

    To implement an actor, subclasses must implement the `_run()` method, which should
    run the actor's logic. The `_run()` method is called by the base class when the
    actor is started, and is expected to run until the actor is stopped.

    If an unhandled exception is raised in the `_run()` method, the actor will be
    restarted automatically. Unhandled [`BaseException`][]s will cause the actor to stop
    immediately and will be re-raised.

    !!! warning

        As actors manage [`asyncio.Task`][] objects, a reference to them must be held
        for as long as the actor is expected to be running, otherwise its tasks will be
        cancelled and the actor will stop. For more information, please refer to the
        [Python `asyncio`
        documentation](https://docs.python.org/3/library/asyncio-task.html#asyncio.create_task).

    Example: Example of an actor receiving from two receivers

        ```python
        from frequenz.channels import Broadcast, Receiver, Sender
        from frequenz.channels.util import select, selected_from

        class EchoActor(Actor):
            def __init__(
                self,
                recv1: Receiver[bool],
                recv2: Receiver[bool],
                output: Sender[bool],
            ) -> None:
                super().__init__()
                self._recv1 = recv1
                self._recv2 = recv2
                self._output = output

            async def _run(self) -> None:
                async for selected in select(self._recv1, self._recv2):
                    if selected_from(selected, self._recv1):
                        await self._output.send(selected.value)
                    elif selected_from(selected, self._recv1):
                        await self._output.send(selected.value)
                    else:
                        assert False, "Unknown selected channel"


        input_channel_1 = Broadcast[bool]("input_channel_1")
        input_channel_2 = Broadcast[bool]("input_channel_2")
        input_channel_2_sender = input_channel_2.new_sender()

        echo_channel = Broadcast[bool]("EchoChannel")
        echo_receiver = echo_channel.new_receiver()

        async with EchoActor(
            input_channel_1.new_receiver(),
            input_channel_2.new_receiver(),
            echo_channel.new_sender(),
        ):
            await input_channel_2_sender.send(True)
            print(await echo_receiver.receive())
        ```

    Example: Example of composing two actors

        ```python
        from frequenz.channels import Broadcast, Receiver, Sender

        class Actor1(Actor):
            def __init__(
                self,
                recv: Receiver[bool],
                output: Sender[bool],
            ) -> None:
                super().__init__()
                self._recv = recv
                self._output = output

            async def _run(self) -> None:
                async for msg in self._recv:
                    await self._output.send(msg)


        class Actor2(Actor):
            def __init__(
                self,
                recv: Receiver[bool],
                output: Sender[bool],
            ) -> None:
                super().__init__()
                self._recv = recv
                self._output = output

            async def _run(self) -> None:
                async for msg in self._recv:
                    await self._output.send(msg)

        input_channel: Broadcast[bool] = Broadcast("Input to Actor1")
        middle_channel: Broadcast[bool] = Broadcast("Actor1 -> Actor2 stream")
        output_channel: Broadcast[bool] = Broadcast("Actor2 output")

        input_sender = input_channel.new_sender()
        output_receiver = output_channel.new_receiver()

        async with (
            Actor1(input_channel.new_receiver(), middle_channel.new_sender()),
            Actor2(middle_channel.new_receiver(), output_channel.new_sender()),
        ):
            await input_sender.send(True)
            print(await output_receiver.receive())
        ```
    """

    _restart_limit: int | None = None
    """The number of times actors can be restarted when they are stopped by unhandled exceptions.

    If this is bigger than 0 or `None`, the actor will be restarted when there is an
    unhanded exception in the `_run()` method.

    If `None`, the actor will be restarted an unlimited number of times.

    !!! note

        This is mostly used for testing purposes and shouldn't be set in production.
    """

    def start(self) -> None:
        """Start this actor.

        If this actor is already running, this method does nothing.
        """
        if self.is_running:
            return
        self._tasks.clear()
        self._tasks.add(asyncio.create_task(self._run_loop()))

    @abc.abstractmethod
    async def _run(self) -> None:
        """Run this actor's logic."""

    async def _run_loop(self) -> None:
        """Run this actor's task in a loop until `_restart_limit` is reached.

        Raises:
            asyncio.CancelledError: If this actor's `_run()` gets cancelled.
            Exception: If this actor's `_run()` raises any other `Exception` and reached
                the maximum number of restarts.
            BaseException: If this actor's `_run()` raises any other `BaseException`.
        """
        _logger.info("Actor %s: Started.", self)
        n_restarts = 0
        while True:
            try:
                await self._run()
                _logger.info("Actor %s: _run() returned without error.", self)
            except asyncio.CancelledError:
                _logger.info("Actor %s: Cancelled.", self)
                raise
            except Exception:  # pylint: disable=broad-except
                _logger.exception("Actor %s: Raised an unhandled exception.", self)
                limit_str = "∞" if self._restart_limit is None else self._restart_limit
                limit_str = f"({n_restarts}/{limit_str})"
                if self._restart_limit is None or n_restarts < self._restart_limit:
                    n_restarts += 1
                    _logger.info("Actor %s: Restarting %s...", self._name, limit_str)
                    continue
                _logger.info(
                    "Actor %s: Maximum restarts attempted %s, bailing out...",
                    self,
                    limit_str,
                )
                raise
            except BaseException:  # pylint: disable=broad-except
                _logger.exception("Actor %s: Raised a BaseException.", self)
                raise
            break

        _logger.info("Actor %s: Stopped.", self)
