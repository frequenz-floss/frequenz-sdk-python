# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""A decorator for creating simple composable actors.

Supports multiple input channels and a single output channel.

Note that if your use-case needs multiple output channels, you may instead
consider using several actors.
"""

import asyncio
import inspect
import logging
from typing import Any, Generic, Optional, Type, TypeVar

logger = logging.Logger(__name__)

OT = TypeVar("OT")


def _check_run_method_exists(cls: Type[Any]) -> None:
    """Check if a run method exists in the given class.

    Args:
        cls: The actor class.

    Raises:
        TypeError: when the class doesn't have a `run` method as per spec.
    """
    run_method = None
    for name, val in inspect.getmembers(cls):
        if name == "run" and inspect.iscoroutinefunction(val):
            run_method = val
            break
    if run_method is None:
        raise TypeError(
            "The `@actor` decorator can only be applied to a class "
            "that has an async `run` method."
        )
    run_params = inspect.signature(run_method).parameters
    num_params = len(run_params)
    if num_params != 1:
        raise TypeError(
            f"The `run` method in {cls.__name__} must have this signature: "
            "`async def run(self)`"
        )


class BaseActor:
    """Base class to provide common attributes for all actors."""

    # None is unlimited, 0 is no restarts. After restarts are exhausted the
    # exception will be re-raised.
    restart_limit: Optional[int] = None


def actor(cls: Type[Any]) -> Type[Any]:
    """Decorate a class into a simple composable actor.

    A actor using the `actor` decorator should define an `async def run(self)`
    method, that loops over incoming data, and sends results out.

    Channels can be used to implement communication between actors, as shown in
    the examples below.

    Args:
        cls: the class to decorate.

    Returns:
        The decorated class.

    Raises:
        TypeError: when the class doesn't have a `run` method as per spec.

    Example (one actor receiving from two receivers):
    ``` python
    @actor
    class EchoActor:
        def __init__(
            self,
            name: str,
            recv1: Receiver[bool],
            recv2: Receiver[bool],
            output: Sender[bool],
        ) -> None:
            self.name = name

            self._recv1 = recv1
            self._recv2 = recv2
            self._output = output

        async def run(self) -> None:
            select = Select(channel_1=self._recv1, channel_2=self._recv2)
            while await select.ready():
                if msg := select.channel_1:
                    await self._output.send(msg.inner)
                elif msg := select.channel_2:
                    await self._output.send(msg.inner)


    input_chan_1: Broadcast[bool] = Broadcast("input_chan_1")
    input_chan_2: Broadcast[bool] = Broadcast("input_chan_2")

    echo_chan: Broadcast[bool] = Broadcast("EchoChannel")

    echo_actor = EchoActor(
        "EchoActor",
        recv1=input_chan_1.new_receiver(),
        recv2=input_chan_2.new_receiver(),
        output=echo_chan.new_sender(),
    )
    echo_rx = echo_chan.new_receiver()

    await input_chan_2.new_sender().send(True)
    msg = await echo_rx.receive()
    ```

    Example (two Actors composed):
    ``` python
    @actor
    class Actor1:
        def __init__(
            self,
            name: str,
            recv: Receiver[bool],
            output: Sender[bool],
        ) -> None:
            self.name = name
            self._recv = recv
            self._output = output

        async def run(self) -> None:
            async for msg in self._recv:
                await self._output.send(msg)


    @actor
    class Actor2:
        def __init__(
            self,
            name: str,
            recv: Receiver[bool],
            output: Sender[bool],
        ) -> None:
            self.name = name
            self._recv = recv
            self._output = output

        async def run(self) -> None:
            async for msg in self._recv:
                await self._output.send(msg)


    input_chan: Broadcast[bool] = Broadcast("Input to A1")
    a1_chan: Broadcast[bool] = Broadcast["A1 stream"]
    a2_chan: Broadcast[bool] = Broadcast["A2 stream"]
    a1 = Actor1(
        name="ActorOne",
        recv=input_chan.new_receiver(),
        output=a1_chan.new_sender(),
    )
    a2 = Actor2(
        name="ActorTwo",
        recv=a1_chan.new_receiver(),
        output=a2_chan.new_sender(),
    )

    a2_rx = a2_chan.new_receiver()

    await input_chan.new_sender().send(True)
    msg = await a2_rx.receive()
    ```

    """
    if not inspect.isclass(cls):
        raise TypeError("The `@actor` decorator can only be applied for classes.")

    _check_run_method_exists(cls)

    class ActorClass(cls, BaseActor, Generic[OT]):  # type: ignore
        """A wrapper class to make an actor."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            """Create an `ActorClass` instance.

            Also call __init__ on `cls`.

            Args:
                *args: Any positional arguments to `cls.__init__`.
                **kwargs: Any keyword arguments to `cls.__init__`.
            """
            super().__init__(*args, **kwargs)
            self._actor_task = asyncio.create_task(self._start_actor())

        async def _start_actor(self) -> None:
            """Run the main logic of the actor as a coroutine.

            Raises:
                asyncio.CancelledError: when the actor's task gets cancelled.
            """
            logger.debug("Starting actor: %s", cls.__name__)
            number_of_restarts = 0
            while True:
                try:
                    await super().run()
                except asyncio.CancelledError:
                    logger.debug("Cancelling actor: %s", cls.__name__)
                    raise
                except Exception as err:  # pylint: disable=broad-except
                    logger.exception(
                        "Actor (%s) crashed with error: %s", cls.__name__, err
                    )
                    if (
                        self.restart_limit is None
                        or number_of_restarts < self.restart_limit
                    ):
                        number_of_restarts += 1
                        logger.info("Restarting actor: %s", cls.__name__)
                    else:
                        logger.info("Shutting down actor: %s", cls.__name__)
                        break

        async def _stop(self) -> None:
            """Stop an running actor."""
            self._actor_task.cancel()
            try:
                await self._actor_task
            except asyncio.CancelledError:
                pass

        async def join(self) -> None:
            """Await the actor's task, and return when the task completes."""
            await self._actor_task

    return ActorClass
