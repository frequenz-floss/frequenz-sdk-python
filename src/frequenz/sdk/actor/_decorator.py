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
from typing import Any, Callable, Optional, Type, TypeVar

from typing_extensions import ParamSpec

from frequenz.sdk._internal._asyncio import cancel_and_await

_logger = logging.getLogger(__name__)


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


_P = ParamSpec("_P")
_R = TypeVar("_R")


def actor(cls: Callable[_P, _R]) -> Type[_R]:
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

    Example (one actor receiving from two receivers): !this example is not updated to channels 0.16!
    ```python
    from frequenz.channels import Broadcast, Receiver, Sender
    from frequenz.channels.util import Select
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
    received_msg = await echo_rx.receive()
    ```

    Example (two Actors composed):
    ```python
    from frequenz.channels import Broadcast, Receiver, Sender
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
    a1_chan: Broadcast[bool] = Broadcast("A1 stream")
    a2_chan: Broadcast[bool] = Broadcast("A2 stream")
    a_1 = Actor1(
        name="ActorOne",
        recv=input_chan.new_receiver(),
        output=a1_chan.new_sender(),
    )
    a_2 = Actor2(
        name="ActorTwo",
        recv=a1_chan.new_receiver(),
        output=a2_chan.new_sender(),
    )

    a2_rx = a2_chan.new_receiver()

    await input_chan.new_sender().send(True)
    received_msg = await a2_rx.receive()
    ```

    """
    if not inspect.isclass(cls):
        raise TypeError("The `@actor` decorator can only be applied for classes.")

    _check_run_method_exists(cls)

    class ActorClass(cls, BaseActor):  # type: ignore
        """A wrapper class to make an actor."""

        def __init__(self, *args: _P.args, **kwargs: _P.kwargs) -> None:
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
            _logger.debug("Starting actor: %s", cls.__name__)
            number_of_restarts = 0
            while (
                self.restart_limit is None or number_of_restarts <= self.restart_limit
            ):
                if number_of_restarts > 0:
                    _logger.info("Restarting actor: %s", cls.__name__)

                try:
                    await super().run()
                except asyncio.CancelledError:
                    _logger.debug("Cancelling actor: %s", cls.__name__)
                    raise
                except Exception:  # pylint: disable=broad-except
                    _logger.exception("Actor (%s) crashed", cls.__name__)
                finally:
                    number_of_restarts += 1

            _logger.info("Shutting down actor: %s", cls.__name__)

        async def _stop(self) -> None:
            """Stop an running actor."""
            await cancel_and_await(self._actor_task)

        async def join(self) -> None:
            """Await the actor's task, and return when the task completes."""
            await self._actor_task

    return ActorClass
