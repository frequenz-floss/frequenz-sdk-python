# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

# Please note this docstring is included in the getting started documentation in docs/.
"""Actors are a primitive unit of computation that runs autonomously.

## Actor Programming Model

!!! quote "From [Wikipedia](https://en.wikipedia.org/wiki/Actor_model)"

    The actor model in computer science is a mathematical model of concurrent
    computation that treats an actor as the basic building block of concurrent
    computation. In response to a message it receives, an actor can: make local
    decisions, create more actors, send more messages, and determine how to
    respond to the next message received. Actors may modify their own private
    state, but can only affect each other indirectly through messaging (removing
    the need for lock-based synchronization).

We won't get into much more detail here because it is outside the scope of this
documentation. However, if you are interested in learning more about the actor
programming model, here are some useful resources:

- [Actor Model (Wikipedia)](https://en.wikipedia.org/wiki/Actor_model)
- [How the Actor Model Meets the Needs of Modern, Distributed Systems
  (Akka)](https://doc.akka.io/docs/akka/current/typed/guide/actors-intro.html)

## Frequenz SDK Actors

The [`Actor`][frequenz.sdk.actor.Actor] class serves as the foundation for
creating concurrent tasks and all actors in the SDK inherit from it. This class
provides a straightforward way to implement actors. It shares similarities with
the traditional actor programming model but also has some unique features:

- **Message Passing:** Like traditional actors, our Actor class communicates
  through message passing. To do that they use [channels][frequenz.channels]
  for communication.

- **Automatic Restart:** If an unhandled exception occurs in an actor's logic
  (`_run` method), the actor will be automatically restarted. This ensures
  robustness in the face of errors.

- **Simplified Task Management:** Actors manage asynchronous tasks using
  [`asyncio`][]. You can create and manage tasks within the actor, and the `Actor`
  class handles task cancellation and cleanup.

- **Simplified lifecycle management:** Actors are [async context
  managers](https://docs.python.org/3/reference/datamodel.html#async-context-managers)
  and also a [`run()`][frequenz.sdk.actor.run] function is provided.

## Example

Here's a simple example to demonstrate how to create two actors and connect
them.

Please note the annotations in the code (like {{code_annotation_marker}}), they
explain step-by-step what's going on in order of execution.

```python title="actors.py"
import asyncio

from frequenz.channels import Broadcast, Receiver, Sender
from frequenz.sdk.actor import Actor

class Actor1(Actor):  # (1)!
    def __init__(
        self,
        receiver: Receiver[str],
        output: Sender[str],
    ) -> None:
        super().__init__()
        self._receiver = receiver
        self._output = output

    async def _run(self) -> None:
        async for msg in self._receiver:
            await self._output.send(f"Actor1 forwarding: {msg!r}")  # (8)!


class Actor2(Actor):
    def __init__(
        self,
        receiver: Receiver[str],
        output: Sender[str],
    ) -> None:
        super().__init__()
        self._receiver = receiver
        self._output = output

    async def _run(self) -> None:
        async for msg in self._receiver:
            await self._output.send(f"Actor2 forwarding: {msg!r}")  # (9)!


async def main() -> None:  # (2)!
    # (4)!
    input_channel: Broadcast[str] = Broadcast("Input to Actor1")
    middle_channel: Broadcast[str] = Broadcast("Actor1 -> Actor2 stream")
    output_channel: Broadcast[str] = Broadcast("Actor2 output")

    input_sender = input_channel.new_sender()
    output_receiver = output_channel.new_receiver()

    async with (  # (5)!
        Actor1(input_channel.new_receiver(), middle_channel.new_sender()),
        Actor2(middle_channel.new_receiver(), output_channel.new_sender()),
    ):
        await input_sender.send("Hello")  # (6)!
        msg = await output_receiver.receive()  # (7)!
        print(msg)  # (10)!
    # (11)!

if __name__ == "__main__":  # (3)!
    asyncio.run(main())
```

1. We define 2 actors: `Actor1` and `Actor2` that will just forward a message
   from an input channel to an output channel, adding some text.

2. We define an async `main()` function with the main logic of our [asyncio][] program.

3. We start the `main()` function in the async loop using [`asyncio.run()`][asyncio.run].

4. We create a bunch of [broadcast][frequenz.channels.Broadcast]
   [channels][frequenz.channels] to connect our actors.

    * `input_channel` is the input channel for `Actor1`.
    * `middle_channel` is the channel that connects `Actor1` and `Actor2`.
    * `output_channel` is the output channel for `Actor2`.

5. We create two actors and use them as async context managers, `Actor1` and
   `Actor2`, and connect them by creating new
   [senders][frequenz.channels.Sender] and
   [receivers][frequenz.channels.Receiver] from the channels.

6. We schedule the [sending][frequenz.channels.Sender.send] of the message
   `Hello` to `Actor1` via `input_channel`.

7. We [receive][frequenz.channels.Receiver.receive] (await) the response from
   `Actor2` via `output_channel`. Between this and the previous steps the
   `async` calls in the actors will be executed.

8. `Actor1` sends the re-formatted message (`Actor1 forwarding: Hello`) to
   `Actor2` via the `middle_channel`.

9. `Actor2` sends the re-formatted message (`Actor2 forwarding: "Actor1
   forwarding: 'Hello'"`) to the `output_channel`.

10. Finally, we print the received message, which will still be `Actor2
    forwarding: "Actor1 forwarding: 'Hello'"`.

11. The actors are stopped and cleaned up automatically when the `async with`
    block ends.
"""

from ..timeseries._resampling import ResamplerConfig
from ._actor import Actor
from ._background_service import BackgroundService
from ._channel_registry import ChannelRegistry
from ._config_managing import ConfigManagingActor
from ._data_sourcing import ComponentMetricRequest, DataSourcingActor
from ._resampling import ComponentMetricsResamplingActor
from ._run_utils import run

__all__ = [
    "Actor",
    "BackgroundService",
    "ChannelRegistry",
    "ComponentMetricRequest",
    "ComponentMetricsResamplingActor",
    "ConfigManagingActor",
    "DataSourcingActor",
    "ResamplerConfig",
    "run",
]
