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

- **Simplified lifecycle management:** Actors are [async context managers] and also
  a [`run()`][frequenz.sdk.actor.run] function is provided.

## Lifecycle

Actors are not started when they are created. There are 3 main ways to start an actor
(from most to least recommended):

1. By using the [`run()`][frequenz.sdk.actor.run] function.
2. By using the actor as an [async context manager].
3. By using the [`start()`][frequenz.sdk.actor.Actor.start] method.

!!! warning

    1. If an actor raises an unhandled exception, it will be restarted automatically.

    2. Actors manage [`asyncio.Task`][] objects, so a reference to the actor object must
       be held for as long as the actor is expected to be running, otherwise its tasks
       will be cancelled and the actor will stop. For more information, please refer to
       the [Python `asyncio`
       documentation](https://docs.python.org/3/library/asyncio-task.html#asyncio.create_task).


### The `run()` Function

The [`run()`][frequenz.sdk.actor.run] function can start many actors at once and waits
for them to finish. If any of the actors are stopped with errors, the errors will be
logged.

???+ example

    ```python
    from frequenz.sdk.actor import Actor, run

    class MyActor(Actor):
        async def _run(self) -> None:
            print("Hello World!")

    await run(MyActor()) # (1)!
    ```

    1. Will block until the actor is stopped.

### Async Context Manager

When an actor is used as an [async context manager], it is started when the
`async with` block is entered and stopped automatically when the block is exited
(even if an exception is raised).

???+ example

    ```python
    from frequenz.sdk.actor import Actor

    class MyActor(Actor):
        async def _run(self) -> None:
            print("Hello World!")

    async with MyActor() as actor: # (1)!
        print("The actor is running")
    # (2)!
    ```

    1. [`start()`][frequenz.sdk.actor.Actor.start] is called automatically when entering
        the `async with` block.
    2. [`stop()`][frequenz.sdk.actor.Actor.stop] is called automatically when exiting
        the `async with` block.

### The `start()` Method

When using the [`start()`][frequenz.sdk.actor.Actor.start] method, the actor is
started in the background and the method returns immediately. The actor will
continue to run until it is **manually** stopped.

???+ example

    ```python
    from frequenz.sdk.actor import Actor

    class MyActor(Actor):
        async def _run(self) -> None:
            print("Hello World!")

    actor = MyActor() # (1)!
    actor.start() # (2)!
    print("The actor is running") # (3)!
    await actor.stop() # (4)!
    ```

    1. The actor is created but not started yet.
    2. The actor is started manually, it keeps running in the background.
    3. !!! danger

        If this function would raise an exception, the actor will never be stopped!

    4. Until the actor is stopped manually.

!!! warning

    This method is not recommended because it is easy to forget to stop the actor
    manually, specially in error conditions.

## Communication

The [`Actor`][frequenz.sdk.actor.Actor] class doesn't impose any specific way to
communicate between actors. However, [Frequenz
Channels](https://github.com/frequenz-floss/frequenz-channels-python/) are always used
as the communication mechanism between actors in the SDK.

## Implementing an Actor

To implement an actor, you must inherit from the [`Actor`][frequenz.sdk.actor.Actor]
class and implement the abstract `_run()` method.

### The `_run()` Method

The abstract `_run()` method is called automatically by the base class when the actor is
[started][frequenz.sdk.actor.Actor.start].

Normally an actor should run forever (or until it is
[stopped][frequenz.sdk.actor.Actor.stop]), so it is very common to have an infinite loop
in the `_run()` method, typically receiving messages from one or more channels
([receivers][frequenz.channels.Receiver]), processing them and sending the results to
other channels ([senders][frequenz.channels.Sender]).

### Stopping

By default, the [`stop()`][frequenz.sdk.actor.Actor.stop] method will call the
[`cancel()`][frequenz.sdk.actor.Actor.cancel] method (which will cancel all the tasks
created by the actor) and will wait for them to finish.

This means that when an actor is stopped, the `_run()` method will receive
a [`CancelledError`][asyncio.CancelledError] exception. You should have this in mind
when implementing your actor and make sure to handle this exception properly if you need
to do any cleanup.

The actor will handle the [`CancelledError`][asyncio.CancelledError] exception
automatically if it is not handled in the `_run()` method, so if there is no need for
extra cleanup, you don't need to worry about it.

If an unhandled exception is raised in the `_run()` method, the actor will re-run the
`_run()` method automatically. This ensures robustness in the face of errors, but you
should also have this in mind if you need to do any cleanup to make sure the re-run
doesn't cause any problems.

???+ tip

    You could implement your own [`stop()`][frequenz.sdk.actor.Actor.stop] method to,
    for example, send a message to a channel to notify that the actor should stop, or
    any other more graceful way to stop the actor if you need to make sure it can't be
    interrupted at any `await` point.

### Examples

Here are a few simple but complete examples to demonstrate how to create create actors
and connect them using [channels][frequenz.channels].

!!! tip

    The code examples are annotated with markers (like {{code_annotation_marker}}), they
    explain step-by-step what's going on in order of execution.

#### Composing actors

This example shows how to create two actors and connect them using
[broadcast][frequenz.channels.Broadcast] channels.

```python title="compose.py"
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

    !!! note

        We don't use the [`run()`][frequenz.sdk.actor.run] function here because we
        want to stop the actors when we are done with them, but the actors will run
        forever (as long as the channel is not closed). So the async context manager
        is a better fit for this example.

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

The expected output is:

```
Actor2 forwarding: "Actor1 forwarding: 'Hello'"
```

[async context manager]: https://docs.python.org/3/reference/datamodel.html#async-context-managers
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
