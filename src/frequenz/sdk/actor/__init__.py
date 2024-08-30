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

- **Message Passing:** Like traditional actors, our [`Actor`][frequenz.sdk.actor.Actor]
  class communicates through message passing. Even when no particular message passing
  mechanism is enforced, the SDK actors use [`frequenz.channels`][] for communication.

- **Automatic Restart:** If an unhandled exception occurs in an actor's logic
  ([`_run()`][_run] method), the actor will be automatically restarted. This ensures
  robustness in the face of errors.

- **Simplified Task Management:** Actors manage asynchronous tasks using
  [`asyncio`][]. You can create and manage tasks within the actor, and the
  [`Actor`][frequenz.sdk.actor.Actor] class handles task cancellation and cleanup.

- **Simplified lifecycle management:** Actors are [async context managers] and also
  a [`run()`][frequenz.sdk.actor.run] function is provided to easily run a group of
  actors and wait for them to finish.

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
for them to finish. If any of the actors is stopped with errors, the errors will be
logged.

???+ example

    ```python
    from frequenz.sdk.actor import Actor, run

    class MyActor(Actor):
        async def _run(self) -> None:
            while True:
                print("Hello World!")
                await asyncio.sleep(1)

    await run(MyActor()) # (1)!
    ```

    1. This line will block until the actor completes its execution or is manually stopped.

### Async Context Manager

When an actor is used as an [async context manager], it is started when the
`async with` block is entered and stopped automatically when the block is exited
(even if an unhandled exception is raised).

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
continue to run until it is **manually** stopped or until it completes its execution.

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
    manually, especially in error conditions.

## Communication

The [`Actor`][frequenz.sdk.actor.Actor] class doesn't impose any specific way to
communicate between actors. However, [`frequenz.channels`][] are always used as the
communication mechanism between actors in the SDK.

## Implementing an Actor

To implement an actor, you must inherit from the [`Actor`][frequenz.sdk.actor.Actor]
class and implement an *initializer* and the abstract [`_run()`][_run] method.

### Initialization

The [initializer][object.__init__] is called when the actor is created. The
[`Actor`][frequenz.sdk.actor.Actor] class initializer
([`__init__`][frequenz.sdk.actor.Actor.__init__]) should be always called first in the
class we are implementing to make sure actors are properly initialized.

The [`Actor.__init__()`][frequenz.sdk.actor.Actor.__init__] takes one optional argument,
a [`name`][frequenz.sdk.actor.Actor.name] that will be used to identify the actor in
logs. If no name is provided, a default name will be generated, but it is recommended
that [`Actor`][frequenz.sdk.actor.Actor] subclasses can also receive a name as an
argument to make it easier to identify individual instances in logs more easily.

The actor initializer normally also accepts as arguments the input channels receivers
and output channels senders that will be used for communication. These channels should
be created outside the actor and passed to it as arguments to ensure actors can be
composed easily.

???+ example "Example echo actor"

    ```python
    from frequenz.channels import Receiver, Sender
    from frequenz.sdk.actor import Actor

    class EchoActor(Actor):  # (1)!
        def __init__(
                self,
                input: Receiver[int],  # (2)!
                output: Sender[int],  # (3)!
                name: str | None = None,  # (4)!
        ) -> None:
            super().__init__(name=name) # (5)!
            self._input: Receiver[int] = input  # (6)!
            self._output: Sender[int] = output  # (7)!
    ```

    1. We define a new actor class called `EchoActor` that inherits from
        [`Actor`][frequenz.sdk.actor.Actor].

    2. We accept an `input` argument that will be used to receive messages from
        a channel.
    3. We accept an `output` argument that will be used to send messages to a channel.
    4. We accept an optional `name` argument that will be used to identify the actor in
        logs.
    5. We call [`Actor.__init__()`][frequenz.sdk.actor.Actor.__init__] to make sure the
        actor is properly initialized.
    6. We store the `input` argument in a *private* attribute to use it later.
    7. We store the `output` argument in a *private* attribute to use it later.

### The `_run()` Method

The abstract `_run()` method is called automatically by the base class when the actor is
[started][frequenz.sdk.actor.Actor.start].

Normally an actor should run forever (or until it is
[stopped][frequenz.sdk.actor.Actor.stop]), so it is very common to have an infinite loop
in the `_run()` method, typically receiving messages from one or more channels
([receivers][frequenz.channels.Receiver]), processing them and sending the results to
other channels ([senders][frequenz.channels.Sender]).
However, it is worth noting that an actor can also be designed for a one-time execution
or a limited number of runs, terminating upon completion.

???+ example "Example echo actor"

    ```python
    from frequenz.channels import Receiver, Sender
    from frequenz.sdk.actor import Actor

    class EchoActor(Actor):
        def __init__(
                self,
                input: Receiver[int],
                output: Sender[int],
                name: str | None = None,
        ) -> None:
            super().__init__(name=name)
            self._input: Receiver[int] = input
            self._output: Sender[int] = output

        async def _run(self) -> None:  # (1)!
            async for msg in self._input:  # (2)!
                await self._output.send(msg)  # (3)!
    ```

    1. We implement the abstract [`_run()`][_run] method.
    2. We receive messages from the `input` channel one by one.
    3. We send the received message to the `output` channel.

### Stopping

By default, the [`stop()`][frequenz.sdk.actor.Actor.stop] method will call the
[`cancel()`][frequenz.sdk.actor.Actor.cancel] method (which will cancel all the tasks
created by the actor) and will wait for them to finish.

This means that when an actor is stopped, the [`_run()`][_run] method will receive
a [`CancelledError`][asyncio.CancelledError] exception. You should have this in mind
when implementing your actor and make sure to handle this exception properly if you need
to do any cleanup.

The actor will handle the [`CancelledError`][asyncio.CancelledError] exception
automatically if it is not handled in the [`_run()`][_run] method, so if there is no
need for extra cleanup, you don't need to worry about it.

If an unhandled exception is raised in the [`_run()`][_run] method, the actor will
re-run the [`_run()`][_run] method automatically. This ensures robustness in the face of
errors, but you should also have this in mind if you need to do any cleanup to make sure
the re-run doesn't cause any problems.

???+ tip

    You could implement your own [`stop()`][frequenz.sdk.actor.Actor.stop] method to,
    for example, send a message to a channel to notify that the actor should stop, or
    any other more graceful way to stop the actor if you need to make sure it can't be
    interrupted at any `await` point.

### Spawning Extra Tasks

Actors run at least one background task, created automatically by the
[`Actor`][frequenz.sdk.actor.Actor] class. But [`Actor`][frequenz.sdk.actor.Actor]
inherits from [`BackgroundService`][frequenz.sdk.actor.BackgroundService], which
provides a few methods to create and manage extra tasks.

If your actor needs to spawn extra tasks, you can use
[`BackgroundService`][frequenz.sdk.actor.BackgroundService] facilities to manage the
tasks, so they are also automatically stopped when the actor is stopped.

All you need to do is add the newly spawned tasks to the actor's
[`tasks`][frequenz.sdk.actor.Actor.tasks] set.

???+ example

    ```python
    import asyncio
    from frequenz.sdk.actor import Actor

    class MyActor(Actor):
        async def _run(self) -> None:
            extra_task = asyncio.create_task(self._extra_task())  # (1)!
            self.tasks.add(extra_task)  # (2)!
            while True:  # (3)!
                print("_run() running")
                await asyncio.sleep(1)

        async def _extra_task(self) -> None:
            while True:  # (4)!
                print("_extra_task() running")
                await asyncio.sleep(1.1)

    async with MyActor() as actor:  # (5)!
        await asyncio.sleep(3)  # (6)!
    # (7)!
    ```

    1. We create a new task using [`asyncio.create_task()`][asyncio.create_task].
    2. We add the task to the actor's [`tasks`][frequenz.sdk.actor.Actor.tasks] set.
        This ensures the task will be cancelled and cleaned up when the actor is stopped.
    3. We leave the actor running forever.
    4. The extra task will also run forever.
    5. The actor is started.
    6. We wait for 3 seconds, the actor should print a bunch of "_run() running" and
        "_extra_task() running" messages while it's running.
    7. The actor is stopped and the extra task is cancelled automatically.

### Examples

Here are a few simple but complete examples to demonstrate how to create actors and
connect them using [channels][frequenz.channels].

!!! tip

    The code examples are annotated with markers (like {{code_annotation_marker}}), you
    can click on them to see the step-by-step explanation of what's going on. The
    annotations are numbered according to the order of execution.

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
        name: str | None = None,
    ) -> None:
        super().__init__(name=name)
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
        name: str | None = None,
    ) -> None:
        super().__init__(name=name)
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
        Actor1(input_channel.new_receiver(), middle_channel.new_sender(), "actor1"),
        Actor2(middle_channel.new_receiver(), output_channel.new_sender(), "actor2"),
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

2. We define an async `main()` function within the main logic of our [asyncio][] program.

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

#### Receiving from multiple channels

This example shows how to create an actor that receives messages from multiple
[broadcast][frequenz.channels.Broadcast] channels using
[`select()`][frequenz.channels.select].

```python title="select.py"
import asyncio

from frequenz.channels import Broadcast, Receiver, Sender
from frequenz.channels.util import select, selected_from
from frequenz.sdk.actor import Actor, run


class EchoActor(Actor):  # (1)!
    def __init__(
        self,
        receiver_1: Receiver[bool],
        receiver_2: Receiver[bool],
        output: Sender[bool],
        name: str | None = None,
    ) -> None:
        super().__init__(name=name)
        self._receiver_1 = receiver_1
        self._receiver_2 = receiver_2
        self._output = output

    async def _run(self) -> None:  # (2)!
        async for selected in select(self._receiver_1, self._receiver_2):  # (10)!
            if selected_from(selected, self._receiver_1):  # (11)!
                print(f"Received from receiver_1: {selected.value}")
                await self._output.send(selected.value)
                if not selected.value:  # (12)!
                    break
            elif selected_from(selected, self._receiver_2):  # (13)!
                print(f"Received from receiver_2: {selected.value}")
                await self._output.send(selected.value)
                if not selected.value:  # (14)!
                    break
            else:
                assert False, "Unknown selected channel"
        print("EchoActor finished")
    # (15)!


# (3)!
input_channel_1 = Broadcast[bool]("input_channel_1")
input_channel_2 = Broadcast[bool]("input_channel_2")
echo_channel = Broadcast[bool]("echo_channel")

echo_actor = EchoActor(  # (4)!
    input_channel_1.new_receiver(),
    input_channel_2.new_receiver(),
    echo_channel.new_sender(),
    "echo-actor",
)

echo_receiver = echo_channel.new_receiver()  # (5)!

async def main() -> None:  # (6)!
    # (8)!
    await input_channel_1.new_sender().send(True)
    await input_channel_2.new_sender().send(False)

    await run(echo_actor)  # (9)!

    await echo_channel.close()  # (16)!

    async for message in echo_receiver:  # (17)!
        print(f"Received {message=}")


if __name__ == "__main__":  # (7)!
    asyncio.run(main())
```

1. We define an `EchoActor` that receives messages from two channels and sends
    them to another channel.

2. We implement the [`_run()`][_run] method that will receive messages from the two
    channels using [`select()`][frequenz.channels.select] and send them to the
    output channel. The `run()` method will stop if a `False` message is received.

3. We create the channels that will be used with the actor.

4. We create the actor and connect it to the channels by creating new receivers and
    senders from the channels.

5. We create a receiver for the `echo_channel` to eventually receive the messages sent
    by the actor.

6. We define the `main()` function that will run the actor.

7. We start the `main()` function in the async loop using [`asyncio.run()`][asyncio.run].

8. We send a message to each of the input channels. These messages will be queued in
    the channels until they are consumed by the actor.

9. We start the actor and wait for it to finish using the
    [`run()`][frequenz.sdk.actor.run] function.

10. The [`select()`][frequenz.channels.select] function will get the first message
    available from the two channels. The order in which they will be handled is
    unknown, but in this example we assume that the first message will be from
    `input_channel_1` (`True`) and the second from `input_channel_1` (`False`).

11. The [`selected_from()`][frequenz.channels.selected_from] function will return
    `True` for the `input_channel_1` receiver. `selected.value` holds the received
    message, so `"Received from receiver_1: True"` will be printed and `True` will be
    sent to the `output` channel.

12. Since `selected.value` is `True`, the loop will continue, going back to the
    [`select()`][frequenz.channels.select] function.

13. The [`selected_from()`][frequenz.channels.selected_from] function will return
    `False` for the `input_channel_1` receiver and `True` for the `input_channel_2`
    receiver. The message stored in `selected.value` will now be `False`, so
    `"Received from receiver_2: False"` will be printed and `False` will be sent to the
    `output` channel.

14. Since `selected.value` is `False`, the loop will break.

15. The [`_run()`][_run] method will finish normally and the actor will be stopped, so
    the [`run()`][frequenz.sdk.actor.run] function will return.

16. We close the `echo_channel` to make sure the `echo_receiver` will stop receiving
    messages after all the queued messages are consumed (otherwise the step 17 will
    never end!).

17. We receive the messages sent by the actor to the `echo_channel` one by one and print
    them, it should print first `Received message=True` and then `Received
    message=False`.

The expected output is:

```
Received from receiver_1: True
Received from receiver_2: False
Received message=True
Received message=False
```

[async context manager]: https://docs.python.org/3/reference/datamodel.html#async-context-managers
[_run]: #the-_run-method
"""

from ..timeseries._resampling import ResamplerConfig
from ._actor import Actor
from ._background_service import BackgroundService
from ._run_utils import run

__all__ = [
    "Actor",
    "BackgroundService",
    "ResamplerConfig",
    "run",
]
