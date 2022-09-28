"""Simple test for the BaseActor.

Copyright
Copyright © 2021 Frequenz Energy-as-a-Service GmbH

License
MIT
"""
from frequenz.channels import Broadcast, Receiver, Select, Sender

from frequenz.sdk.actor import actor


@actor
class FaultyActor:
    """A faulty actor that crashes as soon as it receives a message."""

    def __init__(
        self,
        name: str,
        recv: Receiver[int],
    ) -> None:
        """Create an instance of `FaultyActor`.

        Args:
            name: Name of the actor.
            recv: A channel receiver for int data.
        """
        self.name = name
        self._recv = recv

    async def run(self) -> None:
        """Start the actor and crash upon receiving a message"""
        async for msg in self._recv:
            _ = msg / 0


@actor
class EchoActor:
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
            name (str): Name of the actor.
            recv1 (Receiver[bool]): A channel receiver for test boolean data.
            recv2 (Receiver[bool]): A channel receiver for test boolean data.
        """
        self.name = name

        self._recv1 = recv1
        self._recv2 = recv2
        self._output = output

    async def run(self) -> None:
        """Do computations depending on the selected input message.

        Args:
            output (Sender[OT]): A channel sender, to send actor's results to.
        """
        select = Select(channel_1=self._recv1, channel_2=self._recv2)
        while await select.ready():
            if msg := select.channel_1:
                await self._output.send(msg.inner)
            elif msg := select.channel_2:
                await self._output.send(msg.inner)


async def test_basic_actor() -> None:
    """Initialize the TestActor send a message and wait for the response."""

    input_chan_1: Broadcast[bool] = Broadcast("TestChannel1")
    input_chan_2: Broadcast[bool] = Broadcast("TestChannel2")

    echo_chan: Broadcast[bool] = Broadcast("echo output")

    _echo_actor = EchoActor(
        "EchoActor",
        input_chan_1.get_receiver(),
        input_chan_2.get_receiver(),
        echo_chan.get_sender(),
    )

    echo_rx = echo_chan.get_receiver()

    await input_chan_1.get_sender().send(True)

    msg = await echo_rx.receive()
    assert msg is True

    await input_chan_2.get_sender().send(False)

    msg = await echo_rx.receive()
    assert msg is False


async def test_actor_does_not_restart() -> None:
    """Create a faulty actor and expect it to crash and stop running"""

    channel: Broadcast[int] = Broadcast("channel")

    _faulty_actor = FaultyActor(
        "FaultyActor",
        channel.get_receiver(),
    )

    await channel.get_sender().send(1)
    # pylint: disable=no-member
    await _faulty_actor.join()  # type: ignore
