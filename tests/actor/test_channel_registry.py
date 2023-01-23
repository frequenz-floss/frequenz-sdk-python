# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the ChannelRegistry."""

from frequenz.sdk.actor import ChannelRegistry


async def test_channel_registry() -> None:
    """Tests for ChannelRegistry, with string as key type."""
    reg = ChannelRegistry(name="test-registry")

    sender20 = reg.new_sender("20-hello")
    receiver20 = reg.new_receiver("20-hello")

    sender21 = reg.new_sender("21-hello")
    receiver21 = reg.new_receiver("21-hello")

    await sender20.send(30)
    await sender21.send(31)

    rcvd = await receiver21.receive()
    assert rcvd == 31

    rcvd = await receiver20.receive()
    assert rcvd == 30
