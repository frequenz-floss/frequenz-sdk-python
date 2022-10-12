"""Tests for the ChannelRegistry.

Copyright
Copyright © 2021 Frequenz Energy-as-a-Service GmbH

License
MIT
"""

from frequenz.sdk.actor import ChannelRegistry


async def test_channel_registry_with_string() -> None:
    """Tests for ChannelRegistry, with string as key type."""
    reg = ChannelRegistry[str]()

    sender20 = reg.get_sender("20-hello")
    receiver20 = reg.get_receiver("20-hello")

    sender21 = reg.get_sender("21-hello")
    receiver21 = reg.get_receiver("21-hello")

    await sender20.send(30)
    await sender21.send(31)

    rcvd = await receiver21.receive()
    assert rcvd == 31

    rcvd = await receiver20.receive()
    assert rcvd == 30
