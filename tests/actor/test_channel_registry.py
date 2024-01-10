# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for the ChannelRegistry."""

import pytest
from frequenz.channels import ReceiverError, SenderError

from frequenz.sdk.actor import ChannelRegistry


async def test_channel_registry() -> None:
    """Tests for ChannelRegistry, with string as key type."""
    reg = ChannelRegistry(name="test-registry")

    assert "20-hello" not in reg
    assert "21-hello" not in reg

    chan20 = reg.get_or_create(int, "20-hello")
    assert "20-hello" in reg
    assert reg.message_type("20-hello") == int

    with pytest.raises(ValueError):
        reg.get_or_create(str, "20-hello")

    sender20 = chan20.new_sender()
    receiver20 = chan20.new_receiver()

    assert "21-hello" not in reg

    chan21 = reg.get_or_create(int, "21-hello")
    assert "21-hello" in reg
    assert reg.message_type("21-hello") == int

    sender21 = chan21.new_sender()
    receiver21 = chan21.new_receiver()

    await sender20.send(30)
    await sender21.send(31)

    rcvd = await receiver21.receive()
    assert rcvd == 31

    rcvd = await receiver20.receive()
    assert rcvd == 30

    await reg.close_and_remove("20-hello")
    assert "20-hello" not in reg
    assert chan20._closed  # pylint: disable=protected-access
    with pytest.raises(SenderError):
        await sender20.send(30)
    with pytest.raises(ReceiverError):
        await receiver20.receive()
    with pytest.raises(KeyError):
        reg.message_type("20-hello")
    with pytest.raises(KeyError):
        await reg.close_and_remove("20-hello")

    await reg.close_and_remove("21-hello")
    assert "21-hello" not in reg
    assert chan21._closed  # pylint: disable=protected-access
    with pytest.raises(SenderError):
        await sender21.send(30)
    with pytest.raises(ReceiverError):
        await receiver21.receive()
    with pytest.raises(KeyError):
        reg.message_type("21-hello")
    with pytest.raises(KeyError):
        await reg.close_and_remove("21-hello")
