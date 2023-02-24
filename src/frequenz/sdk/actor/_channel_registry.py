# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""A class that would dynamically create, own and provide access to channels."""

from typing import Any, Dict

from frequenz.channels import Broadcast, Receiver, Sender


class ChannelRegistry:
    """Dynamically creates, own and provide access to channels.

    It can be used by actors to dynamically establish a communication channel
    between each other.  Channels are identified by string names.
    """

    def __init__(self, *, name: str) -> None:
        """Create a `ChannelRegistry` instance.

        Args:
            name: A unique name for the registry.
        """
        self._name = name
        self._channels: Dict[str, Broadcast[Any]] = {}

    def new_sender(self, key: str) -> Sender[Any]:
        """Get a sender to a dynamically created channel with the given key.

        Args:
            key: A key to identify the channel.

        Returns:
            A sender to a dynamically created channel with the given key.
        """
        if key not in self._channels:
            self._channels[key] = Broadcast(f"{self._name}-{key}")
        return self._channels[key].new_sender()

    def new_receiver(self, key: str) -> Receiver[Any]:
        """Get a receiver to a dynamically created channel with the given key.

        Args:
            key: A key to identify the channel.

        Returns:
            A receiver for a dynamically created channel with the given key.
        """
        if key not in self._channels:
            self._channels[key] = Broadcast(f"{self._name}-{key}")
        return self._channels[key].new_receiver()

    async def _close_channel(self, key: str) -> None:
        """Close a channel with the given key.

        This method is private and should only be used in special cases.

        Args:
            key: A key to identify the channel.
        """
        if key in self._channels:
            if channel := self._channels.pop(key, None):
                await channel.close()
