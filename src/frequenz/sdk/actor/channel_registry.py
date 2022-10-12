"""A class that would dynamically create, own and provide access to channels.

Copyright
Copyright © 2022 Frequenz Energy-as-a-Service GmbH

License
MIT
"""

from typing import Any, Dict, Generic, TypeVar

from frequenz.channels import Broadcast, Receiver, Sender

T = TypeVar("T")


class ChannelRegistry(Generic[T]):
    """Dynamically creates, own and provide access to channels.

    It can be used by actors to dynamically establish a communication channel
    between each other.  Channels are identified by hashable key objects.
    """

    def __init__(self) -> None:
        """Create a `ChannelRegistry` instance."""
        self._channels: Dict[T, Broadcast[Any]] = {}

    def get_sender(self, key: T) -> Sender[Any]:
        """Get a sender to a dynamically created channel with the given key.

        Args:
            key: A key to identify the channel.

        Returns:
            A sender to a dynamically created channel with the given key.
        """
        if key not in self._channels:
            self._channels[key] = Broadcast(f"dynamic-channel-{key}")
        return self._channels[key].get_sender()

    def get_receiver(self, key: T) -> Receiver[Any]:
        """Get a receiver to a dynamically created channel with the given key.

        Args:
            key: A key to identify the channel.

        Returns:
            A receiver for a dynamically created channel with the given key.
        """
        if key not in self._channels:
            self._channels[key] = Broadcast(f"dynamic-channel-{key}")
        return self._channels[key].get_receiver()
