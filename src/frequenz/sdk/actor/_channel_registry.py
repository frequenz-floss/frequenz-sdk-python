# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""A class that would dynamically create, own and provide access to channels."""

from __future__ import annotations

import typing

from frequenz.channels import Broadcast, Receiver, Sender

from .._internal._channels import ReceiverFetcher


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
        self._channels: dict[str, Broadcast[typing.Any]] = {}

    def new_sender(self, key: str) -> Sender[typing.Any]:
        """Get a sender to a dynamically created channel with the given key.

        Args:
            key: A key to identify the channel.

        Returns:
            A sender to a dynamically created channel with the given key.
        """
        if key not in self._channels:
            self._channels[key] = Broadcast(f"{self._name}-{key}")
        return self._channels[key].new_sender()

    def new_receiver(self, key: str, maxsize: int = 50) -> Receiver[typing.Any]:
        """Get a receiver to a dynamically created channel with the given key.

        Args:
            key: A key to identify the channel.
            maxsize: The maximum size of the receiver.

        Returns:
            A receiver for a dynamically created channel with the given key.
        """
        if key not in self._channels:
            self._channels[key] = Broadcast(f"{self._name}-{key}")
        return self._channels[key].new_receiver(maxsize=maxsize)

    def new_receiver_fetcher(self, key: str) -> ReceiverFetcher[typing.Any]:
        """Get a receiver fetcher to a dynamically created channel with the given key.

        Args:
            key: A key to identify the channel.

        Returns:
            A receiver fetcher for a dynamically created channel with the given key.
        """
        if key not in self._channels:
            self._channels[key] = Broadcast(f"{self._name}-{key}")
        return _RegistryReceiverFetcher(self, key)

    async def _close_channel(self, key: str) -> None:
        """Close a channel with the given key.

        This method is private and should only be used in special cases.

        Args:
            key: A key to identify the channel.
        """
        if key in self._channels:
            if channel := self._channels.pop(key, None):
                await channel.close()


T = typing.TypeVar("T")


class _RegistryReceiverFetcher(typing.Generic[T]):
    """A receiver fetcher that is bound to a channel registry and a key."""

    def __init__(
        self,
        registry: ChannelRegistry,
        key: str,
    ) -> None:
        """Create a new instance of a receiver fetcher.

        Args:
            registry: The channel registry.
            key: A key to identify the channel.
        """
        self._registry = registry
        self._key = key

    def new_receiver(self, maxsize: int = 50) -> Receiver[T]:
        """Get a receiver from the channel.

        Args:
            maxsize: The maximum size of the receiver.

        Returns:
            A receiver instance.
        """
        return self._registry.new_receiver(self._key, maxsize)
