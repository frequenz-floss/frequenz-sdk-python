# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""A class that would dynamically create, own and provide access to channels."""

from __future__ import annotations

from typing import Generic, TypeVar

from frequenz.channels import Broadcast, Receiver, Sender

from .._internal._channels import ReceiverFetcher

_T = TypeVar("_T")


class ChannelRegistry(Generic[_T]):
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
        self._channels: dict[str, Broadcast[_T]] = {}

    def set_resend_latest(self, key: str, resend_latest: bool) -> None:
        """Set the `resend_latest` flag for a given channel.

        This flag controls whether the latest value of the channel should be resent to
        new receivers, in slow streams.

        `resend_latest` is `False` by default.  It is safe to be set in data/reporting
        channels, but is not recommended for use in channels that stream control
        instructions.

        Args:
            key: The key to identify the channel.
            resend_latest: Whether to resend the latest value to new receivers, for the
                given channel.
        """
        if key not in self._channels:
            self._channels[key] = Broadcast(f"{self._name}-{key}")
        # This attribute is protected in the current version of the channels library,
        # but that will change in the future.
        self._channels[key].resend_latest = resend_latest

    def new_sender(self, key: str) -> Sender[_T]:
        """Get a sender to a dynamically created channel with the given key.

        Args:
            key: A key to identify the channel.

        Returns:
            A sender to a dynamically created channel with the given key.
        """
        if key not in self._channels:
            self._channels[key] = Broadcast(f"{self._name}-{key}")
        return self._channels[key].new_sender()

    def new_receiver(self, key: str, maxsize: int = 50) -> Receiver[_T]:
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

    def new_receiver_fetcher(self, key: str) -> ReceiverFetcher[_T]:
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


class _RegistryReceiverFetcher(Generic[_T]):
    """A receiver fetcher that is bound to a channel registry and a key."""

    def __init__(
        self,
        registry: ChannelRegistry[_T],
        key: str,
    ) -> None:
        """Create a new instance of a receiver fetcher.

        Args:
            registry: The channel registry.
            key: A key to identify the channel.
        """
        self._registry = registry
        self._key = key

    def new_receiver(self, maxsize: int = 50) -> Receiver[_T]:
        """Get a receiver from the channel.

        Args:
            maxsize: The maximum size of the receiver.

        Returns:
            A receiver instance.
        """
        return self._registry.new_receiver(self._key, maxsize)
