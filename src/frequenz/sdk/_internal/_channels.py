# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""General purpose classes for use with channels."""

import abc
import asyncio
import typing

from frequenz.channels import Receiver

T = typing.TypeVar("T")


class ReceiverFetcher(typing.Generic[T], typing.Protocol):
    """An interface that just exposes a `new_receiver` method."""

    @abc.abstractmethod
    def new_receiver(self, maxsize: int = 50) -> Receiver[T]:
        """Get a receiver from the channel.

        Args:
            maxsize: The maximum size of the receiver.

        Returns:
            A receiver instance.
        """


class LatestValueCache(typing.Generic[T]):
    """A cache that stores the latest value in a receiver."""

    def __init__(self, receiver: Receiver[T]) -> None:
        """Create a new cache.

        Args:
            receiver: The receiver to cache.
        """
        self._receiver = receiver
        self._latest_value: T | None = None
        self._task = asyncio.create_task(self._run())

    @property
    def latest_value(self) -> T | None:
        """Get the latest value in the cache."""
        return self._latest_value

    async def _run(self) -> None:
        async for value in self._receiver:
            self._latest_value = value
