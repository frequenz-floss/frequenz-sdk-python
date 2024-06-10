# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""General purpose classes for use with channels."""

import abc
import asyncio
import typing

from frequenz.channels import Receiver

from ._asyncio import cancel_and_await

T_co = typing.TypeVar("T_co", covariant=True)


class ReceiverFetcher(typing.Generic[T_co], typing.Protocol):
    """An interface that just exposes a `new_receiver` method."""

    @abc.abstractmethod
    def new_receiver(self, *, limit: int = 50) -> Receiver[T_co]:
        """Get a receiver from the channel.

        Args:
            limit: The maximum size of the receiver.

        Returns:
            A receiver instance.
        """


class _Sentinel:
    """A sentinel to denote that no value has been received yet."""

    def __str__(self) -> str:
        """Return a string representation of this sentinel."""
        return "<no value received yet>"


class LatestValueCache(typing.Generic[T_co]):
    """A cache that stores the latest value in a receiver."""

    def __init__(
        self, receiver: Receiver[T_co], *, unique_id: str | None = None
    ) -> None:
        """Create a new cache.

        Args:
            receiver: The receiver to cache.
            unique_id: A string to help uniquely identify this instance. If not
                provided, a unique identifier will be generated from the object's
                [`id()`][]. It is used mostly for debugging purposes.
        """
        self._receiver = receiver
        self._unique_id: str = hex(id(self)) if unique_id is None else unique_id
        self._latest_value: T_co | _Sentinel = _Sentinel()
        self._task = asyncio.create_task(
            self._run(), name=f"LatestValueCache«{self._unique_id}»"
        )

    @property
    def unique_id(self) -> str:
        """The unique identifier of this instance."""
        return self._unique_id

    def get(self) -> T_co:
        """Return the latest value that has been received.

        This raises a `ValueError` if no value has been received yet. Use `has_value` to
        check whether a value has been received yet, before trying to access the value,
        to avoid the exception.

        Returns:
            The latest value that has been received.

        Raises:
            ValueError: If no value has been received yet.
        """
        if isinstance(self._latest_value, _Sentinel):
            raise ValueError("No value has been received yet.")
        return self._latest_value

    def has_value(self) -> bool:
        """Check whether a value has been received yet.

        Returns:
            `True` if a value has been received, `False` otherwise.
        """
        return not isinstance(self._latest_value, _Sentinel)

    async def _run(self) -> None:
        async for value in self._receiver:
            self._latest_value = value

    async def stop(self) -> None:
        """Stop the cache."""
        await cancel_and_await(self._task)

    def __repr__(self) -> str:
        """Return a string representation of this cache."""
        return (
            f"<LatestValueCache latest_value={self._latest_value!r}, "
            f"receiver={self._receiver!r}, unique_id={self._unique_id!r}>"
        )

    def __str__(self) -> str:
        """Return the last value seen by this cache."""
        return str(self._latest_value)
