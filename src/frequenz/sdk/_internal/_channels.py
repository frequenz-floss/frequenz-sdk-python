# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""General purpose classes for use with channels."""

import abc
import typing

from frequenz.channels import Receiver

T_co = typing.TypeVar("T_co", covariant=True)
U_co = typing.TypeVar("U_co", covariant=True)


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


class MappingReceiverFetcher(typing.Generic[T_co, U_co]):
    """A receiver fetcher that can manipulate receivers before returning them."""

    def __init__(
        self,
        fetcher: ReceiverFetcher[T_co],
        mapping_function: typing.Callable[[Receiver[T_co]], Receiver[U_co]],
    ) -> None:
        """Initialize this instance.

        Args:
            fetcher: The underlying fetcher to get receivers from.
            mapping_function: The method to be applied on new receivers before returning
                them.
        """
        self._fetcher = fetcher
        self._mapping_function = mapping_function

    def new_receiver(self, *, limit: int = 50) -> Receiver[U_co]:
        """Get a receiver from the channel.

        Args:
            limit: The maximum size of the receiver.

        Returns:
            A receiver instance.
        """
        return self._mapping_function(self._fetcher.new_receiver(limit=limit))
