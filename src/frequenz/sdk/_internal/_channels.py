# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""General purpose classes for use with channels."""

import abc
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
