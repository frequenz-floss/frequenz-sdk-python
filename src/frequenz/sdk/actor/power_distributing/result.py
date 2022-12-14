# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Results from PowerDistributingActor."""

from abc import ABC
from typing import Set

from .request import Request


class Result(ABC):
    """Base class for the power distributor result."""

    def __init__(self, request: Request) -> None:
        """Create class instance.

        Args:
            request: The user's request to which this message responds.
        """
        self.request: Request = request


class Success(Result):
    """Send if setting power for all batteries succeed."""

    def __init__(
        self,
        request: Request,
        succeed_power: int,
        used_batteries: Set[int],
        excess_power: int,
    ) -> None:
        """Create class instance.

        Args:
            request: The user's request to which this message responds.
            succeed_power: Part of the requested power that was successfully set.
            used_batteries: Subset of the requested batteries, that were used to
                realize the request.
            excess_power: Part of the requested power that could not be fulfilled,
                because it was outside available power bounds.
        """
        super().__init__(request)
        self.succeed_power: int = succeed_power
        self.used_batteries: Set[int] = used_batteries
        self.excess_power: int = excess_power


class PartialFailure(Result):
    """Send if any battery failed and didn't perform the request."""

    # It is very simple class with only data so it should be ok to disable pylint.
    # All these results should be dataclass but in python < 3.10 it is risky
    # to derive after dataclass.
    def __init__(  # pylint: disable=too-many-arguments
        self,
        request: Request,
        succeed_power: int,
        succeed_batteries: Set[int],
        failed_power: int,
        failed_batteries: Set[int],
        excess_power: int,
    ) -> None:
        """Create class instance.

        Args:
            request: The user's request to which this message responds.
            succeed_power: Part of the requested power that was successfully set.
            succeed_batteries: Subset of the requested batteries for which the request
                succeed.
            failed_power: Part of the requested power that failed.
            failed_batteries: Subset of the requested batteries for which the request
                failed.
            excess_power: Part of the requested power that could not be fulfilled,
                because it was outside available power bounds.
        """
        super().__init__(request)
        self.succeed_power: int = succeed_power
        self.succeed_batteries: Set[int] = succeed_batteries
        self.failed_power: int = failed_power
        self.failed_batteries: Set[int] = failed_batteries
        self.excess_power: int = excess_power


class Error(Result):
    """Error occurred and power was not set."""

    def __init__(self, request: Request, msg: str) -> None:
        """Create class instance.

        Args:
            request: The user's request to which this message responds.
            msg: Error message explaining why error happened.
        """
        super().__init__(request)
        self.msg: str = msg


class OutOfBound(Result):
    """Send if power was not set because requested power was not within bounds.

    This message is send only if Request.adjust_power = False.
    """

    def __init__(self, request: Request, bound: int) -> None:
        """Create class instance.

        Args:
            request: The user's request to which this message responds.
            bound: Total power bound for the requested batteries.
                If requested power < 0, then this value is lower bound.
                Otherwise it is upper bound.
        """
        super().__init__(request)
        self.bound: int = bound


class Ignored(Result):
    """Send if request was ignored.

    Request was ignored because new request for the same subset of batteries
    was received.
    """
