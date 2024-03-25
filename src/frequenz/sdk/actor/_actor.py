# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Actor model implementation."""

import abc
import asyncio
import logging
from datetime import timedelta

from ._background_service import BackgroundService

_logger = logging.getLogger(__name__)


class Actor(BackgroundService, abc.ABC):
    """A primitive unit of computation that runs autonomously.

    To implement an actor, subclasses must implement the
    [`_run()`][frequenz.sdk.actor--the-_run-method] method, which should run the actor's
    logic. The [`_run()`][frequenz.sdk.actor--the-_run-method] method is called by the
    base class when the actor is started, and is expected to run until the actor is
    stopped.

    !!! info

        Please read the [`actor` module documentation][frequenz.sdk.actor] for more
        comprehensive guide on how to use and implement actors properly.
    """

    RESTART_DELAY: timedelta = timedelta(seconds=2)
    """The delay to wait between restarts of this actor."""

    _restart_limit: int | None = None
    """The number of times actors can be restarted when they are stopped by unhandled exceptions.

    If this is bigger than 0 or `None`, the actor will be restarted when there is an
    unhanded exception in the `_run()` method.

    If `None`, the actor will be restarted an unlimited number of times.

    !!! note

        This is mostly used for testing purposes and shouldn't be set in production.
    """

    def start(self) -> None:
        """Start this actor.

        If this actor is already running, this method does nothing.
        """
        if self.is_running:
            return
        self._tasks.clear()
        self._tasks.add(asyncio.create_task(self._run_loop()))

    @abc.abstractmethod
    async def _run(self) -> None:
        """Run this actor's logic."""

    async def _delay_if_restart(self, iteration: int) -> None:
        """Delay the restart of this actor's n'th iteration.

        Args:
            iteration: The current iteration of the restart.
        """
        # NB: I think it makes sense (in the future) to think about deminishing returns
        # the longer the actor has been running.
        # Not just for the restart-delay but actually for the n_restarts counter as well.
        if iteration > 0:
            delay = self.RESTART_DELAY.total_seconds()
            _logger.info("Actor %s: Waiting %s seconds...", self, delay)
            await asyncio.sleep(delay)

    async def _run_loop(self) -> None:
        """Run the actor's task continuously, managing restarts, cancellation, and termination.

        This method handles the execution of the actor's task, including
        restarts for unhandled exceptions, cancellation, or normal termination.

        Raises:
            asyncio.CancelledError: If the actor's `_run()` method is cancelled.
            Exception: If the actor's `_run()` method raises any other exception.
            BaseException: If the actor's `_run()` method raises any base exception.
        """
        _logger.info("Actor %s: Started.", self)
        n_restarts = 0
        while True:
            try:
                await self._delay_if_restart(n_restarts)
                await self._run()
                _logger.info("Actor %s: _run() returned without error.", self)
            except asyncio.CancelledError:
                _logger.info("Actor %s: Cancelled.", self)
                raise
            except Exception:  # pylint: disable=broad-except
                _logger.exception("Actor %s: Raised an unhandled exception.", self)
                limit_str = "∞" if self._restart_limit is None else self._restart_limit
                limit_str = f"({n_restarts}/{limit_str})"
                if self._restart_limit is None or n_restarts < self._restart_limit:
                    n_restarts += 1
                    _logger.info("Actor %s: Restarting %s...", self._name, limit_str)
                    continue
                _logger.info(
                    "Actor %s: Maximum restarts attempted %s, bailing out...",
                    self,
                    limit_str,
                )
                raise
            except BaseException:  # pylint: disable=broad-except
                _logger.exception("Actor %s: Raised a BaseException.", self)
                raise
            break

        _logger.info("Actor %s: Stopped.", self)
