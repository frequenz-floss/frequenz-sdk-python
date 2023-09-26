# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Actor model implementation."""

import abc
import asyncio
import logging

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

    async def _run_loop(self) -> None:
        """Run this actor's task in a loop until `_restart_limit` is reached.

        Raises:
            asyncio.CancelledError: If this actor's `_run()` gets cancelled.
            Exception: If this actor's `_run()` raises any other `Exception` and reached
                the maximum number of restarts.
            BaseException: If this actor's `_run()` raises any other `BaseException`.
        """
        _logger.info("Actor %s: Started.", self)
        n_restarts = 0
        while True:
            try:
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
