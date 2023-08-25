# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Background service implementation."""

import abc
import asyncio
import collections.abc
from types import TracebackType
from typing import Any, Self


class BackgroundService(abc.ABC):
    """A background service that can be started and stopped.

    A background service is a service that runs in the background spawning one or more
    tasks. The service can be [started][frequenz.sdk.actor.BackgroundService.start]
    and [stopped][frequenz.sdk.actor.BackgroundService.stop] and can work as an
    async context manager to provide deterministic cleanup.

    To implement a background service, subclasses must implement the
    [`start()`][frequenz.sdk.actor.BackgroundService.start] method, which should
    start the background tasks needed by the service, and add them to the `_tasks`
    protected attribute.

    If you need to collect results or handle exceptions of the tasks when stopping the
    service, then you need to also override the
    [`stop()`][frequenz.sdk.actor.BackgroundService.stop] method, as the base
    implementation does not collect any results and re-raises all exceptions.

    !!! warning

        As background services manage [`asyncio.Task`][] objects, a reference to them
        must be held for as long as the background service is expected to be running,
        otherwise its tasks will be cancelled and the service will stop. For more
        information, please refer to the [Python `asyncio`
        documentation](https://docs.python.org/3/library/asyncio-task.html#asyncio.create_task).

    Example:
        ```python
        import datetime
        import asyncio

        class Clock(BackgroundService):
            def __init__(self, resolution_s: float, *, name: str | None = None) -> None:
                super().__init__(name=name)
                self._resolution_s = resolution_s

            def start(self) -> None:
                self._tasks.add(asyncio.create_task(self._tick()))

            async def _tick(self) -> None:
                while True:
                    await asyncio.sleep(self._resolution_s)
                    print(datetime.datetime.now())

        async def main() -> None:
            # As an async context manager
            async with Clock(resolution_s=1):
                await asyncio.sleep(5)

            # Manual start/stop (only use if necessary, as cleanup is more complicated)
            clock = Clock(resolution_s=1)
            clock.start()
            await asyncio.sleep(5)
            await clock.stop()

        asyncio.run(main())
        ```
    """

    def __init__(self, *, name: str | None = None) -> None:
        """Initialize this BackgroundService.

        Args:
            name: The name of this background service. If `None`, `str(id(self))` will
                be used. This is used mostly for debugging purposes.
        """
        self._name: str = str(id(self)) if name is None else name
        self._tasks: set[asyncio.Task[Any]] = set()

    @abc.abstractmethod
    def start(self) -> None:
        """Start this background service."""

    @property
    def name(self) -> str:
        """The name of this background service.

        Returns:
            The name of this background service.
        """
        return self._name

    @property
    def tasks(self) -> collections.abc.Set[asyncio.Task[Any]]:
        """Return the set of running tasks spawned by this background service.

        Users typically should not modify the tasks in the returned set and only use
        them for informational purposes.

        !!! danger

            Changing the returned tasks may lead to unexpected behavior, don't do it
            unless the class explicitly documents it is safe to do so.

        Returns:
            The set of running tasks spawned by this background service.
        """
        return self._tasks

    @property
    def is_running(self) -> bool:
        """Return whether this background service is running.

        A service is considered running when at least one task is running.

        Returns:
            Whether this background service is running.
        """
        return any(not task.done() for task in self._tasks)

    def cancel(self, msg: str | None = None) -> None:
        """Cancel all running tasks spawned by this background service.

        Args:
            msg: The message to be passed to the tasks being cancelled.
        """
        for task in self._tasks:
            task.cancel(msg)

    async def stop(self, msg: str | None = None) -> None:
        """Stop this background service.

        This method cancels all running tasks spawned by this service and waits for them
        to finish.

        Args:
            msg: The message to be passed to the tasks being cancelled.

        Raises:
            BaseExceptionGroup: If any of the tasks spawned by this service raised an
                exception.

        [//]: # (# noqa: DAR401 rest)
        """
        if not self._tasks:
            return
        self.cancel(msg)
        try:
            await self.wait()
        except BaseExceptionGroup as exc_group:
            # We want to ignore CancelledError here as we explicitly cancelled all the
            # tasks.
            _, rest = exc_group.split(asyncio.CancelledError)
            if rest is not None:
                # We are filtering out from an exception group, we really don't want to
                # add the exceptions we just filtered by adding a from clause here.
                raise rest  # pylint: disable=raise-missing-from

    async def __aenter__(self) -> Self:
        """Enter an async context.

        Start this background service.

        Returns:
            This background service.
        """
        self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit an async context.

        Stop this background service.

        Args:
            exc_type: The type of the exception raised, if any.
            exc_val: The exception raised, if any.
            exc_tb: The traceback of the exception raised, if any.
        """
        await self.stop()

    async def wait(self) -> None:
        """Wait this background service to finish.

        Wait until all background service tasks are finished.

        Raises:
            BaseExceptionGroup: If any of the tasks spawned by this service raised an
                exception (`CancelError` is not considered an error and not returned in
                the exception group).
        """
        # We need to account for tasks that were created between when we started
        # awaiting and we finished awaiting.
        while self._tasks:
            done, pending = await asyncio.wait(self._tasks)
            assert not pending

            # We remove the done tasks, but there might be new ones created after we
            # started waiting.
            self._tasks = self._tasks - done

            exceptions: list[BaseException] = []
            for task in done:
                try:
                    # This will raise a CancelledError if the task was cancelled or any
                    # other exception if the task raised one.
                    _ = task.result()
                except BaseException as error:  # pylint: disable=broad-except
                    exceptions.append(error)
            if exceptions:
                raise BaseExceptionGroup(
                    f"Error while stopping background service {self}", exceptions
                )

    def __await__(self) -> collections.abc.Generator[None, None, None]:
        """Await this background service.

        An awaited background service will wait for all its tasks to finish.

        Returns:
            An implementation-specific generator for the awaitable.
        """
        return self.wait().__await__()

    def __del__(self) -> None:
        """Destroy this instance.

        Cancel all running tasks spawned by this background service.
        """
        self.cancel("{self!r} was deleted")

    def __repr__(self) -> str:
        """Return a string representation of this instance.

        Returns:
            A string representation of this instance.
        """
        return f"{type(self).__name__}(name={self._name!r}, tasks={self._tasks!r})"

    def __str__(self) -> str:
        """Return a string representation of this instance.

        Returns:
            A string representation of this instance.
        """
        return f"{type(self).__name__}[{self._name}]"
