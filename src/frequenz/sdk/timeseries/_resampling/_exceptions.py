# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Resampler exceptions."""

import asyncio

from ._base_types import Source


class SourceStoppedError(RuntimeError):
    """A timeseries stopped producing samples."""

    def __init__(self, source: Source) -> None:
        """Create an instance.

        Args:
            source: The source of the timeseries that stopped producing samples.
        """
        super().__init__(f"Timeseries stopped producing samples, source: {source}")
        self.source = source
        """The source of the timeseries that stopped producing samples."""

    def __repr__(self) -> str:
        """Return the representation of the instance.

        Returns:
            The representation of the instance.
        """
        return f"{self.__class__.__name__}({self.source!r})"


class ResamplingError(RuntimeError):
    """An Error occurred while resampling.

    This error is a container for errors raised by the underlying sources and
    or sinks.
    """

    def __init__(
        self,
        exceptions: dict[Source, Exception | asyncio.CancelledError],
    ) -> None:
        """Create an instance.

        Args:
            exceptions: A mapping of timeseries source and the exception
                encountered while resampling that timeseries. Note that the
                error could be raised by the sink, while trying to send
                a resampled data for this timeseries, the source key is only
                used to identify the timeseries with the issue, it doesn't
                necessarily mean that the error was raised by the source. The
                underlying exception should provide information about what was
                the actual source of the exception.
        """
        super().__init__(f"Some error were found while resampling: {exceptions}")
        self.exceptions = exceptions
        """A mapping of timeseries source and the exception encountered.

        Note that the error could be raised by the sink, while trying to send
        a resampled data for this timeseries, the source key is only used to
        identify the timeseries with the issue, it doesn't necessarily mean
        that the error was raised by the source. The underlying exception
        should provide information about what was the actual source of the
        exception.
        """

    def __repr__(self) -> str:
        """Return the representation of the instance.

        Returns:
            The representation of the instance.
        """
        return f"{self.__class__.__name__}({self.exceptions=})"
