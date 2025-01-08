# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Logging utilities for the SDK."""

import logging
from collections.abc import Mapping
from datetime import datetime, timedelta
from types import TracebackType

_ExcInfoType = (
    bool
    | BaseException
    | tuple[None, None, None]
    | tuple[type[BaseException], BaseException, TracebackType | None]
    | None
)

DEFAULT_RATE_LIMIT = timedelta(minutes=15)

# The standard logging.py file uses variadic arguments in the logging methods, but the
# type hints file has more specific parameters.  pylint is not able to handle this, so
# we need to suppress the warning.
#
# pylint: disable=arguments-differ


class RateLimitedLogger:
    """Logger that limits the rate of logging messages.

    The first message is logged immediately.  Subsequent messages are ignored until the
    rate limit interval has elapsed.  After that the next request goes through, and so on.

    This allows a new outage to be reported immediately and subsequent logs to be
    rate-limited.

    When an outage has been resolved, the `reset()` method may be used to reset the
    logger and the next message will get logged immediately.
    """

    def __init__(
        self,
        logger: logging.Logger,
        rate_limit: timedelta = DEFAULT_RATE_LIMIT,
    ) -> None:
        """Initialize the logger.

        Args:
            logger: Logger to rate-limit.
            rate_limit: Time interval between two log messages.
        """
        self._logger = logger
        self._started: bool = False
        self._last_log_time: datetime | None = None
        self._rate_limit: timedelta = rate_limit

    def set_rate_limit(self, rate_limit: timedelta) -> None:
        """Set the rate limit for the logger.

        Args:
            rate_limit: Time interval between two log messages.
        """
        self._rate_limit = rate_limit

    def is_limiting(self) -> bool:
        """Return whether rate limiting is active.

        This is true when a previous message has already been logged, and can be reset
        by calling the `reset()` method.
        """
        return self._started

    def reset(self) -> None:
        """Reset the logger to healthy state."""
        self._started = False
        self._last_log_time = None

    def log(  # pylint: disable=too-many-arguments
        self,
        level: int,
        msg: object,
        *args: object,
        exc_info: _ExcInfoType = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        """Log a message.

        Args:
            level: Log level.
            msg: Log message.
            *args: Arguments for the log message.
            exc_info: Exception information.
            stack_info: Stack information.
            stacklevel: Stack level.
            extra: Extra information.
        """
        if self._rate_limit is None:
            self._logger.log(
                level,
                msg,
                *args,
                exc_info=exc_info,
                stack_info=stack_info,
                stacklevel=stacklevel,
                extra=extra,
            )
            return

        current_time = datetime.now()
        if (
            not self._started
            or self._last_log_time is None
            or (current_time - self._last_log_time) >= self._rate_limit
        ):
            self._logger.log(
                level,
                msg,
                *args,
                exc_info=exc_info,
                stack_info=stack_info,
                stacklevel=stacklevel,
                extra=extra,
            )
            self._last_log_time = current_time
            self._started = True

    def info(  # pylint: disable=too-many-arguments
        self,
        msg: object,
        *args: object,
        exc_info: _ExcInfoType = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        """Log an info message.

        Args:
            msg: Log message.
            *args: Arguments for the log message.
            exc_info: Exception information.
            stack_info: Stack information.
            stacklevel: Stack level.
            extra: Extra information.
        """
        self.log(
            logging.INFO,
            msg,
            *args,
            exc_info=exc_info,
            stack_info=stack_info,
            stacklevel=stacklevel,
            extra=extra,
        )

    def debug(  # pylint: disable=too-many-arguments
        self,
        msg: object,
        *args: object,
        exc_info: _ExcInfoType = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        """Log a debug message.

        Args:
            msg: Log message.
            *args: Arguments for the log message.
            exc_info: Exception information.
            stack_info: Stack information.
            stacklevel: Stack level.
            extra: Extra information.
        """
        self.log(
            logging.DEBUG,
            msg,
            *args,
            exc_info=exc_info,
            stack_info=stack_info,
            stacklevel=stacklevel,
            extra=extra,
        )

    def warning(  # pylint: disable=too-many-arguments
        self,
        msg: object,
        *args: object,
        exc_info: _ExcInfoType = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        """Log a warning message.

        Args:
            msg: Log message.
            *args: Arguments for the log message.
            exc_info: Exception information.
            stack_info: Stack information.
            stacklevel: Stack level.
            extra: Extra information.
        """
        self.log(
            logging.WARNING,
            msg,
            *args,
            exc_info=exc_info,
            stack_info=stack_info,
            stacklevel=stacklevel,
            extra=extra,
        )

    def critical(  # pylint: disable=too-many-arguments
        self,
        msg: object,
        *args: object,
        exc_info: _ExcInfoType = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        """Log a critical message.

        Args:
            msg: Log message.
            *args: Arguments for the log message.
            exc_info: Exception information.
            stack_info: Stack information.
            stacklevel: Stack level.
            extra: Extra information.
        """
        self.log(
            logging.CRITICAL,
            msg,
            *args,
            exc_info=exc_info,
            stack_info=stack_info,
            stacklevel=stacklevel,
            extra=extra,
        )

    def error(  # pylint: disable=too-many-arguments
        self,
        msg: object,
        *args: object,
        exc_info: _ExcInfoType = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        """Log an error message.

        Args:
            msg: Log message.
            *args: Arguments for the log message.
            exc_info: Exception information.
            stack_info: Stack information.
            stacklevel: Stack level.
            extra: Extra information.
        """
        self.log(
            logging.ERROR,
            msg,
            *args,
            exc_info=exc_info,
            stack_info=stack_info,
            stacklevel=stacklevel,
            extra=extra,
        )

    def exception(  # pylint: disable=too-many-arguments
        self,
        msg: object,
        *args: object,
        exc_info: _ExcInfoType = True,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        """Log an exception message.

        Args:
            msg: Log message.
            *args: Arguments for the log message.
            exc_info: Exception information.
            stack_info: Stack information.
            stacklevel: Stack level.
            extra: Extra information.
        """
        self.error(
            msg,
            *args,
            exc_info=exc_info,
            stack_info=stack_info,
            stacklevel=stacklevel,
            extra=extra,
        )
