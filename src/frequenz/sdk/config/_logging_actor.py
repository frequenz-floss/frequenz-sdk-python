# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Read and update logging severity from config."""

import logging
from dataclasses import dataclass, field
from typing import Annotated, Sequence, assert_never

import marshmallow
import marshmallow.validate

from ..actor import Actor
from ._manager import ConfigManager, wait_for_first

_logger = logging.getLogger(__name__)

LogLevel = Annotated[
    str,
    marshmallow.fields.String(
        validate=marshmallow.validate.OneOf(choices=logging.getLevelNamesMapping())
    ),
]
"""A marshmallow field for validating log levels."""


@dataclass(frozen=True, kw_only=True)
class RootLoggerConfig:
    """A configuration for the root logger."""

    level: LogLevel = field(
        default="NOTSET",
        metadata={
            "metadata": {
                "description": "Log level for the logger. Uses standard logging levels."
            },
        },
    )
    """The log level for the root logger."""


@dataclass(frozen=True, kw_only=True)
class LoggerConfig(RootLoggerConfig):
    """A configuration for a logger."""

    name: str = field(
        metadata={
            "metadata": {
                "description": "The name of the logger that will be affected by this "
                "configuration."
            },
        },
    )


@dataclass(frozen=True, kw_only=True)
class LoggingConfig:
    """A configuration for the logging system."""

    root_logger: RootLoggerConfig = field(
        default_factory=lambda: RootLoggerConfig(level="INFO"),
        metadata={
            "metadata": {
                "description": "Default default configuration for all loggers.",
            },
        },
    )
    """The default log level."""

    loggers: dict[str, LoggerConfig] = field(
        default_factory=dict,
        metadata={
            "metadata": {
                "description": "Configuration for a logger (the key is the logger name)."
            },
        },
    )
    """The list of loggers configurations."""


class LoggingConfigUpdatingActor(Actor):
    """Actor that listens for logging configuration changes and sets them.

    Example:
        `config.toml` file:
        ```toml
        [logging.root_logger]
        level = "INFO"

        [logging.loggers.power_dist]
        name = "frequenz.sdk.actor.power_distributing"
        level = "DEBUG"

        [logging.loggers.chan]
        name = "frequenz.channels"
        level = "DEBUG"
        ```

        ```python
        import asyncio

        from frequenz.sdk.config import LoggingConfigUpdatingActor
        from frequenz.sdk.actor import run as run_actors

        async def run() -> None:
            config_manager: ConfigManager = ...
            await run_actors(LoggingConfigUpdatingActor(config_manager))

        asyncio.run(run())
        ```

        Now whenever the `config.toml` file is updated, the logging configuration
        will be updated as well.
    """

    # pylint: disable-next=too-many-arguments
    def __init__(
        self,
        config_manager: ConfigManager,
        /,
        *,
        config_key: str | Sequence[str] = "logging",
        log_datefmt: str = "%Y-%m-%dT%H:%M:%S%z",
        log_format: str = "%(asctime)s %(levelname)-8s %(name)s:%(lineno)s: %(message)s",
        name: str | None = None,
    ):
        """Initialize this instance.

        Args:
            config_manager: The configuration manager to use.
            config_key: The key to use to retrieve the configuration from the
                configuration manager.  If `None`, the whole configuration will be used.
            log_datefmt: Use the specified date/time format in logs.
            log_format: Use the specified format string in logs.
            name: The name of this actor. If `None`, `str(id(self))` will be used. This
                is used mostly for debugging purposes.

        Note:
            The `log_format` and `log_datefmt` parameters are used in a call to
            `logging.basicConfig()`. If logging has already been configured elsewhere
            in the application (through a previous `basicConfig()` call), then the format
            settings specified here will be ignored.
        """
        self._config_receiver = config_manager.new_receiver(
            config_key, LoggingConfig, base_schema=None
        )

        # Setup default configuration.
        # This ensures logging is configured even if actor fails to start or
        # if the configuration cannot be loaded.
        self._current_config: LoggingConfig = LoggingConfig()

        super().__init__(name=name)

        logging.basicConfig(
            format=log_format,
            datefmt=log_datefmt,
            level=logging.INFO,
        )
        _logger.info("Applying initial default logging configuration...")
        self._reconfigure(self._current_config)

    async def _run(self) -> None:
        """Listen for configuration changes and update logging."""
        self._reconfigure(
            await wait_for_first(
                self._config_receiver, receiver_name=str(self), allow_none=True
            )
        )
        async for config_update in self._config_receiver:
            self._reconfigure(config_update)

    def _reconfigure(self, config_update: LoggingConfig | Exception | None) -> None:
        """Update the logging configuration.

        Args:
            config_update: The new configuration, or an exception if there was an error
                parsing the configuration, or `None` if the configuration was unset.
        """
        match config_update:
            case LoggingConfig():
                _logger.info(
                    "New configuration received, updating logging configuration."
                )
                self._update_logging(config_update)
            case None:
                _logger.info(
                    "Configuration was unset, resetting to the default "
                    "logging configuration."
                )
                self._update_logging(LoggingConfig())
            case Exception():
                _logger.info(
                    "New configuration has errors, keeping the old logging "
                    "configuration."
                )
            case unexpected:
                assert_never(unexpected)

    def _update_logging(self, config: LoggingConfig) -> None:
        """Configure the logging level."""
        # If the logger is not in the new config, set it to NOTSET
        old_names = {old.name for old in self._current_config.loggers.values()}
        new_names = {new.name for new in config.loggers.values()}
        loggers_to_unset = old_names - new_names
        for logger_name in loggers_to_unset:
            _logger.debug("Unsetting log level for logger '%s'", logger_name)
            logging.getLogger(logger_name).setLevel(logging.NOTSET)

        self._current_config = config
        _logger.info(
            "Setting root logger level to '%s'", self._current_config.root_logger.level
        )
        logging.getLogger().setLevel(self._current_config.root_logger.level)

        # For each logger in the new config, set the log level
        for logger_config in self._current_config.loggers.values():
            _logger.info(
                "Setting log level for logger '%s' to '%s'",
                logger_config.name,
                logger_config.level,
            )
            logging.getLogger(logger_config.name).setLevel(logger_config.level)

        _logger.info("Logging config update completed.")
