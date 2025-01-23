# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Management of configuration."""

import asyncio
import logging
import pathlib
from collections.abc import Mapping, Sequence
from dataclasses import is_dataclass
from datetime import timedelta
from typing import Any, Final, Literal, TypeGuard, overload

import marshmallow
from frequenz.channels import Broadcast, Receiver, ReceiverStoppedError
from frequenz.channels.experimental import WithPrevious
from marshmallow import Schema, ValidationError
from typing_extensions import override

from ..actor._background_service import BackgroundService
from ._base_schema import BaseConfigSchema
from ._managing_actor import ConfigManagingActor
from ._util import DataclassT, _validate_load_kwargs, load_config

_logger = logging.getLogger(__name__)


class InvalidValueForKeyError(ValueError):
    """An error indicating that the value under the specified key is invalid."""

    def __init__(self, msg: str, *, key: Sequence[str], value: Any) -> None:
        """Initialize this error.

        Args:
            msg: The error message.
            key: The key that has an invalid value.
            value: The actual value that was found that is not a mapping.
        """
        super().__init__(msg)

        self.key: Final[Sequence[str]] = key
        """The key that has an invalid value."""

        self.value: Final[Any] = value
        """The actual value that was found that is not a mapping."""


class ConfigManager(BackgroundService):
    """A manager for configuration files.

    This class reads configuration files and sends the configuration to the receivers,
    providing configuration key filtering and value validation.

    For a more in-depth introduction and examples, please read the [module
    documentation][frequenz.sdk.config].
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        config_paths: str | pathlib.Path | Sequence[pathlib.Path | str],
        /,
        *,
        force_polling: bool = True,
        logging_config_key: str | Sequence[str] | None = "logging",
        name: str | None = None,
        polling_interval: timedelta = timedelta(seconds=1),
    ) -> None:
        """Initialize this config manager.

        Args:
            config_paths: The paths to the TOML files with the configuration. Order
                matters, as the configuration will be read and updated in the order
                of the paths, so the last path will override the configuration set by
                the previous paths. Dict keys will be merged recursively, but other
                objects (like lists) will be replaced by the value in the last path.
            force_polling: Whether to force file polling to check for changes.
            logging_config_key: The key to use for the logging configuration. If `None`,
                logging configuration will not be managed.  If a key is provided, the
                manager update the logging configuration whenever the configuration
                changes.
            name: A name to use when creating actors. If `None`, `str(id(self))` will
                be used. This is used mostly for debugging purposes.
            polling_interval: The interval to poll for changes. Only relevant if
                polling is enabled.
        """
        super().__init__(name=name)

        self.config_channel: Final[Broadcast[Mapping[str, Any]]] = Broadcast(
            name=f"{self}_config", resend_latest=True
        )
        """The channel used for sending configuration updates (resends the latest value).

        This is the channel used to communicate with the
        [`ConfigManagingActor`][frequenz.sdk.config.ConfigManager.config_actor] and will
        receive the complete raw configuration as a mapping.
        """

        self.config_actor: Final[ConfigManagingActor] = ConfigManagingActor(
            config_paths,
            self.config_channel.new_sender(),
            name=self.name,
            force_polling=force_polling,
            polling_interval=polling_interval,
        )
        """The actor that manages the configuration for this manager."""

        # pylint: disable-next=import-outside-toplevel,cyclic-import
        from ._logging_actor import LoggingConfigUpdatingActor

        self.logging_actor: Final[LoggingConfigUpdatingActor | None] = (
            None
            if logging_config_key is None
            else LoggingConfigUpdatingActor(
                self, config_key=logging_config_key, name=self.name
            )
        )
        """The actor that manages the logging configuration for this manager."""

    @override
    def start(self) -> None:
        """Start this config manager."""
        self.config_actor.start()
        if self.logging_actor:
            self.logging_actor.start()

    @property
    @override
    def is_running(self) -> bool:
        """Whether this config manager is running."""
        return self.config_actor.is_running or (
            self.logging_actor is not None and self.logging_actor.is_running
        )

    @override
    def cancel(self, msg: str | None = None) -> None:
        """Cancel all running tasks and actors spawned by this config manager.

        Args:
            msg: The message to be passed to the tasks being cancelled.
        """
        if self.logging_actor:
            self.logging_actor.cancel(msg)
        self.config_actor.cancel(msg)

    @override
    async def wait(self) -> None:
        """Wait this config manager to finish.

        Wait until all tasks and actors are finished.

        Raises:
            BaseExceptionGroup: If any of the tasks spawned by this service raised an
                exception (`CancelError` is not considered an error and not returned in
                the exception group).
        """
        exceptions: list[BaseException] = []
        if self.logging_actor:
            try:
                await self.logging_actor
            except BaseExceptionGroup as err:  # pylint: disable=try-except-raise
                exceptions.append(err)

        try:
            await self.config_actor
        except BaseExceptionGroup as err:  # pylint: disable=try-except-raise
            exceptions.append(err)

        if exceptions:
            raise BaseExceptionGroup(f"Error while stopping {self!r}", exceptions)

    @override
    def __repr__(self) -> str:
        """Return a string representation of this config manager."""
        logging_actor = (
            f"logging_actor={self.logging_actor!r}, " if self.logging_actor else ""
        )
        return (
            f"<{self.__class__.__name__}: "
            f"name={self.name!r}, "
            f"config_channel={self.config_channel!r}, "
            + logging_actor
            + f"config_actor={self.config_actor!r}>"
        )

    def new_receiver(  # pylint: disable=too-many-arguments
        self,
        # This is tricky, because a str is also a Sequence[str], if we would use only
        # Sequence[str], then a regular string would also be accepted and taken as
        # a sequence, like "key" -> ["k", "e", "y"]. We should never remove the str from
        # the allowed types without changing Sequence[str] to something more specific,
        # like list[str] or tuple[str] (but both have their own problems).
        key: str | Sequence[str],
        config_class: type[DataclassT],
        /,
        *,
        skip_unchanged: bool = True,
        base_schema: type[Schema] | None = BaseConfigSchema,
        marshmallow_load_kwargs: dict[str, Any] | None = None,
    ) -> Receiver[DataclassT | Exception | None]:
        """Create a new receiver for receiving the configuration for a particular key.

        This method has a lot of features and functionalities to make it easier to
        receive configurations, but it also imposes some restrictions on how the
        configurations are received. If you need more control over the configuration
        receiver, you can create a receiver directly using
        [`config_channel.new_receiver()`][frequenz.sdk.config.ConfigManager.config_channel].

        For a more in-depth introduction and examples, please read the [module
        documentation][frequenz.sdk.config].

        Args:
            key: The configuration key to be read by the receiver. If a sequence of
                strings is used, it is used as a sub-key.
            config_class: The class object to use to instantiate a configuration. The
                configuration will be validated against this type too using
                [`marshmallow_dataclass`][].
            skip_unchanged: Whether to skip sending the configuration if it hasn't
                changed compared to the last one received.
            base_schema: An optional class to be used as a base schema for the
                configuration class. This allow using custom fields for example. Will be
                passed to [`marshmallow_dataclass.class_schema`][].
            marshmallow_load_kwargs: Additional arguments to be passed to
                [`marshmallow.Schema.load`][].

        Returns:
            The receiver for the configuration.
        """
        _validate_load_kwargs(marshmallow_load_kwargs)

        # We disable warning on overflow, because we are only interested in the latest
        # configuration, it is completely fine to drop old configuration updates.
        receiver = self.config_channel.new_receiver(
            name=f"{self}:{key}", limit=1, warn_on_overflow=False
        ).map(
            lambda config: _load_config_with_logging_and_errors(
                config,
                config_class,
                key=key,
                base_schema=base_schema,
                marshmallow_load_kwargs=marshmallow_load_kwargs,
            )
        )

        if skip_unchanged:
            # For some reason the type argument for WithPrevious is not inferred
            # correctly, so we need to specify it explicitly.
            return receiver.filter(
                WithPrevious[DataclassT | Exception | None](
                    lambda old, new: _not_equal_with_logging(
                        key=key, old_value=old, new_value=new
                    )
                )
            )

        return receiver


@overload
async def wait_for_first(
    receiver: Receiver[DataclassT | Exception | None],
    /,
    *,
    receiver_name: str | None = None,
    allow_none: Literal[False] = False,
    timeout: timedelta = timedelta(minutes=1),
) -> DataclassT: ...


@overload
async def wait_for_first(
    receiver: Receiver[DataclassT | Exception | None],
    /,
    *,
    receiver_name: str | None = None,
    allow_none: Literal[True] = True,
    timeout: timedelta = timedelta(minutes=1),
) -> DataclassT | None: ...


async def wait_for_first(
    receiver: Receiver[DataclassT | Exception | None],
    /,
    *,
    receiver_name: str | None = None,
    allow_none: bool = False,
    timeout: timedelta = timedelta(minutes=1),
) -> DataclassT | None:
    """Wait for and receive the the first configuration.

    For a more in-depth introduction and examples, please read the [module
    documentation][frequenz.sdk.config].

    Args:
        receiver: The receiver to receive the first configuration from.
        receiver_name: The name of the receiver, used for logging. If `None`, the
            string representation of the receiver will be used.
        allow_none: Whether consider a `None` value as a valid configuration.
        timeout: The timeout in seconds to wait for the first configuration.

    Returns:
        The first configuration received.

    Raises:
        asyncio.TimeoutError: If the first configuration is not received within the
            timeout.
        ReceiverStoppedError: If the receiver is stopped before the first configuration
            is received.
    """
    if receiver_name is None:
        receiver_name = str(receiver)

    # We need this type guard because we can't use a TypeVar for isinstance checks or
    # match cases.
    def is_config_class(value: DataclassT | Exception | None) -> TypeGuard[DataclassT]:
        return is_dataclass(value) if value is not None else False

    _logger.info(
        "%s: Waiting %s seconds for the first configuration to arrive...",
        receiver_name,
        timeout.total_seconds(),
    )
    try:
        async with asyncio.timeout(timeout.total_seconds()):
            async for config in receiver:
                match config:
                    case None:
                        if allow_none:
                            return None
                        _logger.error(
                            "%s: Received empty configuration, waiting again for "
                            "a first configuration to be set.",
                            receiver_name,
                        )
                    case Exception() as error:
                        _logger.error(
                            "%s: Error while receiving the first configuration, "
                            "will keep waiting for an update: %s.",
                            receiver_name,
                            error,
                        )
                    case config if is_config_class(config):
                        _logger.info("%s: Received first configuration.", receiver_name)
                        return config
                    case unexpected:
                        assert (
                            False
                        ), f"{receiver_name}: Unexpected value received: {unexpected!r}."
    except asyncio.TimeoutError:
        _logger.error("%s: No configuration received in time.", receiver_name)
        raise
    raise ReceiverStoppedError(receiver)


def _not_equal_with_logging(
    *,
    key: str | Sequence[str],
    old_value: DataclassT | Exception | None,
    new_value: DataclassT | Exception | None,
) -> bool:
    """Return whether the two mappings are not equal, logging if they are the same."""
    if old_value == new_value:
        _logger.info("Configuration has not changed for key %r, skipping update.", key)
        return False

    if isinstance(new_value, InvalidValueForKeyError) and not isinstance(
        old_value, InvalidValueForKeyError
    ):
        subkey_str = ""
        if key != new_value.key:
            subkey_str = f"When looking for sub-key {key!r}: "
        _logger.error(
            "%sConfiguration for key %r has an invalid value: %r",
            subkey_str,
            new_value.key,
            new_value.value,
        )
    return True


def _load_config_with_logging_and_errors(
    config: Mapping[str, Any],
    config_class: type[DataclassT],
    *,
    key: str | Sequence[str],
    base_schema: type[Schema] | None = None,
    marshmallow_load_kwargs: dict[str, Any] | None = None,
) -> DataclassT | Exception | None:
    """Load the configuration for the specified key, logging errors and returning them."""
    try:
        sub_config = _get_key(config, key)
        if sub_config is None:
            _logger.debug("Configuration key %r not found, sending None", key)
            return None

        loaded_config = _load_config(
            sub_config,
            config_class,
            key=key,
            base_schema=base_schema,
            marshmallow_load_kwargs=marshmallow_load_kwargs,
        )
        _logger.debug("Received new configuration: %s", loaded_config)
        return loaded_config
    except InvalidValueForKeyError as error:
        if len(key) > 1 and key != error.key:
            _logger.error("Error when looking for sub-key %r: %s", key, error)
        else:
            _logger.error(str(error))
        return error
    except ValidationError as error:
        _logger.error("The configuration for key %r is invalid: %s", key, error)
        return error
    except Exception as error:  # pylint: disable=broad-except
        _logger.exception(
            "An unexpected error occurred while loading the configuration for key %r: %s",
            key,
            error,
        )
        return error


def _get_key(
    config: Mapping[str, Any],
    # This is tricky, because a str is also a Sequence[str], if we would use only
    # Sequence[str], then a regular string would also be accepted and taken as
    # a sequence, like "key" -> ["k", "e", "y"]. We should never remove the str from
    # the allowed types without changing Sequence[str] to something more specific,
    # like list[str] or tuple[str].
    key: str | Sequence[str],
) -> Mapping[str, Any] | None:
    """Get the value from the configuration under the specified key.

    Args:
        config: The configuration to get the value from.
        key: The key to get the value for.

    Returns:
        The value under the key, or `None` if the key is not found.

    Raises:
        InvalidValueForKeyError: If the value under the key is not a mapping.
    """
    # We first normalize to a Sequence[str] to make it easier to work with.
    if isinstance(key, str):
        key = (key,)
    value = config
    current_path = []
    for subkey in key:
        current_path.append(subkey)
        if value is None:
            return None
        match value.get(subkey):
            case None:
                return None
            case Mapping() as new_value:
                value = new_value
            case invalid_value:
                raise InvalidValueForKeyError(
                    f"Value for key {current_path!r} is not a mapping: {invalid_value!r}",
                    key=current_path,
                    value=invalid_value,
                )
        value = new_value
    return value


def _load_config(
    config: Mapping[str, Any],
    config_class: type[DataclassT],
    *,
    key: str | Sequence[str],
    base_schema: type[Schema] | None = BaseConfigSchema,
    marshmallow_load_kwargs: dict[str, Any] | None = None,
) -> DataclassT | InvalidValueForKeyError | ValidationError | None:
    """Try to load a configuration and log any validation errors."""
    loaded_config = load_config(
        config_class,
        config,
        base_schema=base_schema,
        marshmallow_load_kwargs=marshmallow_load_kwargs,
    )

    marshmallow_load_kwargs = (
        {} if marshmallow_load_kwargs is None else marshmallow_load_kwargs.copy()
    )

    # When excluding unknown fields we still want to notify the user, as
    # this could mean there is a typo in the configuration and some value is
    # not being loaded as desired.
    marshmallow_load_kwargs["unknown"] = marshmallow.RAISE
    try:
        load_config(
            config_class,
            config,
            base_schema=base_schema,
            marshmallow_load_kwargs=marshmallow_load_kwargs,
        )
    except ValidationError as err:
        _logger.warning(
            "The configuration for key %r has extra fields that will be ignored: %s",
            key,
            err,
        )

    return loaded_config
