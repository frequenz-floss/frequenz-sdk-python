# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Configuration management.

# Overview

To provide dynamic configurations to an application, you can use the
[`ConfigManager`][frequenz.sdk.config.ConfigManager] class. This class provides
a convenient interface to manage configurations from multiple config files and receive
updates when the configurations change.  Users can create a receiver to receive
configurations from the manager.

# Setup

To use the `ConfigManager`, you need to create an instance of it and pass the
paths to the configuration files. The configuration files must be in the TOML
format.

When specifying multiple files order matters, as the configuration will be read and
updated in the order of the paths, so the last path will override the configuration set
by the previous paths. Dict keys will be merged recursively, but other objects (like
lists) will be replaced by the value in the last path.

```python
from frequenz.sdk.config import ConfigManager

async with ConfigManager(["base-config.toml", "overrides.toml"]) as config_manager:
    ...
```

# Logging

The `ConfigManager` can also instantiate
a [`LoggingConfigUpdatingActor`][frequenz.sdk.config.LoggingConfigUpdatingActor] to
monitor logging configurations. This actor will listen for logging configuration changes
and update the logging configuration accordingly.

This feature is enabled by default using the key `logging` in the configuration file. To
disable it you can pass `logging_config_key=None` to the `ConfigManager`.

# Receiving configurations

To receive configurations, you can create a receiver using the [`new_receiver()`][
frequenz.sdk.config.ConfigManager.new_receiver] method. The receiver will receive
configurations from the manager for a particular key, and validate and load the
configurations to a dataclass using [`marshmallow_dataclass`][].

If the key is a sequence of strings, it will be treated as a nested key and the
receiver will receive the configuration under the nested key. For example
`["key", "subkey"]` will get only `config["key"]["subkey"]`.

Besides a configuration instance, the receiver can also receive exceptions if there are
errors loading the configuration (typically
a [`ValidationError`][marshmallow.ValidationError]), or `None` if there is no
configuration for the key.

The value under `key` must be another mapping, otherwise
a [`InvalidValueForKeyError`][frequenz.sdk.config.InvalidValueForKeyError] instance will
be sent to the receiver.

If there were any errors loading the configuration, the error will be logged too.

```python
from dataclasses import dataclass
from frequenz.sdk.config import ConfigManager

@dataclass(frozen=True, kw_only=True)
class AppConfig:
    test: int

async with ConfigManager("config.toml") as config_manager:
    receiver = config_manager.new_receiver("app", AppConfig)
    app_config = await receiver.receive()
    match app_config:
        case AppConfig(test=42):
            print("App configured with 42")
        case Exception() as error:
            print(f"Error loading configuration: {error}")
        case None:
            print("There is no configuration for the app key")
```

## Validation and loading

The configuration class used to create the configuration instance is expected to be
a [`dataclasses.dataclass`][], which is used to create a [`marshmallow.Schema`][] via
the [`marshmallow_dataclass.class_schema`][] function.

This means you can customize the schema derived from the configuration
dataclass using [`marshmallow_dataclass`][] to specify extra validation and
options via field metadata.

Customization can also be done via a `base_schema`. By default
[`BaseConfigSchema`][frequenz.sdk.config.BaseConfigSchema] is used to provide support
for some extra commonly used fields (like [quantities][frequenz.quantities]) and to
exclude unknown fields by default.

```python
import marshmallow.validate
from dataclasses import dataclass, field

@dataclass(frozen=True, kw_only=True)
class Config:
    test: int = field(
        metadata={"validate": marshmallow.validate.Range(min=0)},
    )
```

Additional arguments can be passed to [`marshmallow.Schema.load`][] using
the `marshmallow_load_kwargs` keyword arguments.

When [`marshmallow.EXCLUDE`][] is used, a warning will be logged if there are extra
fields in the configuration that are excluded. This is useful, for example, to catch
typos in the configuration file.

## Skipping superfluous updates

If there is a burst of configuration updates, the receiver will only receive the
last configuration, older configurations will be ignored.

If `skip_unchanged` is set to `True`, then a configuration that didn't change
compared to the last one received will be ignored and not sent to the receiver.
The comparison is done using the *raw* `dict` to determine if the configuration
has changed.

## Error handling

The value under `key` must be another mapping, otherwise an error
will be logged and a [`frequenz.sdk.config.InvalidValueForKeyError`][] instance
will be sent to the receiver.

Configurations that don't pass the validation will be logged as an error and
the [`ValidationError`][marshmallow.ValidationError] sent to the receiver.

Any other unexpected error raised during the configuration loading will be
logged as an error and the error instance sent to the receiver.

## Further customization

If you have special needs for receiving the configurations (for example validating using
`marshmallow` doesn't fit your needs), you can create a custom receiver using
[`config_channel.new_receiver()`][frequenz.sdk.config.ConfigManager.config_channel]
directly. Please bear in mind that this provides a low-level access to the whole config
in the file as a raw Python mapping.

# Recommended usage

Actors that need to be reconfigured should take a configuration manager and a key to
receive configurations updates, and instantiate the new receiver themselves. This allows
actors to have full control over how the configuration is loaded (for example providing
a custom base schema or marshmallow options).

Passing the key explicitly too allows application to structure the configuration in
whatever way is most convenient for the application.

Actors can use the [`wait_for_first()`][frequenz.sdk.config.wait_for_first] function to
wait for the first configuration to be received, and cache the configuration for later
use and in case the actor is restarted. If the configuration is not received after some
timeout, a [`asyncio.TimeoutError`][] will be raised (and if uncaught, the actor will
be automatically restarted after some delay).

Example: Actor that can run without a configuration (using a default configuration)
    ```python title="actor.py" hl_lines="18 34 42 62 64"
    import dataclasses
    import logging
    from collections.abc import Sequence
    from datetime import timedelta
    from typing import assert_never

    from frequenz.channels import select, selected_from
    from frequenz.channels.event import Event

    from frequenz.sdk.actor import Actor
    from frequenz.sdk.config import ConfigManager, wait_for_first

    _logger = logging.getLogger(__name__)

    @dataclasses.dataclass(frozen=True, kw_only=True)
    class MyActorConfig:
        some_config: timedelta = dataclasses.field(
            default=timedelta(seconds=42), # (1)!
            metadata={"metadata": {"description": "Some optional configuration"}},
        )

    class MyActor(Actor):
        def __init__(
            self,
            config_manager: ConfigManager,
            /,
            *,
            config_key: str | Sequence[str],
            name: str | None = None,
        ) -> None:
            super().__init__(name=name)
            self._config_manager = config_manager
            self._config_key = config_key
            self._config: MyActorConfig = MyActorConfig() # (2)!

        async def _run(self) -> None:
            config_receiver = self._config_manager.new_receiver(
                self._config_key, MyActorConfig
            )
            self._update_config(
                await wait_for_first(
                    config_receiver, receiver_name=str(self), allow_none=True # (3)!
                )
            )

            other_receiver = Event()

            async for selected in select(config_receiver, other_receiver):
                if selected_from(selected, config_receiver):
                    self._update_config(selected.message)
                elif selected_from(selected, other_receiver):
                    # Do something else
                    ...

        def _update_config(self, config_update: MyActorConfig | Exception | None) -> None:
            match config_update:
                case MyActorConfig() as config:
                    _logger.info("New configuration received, updating.")
                    self._reconfigure(config)
                case None:
                    _logger.info("Configuration was unset, resetting to the default")
                    self._reconfigure(MyActorConfig()) # (4)!
                case Exception():
                    _logger.info( # (5)!
                        "New configuration has errors, keeping the old configuration."
                    )
                case unexpected:
                    assert_never(unexpected)

        def _reconfigure(self, config: MyActorConfig) -> None:
            self._config = config
            # Do something with the new configuration
    ```

    1. This is different when the actor requires a configuration to run. Here, the
        config has a default value.
    2. This is different when the actor requires a configuration to run. Here, the actor
        can just instantiate a default configuration.
    3. This is different when the actor requires a configuration to run. Here, the actor
        can accept a `None` configuration.
    4. This is different when the actor requires a configuration to run. Here, the actor
        can reset to a default configuration.
    5. There is no need to log the error itself, the configuration manager will log it
        automatically.

Example: Actor that requires a configuration to run
    ```python title="actor.py" hl_lines="17 33 40 58 60"
    import dataclasses
    import logging
    from collections.abc import Sequence
    from datetime import timedelta
    from typing import assert_never

    from frequenz.channels import select, selected_from
    from frequenz.channels.event import Event

    from frequenz.sdk.actor import Actor
    from frequenz.sdk.config import ConfigManager, wait_for_first

    _logger = logging.getLogger(__name__)

    @dataclasses.dataclass(frozen=True, kw_only=True)
    class MyActorConfig:
        some_config: timedelta = dataclasses.field( # (1)!
            metadata={"metadata": {"description": "Some required configuration"}},
        )

    class MyActor(Actor):
        def __init__(
            self,
            config_manager: ConfigManager,
            /,
            *,
            config_key: str | Sequence[str],
            name: str | None = None,
        ) -> None:
            super().__init__(name=name)
            self._config_manager = config_manager
            self._config_key = config_key
            self._config: MyActorConfig # (2)!

        async def _run(self) -> None:
            config_receiver = self._config_manager.new_receiver(
                self._config_key, MyActorConfig
            )
            self._update_config(
                await wait_for_first(config_receiver, receiver_name=str(self)) # (3)!
            )

            other_receiver = Event()

            async for selected in select(config_receiver, other_receiver):
                if selected_from(selected, config_receiver):
                    self._update_config(selected.message)
                elif selected_from(selected, other_receiver):
                    # Do something else
                    ...

        def _update_config(self, config_update: MyActorConfig | Exception | None) -> None:
            match config_update:
                case MyActorConfig() as config:
                    _logger.info("New configuration received, updating.")
                    self._reconfigure(config)
                case None:
                    _logger.info("Configuration was unset, keeping the old configuration.") # (4)!
                case Exception():
                    _logger.info( # (5)!
                        "New configuration has errors, keeping the old configuration."
                    )
                case unexpected:
                    assert_never(unexpected)

        def _reconfigure(self, config: MyActorConfig) -> None:
            self._config = config
            # Do something with the new configuration
    ```

    1. This is different when the actor can use a default configuration. Here, the
        field is required, so there is no default configuration possible.
    2. This is different when the actor can use a default configuration. Here, the
       assignment of the configuration is delayed to the `_run()` method.
    3. This is different when the actor can use a default configuration. Here, the actor
        doesn't accept `None` as a valid configuration as it can't create a default
        configuration.
    4. This is different when the actor can use a default configuration. Here, the actor
        doesn't accept `None` as a valid configuration as it can't create a default
        configuration, so it needs to keep the old configuration.
    5. There is no need to log the error itself, the configuration manager will log it
        automatically.


Example: Application
    The pattern used by the application is very similar to the one used by actors. In
    this case the application requires a configuration to run, but if it could also use
    a default configuration, the changes would be the same as in the actor examples.

    ```python title="app.py" hl_lines="14"
    import asyncio
    import dataclasses
    import logging
    import pathlib
    from collections.abc import Sequence
    from datetime import timedelta
    from typing import Sequence, assert_never

    from frequenz.sdk.actor import Actor
    from frequenz.sdk.config import ConfigManager, wait_for_first

    _logger = logging.getLogger(__name__)

    class MyActor(Actor): # (1)!
        def __init__(
            self, config_manager: ConfigManager, /, *, config_key: str | Sequence[str]
        ) -> None:
            super().__init__()
            self._config_manager = config_manager
            self._config_key = config_key
        async def _run(self) -> None: ...

    @dataclasses.dataclass(frozen=True, kw_only=True)
    class AppConfig:
        enable_actor: bool = dataclasses.field(
            metadata={"metadata": {"description": "Whether to enable the actor"}},
        )

    class App:
        def __init__(self, *, config_paths: Sequence[pathlib.Path]):
            self._config_manager = ConfigManager(config_paths)
            self._config_receiver = self._config_manager.new_receiver("app", AppConfig)
            self._actor = MyActor(self._config_manager, config_key="actor")

        async def _update_config(self, config_update: AppConfig | Exception | None) -> None:
            match config_update:
                case AppConfig() as config:
                    _logger.info("New configuration received, updating.")
                    await self._reconfigure(config)
                case None:
                    _logger.info("Configuration was unset, keeping the old configuration.")
                case Exception():
                    _logger.info("New configuration has errors, keeping the old configuration.")
                case unexpected:
                    assert_never(unexpected)

        async def _reconfigure(self, config: AppConfig) -> None:
            if config.enable_actor:
                self._actor.start()
            else:
                await self._actor.stop()

        async def run(self) -> None:
            _logger.info("Starting App...")

            async with self._config_manager:
                await self._update_config(
                    await wait_for_first(self._config_receiver, receiver_name="app")
                )

                _logger.info("Waiting for configuration updates...")
                async for config_update in self._config_receiver:
                    await self._reconfigure(config_update)

    if __name__ == "__main__":
        asyncio.run(App(config_paths="config.toml").run())
    ```

    1. Look for the actor examples for a proper implementation of the actor.

    Example configuration file:

    ```toml title="config.toml"
    [app]
    enable_actor = true

    [actor]
    some_config = 10

    [logging.root_logger]
    level = "DEBUG"
    ```
"""

from ._base_schema import BaseConfigSchema
from ._logging_actor import (
    LoggerConfig,
    LoggingConfig,
    LoggingConfigUpdatingActor,
    RootLoggerConfig,
)
from ._manager import ConfigManager, InvalidValueForKeyError, wait_for_first
from ._managing_actor import ConfigManagingActor
from ._util import load_config

__all__ = [
    "BaseConfigSchema",
    "ConfigManager",
    "ConfigManagingActor",
    "InvalidValueForKeyError",
    "LoggerConfig",
    "LoggingConfig",
    "LoggingConfigUpdatingActor",
    "RootLoggerConfig",
    "load_config",
    "wait_for_first",
]
