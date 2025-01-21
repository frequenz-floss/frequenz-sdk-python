# Frequenz Python SDK Release Notes

## Summary

This release includes a new `ConfigManager` class to simplify managing the configuration, and ships other improvements and fixes to the config system in general.

## Upgrading

- `frequenz.sdk.config`

    * `LoggingConfigUpdater`

        + Renamed to `LoggingConfigUpdatingActor` to follow the actor naming convention.
        + The actor must now be constructed using a `ConfigManager` instead of a receiver.
        + Make all arguments to the constructor keyword-only, except for the `config_manager` argument.
        + If the configuration is removed, the actor will now load back the default configuration.

    * `LoggingConfig`

        + The `load()` method was removed. Please use `frequenz.sdk.config.load_config()` instead.
        + The class is now a standard `dataclass` instead of a `marshmallow_dataclass`.
        + The class is now immutable.
        + The constructor now accepts only keyword arguments.

    * `LoggerConfig`

        + The class is now a standard `dataclass` instead of a `marshmallow_dataclass`.
        + The class is now immutable.
        + The constructor now accepts only keyword arguments.

    * `load_config()`:

         + The `base_schema` argument is now keyword-only and defaults to `BaseConfigSchema` (and because of this, it uses `unknown=EXCLUDE` by default).
         + The arguments forwarded to `marshmallow.Schema.load()` now must be passed explicitly via the `marshmallow_load_kwargs` argument, as a `dict`, to improve the type-checking.
         + Will now raise a `ValueError` if `unknown` is set to `INCLUDE` in `marshmallow_load_kwargs`.

    * `ConfigManagingActor`: Raise a `ValueError` if the `config_files` argument an empty sequence.

## New Features

- `frequenz.sdk.config`

    - Logging was improved in general.

    - Added documentation and user guide.

    - `LoggingConfigUpdatingActor`

        * Added a new `name` argument to the constructor to be able to override the actor's name.

    - `ConfigManager`: Added a class to simplify managing the configuration. It takes care of instantiating the config actors and provides a convenient method for creating receivers with a lot of common functionality.

    - `BaseConfigSchema`: Added a `marshmallow` base `Schema` that includes custom fields for `frequenz-quantities`. In the futute more commonly used fields might be added.

    - `wait_for_first()`: Added a function to make it easy to wait for the first configuration to be received with a timeout.

    - `ConfigManagingActor`: Allow passing a single configuration file.

## Bug Fixes

- Fix a bug in `BackgroundService` where it won't try to `self.cancel()` and `await self.wait()` if there are no internal tasks. This prevented to properly implement custom stop logic without having to redefine the `stop()` method too.

- Fix a bug where if a string was passed to the `ConfigManagingActor` it would be interpreted as a sequence of 1 character strings.

- Remove a confusing log message in the power distributing actor.

- Close all receivers owned by a *pool when stopping the pool.
