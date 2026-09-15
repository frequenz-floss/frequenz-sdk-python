# Frequenz Python SDK Release Notes

## Upgrading

- Custom validators passed through a configuration dataclass field's `metadata` (as used by `load_config()` and `ConfigManager.new_receiver()`) must now raise a `marshmallow.ValidationError` to reject a value. `marshmallow` 4 removed support for validators that signal failure by returning `False`, so such validators are now ignored and the invalid value is accepted silently. This affects you as soon as `marshmallow` 4 is installed, which `frequenz-quantities` 1.0.2 and later require.

## New Features

* Added `SteamBoilerPool` for monitoring and controlling pools of steam boilers, available via `microgrid.new_steam_boiler_pool()`.

## Bug Fixes

* `COALESCE()` no longer unsubscribes from the parameter that is producing values when the parameter it was tracking evaluates to nothing at all.  This could only happen when a parameter is an expression that has no value yet, like a nested function call, in which case the samples of the producing parameter were counted against the tracked one.
