# Frequenz Python SDK Release Notes

## Upgrading

- The `frequenz-client-microgrid` dependency was bumped to v0.4.0. If you are using the client directly in your code, you will need to upgrade too.

- Calls to `microgrid.*_pool` methods now always need to specified a priority value, corresponding to the requirements/priority of the actor making the call.

- The `microgrid.*_pool` methods would only accept keyword arguments from now on.

- The `microgrid.initialize()` method now takes a `server_url` instead of a `host` and `port`.

   The following format is expected: `grpc://hostname{:port}{?ssl=ssl}`, where the port should be an int between `0` and `65535` (defaulting to `9090`) and `ssl` should be a boolean (defaulting to `false`). For example: `grpc://localhost` or `grpc://localhost:1090?ssl=true`.

   The default was also removed, so you always need to specify the server URL.

   This applies to the `ConnectionManager` as well, which also now doesn't expose the `host` and `port` attributes, only the `server_url`. If you need to extract the host or port from the `server_url`, you can use the standard Python `urllib.parse.urlparse()` function.

- The `Config` class was removed and the `ConfigManagingActor` now sends a plain dictionary rather than a `Config` object.

## New Features

- Calls to `microgrid.*_pool` methods now accept an optional `set_operating_point` parameter, for setting an operating point for the other actors.  This would shift the target power by the operating point before actually applying it to the components.

## Bug Fixes

- When the PowerDistributor receives a zero power request for PV inverters, it now correctly sets zero power to the inverters, and no longer crashes.
