# Frequenz Python SDK Release Notes

## Summary


## Upgrading

- `microgrid.grid()`

  * Similar to `microgrid.battery_pool()`, the Grid is now similarily accessed.

- `BatteryPool`'s control methods

  * They no longer have a `include_broken_batteries` parameter.  The feature has been removed.

- Move `microgrid.ComponentGraph` class to `microgrid.component_graph.ComponentGraph`, exposing only the high level interface functions through the `microgrid` package.

- An actor that is crashing will no longer instantly restart but induce an artificial delay to avoid potential spam-restarting.

## New Features


- Allow configuration of the `resend_latest` flag in channels owned by the `ChannelRegistry`.

- Add consumption and production operators that will replace the logical meters production and consumption function variants.

- Consumption and production power formulas have been removed.

- The documentation was improved to:

  * Show signatures with types.
  * Show the inherited members.
  * Documentation for pre-releases are now published.
  * Show the full tag name as the documentation version.
  * All development branches now have their documentation published (there is no `next` version anymore).
  * Fix the order of the documentation versions.

- The `ConnectionManager` fetches microgrid metadata when connecting to the microgrid and exposes `microgrid_id` and `location` properties of the connected microgrid.

## Bug Fixes

- Fix incorrect grid current calculations in locations where the calculations depended on current measurements from an inverter.
- Fix power failure report to exclude any failed power from the succeeded power.
