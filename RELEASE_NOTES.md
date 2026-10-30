# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->

* `new_battery_pool(component_ids=set())` uses all batteries again, like before v1.0.0-rc2206.
* `new_pv_pool()`, `new_ev_charger_pool()` and `new_steam_boiler_pool()` now also treat an empty `component_ids` set as "all components". Before, it gave a pool with no components, unless a pool for all components of that type had already been created (both pools then shared one cache entry).
* Because of that shared cache entry, the result used to depend on which pool was created first. Now `None` and `set()` always give the same pool.
* A pool created without component IDs now shares its state with a pool created for the explicit IDs of all the components of its type, like the one the power manager creates to track the pool's power bounds. Before, each of them tracked the power bounds of the same components on its own.
* `new_battery_pool()`, `new_pv_pool()`, `new_ev_charger_pool()` and `new_steam_boiler_pool()` now always log the warning that a pool was already created when the same pool is created twice at the same priority. The warning now names the pool's components.
  - Before, the warning was missed when a pool was created without IDs and again for all the IDs of its type, and sometimes for two equal sets of IDs that were built in different ways.
  - Creating pools of different types without IDs at the same priority also no longer logs the warning by mistake.
* Power requests to batteries no longer fail while a battery that isn't working shares an inverter with a working battery. The power distributor counted that battery as unreachable, and the component graph can't make a power formula for only some of the batteries behind an inverter, so no result was sent for the request.
* The battery pool's power bounds now include the power measured on the batteries that aren't working. Those batteries can still charge or discharge. The power distributor subtracts their measured power from the target power, so leaving it out of the bounds took it off the target twice.
  - For example, take 4 batteries that can each discharge up to 1000 W, with one of them not working but still discharging at -1000 W. A -4000 W request used to give the other three batteries -667 W each, instead of -1000 W.
  - When batteries stop working, `system_power_bounds` and the bounds in `power_status` now shift by the batteries' measured power, instead of only leaving those batteries' bounds out. A battery that shares an inverter with a working battery still counts as working. The exclusion bounds are not shifted.
  - The bounds measure that power with the same component graph formula as the power distributor, so both use the same meter fallback when an inverter stops sending data.
* The PV pool and the steam boiler pool now also include the power measured on their components that aren't working in their power bounds, for the same reason.
