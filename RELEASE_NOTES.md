# Frequenz Python SDK Release Notes

## Summary

This release replaces the `@actor` decorator with a new `Actor` class.

## Upgrading


- The `frequenz.sdk.power` package contained the power distribution algorithm, which is for internal use in the sdk, and is no longer part of the public API.

- `PowerDistributingActor`'s result type `OutOfBound` has been renamed to `OutOfBounds`, and its member variable `bound` has been renamed to `bounds`.

- The `@actor` decorator was replaced by the new `Actor` class. The main differences between the new class and the old decorator are:

  * It doesn't start automatically, `start()` needs to be called to start an actor (using the `frequenz.sdk.actor.run()` function is recommended).
  * The method to implement the main logic was renamed from `run()` to `_run()`, as it is not intended to be run externally.
  * Actors can have an optional `name` (useful for debugging/logging purposes).
  * The actor will only be restarted if an unhandled `Exception` is raised by `_run()`. It will not be restarted if the `_run()` method finishes normally. If an unhandled `BaseException` is raised instead, it will be re-raised. For normal cancellation the `_run()` method should handle `asyncio.CancelledError` if the cancellation shouldn't be propagated (this is the same as with the decorator).
  * The `_stop()` method is public (`stop()`) and will `cancel()` and `await` for the task to finish, catching the `asyncio.CancelledError`.
  * The `join()` method is renamed to `wait()`, but they can also be awaited directly ( `await actor`).
  * For deterministic cleanup, actors can now be used as `async` context managers.

  Most actors can be migrated following these steps:

  1. Remove the decorator
  2. Add `Actor` as a base class
  3. Rename `run()` to `_run()`
  4. Forward the `name` argument (optional but recommended)

  For example, this old actor:

  ```python
  from frequenz.sdk.actor import actor

  @actor
  class TheActor:
      def __init__(self, actor_args) -> None:
          # init code

      def run(self) -> None:
          # run code
  ```

  Can be migrated as:

  ```python
  import asyncio
  from frequenz.sdk.actor import Actor

  class TheActor(Actor):
      def __init__(self, actor_args,
          *,
          name: str | None = None,
      ) -> None:
          super().__init__(name=name)
          # init code

      def _run(self) -> None:
          # run code
  ```

  Then you can instantiate all your actors first and then run them using:

  ```python
  from frequenz.sdk.actor import run
  # Init code
  actor = TheActor()
  other_actor = OtherActor()
  # more setup
  await run(actor, other_actor)  # Start and await for all the actors
  ```

- The `MovingWindow` is now a `BackgroundService`, so it needs to be started manually with `await window.start()`. It is recommended to use it as an `async` context manager if possible though:

    ```python
    async with MovingWindow(...) as window:
        # The moving windows is started here
        use(window)
    # The moving window is stopped here
    ```

- The base actors (`ConfigManagingActor`, `ComponentMetricsResamplingActor`, `DataSourcingActor`, `PowerDistributingActor`) now inherit from the new `Actor` class, if you are using them directly, you need to start them manually with `await actor.start()` and you might need to do some other adjustments.

- The `BatteryPool.power_distribution_results` method has been enhanced to provide power distribution results in the form of `Power` objects, replacing the previous use of `float` values.

- In the `Request` class:
  * The attribute `request_timeout_sec` has been updated and is now named `request_timeout` and it is represented by a `timedelta` object rather than a `float`.
  * The attribute `power` is now presented as a `Power` object, as opposed to a `float`.

- Within the `EVChargerPool.set_bounds` method, the parameter `max_amps` has been redefined as `max_current`, and it is now represented using a `Current` object instead of a `float`.

## New Features

- DFS for compentent graph

- `BackgroundService`: This new abstract base class can be used to write other classes that runs one or more tasks in the background. It provides a consistent API to start and stop these services and also takes care of the handling of the background tasks. It can also work as an `async` context manager, giving the service a deterministic lifetime and guaranteed cleanup.

  All classes spawning tasks that are expected to run for an indeterminate amount of time are likely good candidates to use this as a base class.

- `Actor`: This new class inherits from `BackgroundService` and it replaces the `@actor` decorator.

## Bug Fixes

- Fixes a bug in the ring buffer updating the end timestamp of gaps when they are outdated.

- Properly handles PV configurations with no or only some meters before the PV component.

  So far we only had configurations like this: `Meter -> Inverter -> PV`. However the scenario with `Inverter -> PV` is also possible and now handled correctly.

- Fix `consumer_power()` not working certain configurations.

  In microgrids without consumers and no main meter, the formula would never return any values.

- Fix `pv_power` not working in setups with 2 grid meters by using a new reliable function to search for components in the components graph

- Fix `consumer_power` similar to `pv_power`

- Zero value requests received by the `PowerDistributingActor` will now always be accepted, even when there are non-zero exclusion bounds.
