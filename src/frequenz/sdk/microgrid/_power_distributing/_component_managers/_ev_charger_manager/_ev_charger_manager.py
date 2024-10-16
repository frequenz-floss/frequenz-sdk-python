# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Manage EV chargers for the power distributor."""

import asyncio
import collections.abc
import logging
from datetime import datetime, timedelta, timezone

from frequenz.channels import (
    Broadcast,
    LatestValueCache,
    Sender,
    merge,
    select,
    selected_from,
)
from frequenz.client.microgrid import (
    ApiClient,
    ApiClientError,
    ComponentCategory,
    EVChargerData,
)
from frequenz.quantities import Power, Voltage
from typing_extensions import override

from ....._internal._asyncio import run_forever
from ....._internal._math import is_close_to_zero
from .....timeseries import Sample3Phase
from .... import _data_pipeline, connection_manager
from ..._component_pool_status_tracker import ComponentPoolStatusTracker
from ..._component_status import ComponentPoolStatus, EVChargerStatusTracker
from ...request import Request
from ...result import PartialFailure, Result, Success
from .._component_manager import ComponentManager
from ._config import EVDistributionConfig
from ._states import EvcState, EvcStates

_logger = logging.getLogger(__name__)


class EVChargerManager(ComponentManager):
    """Manage ev chargers for the power distributor."""

    @override
    def __init__(
        self,
        component_pool_status_sender: Sender[ComponentPoolStatus],
        results_sender: Sender[Result],
        api_power_request_timeout: timedelta,
    ):
        """Initialize the ev charger data manager.

        Args:
            component_pool_status_sender: Channel for sending information about which
                components are expected to be working.
            results_sender: Channel for sending results of power distribution.
            api_power_request_timeout: Timeout to use when making power requests to
                the microgrid API.
        """
        self._results_sender = results_sender
        self._api_power_request_timeout = api_power_request_timeout
        self._ev_charger_ids = self._get_ev_charger_ids()
        self._evc_states = EvcStates()
        self._voltage_cache: LatestValueCache[Sample3Phase[Voltage]] = LatestValueCache(
            _data_pipeline.voltage_per_phase().new_receiver(),
            unique_id=f"{type(self).__name__}«{hex(id(self))}»:voltage_cache",
        )
        self._config = EVDistributionConfig(component_ids=self._ev_charger_ids)
        self._component_pool_status_tracker = ComponentPoolStatusTracker(
            component_ids=self._ev_charger_ids,
            component_status_sender=component_pool_status_sender,
            max_data_age=timedelta(seconds=10.0),
            max_blocking_duration=timedelta(seconds=30.0),
            component_status_tracker_type=EVChargerStatusTracker,
        )
        self._target_power = Power.zero()
        self._target_power_channel = Broadcast[Request](name="target_power")
        self._target_power_tx = self._target_power_channel.new_sender()
        self._task: asyncio.Task[None] | None = None
        self._latest_request: Request = Request(Power.zero(), set())

    @override
    def component_ids(self) -> collections.abc.Set[int]:
        """Return the set of ev charger ids."""
        return self._ev_charger_ids

    @override
    async def start(self) -> None:
        """Start the ev charger data manager."""
        # Need to start a task only if there are EV chargers in the component graph.
        if self._ev_charger_ids:
            self._task = asyncio.create_task(run_forever(self._run))

    @override
    async def distribute_power(self, request: Request) -> None:
        """Distribute the requested power to the ev chargers.

        Args:
            request: Request to get the distribution for.
        """
        if self._ev_charger_ids:
            await self._target_power_tx.send(request)

    @override
    async def stop(self) -> None:
        """Stop the ev charger manager."""
        await self._voltage_cache.stop()
        await self._component_pool_status_tracker.stop()

    def _get_ev_charger_ids(self) -> collections.abc.Set[int]:
        """Return the IDs of all EV chargers present in the component graph."""
        return {
            evc.component_id
            for evc in connection_manager.get().component_graph.components(
                component_categories={ComponentCategory.EV_CHARGER}
            )
        }

    def _allocate_new_ev(self, component_id: int) -> dict[int, Power]:
        """Allocate power to a newly connected EV charger.

        Args:
            component_id: ID of the EV charger to allocate power to.

        Returns:
            A dictionary containing updated power allocations for the EV chargers.
        """
        available_power = (
            self._target_power - self._evc_states.get_total_allocated_power()
        )
        voltage = self._voltage_cache.get().min()
        if voltage is None:
            _logger.warning(
                "Voltage data is not available. Cannot allocate power to EV charger %s",
                component_id,
            )
            return {}
        initial_power = voltage * self._config.initial_current * 3.0
        if available_power > initial_power:
            return {component_id: initial_power}

        min_power = voltage * self._config.min_current * 3.0
        if available_power > min_power:
            return {component_id: min_power}

        return {}

    def _act_on_new_data(self, ev_data: EVChargerData) -> dict[int, Power]:
        """Act on new data from an EV charger.

        Args:
            ev_data: New data from the EV charger.

        Returns:
            A dictionary containing updated power allocations for the EV chargers.
        """
        component_id = ev_data.component_id
        ev_connected = ev_data.is_ev_connected()
        ev_previously_connected = self._evc_states.get(
            component_id
        ).last_data.is_ev_connected()

        # if EV is just connected, try to set config.initial_current, throttle other
        # EVs if necessary
        ev_newly_connected = ev_connected and not ev_previously_connected
        if ev_newly_connected:
            _logger.info("New EV connected to EV charger %s", component_id)
            return self._allocate_new_ev(component_id)

        # if EV is disconnected, set limit to 0.0.  redistribution to other EVs will
        # happen separately, when possible.
        if not ev_connected:
            if ev_previously_connected:
                _logger.info("EV disconnected from EV charger %s", component_id)
            if self._evc_states.get(component_id).last_allocation > Power.zero():
                return {component_id: Power.zero()}

        # else if last throttling was less than 'increase_power_interval', do nothing.
        now = datetime.now(tz=timezone.utc)
        last_throttling_time = self._evc_states.get(component_id).last_reallocation_time
        if last_throttling_time is not None:
            dur = now - last_throttling_time
            if dur < self._config.increase_power_interval:
                return {}

        # if ev's target power was previously set to zero, treat it like it is newly
        # connected
        evc = self._evc_states.get(component_id)
        if is_close_to_zero(evc.last_allocation.as_watts()):
            return self._allocate_new_ev(component_id)

        # if the ev charger is already allocated the max power, do nothing
        allottable_power = Power.from_watts(
            evc.last_data.active_power_inclusion_upper_bound
            - evc.last_allocation.as_watts()
        )
        available_power = (
            self._target_power - self._evc_states.get_total_allocated_power()
        )
        allottable_power = min(allottable_power, available_power)

        if (
            is_close_to_zero(allottable_power.as_watts())
            or allottable_power < Power.zero()
        ):
            return {}

        target_power = min(
            evc.last_allocation + allottable_power,
            Power.from_watts(evc.last_data.active_power_inclusion_upper_bound),
        )
        _logger.debug(
            "Increasing power to EV charger %s from %s to %s",
            component_id,
            evc.last_allocation,
            target_power,
        )
        return {component_id: target_power}

    async def _run(self) -> None:  # pylint: disable=too-many-locals
        """Run the main event loop of the EV charger manager."""
        api = connection_manager.get().api_client
        ev_charger_data_rx = merge(
            *[await api.ev_charger_data(evc_id) for evc_id in self._ev_charger_ids]
        )
        target_power_rx = self._target_power_channel.new_receiver()
        latest_target_powers: dict[int, Power] = {}
        async for selected in select(ev_charger_data_rx, target_power_rx):
            target_power_changes = {}
            now = datetime.now(tz=timezone.utc)

            if selected_from(selected, ev_charger_data_rx):
                evc_data = selected.message
                # If a new ev charger is added, add it to the state tracker, with
                # now as the last reallocation time and last charging time.
                #
                # This means it won't be assigned any power until the reallocation
                # duration has passed.
                if evc_data.component_id not in self._evc_states:
                    self._evc_states.add_evc(
                        EvcState(
                            component_id=evc_data.component_id,
                            last_data=evc_data,
                            power=Power.zero(),
                            last_allocation=Power.zero(),
                            last_reallocation_time=now,
                            last_charging_time=now,
                        )
                    )
                    target_power_changes = {evc_data.component_id: Power.zero()}

                # See if the ev charger has room for more power, and if the last
                # allocation was not in the last reallocation duration.
                else:
                    target_power_changes = self._act_on_new_data(evc_data)
                    self._evc_states.get(evc_data.component_id).update_state(evc_data)

            elif selected_from(selected, target_power_rx):
                self._latest_request = selected.message
                self._target_power = selected.message.power
                _logger.debug("New target power: %s", self._target_power)
                used_power = self._evc_states.get_ev_total_used_power()
                allocated_power = self._evc_states.get_total_allocated_power()
                if self._target_power < used_power:
                    diff_power = used_power - self._target_power
                    target_power_changes = self._throttle_ev_chargers(diff_power)
                    _logger.warning(
                        "Throttling EV chargers by %s-%s=%s: %s",
                        used_power,
                        self._target_power,
                        diff_power,
                        target_power_changes,
                    )
                elif self._target_power < allocated_power:
                    diff_power = allocated_power - self._target_power
                    target_power_changes = self._deallocate_unused_power(diff_power)

            if target_power_changes:
                _logger.debug("Setting power to EV chargers: %s", target_power_changes)
            else:
                continue
            for component_id, power in target_power_changes.items():
                self._evc_states.get(component_id).update_last_allocation(power, now)

            latest_target_powers.update(target_power_changes)
            result = await self._set_api_power(
                api, target_power_changes, self._api_power_request_timeout
            )
            await self._results_sender.send(result)

    async def _set_api_power(
        self,
        api: ApiClient,
        target_power_changes: dict[int, Power],
        api_request_timeout: timedelta,
    ) -> Result:
        """Send the EV charger power changes to the microgrid API.

        Args:
            api: The microgrid API client to use for setting the power.
            target_power_changes: A dictionary containing the new power allocations for
                the EV chargers.
            api_request_timeout: The timeout for the API request.

        Returns:
            Power distribution result, corresponding to the result of the API
                request.
        """
        tasks: dict[int, asyncio.Task[None]] = {}
        for component_id, power in target_power_changes.items():
            tasks[component_id] = asyncio.create_task(
                api.set_power(component_id, power.as_watts())
            )
        _, pending = await asyncio.wait(
            tasks.values(),
            timeout=api_request_timeout.total_seconds(),
            return_when=asyncio.ALL_COMPLETED,
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

        failed_components: set[int] = set()
        succeeded_components: set[int] = set()
        failed_power = Power.zero()
        for component_id, task in tasks.items():
            try:
                task.result()
            except asyncio.CancelledError:
                _logger.warning(
                    "Timeout while setting power to EV charger %s", component_id
                )
            except ApiClientError as exc:
                _logger.warning(
                    "Got a client error while setting power to EV charger %s: %s",
                    component_id,
                    exc,
                )
            except Exception:  # pylint: disable=broad-except
                _logger.exception(
                    "Unknown error while setting power to EV charger: %s", component_id
                )
            else:
                succeeded_components.add(component_id)
                continue

            failed_components.add(component_id)
            failed_power += target_power_changes[component_id]

        if failed_components:
            return PartialFailure(
                failed_components=failed_components,
                succeeded_components=succeeded_components,
                failed_power=failed_power,
                succeeded_power=self._target_power - failed_power,
                excess_power=Power.zero(),
                request=self._latest_request,
            )
        return Success(
            succeeded_components=succeeded_components,
            succeeded_power=self._target_power,
            excess_power=Power.zero(),
            request=self._latest_request,
        )

    def _deallocate_unused_power(self, to_deallocate: Power) -> dict[int, Power]:
        """Reduce the power allocated to the EV chargers to meet the target power.

        This prioritizes reducing power to EV chargers that aren't consuming the
        allocated power.

        Args:
            to_deallocate: The amount of power to reduce the total allocated power by.

        Returns:
            A list of new (reduced) charging current limits for a subset of ev
                chargers, required to bring down the consumption by the given value.
        """
        voltage = self._voltage_cache.get().min()
        if voltage is None:
            _logger.warning(
                "Voltage data is unavailable. Can't deallocate power from EV chargers",
            )
            return {}
        min_power = voltage * self._config.min_current * 3.0

        evc_list = list(self._evc_states.values())
        evc_list.sort(key=lambda st: st.last_allocation - st.power, reverse=True)

        deallocated_power = Power.zero()
        target_power_changes = {}

        for evc in evc_list:
            if deallocated_power >= to_deallocate:
                break
            ev_to_deallocate = evc.last_allocation - evc.power
            if ev_to_deallocate <= Power.zero():
                continue
            ev_to_deallocate = min(ev_to_deallocate, to_deallocate - deallocated_power)
            tgt_power = evc.last_allocation - ev_to_deallocate
            if tgt_power < min_power:
                tgt_power = Power.zero()
            deallocated_power += evc.last_allocation - tgt_power
            target_power_changes[evc.component_id] = tgt_power
        return target_power_changes

    def _throttle_ev_chargers(  # pylint: disable=too-many-locals
        self,
        throttle_by: Power,
    ) -> dict[int, Power]:
        """Reduce EV charging power to meet the target power.

        This targets EV chargers that are currently consuming the most.

        Level 1 throttling is done by reducing the power to the minimum current required
        to charge the EV.  When the consumption is still above the target power, level 2
        throttling is done by reducing the power to 0.

        Args:
            throttle_by: The amount of power to reduce the total EV charging power by.

        Returns:
            A list of new (reduced) charging current limits for a subset of ev
                chargers, required to bring down the consumption by the given value.
        """
        min_power = Power.zero()
        voltage = self._voltage_cache.get().min()
        if voltage is None:
            _logger.warning(
                "Voltage data is not available. Cannot perform level 1 throttling.",
            )
        else:
            min_power = voltage * self._config.min_current * 3.0

        evc_list = list(self._evc_states.values())
        evc_list.sort(key=lambda st: (st.power, st.last_allocation), reverse=True)

        level1_throttling_count = 0
        level1_amps_achieved = Power.zero()

        level2_throttling_count = 0
        level2_amps_achieved = Power.zero()

        for evc in evc_list:
            evc_power = evc.power
            evc_level1_power = Power.zero()
            if evc_power > min_power:
                evc_level1_power = evc_power - min_power

            if evc_power == Power.zero():
                evc_power = evc.last_allocation

            if evc_power == Power.zero():
                break

            if level1_amps_achieved < throttle_by:
                level1_amps_achieved += evc_level1_power
                level1_throttling_count += 1
            else:
                break
            if level2_amps_achieved < throttle_by:
                level2_amps_achieved += evc_power
                level2_throttling_count += 1

        if level1_amps_achieved >= throttle_by:
            throttled_powers = {
                evc.component_id: min_power
                for evc in evc_list[:level1_throttling_count]
            }
        else:
            throttled_powers = {
                evc.component_id: Power.zero()
                for evc in evc_list[:level2_throttling_count]
            }
        _logger.debug("Throttling: %s", throttled_powers)
        return throttled_powers
