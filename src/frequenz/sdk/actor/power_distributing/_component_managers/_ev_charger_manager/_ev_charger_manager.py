# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Manage _ev chargers for the power distributor."""

import collections.abc
import logging
from datetime import datetime, timedelta, timezone

from frequenz.channels import Sender
from frequenz.channels.util import Merge
from typing_extensions import override

from frequenz.sdk import microgrid

from ....._internal._channels import LatestValueCache
from ....._internal._math import is_close_to_zero
from .....microgrid.component import ComponentCategory, EVChargerData
from .....timeseries import Power, Sample3Phase, Voltage
from ..._component_pool_status_tracker import ComponentPoolStatusTracker

# from .._component_pool_status_tracker import ComponentPoolStatusTracker
from ..._component_status import ComponentPoolStatus, EVChargerStatusTracker
from ...request import Request
from ...result import Result, Success
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
    ):
        """Initialize the ev charger data manager.

        Args:
            component_pool_status_sender: Channel for sending information about which
                components are expected to be working.
        """
        self._ev_charger_ids = self._get_ev_charger_ids()
        self._evc_states = EvcStates()
        self._voltage_cache: LatestValueCache[Sample3Phase[Voltage]] = LatestValueCache(
            microgrid.voltage().new_receiver()
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

    @override
    def component_ids(self) -> collections.abc.Set[int]:
        """Return the set of ev charger ids."""
        return self._ev_charger_ids

    @override
    async def start(self) -> None:
        """Start the ev charger data manager."""

    @override
    async def distribute_power(self, request: Request) -> Result:
        """Distribute the requested power to the ev chargers.

        Args:
            request: Request to get the distribution for.

        Returns:
            Result of the distribution.
        """
        self._target_power = request.power
        used_power = self._evc_states.get_ev_total_used_power()
        if self._target_power < used_power:
            self._throttle_ev_chargers(used_power - self._target_power)
        return Success(
            request,
            Power.zero(),
            set(),
            Power.zero(),
        )

    @override
    async def stop(self) -> None:
        """Stop the ev charger data manager."""

    def _get_ev_charger_ids(self) -> collections.abc.Set[int]:
        return {
            evc.component_id
            for evc in microgrid.connection_manager.get().component_graph.components(
                component_categories={ComponentCategory.EV_CHARGER}
            )
        }

    def _allocate_new_ev(self, component_id: int) -> list[tuple[int, Power]]:
        available_power = (
            self._target_power - self._evc_states.get_total_allocated_power()
        )
        voltage = self._voltage_cache.get().min()
        if voltage is None:
            _logger.warning(
                "Voltage data is not available. Cannot allocate power to EV charger %s",
                component_id,
            )
            return []
        initial_power = voltage * self._config.initial_current
        if available_power > initial_power:
            return [(component_id, initial_power)]

        min_power = voltage * self._config.min_current
        if available_power > min_power:
            return [(component_id, min_power)]

        return []

    def _act_on_new_data(self, ev_data: EVChargerData) -> list[tuple[int, Power]]:
        component_id = ev_data.component_id
        ev_connected = ev_data.is_ev_connected()
        ev_previously_connected = self._evc_states.get(
            component_id
        ).last_data.is_ev_connected()

        # if EV is just connected, try to set config.initial_current, throttle other
        # EVs if necessary
        ev_newly_connected = ev_connected and not ev_previously_connected
        if ev_newly_connected:
            self._evc_states.get(component_id).update_state(ev_data)
            _logger.info("New EV connected to EV charger %s", component_id)
            return self._allocate_new_ev(component_id)

        # if EV is disconnected, set limit to 0.0.  redistribution to other EVs will
        # happen separately, when possible.
        if not ev_connected:
            if ev_previously_connected:
                _logger.info("EV disconnected from EV charger %s", component_id)
            self._evc_states.get(component_id).update_state(ev_data)
            return [(component_id, Power.zero())]

        # else if last throttling was less than 'increase_power_interval', do nothing.
        now = datetime.utcnow()
        last_throttling_time = self._evc_states.get(component_id).last_reallocation_time
        if last_throttling_time is not None:
            dur = now - last_throttling_time
            if dur < self._config.increase_power_interval:
                return []

        self._evc_states.get(ev_data.component_id).update_state(ev_data)

        # if ev's bounds were previously set to zero, treat it like it is newly
        # connected
        evc = self._evc_states.get(component_id)
        if is_close_to_zero(evc.last_allocation.as_watts()):
            return self._allocate_new_ev(component_id)

        # if the ev charger is already allocated the max power, do nothing
        allottable_power = Power.from_watts(
            evc.last_data.active_power_inclusion_upper_bound
            - evc.last_allocation.as_watts()
        )
        if (
            is_close_to_zero(allottable_power.as_watts())
            or allottable_power < Power.zero()
        ):
            return []

        available_power = (
            self._target_power - self._evc_states.get_total_allocated_power()
        )
        allottable_power = min(allottable_power, available_power)

        target_power = min(
            evc.last_allocation + allottable_power,
            Power.from_watts(evc.last_data.active_power_inclusion_upper_bound),
        )
        evc.update_last_allocation(target_power, now)
        _logger.info(
            "Increasing power to EV charger %s from %s to %s",
            component_id,
            evc.last_allocation,
            target_power,
        )
        return [(component_id, target_power)]

    async def _run(self) -> None:
        api = microgrid.connection_manager.get().api_client
        ev_charger_data_rx = Merge(
            *[await api.ev_charger_data(evc_id) for evc_id in self._ev_charger_ids]
        )
        async for evc_data in ev_charger_data_rx:
            bounds_changes = []
            # If a new ev charger is added, add it to the state tracker, with
            # now as the last reallocation time and last charging time.
            #
            # This means it won't be assigned any power until the reallocation duration
            # has passed.
            if evc_data.component_id not in self._evc_states:
                now = datetime.now(tz=timezone.utc)
                self._evc_states.add_evc(
                    EvcState(
                        component_id=evc_data.component_id,
                        last_data=evc_data,
                        power=Power.from_watts(evc_data.active_power),
                        last_allocation=Power.zero(),
                        last_reallocation_time=now,
                        last_charging_time=now,
                    )
                )

            # See if the ev charger has room for more power, and if the last allocation
            # was not in the last reallocation duration.
            else:
                bounds_changes = self._act_on_new_data(evc_data)

            for component_id, power in bounds_changes:
                await api.set_power(component_id, power.as_watts())

    def _throttle_ev_chargers(self, throttle_by: Power) -> list[tuple[int, Power]]:
        """Reduce EV charging power to meet the target power.

        Level 1 throttling is done by reducing the power to the minimum current required
        to charge the EV.  When the consumption is still above the target power, level 2
        throttling is done by reducing the power to 0.

        Args:
            throttle_by: The amount of power to reduce the total EV charging power by.

        Returns:
            A list of new (reduced) charging current limits for a subset of ev
                chargers, required to bring down the consumption by the given value.
        """
        if throttle_by <= Power.zero():
            return []

        min_power = Power.zero()
        voltage = self._voltage_cache.get().min()
        if voltage is None:
            _logger.warning(
                "Voltage data is not available. Cannot perform level 1 throttling.",
            )
        else:
            min_power = voltage * self._config.min_current

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
            throttling_bounds = [
                (evc.component_id, min_power)
                for evc in evc_list[:level1_throttling_count]
            ]
        else:
            throttling_bounds = [
                (evc.component_id, Power.zero())
                for evc in evc_list[:level2_throttling_count]
            ]
        _logger.debug("Throttling: %s", throttling_bounds)
        return throttling_bounds
