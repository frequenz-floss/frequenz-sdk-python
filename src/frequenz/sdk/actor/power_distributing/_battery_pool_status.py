# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH
"""Class that stores pool of batteries and manage them."""

import asyncio
import logging
from typing import Set

from ...microgrid._battery import BatteryStatus, StatusTracker
from .result import PartialFailure, Result, Success

_logger = logging.getLogger(__name__)


class BatteryPoolStatus:
    """Holds pool of batteries and returns data from them."""

    def __init__(
        self,
        battery_ids: Set[int],
        max_data_age_sec: float,
        max_blocking_duration_sec: float,
    ) -> None:
        """Create partially initialized object instance.

        Note:
            Please call `async_init` method to fully initialize BatteryPoolStatus. Otherwise
            it is not possible to use BatteryPoolStatus.

        Args:
            battery_ids: set of batteries ids that should be stored in pool.
            max_data_age_sec: If component stopped sending data, then
                this is the maximum time when its last message should be considered as
                valid. After that time, component won't be used until it starts sending
                data.
            max_blocking_duration_sec: This value tell what should be the maximum
                timeout used for blocking failing component.
        """
        self._batteries = {
            id: StatusTracker(id, max_data_age_sec, max_blocking_duration_sec)
            for id in battery_ids
        }
        self._init_method_called: bool = False

    async def async_init(self) -> None:
        """Init battery pool."""
        await asyncio.gather(
            *[bat.async_init() for bat in self._batteries.values()],
            return_exceptions=True,
        )
        self._init_method_called = True

    def get_working_batteries(self, battery_ids: Set[int]) -> Set[int]:
        """Get subset of battery_ids with working batteries.

        Args:
            battery_ids: batteries ids

        Raises:
            RuntimeError: If `async_init` method was not called at the beginning to
                initialized object.
            KeyError: If any battery in the given batteries is not in the pool.

        Returns:
            Subset of given batteries with working batteries.
        """
        if not self._init_method_called:
            raise RuntimeError(
                "`async_init` method not called or not awaited. Run it before first use"
            )

        working: Set[int] = set()
        uncertain: Set[int] = set()
        for bat_id in battery_ids:
            if bat_id not in battery_ids:
                ids = str(self._batteries.keys())
                raise KeyError(f"No battery {bat_id} in pool. All batteries: {ids}")
            battery_status = self._batteries[bat_id].get_status()
            if battery_status == BatteryStatus.WORKING:
                working.add(bat_id)
            elif battery_status == BatteryStatus.UNCERTAIN:
                uncertain.add(bat_id)

        if len(working) > 0:
            return working

        _logger.warning(
            "There are no working batteries in %s. Falling back to using uncertain batteries %s.",
            str(battery_ids),
            str(uncertain),
        )
        return uncertain

    def update_last_request_status(self, result: Result):
        """Update batteries in pool based on the last result from the request.

        Args:
            result: Summary of what batteries failed and succeed in last request.

        Raises:
            RuntimeError: If `async_init` method was not called at the beginning to
                initialize object.
        """
        if not self._init_method_called:
            raise RuntimeError(
                "`async_init` method not called or not awaited. Run it before first use"
            )

        if isinstance(result, Success):
            for bat_id in result.used_batteries:
                self._batteries[bat_id].unblock()

        elif isinstance(result, PartialFailure):
            for bat_id in result.failed_batteries:
                duration = self._batteries[bat_id].block()
                if duration > 0:
                    _logger.warning(
                        "Battery %d failed last response. Block it for %f sec",
                        bat_id,
                        duration,
                    )

            for bat_id in result.succeed_batteries:
                self._batteries[bat_id].unblock()
