# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Utility for component managers."""

import asyncio
import logging
from datetime import datetime, timedelta

from frequenz.client.base.exception import ApiClientError
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.quantities import Power

from ... import connection_manager
from ..request import Request
from ..result import PartialFailure, Result, Success

_logger = logging.getLogger(__name__)


async def _set_component_power(  # pylint: disable=too-many-locals,too-many-arguments
    *,
    request: Request,
    target_power: Power,
    allocations: dict[ComponentId, Power],
    api_request_timeout: timedelta,
    remaining_power: Power,
    component_category: str,
) -> Result:
    """Send the component power changes to the microgrid API.

    Args:
        request: Set-power request sent to the `PowerDistributingActor`.
        target_power: The requested power.
        allocations: A dictionary containing the new power allocations for
            each component.
        api_request_timeout: The timeout for the API request.
        remaining_power: Any excess (remaining) power.
        component_category: Component category name, for display purposes.

    Returns:
        Power distribution result, corresponding to the result of the API
            request.
    """
    api_client = connection_manager.get().api_client
    tasks: dict[ComponentId, asyncio.Task[datetime | None]] = {}
    for component_id, power in allocations.items():
        tasks[component_id] = asyncio.create_task(
            api_client.set_component_power_active(component_id, power.as_watts())
        )
    _, pending = await asyncio.wait(
        tasks.values(),
        timeout=api_request_timeout.total_seconds(),
        return_when=asyncio.ALL_COMPLETED,
    )
    # collect the timed out tasks and cancel them while keeping the
    # exceptions, so that they can be processed later.
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)

    failed_components: set[ComponentId] = set()
    succeeded_components: set[ComponentId] = set()
    failed_power = Power.zero()
    for component_id, task in tasks.items():
        try:
            task.result()
        except asyncio.CancelledError:
            _logger.warning(
                "Timeout while setting power to %s %s",
                component_category,
                component_id,
            )
        except ApiClientError as exc:
            _logger.warning(
                "Got a client error while setting power to %s %s: %s",
                component_category,
                component_id,
                exc,
            )
        except Exception:  # pylint: disable=broad-except
            _logger.exception(
                "Unknown error while setting power to %s: %s",
                component_category,
                component_id,
            )
        else:
            succeeded_components.add(component_id)
            continue

        failed_components.add(component_id)
        failed_power += allocations[component_id]

    if failed_components:
        return PartialFailure(
            failed_components=failed_components,
            succeeded_components=succeeded_components,
            failed_power=failed_power,
            succeeded_power=target_power - failed_power - remaining_power,
            excess_power=remaining_power,
            request=request,
        )
    return Success(
        succeeded_components=succeeded_components,
        succeeded_power=target_power - remaining_power,
        excess_power=remaining_power,
        request=request,
    )
