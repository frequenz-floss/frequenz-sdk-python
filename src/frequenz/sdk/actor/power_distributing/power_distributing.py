# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Actor to distribute power between components.

The purpose of this actor is to distribute power between components in a microgrid.

The actor receives power requests from the power manager, process them by
distributing the power between the components and sends the results back to it.
"""


import asyncio
import logging
from datetime import timedelta

from frequenz.channels import Receiver, Sender
from frequenz.client.microgrid import ComponentCategory, ComponentType, InverterType
from typing_extensions import override

from ...actor._actor import Actor
from ._component_managers import (
    BatteryManager,
    ComponentManager,
    EVChargerManager,
    PVManager,
)
from ._component_status import ComponentPoolStatus
from .request import Request
from .result import Result

_logger = logging.getLogger(__name__)


class PowerDistributingActor(Actor):
    # pylint: disable=too-many-instance-attributes
    """Actor to distribute the power between components in a microgrid.

    One instance of the actor can handle only one component category and type,
    which needs to be specified at actor startup and it will setup the correct
    component manager based on the given category and type.

    Only one power request is processed at a time to prevent from sending
    multiple requests for the same components to the microgrid API at the
    same time.

    Edge cases:
    * If a new power request is received while a power request with the same
    set of components is being processed, the new request will be added to
    the pending requests. Then the pending request will be processed after the
    request with the same set of components being processed is done. Only one
    pending request is kept for each set of components, the latest request will
    overwrite the previous one if there is any.

    * If there are 2 requests and their set of components is different but they
    overlap (they have at least one common component), then both requests will
    be processed concurrently. Though, the power manager will make sure this
    doesn't happen as overlapping component IDs are not possible at the moment.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        requests_receiver: Receiver[Request],
        results_sender: Sender[Result],
        component_pool_status_sender: Sender[ComponentPoolStatus],
        *,
        api_power_request_timeout: timedelta,
        component_category: ComponentCategory,
        component_type: ComponentType | None = None,
        name: str | None = None,
    ) -> None:
        """Create actor instance.

        Args:
            requests_receiver: Receiver for receiving power requests from the power
                manager.
            results_sender: Sender for sending results to the power manager.
            component_pool_status_sender: Channel for sending information about which
                components are expected to be working.
            api_power_request_timeout: Timeout to use when making power requests to
                the microgrid API.
            component_category: The category of the components that this actor is
                responsible for.
            component_type: The type of the component of the given category that this
                actor is responsible for.  This is used only when the component category
                is not enough to uniquely identify the component.  For example, when the
                category is `ComponentCategory.INVERTER`, the type is needed to identify
                the inverter as a solar inverter or a battery inverter.  This can be
                `None` when the component category is enough to uniquely identify the
                component.
            name: The name of the actor. If `None`, `str(id(self))` will be used. This
                is used mostly for debugging purposes.

        Raises:
            ValueError: If the given component category is not supported.
        """
        super().__init__(name=name)
        self._component_category = component_category
        self._component_type = component_type
        self._requests_receiver = requests_receiver
        self._result_sender = results_sender
        self._api_power_request_timeout = api_power_request_timeout

        self._processing_tasks: dict[frozenset[int], asyncio.Task[None]] = {}
        """Track the power request tasks currently being processed."""

        self._pending_requests: dict[frozenset[int], Request] = {}
        """Track the power requests that are waiting to be processed.

        Only one pending power request is kept for each set of components, the
        latest request will overwrite the previous one.
        """

        self._component_manager: ComponentManager
        if component_category == ComponentCategory.BATTERY:
            self._component_manager = BatteryManager(
                component_pool_status_sender, results_sender, api_power_request_timeout
            )
        elif component_category == ComponentCategory.EV_CHARGER:
            self._component_manager = EVChargerManager(
                component_pool_status_sender, results_sender, api_power_request_timeout
            )
        elif (
            component_category == ComponentCategory.INVERTER
            and component_type == InverterType.SOLAR
        ):
            self._component_manager = PVManager(
                component_pool_status_sender, results_sender, api_power_request_timeout
            )
        else:
            raise ValueError(
                f"PowerDistributor doesn't support controlling: {component_category}"
            )

    @override
    async def _run(self) -> None:
        """Run this actor's logic.

        It waits for new power requests and process them. Only one power request
        can be processed at a time to prevent from sending multiple requests for
        the same components to the microgrid API at the same time.

        A new power request will be ignored if a power request with the same
        components is currently being processed.

        Every component that failed or didn't respond in time will be marked
        as broken for some time.
        """
        await self._component_manager.start()

        async for request in self._requests_receiver:
            req_id = frozenset(request.component_ids)

            if req_id in self._processing_tasks:
                if pending_request := self._pending_requests.get(req_id):
                    _logger.debug(
                        "Pending request: %s, overwritten with request: %s",
                        pending_request,
                        request,
                    )
                self._pending_requests[req_id] = request
            else:
                self._process_request(req_id, request)

    @override
    async def stop(self, msg: str | None = None) -> None:
        """Stop this actor.

        Args:
            msg: The message to be passed to the tasks being cancelled.
        """
        await self._component_manager.stop()
        await super().stop(msg)

    def _handle_task_completion(
        self, req_id: frozenset[int], request: Request, task: asyncio.Task[None]
    ) -> None:
        """Handle the completion of a power request task.

        Args:
            req_id: The id to identify the power request.
            request: The power request that has been processed.
            task: The task that has completed.
        """
        try:
            task.result()
        except Exception:  # pylint: disable=broad-except
            _logger.exception("Failed power request: %s", request)

        if req_id in self._pending_requests:
            self._process_request(req_id, self._pending_requests.pop(req_id))
        elif req_id in self._processing_tasks:
            del self._processing_tasks[req_id]
        else:
            _logger.error("Request id not found in processing tasks: %s", req_id)

    def _process_request(self, req_id: frozenset[int], request: Request) -> None:
        """Process a power request.

        Args:
            req_id: The id to identify the power request.
            request: The power request to process.
        """
        task = asyncio.create_task(
            self._component_manager.distribute_power(request),
            name=f"{type(self).__name__}:{request}",
        )
        task.add_done_callback(
            lambda t: self._handle_task_completion(req_id, request, t)
        )
        self._processing_tasks[req_id] = task
