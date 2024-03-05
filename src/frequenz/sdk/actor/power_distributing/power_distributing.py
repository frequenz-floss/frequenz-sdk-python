# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Actor to distribute power between batteries.

When charge/discharge method is called the power should be distributed so that
the SoC in batteries stays at the same level. That way of distribution
prevents using only one battery, increasing temperature, and maximize the total
amount power to charge/discharge.

Purpose of this actor is to keep SoC level of each component at the equal level.
"""


import asyncio

from frequenz.channels import Receiver, Sender

from ...actor._actor import Actor
from ...microgrid.component import ComponentCategory
from ._component_managers import BatteryManager, ComponentManager, EVChargerManager
from ._component_status import ComponentPoolStatus
from .request import Request
from .result import Result


class PowerDistributingActor(Actor):
    # pylint: disable=too-many-instance-attributes
    """Actor to distribute the power between batteries in a microgrid.

    The purpose of this tool is to keep an equal SoC level in all batteries.
    The PowerDistributingActor can have many concurrent users which at this time
    need to be known at construction time.

    For each user a bidirectional channel needs to be created through which
    they can send and receive requests and responses.

    It is recommended to wait for PowerDistributingActor output with timeout. Otherwise if
    the processing function fails then the response will never come.
    The timeout should be Result:request_timeout + time for processing the request.

    Edge cases:
    * If there are 2 requests to be processed for the same subset of batteries, then
    only the latest request will be processed. Older request will be ignored. User with
    older request will get response with Result.Status.IGNORED.

    * If there are 2 requests and their subset of batteries is different but they
    overlap (they have at least one common battery), then then both batteries
    will be processed. However it is not expected so the proper error log will be
    printed.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        component_category: ComponentCategory,
        requests_receiver: Receiver[Request],
        results_sender: Sender[Result],
        component_pool_status_sender: Sender[ComponentPoolStatus],
        wait_for_data_sec: float = 2,
        *,
        name: str | None = None,
    ) -> None:
        """Create class instance.

        Args:
            component_category: The category of the components that this actor is
                responsible for.
            requests_receiver: Receiver for receiving power requests from the power
                manager.
            results_sender: Sender for sending results to the power manager.
            component_pool_status_sender: Channel for sending information about which
                components are expected to be working.
            wait_for_data_sec: How long actor should wait before processing first
                request. It is a time needed to collect first components data.
            name: The name of the actor. If `None`, `str(id(self))` will be used. This
                is used mostly for debugging purposes.

        Raises:
            ValueError: If the given component category is not supported.
        """
        super().__init__(name=name)
        self._component_category = component_category
        self._requests_receiver = requests_receiver
        self._result_sender = results_sender
        self._wait_for_data_sec = wait_for_data_sec

        self._component_manager: ComponentManager
        if component_category == ComponentCategory.BATTERY:
            self._component_manager = BatteryManager(component_pool_status_sender)
        elif component_category == ComponentCategory.EV_CHARGER:
            self._component_manager = EVChargerManager(component_pool_status_sender)
        else:
            raise ValueError(
                f"PowerDistributor doesn't support controlling: {component_category}"
            )

    async def _run(self) -> None:  # pylint: disable=too-many-locals
        """Run actor main function.

        It waits for new requests in task_queue and process it, and send
        `set_power` request with distributed power.
        The output of the `set_power` method is processed.
        Every battery and inverter that failed or didn't respond in time will be marked
        as broken for some time.
        """
        await self._component_manager.start()

        # Wait few seconds to get data from the channels created above.
        await asyncio.sleep(self._wait_for_data_sec)

        async for request in self._requests_receiver:
            result = await self._component_manager.distribute_power(request)
            await self._result_sender.send(result)

    async def stop(self, msg: str | None = None) -> None:
        """Stop this actor.

        Args:
            msg: The message to be passed to the tasks being cancelled.
        """
        await self._component_manager.stop()
        await super().stop(msg)
