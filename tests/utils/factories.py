# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Utility factory functions to mock data returned by components."""

import asyncio
import time
from typing import Any, Callable, Coroutine, Dict, Iterable, Optional

from frequenz.channels import Broadcast, Receiver, Sender

from frequenz.sdk.microgrid import (
    BatteryData,
    Component,
    Connection,
    EVChargerData,
    InverterData,
    MeterData,
)
from frequenz.sdk.microgrid.client import MicrogridGrpcClient

from .data_generation import (
    generate_battery_data,
    generate_ev_charger_data,
    generate_inverter_data,
    generate_meter_data,
)


def component_factory(
    components: Iterable[Component],
) -> Callable[[MicrogridGrpcClient], Iterable[Component]]:
    """Use to mock the `components` method in MicrogridGrpcClient class.

    Args:
        components: Components to be returned by the `components` method.

    Returns:
        A mocked function that will return components provided by the caller.
    """

    def wrapper(
        self: MicrogridGrpcClient,  # pylint: disable=unused-argument # mocking a function within class
    ) -> Iterable[Component]:
        return components

    return wrapper


def connection_factory(
    connections: Iterable[Connection],
) -> Callable[[MicrogridGrpcClient], Iterable[Connection]]:
    """Use to mock the `connections` method in MicrogridGrpcClient class.

    Args:
        connections: Connections to be returned by
            the `connections` method.

    Returns:
        A mocked function that will return connections provided by the caller.
    """

    def wrapper(
        self: MicrogridGrpcClient,  # pylint: disable=unused-argument # mocking a function within class
    ) -> Iterable[Connection]:
        return connections

    return wrapper


def battery_data_factory(
    interval: float = 0.2,
    timeout: float = 1.0,
    params: Optional[Dict[str, Any]] = None,
    overrides: Optional[Dict[int, Dict[str, Any]]] = None,
) -> Callable[[int], Coroutine[Any, Any, Receiver[BatteryData]]]:
    """Use to mock the `battery_data` method in MicrogridGrpcClient class.

    Args:
        interval: Value describing how often to send data.
        timeout: Value describing when factory should stop sending data.
        params: Params to be returned by all battery components. If not provided,
            default params will be used.
        overrides: Params to be returned by battery components with specific IDs. If not
            provided, all battery components will return the same data, either `params`
            or default params.

    Returns:
        A mocked function used to replace `battery_data` in the MicrogridGrpcClient
            class. It will return a channel receiver that provides an `BatteryData`
            stream.
    """

    async def send_data(component_id: int, sender: Sender[BatteryData]) -> None:
        start = time.time()
        while time.time() - start <= timeout:
            await asyncio.sleep(interval)
            battery_data = generate_battery_data(
                component_id=component_id, params=params, overrides=overrides
            )
            await sender.send(battery_data)

    async def wrapped(
        component_id: int,  # pylint: disable=unused-argument
    ) -> Receiver[BatteryData]:
        channel = Broadcast[BatteryData](f"raw-component-data-{component_id}")
        asyncio.create_task(send_data(component_id, channel.new_sender()))
        return channel.new_receiver()

    return wrapped


def inverter_data_factory(
    interval: float = 0.2,
    timeout: float = 1.0,
    params: Optional[Dict[str, Any]] = None,
    overrides: Optional[Dict[int, Dict[str, Any]]] = None,
) -> Callable[[int], Coroutine[Any, Any, Receiver[InverterData]]]:
    """Use to mock the `inverter_data` method in MicrogridGrpcClient class.

    Args:
        interval: Value describing how often to send data.
        timeout: Value describing when factory should stop sending data.
        params: Params to be returned by all inverter components. If not provided,
            default params will be used.
        overrides: Params to be returned by inverter components with specific IDs. If
            not provided, all inverter components will return the same data, either
            `params` or default params.

    Returns:
        A mocked function used to replace `inverter_data` in the MicrogridGrpcClient
            class. It will return a channel receiver that provides an `InverterData`
            stream.
    """

    async def send_data(component_id: int, sender: Sender[InverterData]) -> None:
        start = time.time()
        while time.time() - start <= timeout:
            await asyncio.sleep(interval)
            inverter_data = generate_inverter_data(
                component_id=component_id, params=params, overrides=overrides
            )
            await sender.send(inverter_data)

    async def wrapped(
        component_id: int,  # pylint: disable=unused-argument
    ) -> Receiver[InverterData]:
        channel = Broadcast[InverterData](f"raw-component-data-{component_id}")
        asyncio.create_task(send_data(component_id, channel.new_sender()))
        return channel.new_receiver()

    return wrapped


def meter_data_factory(
    interval: float = 0.2,
    timeout: float = 1.0,
    params: Optional[Dict[str, Any]] = None,
    overrides: Optional[Dict[int, Dict[str, Any]]] = None,
) -> Callable[[int], Coroutine[Any, Any, Receiver[MeterData]]]:
    """Use to mock the `meter_data` method in MicrogridGrpcClient class.

    Args:
        interval: Value describing how often to send data.
        timeout: Value describing when factory should stop sending data.
        params: Params to be returned by all meter components. If not provided, default
            params will be used.
        overrides: Params to be returned by meter components with specific IDs. If not
            provided, all meter components will return the same data, either `params` or
            default params.

    Returns:
        A mocked function used to replace `meter_data` in the MicrogridGrpcClient
            class. It will return a channel receiver that provides an `MeterData`
            stream.
    """

    async def send_data(component_id: int, sender: Sender[MeterData]) -> None:
        start = time.time()
        while time.time() - start <= timeout:
            await asyncio.sleep(interval)
            meter_data = generate_meter_data(
                component_id=component_id,
                params=params,
                overrides=overrides,
            )
            await sender.send(meter_data)

    async def wrapped(
        component_id: int,  # pylint: disable=unused-argument
    ) -> Receiver[MeterData]:
        channel = Broadcast[MeterData](f"raw-component-data-{component_id}")
        asyncio.create_task(send_data(component_id, channel.new_sender()))
        return channel.new_receiver()

    return wrapped


def ev_charger_data_factory(
    interval: float = 0.2,
    timeout: float = 1.0,
    params: Optional[Dict[str, Any]] = None,
    overrides: Optional[Dict[int, Dict[str, Any]]] = None,
) -> Callable[[int], Coroutine[Any, Any, Receiver[EVChargerData]]]:
    """Use to mock the `ev_charger_data` method in MicrogridGrpcClient class.

    Args:
        interval: Value describing how often to send data.
        timeout: Value describing when factory should stop sending data.
        params: Params to be returned by all EV charger components. If not provided,
            default params will be used.
        overrides: Params to be returned by EV charger components with specific IDs. If
            not provided, all EV charger components will return the same data, either
            `params` or default params.

    Returns:
        A mocked function used to replace `ev_charger_data` in the MicrogridGrpcClient
            class. It will return a channel receiver that provides an `EVChargerData`
            stream.
    """

    async def send_data(component_id: int, sender: Sender[EVChargerData]) -> None:
        start = time.time()
        while time.time() - start <= timeout:
            await asyncio.sleep(interval)
            ev_charger_data = generate_ev_charger_data(
                component_id=component_id,
                params=params,
                overrides=overrides,
            )
            await sender.send(ev_charger_data)

    async def wrapped(
        component_id: int,  # pylint: disable=unused-argument
    ) -> Receiver[EVChargerData]:
        channel = Broadcast[EVChargerData](f"raw-component-data-{component_id}")
        asyncio.create_task(send_data(component_id, channel.new_sender()))
        return channel.new_receiver()

    return wrapped
