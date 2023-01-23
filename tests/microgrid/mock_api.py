# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Mock implementation of the microgrid gRPC API.

This is intended to support the narrow set of test cases that have to
check integration with the API.  Note that this should exclude almost
all framework code, as API integration should be highly encapsulated.
"""

from __future__ import annotations

import asyncio

# pylint: disable=invalid-name,no-name-in-module,unused-import
from concurrent import futures
from typing import Iterable, Iterator, List, Optional, Tuple

import grpc
from frequenz.api.microgrid.battery_pb2 import Battery
from frequenz.api.microgrid.battery_pb2 import Data as BatteryData
from frequenz.api.microgrid.common_pb2 import AC, Metric, MetricAggregation
from frequenz.api.microgrid.ev_charger_pb2 import EVCharger
from frequenz.api.microgrid.inverter_pb2 import Inverter
from frequenz.api.microgrid.inverter_pb2 import Type as InverterType
from frequenz.api.microgrid.meter_pb2 import Data as MeterData
from frequenz.api.microgrid.meter_pb2 import Meter
from frequenz.api.microgrid.microgrid_pb2 import (
    COMPONENT_CATEGORY_BATTERY,
    COMPONENT_CATEGORY_EV_CHARGER,
    COMPONENT_CATEGORY_INVERTER,
    COMPONENT_CATEGORY_METER,
    Component,
    ComponentCategory,
    ComponentData,
    ComponentFilter,
    ComponentIdParam,
    ComponentList,
    Connection,
    ConnectionFilter,
    ConnectionList,
    PowerLevelParam,
    SetBoundsParam,
)
from frequenz.api.microgrid.microgrid_pb2_grpc import (
    MicrogridServicer,
    add_MicrogridServicer_to_server,
)
from google.protobuf.empty_pb2 import Empty
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.wrappers_pb2 import BoolValue


class MockMicrogridServicer(  # pylint: disable=too-many-public-methods
    MicrogridServicer
):
    """Servicer implementation mock for the microgrid API.

    This class implements customizable mocks of the individual API methods,
    and can be bound to a gRPC server instance to create the complete API
    mock.
    """

    def __init__(
        self,
        components: Optional[List[Tuple[int, ComponentCategory.V]]] = None,
        connections: Optional[List[Tuple[int, int]]] = None,
    ) -> None:
        """Create a MockMicrogridServicer instance."""
        self._components: List[Component] = []
        self._connections: List[Connection] = []
        self._bounds: List[SetBoundsParam] = []

        if components is not None:
            self.set_components(components)
        if connections is not None:
            self.set_connections(connections)

        self._latest_charge: Optional[PowerLevelParam] = None
        self._latest_discharge: Optional[PowerLevelParam] = None

    def add_component(
        self,
        component_id: int,
        component_category: ComponentCategory.V,
        inverter_type: InverterType.V = InverterType.TYPE_UNSPECIFIED,
    ) -> None:
        """Add a component to the mock service."""
        if component_category == ComponentCategory.COMPONENT_CATEGORY_INVERTER:
            self._components.append(
                Component(
                    id=component_id, category=component_category, inverter=inverter_type
                )
            )
        else:
            self._components.append(
                Component(id=component_id, category=component_category)
            )

    def add_connection(self, start: int, end: int) -> None:
        """Add a connection to the mock service."""
        self._connections.append(Connection(start=start, end=end))

    def set_components(self, components: List[Tuple[int, ComponentCategory.V]]) -> None:
        """Set components to mock service, dropping existing."""
        self._components.clear()
        self._components.extend(
            map(lambda c: Component(id=c[0], category=c[1]), components)
        )

    def set_connections(self, connections: List[Tuple[int, int]]) -> None:
        """Set connections to mock service, dropping existing."""
        self._connections.clear()
        self._connections.extend(
            map(lambda c: Connection(start=c[0], end=c[1]), connections)
        )

    @property
    def latest_charge(self) -> Optional[PowerLevelParam]:
        """Get argumetns of the latest charge request."""
        return self._latest_charge

    @property
    def latest_discharge(self) -> Optional[PowerLevelParam]:
        """Get arguments of the latest discharge request."""
        return self._latest_discharge

    def get_bounds(self) -> List[SetBoundsParam]:
        """Return the list of received bounds."""
        return self._bounds

    def clear_bounds(self) -> None:
        """Drop all received bounds."""
        self._bounds.clear()

    # pylint: disable=unused-argument
    def ListComponents(
        self,
        request: ComponentFilter,
        context: grpc.ServicerContext,
    ) -> ComponentList:
        """List components."""
        return ComponentList(components=self._components)

    def ListAllComponents(
        self, request: Empty, context: grpc.ServicerContext
    ) -> ComponentList:
        """Return a list of all components."""
        return ComponentList(components=self._components)

    def ListConnections(
        self, request: ConnectionFilter, context: grpc.ServicerContext
    ) -> ConnectionList:
        """Return a list of all connections."""
        connections: Iterable[Connection] = self._connections
        if request.starts is not None and len(request.starts) > 0:
            connections = filter(lambda c: c.start in request.starts, connections)
        if request.ends is not None and len(request.ends) > 0:
            connections = filter(lambda c: c.end in request.ends, connections)
        return ConnectionList(connections=connections)

    def GetComponentData(
        self, request: ComponentIdParam, context: grpc.ServicerContext
    ) -> Iterator[ComponentData]:
        """Return an iterator for mock ComponentData."""
        # pylint: disable=stop-iteration-return
        component = next(filter(lambda c: c.id == request.id, self._components))

        def next_msg() -> ComponentData:
            ts = Timestamp()
            ts.GetCurrentTime()
            if component.category == COMPONENT_CATEGORY_BATTERY:
                return ComponentData(
                    id=request.id,
                    ts=ts,
                    battery=Battery(
                        data=BatteryData(
                            soc=MetricAggregation(avg=float(request.id % 100)),
                        )
                    ),
                )
            if component.category == COMPONENT_CATEGORY_METER:
                return ComponentData(
                    id=request.id,
                    ts=ts,
                    meter=Meter(
                        data=MeterData(
                            ac=AC(
                                power_active=Metric(value=100.0),
                            ),
                        )
                    ),
                )
            if component.category == COMPONENT_CATEGORY_INVERTER:
                return ComponentData(id=request.id, inverter=Inverter())
            if component.category == COMPONENT_CATEGORY_EV_CHARGER:
                return ComponentData(id=request.id, ev_charger=EVCharger())
            return ComponentData()

        num_messages = 3
        for _ in range(num_messages):
            msg = next_msg()
            yield msg

    def SetBounds(
        self, request_iterator: Iterator[SetBoundsParam], context: grpc.ServicerContext
    ) -> Empty:
        """/nitrogen.Nitrogen/SetBounds method stub."""
        for bound in request_iterator:
            self._bounds.append(bound)
        return Empty()

    def Charge(self, request: PowerLevelParam, context: grpc.ServicerContext) -> Empty:
        """/nitrogen.Nitrogen/Charge method stub."""
        self._latest_charge = request
        return Empty()

    def Discharge(
        self, request: PowerLevelParam, context: grpc.ServicerContext
    ) -> Empty:
        """/nitrogen.Nitrogen/Discharge method stub."""
        self._latest_discharge = request
        return Empty()

    def CanStreamData(
        self, request: ComponentIdParam, context: grpc.ServicerContext
    ) -> BoolValue:
        """/nitrogen.Nitrogen/CanStreamData method stub."""
        return BoolValue(value=True)

    def HotStandby(
        self, request: ComponentIdParam, context: grpc.ServicerContext
    ) -> Empty:
        """/nitrogen.Nitrogen/HotStandby method stub."""
        return Empty()

    def ColdStandby(
        self, request: ComponentIdParam, context: grpc.ServicerContext
    ) -> Empty:
        """/nitrogen.Nitrogen/ColdStandby method stub."""
        return Empty()

    def ErrorAck(
        self, request: ComponentIdParam, context: grpc.ServicerContext
    ) -> Empty:
        """/nitrogen.Nitrogen/ErrorAck method stub."""
        return Empty()

    def Start(self, request: ComponentIdParam, context: grpc.ServicerContext) -> Empty:
        """/nitrogen.Nitrogen/Start method stub."""
        return Empty()

    def Stop(self, request: ComponentIdParam, context: grpc.ServicerContext) -> Empty:
        """/nitrogen.Nitrogen/Stop method stub."""
        return Empty()


class MockGrpcServer:
    """Helper class to instantiate a gRPC server for a microgrid servicer."""

    def __init__(
        self, servicer: MicrogridServicer, host: str = "[::]", port: int = 61060
    ) -> None:
        """Create a MockGrpcServicer instance."""
        self._server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=20))
        add_MicrogridServicer_to_server(servicer, self._server)
        self._server.add_insecure_port(f"{host}:{port}")

    async def start(self) -> None:
        """Start the server."""
        await self._server.start()

    async def _stop(self, grace: Optional[float]) -> None:
        """Stop the server."""
        await self._server.stop(grace)

    async def _wait_for_termination(self, timeout: Optional[float] = None) -> None:
        """Wait for termination."""
        await self._server.wait_for_termination(timeout)

    async def graceful_shutdown(
        self, stop_timeout: float = 0.1, terminate_timeout: float = 0.2
    ) -> bool:
        """Shutdown server gracefully.

        Args:
            stop_timeout: Argument for self.stop method
            terminate_timeout: Argument for self.wait_for_termination method.

        Returns:
            True if server was stopped in given timeout. False otherwise.
        """
        await self._stop(stop_timeout)
        try:
            await asyncio.wait_for(
                self._wait_for_termination(None), timeout=terminate_timeout
            )
        except TimeoutError:
            return False
        return True
