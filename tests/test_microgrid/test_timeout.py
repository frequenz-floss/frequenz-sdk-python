"""Benchmark for microgrid data.

Copyright
Copyright © 2022 Frequenz Energy-as-a-Service GmbH

License
MIT
"""
import time
from typing import Any
from unittest.mock import patch

import grpc
import pytest
from frequenz.api.microgrid.microgrid_pb2 import (
    ComponentFilter,
    ComponentList,
    ConnectionFilter,
    ConnectionList,
    PowerLevelParam,
)
from google.protobuf.empty_pb2 import Empty  # pylint: disable=no-name-in-module
from pytest_mock import MockerFixture

from frequenz.sdk.microgrid.client import MicrogridGrpcClient

from .mock_api import MockGrpcServer, MockMicrogridServicer

# Timeout applied to all gRPC calls under test. It is expected after that the gRPC
# calls will raise an AioRpcError with status code equal to DEADLINE_EXCEEDED.
GRPC_CALL_TIMEOUT: float = 0.1

# How much late a response to a gRPC call should be. It is used to trigger a timeout
# error and needs to be greater than `GRPC_CALL_TIMEOUT`.
GRPC_SERVER_DELAY: float = 0.3

# How long a mocked Microgrid server should be running for a single test function,
# before it gets shut down so that the tests can finish.
GRPC_SERVER_SHUTDOWN_DELAY: float = 1.0


@patch("frequenz.sdk.microgrid.client.DEFAULT_GRPC_CALL_TIMEOUT", GRPC_CALL_TIMEOUT)
async def test_components_timeout(mocker: MockerFixture) -> None:
    """Test if the components() method properly raises AioRpcError"""
    servicer = MockMicrogridServicer()

    # pylint: disable=unused-argument
    def mock_list_components(request: ComponentFilter, context: Any) -> ComponentList:
        time.sleep(GRPC_SERVER_DELAY)
        return ComponentList(components=[])

    mocker.patch.object(servicer, "ListComponents", mock_list_components)
    server = MockGrpcServer(servicer, port=57809)
    await server.start()

    target = "[::]:57809"
    grpc_channel = grpc.aio.insecure_channel(target)
    client = MicrogridGrpcClient(grpc_channel=grpc_channel, target=target)

    with pytest.raises(grpc.aio.AioRpcError) as err_ctx:
        _ = await client.components()
    assert err_ctx.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
    await server.wait_for_termination(GRPC_SERVER_SHUTDOWN_DELAY)


@patch("frequenz.sdk.microgrid.client.DEFAULT_GRPC_CALL_TIMEOUT", GRPC_CALL_TIMEOUT)
async def test_connections_timeout(mocker: MockerFixture) -> None:
    """Test if the connections() method properly raises AioRpcError"""
    servicer = MockMicrogridServicer()

    # pylint: disable=unused-argument
    def mock_list_connections(
        request: ConnectionFilter, context: Any
    ) -> ConnectionList:
        time.sleep(GRPC_SERVER_DELAY)
        return ConnectionList(connections=[])

    mocker.patch.object(servicer, "ListConnections", mock_list_connections)
    server = MockGrpcServer(servicer, port=57809)
    await server.start()

    target = "[::]:57809"
    grpc_channel = grpc.aio.insecure_channel(target)
    client = MicrogridGrpcClient(grpc_channel=grpc_channel, target=target)

    with pytest.raises(grpc.aio.AioRpcError) as err_ctx:
        _ = await client.connections()
    assert err_ctx.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
    await server.wait_for_termination(GRPC_SERVER_SHUTDOWN_DELAY)


@patch("frequenz.sdk.microgrid.client.DEFAULT_GRPC_CALL_TIMEOUT", GRPC_CALL_TIMEOUT)
async def test_set_power_timeout(mocker: MockerFixture) -> None:
    """Test if the set_power() method properly raises AioRpcError"""
    servicer = MockMicrogridServicer()

    # pylint: disable=unused-argument
    def mock_set_power(request: PowerLevelParam, context: Any) -> Empty:
        time.sleep(GRPC_SERVER_DELAY)
        return Empty()

    mocker.patch.object(servicer, "Charge", mock_set_power)
    mocker.patch.object(servicer, "Discharge", mock_set_power)
    server = MockGrpcServer(servicer, port=57809)
    await server.start()

    target = "[::]:57809"
    grpc_channel = grpc.aio.insecure_channel(target)
    client = MicrogridGrpcClient(grpc_channel=grpc_channel, target=target)

    power_values = [-100, 100]
    for power_w in power_values:
        with pytest.raises(grpc.aio.AioRpcError) as err_ctx:
            _ = await client.set_power(component_id=1, power_w=power_w)
        assert err_ctx.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED

    await server.stop(GRPC_SERVER_SHUTDOWN_DELAY)
