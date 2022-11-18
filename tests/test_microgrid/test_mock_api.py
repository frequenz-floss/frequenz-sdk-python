# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""
Tests for the microgrid mock api.
"""

# pylint: disable=missing-function-docstring,use-implicit-booleaness-not-comparison
# pylint: disable=invalid-name,no-name-in-module,no-member

import grpc
from frequenz.api.microgrid.microgrid_pb2 import (
    Component,
    ComponentCategory,
    ComponentFilter,
    Connection,
    ConnectionFilter,
)
from frequenz.api.microgrid.microgrid_pb2_grpc import MicrogridStub
from google.protobuf.empty_pb2 import Empty

from . import mock_api


def test_MockMicrogridServicer() -> None:
    api = mock_api.MockMicrogridServicer()
    assert list(api.ListComponents(ComponentFilter(), None).components) == []
    assert list(api.ListConnections(ConnectionFilter(), None).connections) == []

    # adding new components just appends them to the list:
    # duplicates are not prevented, as this may be wanted
    # behaviour of the mock
    api.add_component(0, ComponentCategory.COMPONENT_CATEGORY_METER)
    assert list(api.ListComponents(ComponentFilter(), None).components) == [
        Component(id=0, category=ComponentCategory.COMPONENT_CATEGORY_METER)
    ]
    assert list(api.ListConnections(ConnectionFilter(), None).connections) == []

    api.add_component(0, ComponentCategory.COMPONENT_CATEGORY_BATTERY)
    assert list(api.ListComponents(ComponentFilter(), None).components) == [
        Component(id=0, category=ComponentCategory.COMPONENT_CATEGORY_METER),
        Component(id=0, category=ComponentCategory.COMPONENT_CATEGORY_BATTERY),
    ]
    assert list(api.ListConnections(ConnectionFilter(), None).connections) == []

    api.add_component(0, ComponentCategory.COMPONENT_CATEGORY_METER)
    assert list(api.ListComponents(ComponentFilter(), None).components) == [
        Component(id=0, category=ComponentCategory.COMPONENT_CATEGORY_METER),
        Component(id=0, category=ComponentCategory.COMPONENT_CATEGORY_BATTERY),
        Component(id=0, category=ComponentCategory.COMPONENT_CATEGORY_METER),
    ]
    assert list(api.ListConnections(ConnectionFilter(), None).connections) == []

    # similarly, duplicates are allowed when adding new connections
    api.add_connection(0, 0)
    assert list(api.ListComponents(ComponentFilter(), None).components) == [
        Component(id=0, category=ComponentCategory.COMPONENT_CATEGORY_METER),
        Component(id=0, category=ComponentCategory.COMPONENT_CATEGORY_BATTERY),
        Component(id=0, category=ComponentCategory.COMPONENT_CATEGORY_METER),
    ]
    assert list(api.ListConnections(ConnectionFilter(), None).connections) == [
        Connection(start=0, end=0)
    ]

    api.add_connection(7, 9)
    assert list(api.ListComponents(ComponentFilter(), None).components) == [
        Component(id=0, category=ComponentCategory.COMPONENT_CATEGORY_METER),
        Component(id=0, category=ComponentCategory.COMPONENT_CATEGORY_BATTERY),
        Component(id=0, category=ComponentCategory.COMPONENT_CATEGORY_METER),
    ]
    assert list(api.ListConnections(ConnectionFilter(), None).connections) == [
        Connection(start=0, end=0),
        Connection(start=7, end=9),
    ]

    api.add_connection(0, 0)
    assert list(api.ListComponents(ComponentFilter(), None).components) == [
        Component(id=0, category=ComponentCategory.COMPONENT_CATEGORY_METER),
        Component(id=0, category=ComponentCategory.COMPONENT_CATEGORY_BATTERY),
        Component(id=0, category=ComponentCategory.COMPONENT_CATEGORY_METER),
    ]
    assert list(api.ListConnections(ConnectionFilter(), None).connections) == [
        Connection(start=0, end=0),
        Connection(start=7, end=9),
        Connection(start=0, end=0),
    ]

    # `set_components` overrides all the components but leaves
    # the connections alone
    api.set_components(
        [
            (9, ComponentCategory.COMPONENT_CATEGORY_METER),
            (99, ComponentCategory.COMPONENT_CATEGORY_INVERTER),
            (999, ComponentCategory.COMPONENT_CATEGORY_BATTERY),
        ]
    )
    assert list(api.ListComponents(ComponentFilter(), None).components) == [
        Component(id=9, category=ComponentCategory.COMPONENT_CATEGORY_METER),
        Component(id=99, category=ComponentCategory.COMPONENT_CATEGORY_INVERTER),
        Component(id=999, category=ComponentCategory.COMPONENT_CATEGORY_BATTERY),
    ]
    assert list(api.ListConnections(ConnectionFilter(), None).connections) == [
        Connection(start=0, end=0),
        Connection(start=7, end=9),
        Connection(start=0, end=0),
    ]

    # similarly `set_connections` overrides all the existing connections
    api.set_connections([(999, 9), (99, 19), (909, 101), (99, 91)])
    assert list(api.ListComponents(ComponentFilter(), None).components) == [
        Component(id=9, category=ComponentCategory.COMPONENT_CATEGORY_METER),
        Component(id=99, category=ComponentCategory.COMPONENT_CATEGORY_INVERTER),
        Component(id=999, category=ComponentCategory.COMPONENT_CATEGORY_BATTERY),
    ]
    assert list(api.ListConnections(ConnectionFilter(), None).connections) == [
        Connection(start=999, end=9),
        Connection(start=99, end=19),
        Connection(start=909, end=101),
        Connection(start=99, end=91),
    ]

    # connection requests can be filtered
    assert list(
        api.ListConnections(ConnectionFilter(starts=[999]), None).connections
    ) == [Connection(start=999, end=9)]
    assert list(
        api.ListConnections(ConnectionFilter(starts=[99]), None).connections
    ) == [Connection(start=99, end=19), Connection(start=99, end=91)]
    assert list(api.ListConnections(ConnectionFilter(ends=[19]), None).connections) == [
        Connection(start=99, end=19)
    ]
    assert list(
        api.ListConnections(ConnectionFilter(ends=[101]), None).connections
    ) == [Connection(start=909, end=101)]
    assert list(
        api.ListConnections(
            ConnectionFilter(starts=[99, 999], ends=[9, 19]), None
        ).connections
    ) == [Connection(start=999, end=9), Connection(start=99, end=19)]

    # mock API instances can be initialized with components and connections
    # already in place (note there are no checks for consistency of data so
    # in this case we can include connections to a non-existent component)
    api_prealloc = mock_api.MockMicrogridServicer(
        components=[
            (7, ComponentCategory.COMPONENT_CATEGORY_GRID),
            (9, ComponentCategory.COMPONENT_CATEGORY_LOAD),
        ],
        connections=[(7, 8), (8, 9)],
    )
    assert list(api_prealloc.ListComponents(ComponentFilter(), None).components) == [
        Component(id=7, category=ComponentCategory.COMPONENT_CATEGORY_GRID),
        Component(id=9, category=ComponentCategory.COMPONENT_CATEGORY_LOAD),
    ]
    assert list(api_prealloc.ListConnections(ConnectionFilter(), None).connections) == [
        Connection(start=7, end=8),
        Connection(start=8, end=9),
    ]


async def test_MockGrpcServer() -> None:
    servicer1 = mock_api.MockMicrogridServicer(
        components=[
            (1, ComponentCategory.COMPONENT_CATEGORY_GRID),
            (2, ComponentCategory.COMPONENT_CATEGORY_METER),
            (3, ComponentCategory.COMPONENT_CATEGORY_LOAD),
        ],
        connections=[(1, 2), (2, 3)],
    )
    server1 = mock_api.MockGrpcServer(servicer1, port=57809)
    await server1.start()

    client = MicrogridStub(grpc.aio.insecure_channel("[::]:57809"))

    components1 = await client.ListComponents(Empty())
    assert list(components1.components) == [
        Component(id=1, category=ComponentCategory.COMPONENT_CATEGORY_GRID),
        Component(id=2, category=ComponentCategory.COMPONENT_CATEGORY_METER),
        Component(id=3, category=ComponentCategory.COMPONENT_CATEGORY_LOAD),
    ]

    connections1 = await client.ListConnections(ConnectionFilter())
    assert list(connections1.connections) == [
        Connection(start=1, end=2),
        Connection(start=2, end=3),
    ]

    await server1.stop(1)

    servicer2 = mock_api.MockMicrogridServicer(
        components=[
            (6, ComponentCategory.COMPONENT_CATEGORY_GRID),
            (77, ComponentCategory.COMPONENT_CATEGORY_METER),
            (888, ComponentCategory.COMPONENT_CATEGORY_EV_CHARGER),
            (9999, ComponentCategory.COMPONENT_CATEGORY_LOAD),
        ],
        connections=[(6, 77), (6, 888), (77, 9999)],
    )
    server2 = mock_api.MockGrpcServer(servicer2, port=57809)
    await server2.start()

    components2 = await client.ListComponents(Empty())
    assert list(components2.components) == [
        Component(id=6, category=ComponentCategory.COMPONENT_CATEGORY_GRID),
        Component(id=77, category=ComponentCategory.COMPONENT_CATEGORY_METER),
        Component(id=888, category=ComponentCategory.COMPONENT_CATEGORY_EV_CHARGER),
        Component(id=9999, category=ComponentCategory.COMPONENT_CATEGORY_LOAD),
    ]

    connections2 = await client.ListConnections(ConnectionFilter())
    assert list(connections2.connections) == [
        Connection(start=6, end=77),
        Connection(start=6, end=888),
        Connection(start=77, end=9999),
    ]

    connections2b = await client.ListConnections(ConnectionFilter(starts=[6]))
    assert list(connections2b.connections) == [
        Connection(start=6, end=77),
        Connection(start=6, end=888),
    ]

    connections2c = await client.ListConnections(ConnectionFilter(ends=[9999]))
    assert list(connections2c.connections) == [Connection(start=77, end=9999)]

    connections2d = await client.ListConnections(
        ConnectionFilter(starts=[6, 77], ends=[888, 9999])
    )
    assert list(connections2d.connections) == [
        Connection(start=6, end=888),
        Connection(start=77, end=9999),
    ]

    await server2.wait_for_termination(0.1)
