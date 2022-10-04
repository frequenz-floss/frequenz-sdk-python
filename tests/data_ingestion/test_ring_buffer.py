"""
Test for the `RingBuffer`

Copyright
Copyright © 2021 Frequenz Energy-as-a-Service GmbH

License
MIT
"""
import datetime

from frequenz.sdk.data_ingestion.resampling.ring_buffer import DataPoint, RingBuffer
from frequenz.sdk.microgrid import Component, ComponentCategory

now = datetime.datetime.utcnow()
timestamp = now.replace(tzinfo=datetime.timezone.utc)


def test_ring_buffer_insert() -> None:
    """Check if ComponentData is properly inserted into the ring buffer"""

    components = {Component(component_id=0, category=ComponentCategory.BATTERY)}

    ring_buffer = RingBuffer(size=5, components=components)

    items = [
        DataPoint(component_id=0, timestamp=timestamp + datetime.timedelta(seconds=i))
        for i in range(5)
    ]

    for item in items:
        ring_buffer.insert(item)

    for _, data_points in ring_buffer.items():
        assert items == list(data_points)


def test_ring_buffer_size() -> None:
    """Check if ring buffer size limit is properly applied"""

    components = {Component(component_id=0, category=ComponentCategory.BATTERY)}

    buffer_size = 5

    ring_buffer = RingBuffer(size=buffer_size, components=components)

    items = [
        DataPoint(component_id=0, timestamp=timestamp + datetime.timedelta(seconds=i))
        for i in range(100)
    ]

    for item in items:
        ring_buffer.insert(item)

    for _, data_points in ring_buffer.items():
        assert len(data_points) == buffer_size
        assert list(data_points) == items[-buffer_size:]


def test_ring_buffer_size_multiple_components() -> None:
    """Check if ring buffer size limit is properly applied for multiple components"""

    components = {
        Component(component_id=i, category=ComponentCategory.BATTERY)
        for i in range(100)
    }

    buffer_size = 10

    ring_buffer = RingBuffer(size=buffer_size, components=components)

    items = [
        DataPoint(
            component_id=component.component_id,
            timestamp=timestamp + datetime.timedelta(seconds=i),
        )
        for i in range(100)
        for component in components
    ]

    for item in items:
        ring_buffer.insert(item)

    total_ring_buffer_size = 0
    for _, data_points in ring_buffer.items():
        total_ring_buffer_size += len(data_points)
        assert len(data_points) == buffer_size
    assert total_ring_buffer_size == len(components) * buffer_size


def test_ring_buffer_insert_order() -> None:
    """Check if an item "from the past" is inserted in order into the ring buffer."""

    components = {Component(component_id=0, category=ComponentCategory.BATTERY)}

    ring_buffer = RingBuffer(size=11, components=components)

    items = [
        DataPoint(component_id=0, timestamp=timestamp + datetime.timedelta(seconds=i))
        for i in range(10)
    ]

    for item in items:
        ring_buffer.insert(item)

    for _, data_points in ring_buffer.items():
        assert len(data_points) == 10
        assert list(data_points) == items

    delayed_item = DataPoint(
        component_id=0, timestamp=timestamp + datetime.timedelta(seconds=5)
    )

    ring_buffer.insert(delayed_item)

    items.insert(5, delayed_item)

    for _, data_points in ring_buffer.items():
        assert len(data_points) == 11
        assert list(data_points) == items
