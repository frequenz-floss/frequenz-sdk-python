# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Frequenz Python SDK resampling example."""
import dataclasses
from datetime import datetime, timezone
from typing import Iterator

import async_solipsism
import pytest
import time_machine
from frequenz.channels import Broadcast

from frequenz.sdk.actor import (
    ChannelRegistry,
    ComponentMetricRequest,
    ComponentMetricsResamplingActor,
)
from frequenz.sdk.microgrid.component import ComponentMetricId
from frequenz.sdk.timeseries import Sample

# pylint: disable=too-many-locals,redefined-outer-name
#


@pytest.fixture(autouse=True)
def fake_loop() -> Iterator[async_solipsism.EventLoop]:
    """Replace the loop with one that doesn't interact with the outside world."""
    loop = async_solipsism.EventLoop()
    yield loop
    loop.close()


@pytest.fixture
def fake_time() -> Iterator[time_machine.Coordinates]:
    """Replace real time with a time machine that doesn't automatically tick."""
    with time_machine.travel(0, tick=False) as traveller:
        yield traveller


def _now() -> datetime:
    return datetime.now(timezone.utc)


# Even when it could be refactored to use smaller functions, I'm allowing
# too many statements because it makes following failures in tests more easy
# when the code is very flat.
async def test_component_metrics_resampling_actor(  # pylint: disable=too-many-statements
    fake_time: time_machine.Coordinates,
) -> None:
    """Run main functions that initializes and creates everything."""

    channel_registry = ChannelRegistry(name="test")
    data_source_req_chan = Broadcast[ComponentMetricRequest]("data-source-req")
    data_source_req_recv = data_source_req_chan.new_receiver()
    resampling_req_chan = Broadcast[ComponentMetricRequest]("resample-req")
    resampling_req_sender = resampling_req_chan.new_sender()

    resampling_actor = ComponentMetricsResamplingActor(
        channel_registry=channel_registry,
        subscription_sender=data_source_req_chan.new_sender(),
        subscription_receiver=resampling_req_chan.new_receiver(),
        resampling_period_s=0.2,
        max_data_age_in_periods=2,
    )

    subs_req = ComponentMetricRequest(
        namespace="Resampling",
        component_id=9,
        metric_id=ComponentMetricId.SOC,
        start_time=None,
    )

    await resampling_req_sender.send(subs_req)
    data_source_req = await data_source_req_recv.receive()
    assert data_source_req is not None
    assert data_source_req == dataclasses.replace(
        subs_req, namespace="Resampling:Source"
    )

    timeseries_receiver = channel_registry.new_receiver(subs_req.get_channel_name())
    timeseries_sender = channel_registry.new_sender(data_source_req.get_channel_name())

    fake_time.shift(0.2)
    new_sample = await timeseries_receiver.receive()  # At 0.2s (timer)
    assert new_sample is not None
    assert new_sample.value is None

    fake_time.shift(0.1)
    sample = Sample(_now(), 3)  # ts = 0.3s
    await timeseries_sender.send(sample)

    fake_time.shift(0.1)
    new_sample = await timeseries_receiver.receive()  # At 0.4s (timer)
    assert new_sample is not None
    assert new_sample.value == 3
    assert new_sample.timestamp >= sample.timestamp
    assert new_sample.timestamp == _now()

    fake_time.shift(0.05)
    sample = Sample(_now(), 4)  # ts = 0.45s
    await timeseries_sender.send(sample)
    fake_time.shift(0.15)
    new_sample = await timeseries_receiver.receive()  # At 0.6s (timer)
    assert new_sample is not None
    assert new_sample.value == 3.5  # avg(3, 4)
    assert new_sample.timestamp >= sample.timestamp

    fake_time.shift(0.05)
    await timeseries_sender.send(Sample(_now(), 8))  # ts = 0.65s
    fake_time.shift(0.05)
    await timeseries_sender.send(Sample(_now(), 1))  # ts = 0.7s
    fake_time.shift(0.05)
    sample = Sample(_now(), 9)  # ts = 0.75s
    await timeseries_sender.send(sample)
    fake_time.shift(0.05)
    new_sample = await timeseries_receiver.receive()  # At 0.8s (timer)
    assert new_sample is not None
    assert new_sample.value == 5.5  # avg(4, 8, 1, 9)
    assert new_sample.timestamp >= sample.timestamp

    # No more samples sent
    fake_time.shift(0.2)
    new_sample = await timeseries_receiver.receive()  # At 1.0s (timer)
    assert new_sample is not None
    assert new_sample.value == 6  # avg(8, 1, 9)
    assert new_sample.timestamp >= sample.timestamp

    # No more samples sent
    fake_time.shift(0.2)
    new_sample = await timeseries_receiver.receive()  # At 1.2s (timer)
    assert new_sample is not None
    assert new_sample.value is None

    await resampling_actor._stop()  # type: ignore # pylint: disable=no-member,protected-access
    await resampling_actor._resampler.stop()  # pylint: disable=protected-access
