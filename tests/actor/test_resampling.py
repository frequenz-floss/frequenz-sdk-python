"""Frequenz Python SDK resampling example.

Copyright
Copyright © 2022 Frequenz Energy-as-a-Service GmbH

License
MIT
"""
import dataclasses
from datetime import datetime, timedelta, timezone

import time_machine
from frequenz.channels import Broadcast

from frequenz.sdk.actor import ChannelRegistry
from frequenz.sdk.actor.resampling import ComponentMetricsResamplingActor
from frequenz.sdk.data_pipeline import ComponentMetricId, ComponentMetricRequest
from frequenz.sdk.timeseries import Sample


def _now(*, shift: float = 0.0) -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=shift)


@time_machine.travel(0)
async def test_component_metrics_resampling_actor() -> None:
    """Run main functions that initializes and creates everything."""

    channel_registry = ChannelRegistry(name="test")
    data_source_req_chan = Broadcast[ComponentMetricRequest]("data-source-req")
    data_source_req_recv = data_source_req_chan.get_receiver()
    resampling_req_chan = Broadcast[ComponentMetricRequest]("resample-req")
    resampling_req_sender = resampling_req_chan.get_sender()

    resampling_actor = ComponentMetricsResamplingActor(
        channel_registry=channel_registry,
        subscription_sender=data_source_req_chan.get_sender(),
        subscription_receiver=resampling_req_chan.get_receiver(),
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
    assert data_source_req == dataclasses.replace(subs_req, namespace="Source")

    timeseries_receiver = channel_registry.get_receiver(subs_req.get_channel_name())
    timeseries_sender = channel_registry.get_sender(data_source_req.get_channel_name())

    new_sample = await timeseries_receiver.receive()  # At ~0.2s (timer)
    assert new_sample is not None
    assert new_sample.value is None

    sample = Sample(_now(shift=0.1), 3)  # ts = ~0.3s
    await timeseries_sender.send(sample)
    new_sample = await timeseries_receiver.receive()  # At ~0.4s (timer)
    assert new_sample is not None
    assert new_sample.value == 3
    assert new_sample.timestamp >= sample.timestamp

    sample = Sample(_now(shift=0.05), 4)  # ts = ~0.45s
    await timeseries_sender.send(sample)
    new_sample = await timeseries_receiver.receive()  # At ~0.6s (timer)
    assert new_sample is not None
    assert new_sample.value == 3.5  # avg(3, 4)
    assert new_sample.timestamp >= sample.timestamp

    await timeseries_sender.send(Sample(_now(shift=0.05), 8))  # ts = ~0.65s
    await timeseries_sender.send(Sample(_now(shift=0.1), 1))  # ts = ~0.7s
    sample = Sample(_now(shift=0.15), 9)  # ts = ~0.75s
    await timeseries_sender.send(sample)
    new_sample = await timeseries_receiver.receive()  # At ~0.8s (timer)
    assert new_sample is not None
    assert new_sample.value == 5.5  # avg(4, 8, 1, 9)
    assert new_sample.timestamp >= sample.timestamp

    # No more samples sent
    new_sample = await timeseries_receiver.receive()  # At ~1.0s (timer)
    assert new_sample is not None
    assert new_sample.value == 6  # avg(8, 1, 9)
    assert new_sample.timestamp >= sample.timestamp

    # No more samples sent
    new_sample = await timeseries_receiver.receive()  # At ~1.2s (timer)
    assert new_sample is not None
    assert new_sample.value is None

    await resampling_actor._stop()  # type: ignore # pylint: disable=no-member, protected-access
