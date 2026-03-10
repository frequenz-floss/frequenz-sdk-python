# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Frequenz Python SDK resampling example."""
import asyncio
from datetime import datetime, timedelta, timezone

import async_solipsism
import pytest
import time_machine
from frequenz.channels import Broadcast, OneshotChannel, Receiver, Sender
from frequenz.channels._broadcast import BroadcastReceiver
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.metrics import Metric
from frequenz.quantities import Quantity

from frequenz.sdk.microgrid._data_sourcing import ComponentMetricRequest
from frequenz.sdk.microgrid._resampling import ComponentMetricsResamplingActor
from frequenz.sdk.timeseries import ResamplerConfig2, Sample


@pytest.fixture(autouse=True)
def event_loop_policy() -> async_solipsism.EventLoopPolicy:
    """Return an event loop policy that uses the async solipsism event loop."""
    return async_solipsism.EventLoopPolicy()


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def _assert_resampling_works(
    timeseries_sender: Sender[Sample[Quantity]],
    timeseries_receiver: Receiver[Sample[Quantity]],
    fake_time: time_machine.Coordinates,
) -> None:
    fake_time.shift(0.2)
    new_sample = await timeseries_receiver.receive()  # At 0.2s (timer)
    assert new_sample == Sample(_now(), None)

    fake_time.shift(0.1)
    sample = Sample(_now(), Quantity(3))  # ts = 0.3s
    await timeseries_sender.send(sample)

    fake_time.shift(0.1)
    new_sample = await timeseries_receiver.receive()  # At 0.4s (timer)
    assert new_sample is not None and new_sample.value is not None
    assert new_sample.value.base_value == 3
    assert new_sample.timestamp >= sample.timestamp
    assert new_sample.timestamp == _now()

    fake_time.shift(0.05)
    sample = Sample(_now(), Quantity(4))  # ts = 0.45s
    await timeseries_sender.send(sample)
    fake_time.shift(0.15)
    new_sample = await timeseries_receiver.receive()  # At 0.6s (timer)
    assert new_sample is not None and new_sample.value is not None
    assert new_sample.value.base_value == 3.5  # avg(3, 4)
    assert new_sample.timestamp >= sample.timestamp
    assert new_sample.timestamp == _now()

    fake_time.shift(0.05)
    await timeseries_sender.send(Sample(_now(), Quantity(8)))  # ts = 0.65s
    fake_time.shift(0.05)
    await timeseries_sender.send(Sample(_now(), Quantity(1)))  # ts = 0.7s
    fake_time.shift(0.05)
    sample = Sample(_now(), Quantity(9))  # ts = 0.75s
    await timeseries_sender.send(sample)
    fake_time.shift(0.05)
    new_sample = await timeseries_receiver.receive()  # At 0.8s (timer)
    assert new_sample is not None and new_sample.value is not None
    assert new_sample.value.base_value == 5.5  # avg(4, 8, 1, 9)
    assert new_sample.timestamp >= sample.timestamp
    assert new_sample.timestamp == _now()

    # No more samples sent
    fake_time.shift(0.2)
    new_sample = await timeseries_receiver.receive()  # At 1.0s (timer)
    assert new_sample is not None and new_sample.value is not None
    assert new_sample.value.base_value == 6  # avg(8, 1, 9)
    assert new_sample.timestamp >= sample.timestamp
    assert new_sample.timestamp == _now()

    # No more samples sent
    fake_time.shift(0.2)
    new_sample = await timeseries_receiver.receive()  # At 1.2s (timer)
    assert new_sample is not None
    assert new_sample.value is None
    assert new_sample.timestamp == _now()


async def test_single_request(
    fake_time: time_machine.Coordinates,
) -> None:
    """Run main functions that initializes and creates everything."""
    data_source_req_chan = Broadcast[ComponentMetricRequest](name="data-source-req")
    data_source_req_recv = data_source_req_chan.new_receiver()
    resampling_req_chan = Broadcast[ComponentMetricRequest](name="resample-req")
    resampling_req_sender = resampling_req_chan.new_sender()

    async with ComponentMetricsResamplingActor(
        data_sourcing_request_sender=data_source_req_chan.new_sender(),
        resampling_request_receiver=resampling_req_chan.new_receiver(),
        config=ResamplerConfig2(
            resampling_period=timedelta(seconds=0.2),
            max_data_age_in_periods=2,
        ),
    ) as resampling_actor:
        telem_stream_sender, telem_stream_receiver = OneshotChannel[
            BroadcastReceiver[Sample[Quantity]]
        ]()
        subs_req = ComponentMetricRequest(
            namespace="Resampling",
            component_id=ComponentId(9),
            metric=Metric.BATTERY_SOC_PCT,
            start_time=None,
            telem_stream_sender=telem_stream_sender,
        )

        await resampling_req_sender.send(subs_req)
        data_source_req = await data_source_req_recv.receive()
        assert data_source_req is not None

        assert data_source_req.namespace == "Resampling:Source"
        assert data_source_req.component_id == ComponentId(9)
        assert data_source_req.metric == Metric.BATTERY_SOC_PCT
        assert data_source_req.start_time is None
        assert data_source_req.telem_stream_sender != telem_stream_sender

        # Create the telemetry stream on behalf of nonexisting data sourcing actor
        telem_stream: Broadcast[Sample[Quantity]] = Broadcast(name="Telemetry stream")
        await data_source_req.telem_stream_sender.send(telem_stream.new_receiver())

        await _assert_resampling_works(
            timeseries_sender=telem_stream.new_sender(),
            timeseries_receiver=await telem_stream_receiver.receive(),
            fake_time=fake_time,
        )

        await resampling_actor._resampler.stop()  # pylint: disable=protected-access


async def test_duplicate_request(
    fake_time: time_machine.Coordinates,
) -> None:
    """Run main functions that initializes and creates everything."""
    data_source_req_chan = Broadcast[ComponentMetricRequest](name="data-source-req")
    data_source_req_recv = data_source_req_chan.new_receiver()
    resampling_req_chan = Broadcast[ComponentMetricRequest](name="resample-req")
    resampling_req_sender = resampling_req_chan.new_sender()

    async with ComponentMetricsResamplingActor(
        data_sourcing_request_sender=data_source_req_chan.new_sender(),
        resampling_request_receiver=resampling_req_chan.new_receiver(),
        config=ResamplerConfig2(
            resampling_period=timedelta(seconds=0.2),
            max_data_age_in_periods=2,
        ),
    ) as resampling_actor:
        telem_stream_sender, telem_stream_receiver = OneshotChannel[
            BroadcastReceiver[Sample[Quantity]]
        ]()
        subs_req = ComponentMetricRequest(
            namespace="Resampling",
            component_id=ComponentId(9),
            metric=Metric.BATTERY_SOC_PCT,
            start_time=None,
            telem_stream_sender=telem_stream_sender,
        )

        await resampling_req_sender.send(subs_req)
        data_source_req = await data_source_req_recv.receive()

        # Send duplicate request
        await resampling_req_sender.send(subs_req)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(data_source_req_recv.receive(), timeout=0.1)

        # Create the telemetry stream on behalf of nonexisting data sourcing actor
        telem_stream: Broadcast[Sample[Quantity]] = Broadcast(name="Telemetry stream")
        await data_source_req.telem_stream_sender.send(telem_stream.new_receiver())

        await _assert_resampling_works(
            timeseries_sender=telem_stream.new_sender(),
            timeseries_receiver=await telem_stream_receiver.receive(),
            fake_time=fake_time,
        )

        await resampling_actor._resampler.stop()  # pylint: disable=protected-access


async def test_resubscribe(fake_time: time_machine.Coordinates) -> None:
    """Test that resampling works when e receiver resubscribes.

    For example, Coalesce may close its receivers and resubscribe to the
    same component later on. ComponentMetricsResamplingActor must provide
    a new receiver in that case.
    """
    data_source_req_chan = Broadcast[ComponentMetricRequest](name="data-source-req")
    data_source_req_recv = data_source_req_chan.new_receiver()
    resampling_req_chan = Broadcast[ComponentMetricRequest](name="resample-req")
    resampling_req_sender = resampling_req_chan.new_sender()

    async with ComponentMetricsResamplingActor(
        data_sourcing_request_sender=data_source_req_chan.new_sender(),
        resampling_request_receiver=resampling_req_chan.new_receiver(),
        config=ResamplerConfig2(
            resampling_period=timedelta(seconds=0.2),
            max_data_age_in_periods=2,
        ),
    ) as resampling_actor:

        async def send_metric_request() -> Receiver[Receiver[Sample[Quantity]]]:
            telem_stream_sender, telem_stream_receiver = OneshotChannel[
                BroadcastReceiver[Sample[Quantity]]
            ]()
            subs_req = ComponentMetricRequest(
                namespace="Resampling",
                component_id=ComponentId(9),
                metric=Metric.BATTERY_SOC_PCT,
                start_time=None,
                telem_stream_sender=telem_stream_sender,
            )
            await resampling_req_sender.send(subs_req)
            return telem_stream_receiver

        telem_stream_receiver = await send_metric_request()

        # Create the telemetry stream on behalf of nonexisting data sourcing actor
        data_source_req = await data_source_req_recv.receive()
        telem_stream: Broadcast[Sample[Quantity]] = Broadcast(name="Telemetry stream")
        await data_source_req.telem_stream_sender.send(telem_stream.new_receiver())

        resampled_stream_receiver = await telem_stream_receiver.receive()

        await _assert_resampling_works(
            timeseries_sender=telem_stream.new_sender(),
            timeseries_receiver=resampled_stream_receiver,
            fake_time=fake_time,
        )

        resampled_stream_receiver.close()

        # Resubscribe to the same metric data
        telem_stream_receiver = await send_metric_request()
        resampled_stream_receiver = await telem_stream_receiver.receive()

        # No need to answer the request in the data sourcing actor - The resampler answers

        # New subscriptions receive the latest resampled value immediately.
        # This must be drained for _assert_resampling_works to have a clean start.
        resent_sample = await resampled_stream_receiver.receive()
        assert resent_sample is not None
        assert resent_sample.value is None
        assert resent_sample.timestamp == _now()

        await _assert_resampling_works(
            timeseries_sender=telem_stream.new_sender(),
            timeseries_receiver=resampled_stream_receiver,
            fake_time=fake_time,
        )

        await resampling_actor._resampler.stop()  # pylint: disable=protected-access
