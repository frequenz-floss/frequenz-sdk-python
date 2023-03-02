import asyncio
from time import perf_counter
from typing import Tuple
from unittest.mock import patch

from frequenz.channels import Broadcast, ReceiverStoppedError
from pytest_mock import MockerFixture

from frequenz.sdk import microgrid
from frequenz.sdk.actor import (
    ChannelRegistry,
    ComponentMetricRequest,
    DataSourcingActor,
)
from frequenz.sdk.microgrid.component import ComponentMetricId
from tests.timeseries.mock_microgrid import MockMicrogrid
from tests.utils.mock_microgrid import MockMicrogridClient


def enable_mock_client(client: MockMicrogridClient) -> None:
    """Callback to enable the mock microgrid client.

    Args:
        client: the mock microgrid client to enable.
    """
    microgrid._microgrid._MICROGRID = client


@patch("frequenz.sdk.microgrid._microgrid._MICROGRID")
async def benchmark_data_sourcing(
    num_evc_chargers: int, num_msgs_per_battery: int, mock_microgrid: MockerFixture
) -> None:
    """Benchmark the data sourcing actor.

    Args:
        num_evc_chargers: number of EV Chargers to create for the mock microgrid.
        num_msgs_per_battery: number of messages to send out for each battery.

    Returns:
        A tuple of (number of messages sent, time taken to stream the messages).
    """
    COMPONENT_METRIC_IDS = [
        ComponentMetricId.CURRENT_PHASE_1,
        ComponentMetricId.CURRENT_PHASE_2,
        ComponentMetricId.CURRENT_PHASE_3,
    ]
    NUM_EXPECTED_MESSAGES = (
        num_evc_chargers * len(COMPONENT_METRIC_IDS) * num_msgs_per_battery
    )

    mock_grid = MockMicrogrid(
        grid_side_meter=False, num_values=num_msgs_per_battery, sample_rate_s=0.0
    )

    mock_grid.add_ev_chargers(num_evc_chargers)
    mock_grid.start_mock_client(enable_mock_client)

    request_channel = Broadcast[ComponentMetricRequest](
        "DataSourcingActor Request Channel"
    )

    channel_registry = ChannelRegistry(name="Microgrid Channel Registry")
    request_receiver = request_channel.new_receiver(
        "datasourcing-benchmark", maxsize=(num_evc_chargers * len(COMPONENT_METRIC_IDS))
    )
    request_sender = request_channel.new_sender()

    consume_tasks = []

    start_time = perf_counter()
    samples_sent = 0

    for evc_id in mock_grid.evc_ids:
        for component_metric_id in COMPONENT_METRIC_IDS:
            request = ComponentMetricRequest(
                "current_phase_requests", evc_id, component_metric_id, None
            )

            recv_channel = channel_registry.new_receiver(request.get_channel_name())

            async def consume(channel):
                while True:
                    try:
                        await channel.ready()
                        channel.consume()
                    except ReceiverStoppedError as e:
                        return

                    nonlocal samples_sent
                    samples_sent += 1

            await request_sender.send(request)
            consume_tasks.append(asyncio.create_task(consume(recv_channel)))

    DataSourcingActor(request_receiver, channel_registry)

    await asyncio.gather(*consume_tasks)

    time_taken = perf_counter() - start_time

    await mock_grid.cleanup()

    print(f"Samples Sent: {samples_sent}, time taken: {time_taken}")
    print(f"Samples per second: {samples_sent / time_taken}")
    print(
        f"Expected samples: {NUM_EXPECTED_MESSAGES}, missing: {NUM_EXPECTED_MESSAGES - samples_sent}"
    )
    print(
        f"Missing per EVC: {(NUM_EXPECTED_MESSAGES - samples_sent) / num_evc_chargers}"
    )


if __name__ == "__main__":
    import sys

    asyncio.run(benchmark_data_sourcing(int(sys.argv[1]), int(sys.argv[2])))
