# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Mock data_pipeline."""


import asyncio
import math
from datetime import datetime

from frequenz.channels import Broadcast, Receiver, Sender
from frequenz.client.microgrid import ComponentMetricId
from pytest_mock import MockerFixture

from frequenz.sdk._internal._asyncio import cancel_and_await
from frequenz.sdk.microgrid._data_pipeline import _DataPipeline
from frequenz.sdk.microgrid._data_sourcing import ComponentMetricRequest
from frequenz.sdk.timeseries import ResamplerConfig, Sample
from frequenz.sdk.timeseries._quantities import Power, Quantity
from frequenz.sdk.timeseries.formula_engine._formula_generators._formula_generator import (
    NON_EXISTING_COMPONENT_ID,
)

# pylint: disable=too-many-instance-attributes disable=too-many-locals


class MockResampler:
    """Mock resampler."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        mocker: MockerFixture,
        resampler_config: ResamplerConfig,
        bat_inverter_ids: list[int],
        pv_inverter_ids: list[int],
        evc_ids: list[int],
        chp_ids: list[int],
        meter_ids: list[int],
        namespaces: int,
    ) -> None:
        """Create a `MockDataPipeline` instance."""
        self._data_pipeline = _DataPipeline(
            resampler_config, pv_fallback_power=Power.zero()
        )

        self._channel_registry = self._data_pipeline._channel_registry
        self._resampler_request_channel = Broadcast[ComponentMetricRequest](
            name="resampler-request"
        )
        self._input_channels_receivers: dict[str, list[Receiver[Sample[Quantity]]]] = {}

        def power_senders(
            comp_ids: list[int],
        ) -> list[Sender[Sample[Quantity]]]:
            senders: list[Sender[Sample[Quantity]]] = []
            for comp_id in comp_ids:
                name = f"{comp_id}:{ComponentMetricId.ACTIVE_POWER}"
                senders.append(
                    self._channel_registry.get_or_create(
                        Sample[Quantity], name
                    ).new_sender()
                )
                self._input_channels_receivers[name] = [
                    self._channel_registry.get_or_create(
                        Sample[Quantity], name
                    ).new_receiver()
                    for _ in range(namespaces)
                ]
            return senders

        def frequency_senders(
            comp_ids: list[int],
        ) -> list[Sender[Sample[Quantity]]]:
            senders: list[Sender[Sample[Quantity]]] = []
            for comp_id in comp_ids:
                name = f"{comp_id}:{ComponentMetricId.FREQUENCY}"
                senders.append(
                    self._channel_registry.get_or_create(
                        Sample[Quantity], name
                    ).new_sender()
                )
                self._input_channels_receivers[name] = [
                    self._channel_registry.get_or_create(
                        Sample[Quantity], name
                    ).new_receiver(name=name)
                    for _ in range(namespaces)
                ]
            return senders

        self._bat_inverter_power_senders = power_senders(bat_inverter_ids)
        self._bat_inverter_frequency_senders = frequency_senders(bat_inverter_ids)
        self._pv_inverter_power_senders = power_senders(pv_inverter_ids)
        self._ev_power_senders = power_senders(evc_ids)
        self._chp_power_senders = power_senders(chp_ids)
        self._meter_power_senders = power_senders(meter_ids)
        self._meter_frequency_senders = frequency_senders(meter_ids)
        self._non_existing_component_sender = power_senders(
            [NON_EXISTING_COMPONENT_ID]
        )[0]

        def multi_phase_senders(
            ids: list[int],
            metrics: tuple[ComponentMetricId, ComponentMetricId, ComponentMetricId],
        ) -> list[list[Sender[Sample[Quantity]]]]:
            senders: list[list[Sender[Sample[Quantity]]]] = []
            for comp_id in ids:
                p1_name = f"{comp_id}:{metrics[0]}"
                p2_name = f"{comp_id}:{metrics[1]}"
                p3_name = f"{comp_id}:{metrics[2]}"

                senders.append(
                    [
                        self._channel_registry.get_or_create(
                            Sample[Quantity], p1_name
                        ).new_sender(),
                        self._channel_registry.get_or_create(
                            Sample[Quantity], p2_name
                        ).new_sender(),
                        self._channel_registry.get_or_create(
                            Sample[Quantity], p3_name
                        ).new_sender(),
                    ]
                )
                self._input_channels_receivers[p1_name] = [
                    self._channel_registry.get_or_create(
                        Sample[Quantity], p1_name
                    ).new_receiver()
                    for _ in range(namespaces)
                ]
                self._input_channels_receivers[p2_name] = [
                    self._channel_registry.get_or_create(
                        Sample[Quantity], p2_name
                    ).new_receiver()
                    for _ in range(namespaces)
                ]
                self._input_channels_receivers[p3_name] = [
                    self._channel_registry.get_or_create(
                        Sample[Quantity], p3_name
                    ).new_receiver()
                    for _ in range(namespaces)
                ]
            return senders

        def current_senders(ids: list[int]) -> list[list[Sender[Sample[Quantity]]]]:
            return multi_phase_senders(
                ids,
                (
                    ComponentMetricId.CURRENT_PHASE_1,
                    ComponentMetricId.CURRENT_PHASE_2,
                    ComponentMetricId.CURRENT_PHASE_3,
                ),
            )

        def voltage_senders(ids: list[int]) -> list[list[Sender[Sample[Quantity]]]]:
            return multi_phase_senders(
                ids,
                (
                    ComponentMetricId.VOLTAGE_PHASE_1,
                    ComponentMetricId.VOLTAGE_PHASE_2,
                    ComponentMetricId.VOLTAGE_PHASE_3,
                ),
            )

        def power_3_phase_senders(
            ids: list[int],
        ) -> list[list[Sender[Sample[Quantity]]]]:
            return multi_phase_senders(
                ids,
                (
                    ComponentMetricId.ACTIVE_POWER_PHASE_1,
                    ComponentMetricId.ACTIVE_POWER_PHASE_2,
                    ComponentMetricId.ACTIVE_POWER_PHASE_3,
                ),
            )

        self._bat_inverter_current_senders = current_senders(bat_inverter_ids)
        self._pv_inverter_current_senders = current_senders(pv_inverter_ids)
        self._ev_current_senders = current_senders(evc_ids)
        self._chp_current_senders = current_senders(chp_ids)
        self._meter_current_senders = current_senders(meter_ids)

        self._meter_voltage_senders = voltage_senders(meter_ids)
        self._meter_power_3_phase_senders = power_3_phase_senders(meter_ids)

        self._next_ts = datetime.now()

        mocker.patch(
            "frequenz.sdk.microgrid._data_pipeline._DataPipeline"
            "._resampling_request_sender",
            self._resampling_request_sender,
        )
        mocker.patch(
            "frequenz.sdk.microgrid._data_pipeline._DATA_PIPELINE", self._data_pipeline
        )

        self._forward_tasks: dict[str, asyncio.Task[None]] = {}
        task = asyncio.create_task(self._handle_resampling_requests())
        task.add_done_callback(self._handle_task_done)
        self._request_handler_task = task

    def _handle_task_done(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        if exc := task.exception():
            raise SystemExit(f"Task {task.get_name()!r} failed: {exc}") from exc

    async def _stop(self) -> None:
        tasks_to_stop = [
            cancel_and_await(task)
            for task in list(self._forward_tasks.values())
            + [self._request_handler_task]
        ]
        await asyncio.gather(*tasks_to_stop)

    def _resampling_request_sender(self) -> Sender[ComponentMetricRequest]:
        return self._resampler_request_channel.new_sender()

    async def _channel_forward_messages(
        self, receiver: Receiver[Sample[Quantity]], sender: Sender[Sample[Quantity]]
    ) -> None:
        async for sample in receiver:
            await sender.send(sample)

    async def _handle_resampling_requests(self) -> None:
        async for request in self._resampler_request_channel.new_receiver():
            name = request.get_channel_name()
            if name in self._forward_tasks:
                continue
            input_chan_recv_name = f"{request.component_id}:{request.metric_id}"
            input_chan_recv = self._input_channels_receivers[input_chan_recv_name].pop()
            assert input_chan_recv is not None
            output_chan_sender: Sender[Sample[Quantity]] = (
                self._channel_registry.get_or_create(
                    Sample[Quantity], name
                ).new_sender()
            )
            task = asyncio.create_task(
                self._channel_forward_messages(
                    input_chan_recv,
                    output_chan_sender,
                ),
                name=name,
            )

            def _done_callback(task: asyncio.Task[None]) -> None:
                del self._forward_tasks[task.get_name()]
                self._handle_task_done(task)

            task.add_done_callback(_done_callback)
            self._forward_tasks[name] = task

    def make_sample(self, value: float | None) -> Sample[Quantity]:
        """Create a sample with the given value."""
        return Sample(
            self._next_ts,
            None if value is None or math.isnan(value) else Quantity(value),
        )

    async def send_meter_power(self, values: list[float | None]) -> None:
        """Send the given values as resampler output for meter power."""
        assert len(values) == len(self._meter_power_senders)
        for chan, value in zip(self._meter_power_senders, values):
            sample = self.make_sample(value)
            await chan.send(sample)

    async def send_chp_power(self, values: list[float | None]) -> None:
        """Send the given values as resampler output for CHP power."""
        assert len(values) == len(self._chp_power_senders)
        for chan, value in zip(self._chp_power_senders, values):
            sample = self.make_sample(value)
            await chan.send(sample)

    async def send_pv_inverter_power(self, values: list[float | None]) -> None:
        """Send the given values as resampler output for PV Inverter power."""
        assert len(values) == len(self._pv_inverter_power_senders)
        for chan, value in zip(self._pv_inverter_power_senders, values):
            sample = self.make_sample(value)
            await chan.send(sample)

    async def send_meter_frequency(self, values: list[float | None]) -> None:
        """Send the given values as resampler output for meter frequency."""
        assert len(values) == len(self._meter_frequency_senders)
        for sender, value in zip(self._meter_frequency_senders, values):
            sample = self.make_sample(value)
            await sender.send(sample)

    async def send_bat_inverter_frequency(self, values: list[float | None]) -> None:
        """Send the given values as resampler output for battery inverter frequency."""
        assert len(values) == len(self._bat_inverter_frequency_senders)
        for chan, value in zip(self._bat_inverter_frequency_senders, values):
            sample = self.make_sample(value)
            await chan.send(sample)

    async def send_evc_power(self, values: list[float | None]) -> None:
        """Send the given values as resampler output for EV Charger power."""
        assert len(values) == len(self._ev_power_senders)
        for chan, value in zip(self._ev_power_senders, values):
            sample = self.make_sample(value)
            await chan.send(sample)

    async def send_bat_inverter_power(self, values: list[float | None]) -> None:
        """Send the given values as resampler output for battery inverter power."""
        assert len(values) == len(self._bat_inverter_power_senders)
        for chan, value in zip(self._bat_inverter_power_senders, values):
            sample = self.make_sample(value)
            await chan.send(sample)

    async def send_non_existing_component_value(self) -> None:
        """Send a value for a non existing component."""
        sample = self.make_sample(None)
        await self._non_existing_component_sender.send(sample)

    async def send_evc_current(self, values: list[list[float | None]]) -> None:
        """Send the given values as resampler output for EV Charger current."""
        assert len(values) == len(self._ev_current_senders)
        for chan, evc_values in zip(self._ev_current_senders, values):
            assert len(evc_values) == 3  # 3 values for phases
            for phase, value in enumerate(evc_values):
                sample = self.make_sample(value)
                await chan[phase].send(sample)

    async def send_bat_inverter_current(self, values: list[list[float | None]]) -> None:
        """Send the given values as resampler output for battery inverter current."""
        assert len(values) == len(self._bat_inverter_current_senders)
        for chan, bat_values in zip(self._bat_inverter_current_senders, values):
            assert len(bat_values) == 3  # 3 values for phases
            for phase, value in enumerate(bat_values):
                sample = self.make_sample(value)
                await chan[phase].send(sample)

    async def send_meter_current(self, values: list[list[float | None]]) -> None:
        """Send the given values as resampler output for meter current."""
        assert len(values) == len(self._meter_current_senders)
        for chan, meter_values in zip(self._meter_current_senders, values):
            assert len(meter_values) == 3  # 3 values for phases
            for phase, value in enumerate(meter_values):
                sample = self.make_sample(value)
                await chan[phase].send(sample)

    async def send_meter_voltage(self, values: list[list[float | None]]) -> None:
        """Send the given values as resampler output for meter voltage."""
        assert len(values) == len(self._meter_voltage_senders)
        for chan, meter_values in zip(self._meter_voltage_senders, values):
            assert len(meter_values) == 3  # 3 values for phases
            for phase, value in enumerate(meter_values):
                sample = self.make_sample(value)
                await chan[phase].send(sample)

    async def send_meter_power_3_phase(self, values: list[list[float | None]]) -> None:
        """Send the given values as resampler output for meter active power."""
        assert len(values) == len(self._meter_power_3_phase_senders)
        for chan, meter_values in zip(self._meter_power_3_phase_senders, values):
            assert len(meter_values) == 3  # 3 values for phases
            for phase, value in enumerate(meter_values):
                sample = self.make_sample(value)
                await chan[phase].send(sample)
