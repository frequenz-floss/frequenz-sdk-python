# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Mock data_pipeline."""

from __future__ import annotations

import asyncio
from datetime import datetime

from frequenz.channels import Broadcast, Receiver, Sender
from pytest_mock import MockerFixture

from frequenz.sdk.actor import ComponentMetricRequest, ResamplerConfig
from frequenz.sdk.microgrid._data_pipeline import _DataPipeline
from frequenz.sdk.microgrid.component import ComponentMetricId
from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries._formula_engine._formula_generators._formula_generator import (
    NON_EXISTING_COMPONENT_ID,
)
from frequenz.sdk.timeseries._quantities import Quantity

# pylint: disable=too-many-instance-attributes


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
        self._data_pipeline = _DataPipeline(resampler_config)

        self._channel_registry = self._data_pipeline._channel_registry
        self._resampler_request_channel = Broadcast[ComponentMetricRequest](
            "resampler-request"
        )
        self._basic_receivers: dict[str, list[Receiver[Sample[Quantity]]]] = {}

        def power_senders(
            comp_ids: list[int],
        ) -> list[Sender[Sample[Quantity]]]:
            senders: list[Sender[Sample[Quantity]]] = []
            for comp_id in comp_ids:
                name = f"{comp_id}:{ComponentMetricId.ACTIVE_POWER}"
                senders.append(self._channel_registry.new_sender(name))
                self._basic_receivers[name] = [
                    self._channel_registry.new_receiver(name) for _ in range(namespaces)
                ]
            return senders

        self._bat_inverter_power_senders = power_senders(bat_inverter_ids)
        self._pv_inverter_power_senders = power_senders(pv_inverter_ids)
        self._ev_power_senders = power_senders(evc_ids)
        self._chp_power_senders = power_senders(chp_ids)
        self._meter_power_senders = power_senders(meter_ids)
        self._non_existing_component_sender = power_senders(
            [NON_EXISTING_COMPONENT_ID]
        )[0]

        def current_senders(ids: list[int]) -> list[list[Sender[Sample[Quantity]]]]:
            senders: list[list[Sender[Sample[Quantity]]]] = []
            for comp_id in ids:
                p1_name = f"{comp_id}:{ComponentMetricId.CURRENT_PHASE_1}"
                p2_name = f"{comp_id}:{ComponentMetricId.CURRENT_PHASE_2}"
                p3_name = f"{comp_id}:{ComponentMetricId.CURRENT_PHASE_3}"

                senders.append(
                    [
                        self._channel_registry.new_sender(p1_name),
                        self._channel_registry.new_sender(p2_name),
                        self._channel_registry.new_sender(p3_name),
                    ]
                )
                self._basic_receivers[p1_name] = [
                    self._channel_registry.new_receiver(p1_name)
                    for _ in range(namespaces)
                ]
                self._basic_receivers[p2_name] = [
                    self._channel_registry.new_receiver(p2_name)
                    for _ in range(namespaces)
                ]
                self._basic_receivers[p3_name] = [
                    self._channel_registry.new_receiver(p3_name)
                    for _ in range(namespaces)
                ]
            return senders

        self._bat_inverter_current_senders = current_senders(bat_inverter_ids)
        self._pv_inverter_current_senders = current_senders(pv_inverter_ids)
        self._ev_current_senders = current_senders(evc_ids)
        self._chp_current_senders = current_senders(chp_ids)
        self._meter_current_senders = current_senders(meter_ids)

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
        asyncio.create_task(self._handle_resampling_requests())

    def _resampling_request_sender(self) -> Sender[ComponentMetricRequest]:
        return self._resampler_request_channel.new_sender()

    async def _channel_forward_messages(
        self, receiver: Receiver[Sample[Quantity]], sender: Sender[Sample[Quantity]]
    ) -> None:
        async for sample in receiver:
            await sender.send(sample)

    async def _handle_resampling_requests(self) -> None:
        async for request in self._resampler_request_channel.new_receiver():
            if request.get_channel_name() in self._forward_tasks:
                continue
            basic_recv_name = f"{request.component_id}:{request.metric_id}"
            recv = self._basic_receivers[basic_recv_name].pop()
            assert recv is not None
            self._forward_tasks[request.get_channel_name()] = asyncio.create_task(
                self._channel_forward_messages(
                    recv,
                    self._channel_registry.new_sender(request.get_channel_name()),
                )
            )

    async def send_meter_power(self, values: list[float | None]) -> None:
        """Send the given values as resampler output for meter power."""
        assert len(values) == len(self._meter_power_senders)
        for chan, value in zip(self._meter_power_senders, values):
            sample = Sample(self._next_ts, None if not value else Quantity(value))
            await chan.send(sample)

    async def send_chp_power(self, values: list[float | None]) -> None:
        """Send the given values as resampler output for CHP power."""
        assert len(values) == len(self._chp_power_senders)
        for chan, value in zip(self._chp_power_senders, values):
            sample = Sample(self._next_ts, None if not value else Quantity(value))
            await chan.send(sample)

    async def send_pv_inverter_power(self, values: list[float | None]) -> None:
        """Send the given values as resampler output for PV Inverter power."""
        assert len(values) == len(self._pv_inverter_power_senders)
        for chan, value in zip(self._pv_inverter_power_senders, values):
            sample = Sample(self._next_ts, None if not value else Quantity(value))
            await chan.send(sample)

    async def send_evc_power(self, values: list[float | None]) -> None:
        """Send the given values as resampler output for EV Charger power."""
        assert len(values) == len(self._ev_power_senders)
        for chan, value in zip(self._ev_power_senders, values):
            sample = Sample(self._next_ts, None if not value else Quantity(value))
            await chan.send(sample)

    async def send_bat_inverter_power(self, values: list[float | None]) -> None:
        """Send the given values as resampler output for battery inverter power."""
        assert len(values) == len(self._bat_inverter_power_senders)
        for chan, value in zip(self._bat_inverter_power_senders, values):
            sample = Sample(self._next_ts, None if not value else Quantity(value))
            await chan.send(sample)

    async def send_non_existing_component_value(self) -> None:
        """Send a value for a non existing component."""
        sample = Sample[Quantity](self._next_ts, None)
        await self._non_existing_component_sender.send(sample)

    async def send_evc_current(self, values: list[list[float | None]]) -> None:
        """Send the given values as resampler output for EV Charger current."""
        assert len(values) == len(self._ev_current_senders)
        for chan, evc_values in zip(self._ev_current_senders, values):
            assert len(evc_values) == 3  # 3 values for phases
            for phase, value in enumerate(evc_values):
                sample = Sample(self._next_ts, None if not value else Quantity(value))
                await chan[phase].send(sample)

    async def send_bat_inverter_current(self, values: list[list[float | None]]) -> None:
        """Send the given values as resampler output for battery inverter current."""
        assert len(values) == len(self._bat_inverter_current_senders)
        for chan, bat_values in zip(self._bat_inverter_current_senders, values):
            assert len(bat_values) == 3  # 3 values for phases
            for phase, value in enumerate(bat_values):
                sample = Sample(self._next_ts, None if not value else Quantity(value))
                await chan[phase].send(sample)

    async def send_meter_current(self, values: list[list[float | None]]) -> None:
        """Send the given values as resampler output for meter current."""
        assert len(values) == len(self._meter_current_senders)
        for chan, meter_values in zip(self._meter_current_senders, values):
            assert len(meter_values) == 3  # 3 values for phases
            for phase, value in enumerate(meter_values):
                sample = Sample(self._next_ts, None if not value else Quantity(value))
                await chan[phase].send(sample)
