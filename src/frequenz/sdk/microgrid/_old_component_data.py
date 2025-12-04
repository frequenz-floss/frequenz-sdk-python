# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Component data types for data coming from a microgrid.

This is a transitional module for migrating from the microgrid API v0.15 to v0.17.
It maps the new component data types to the old ones, so the rest of the code can
be updated incrementally.

This module should be removed once the migration is complete.
"""

# pylint: disable=too-many-lines,fixme

from __future__ import annotations

import logging
import math
from abc import ABC, abstractmethod
from collections.abc import Set
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import ClassVar, Self, TypeAlias, TypeGuard, TypeVar, assert_never, cast

from frequenz.channels import Receiver
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid import MicrogridApiClient
from frequenz.client.microgrid.component import (
    ComponentCategory,
    ComponentDataSamples,
    ComponentErrorCode,
    ComponentStateCode,
    ComponentStateSample,
)
from frequenz.client.microgrid.metrics import Bounds, Metric, MetricSample
from typing_extensions import override

_logger = logging.getLogger(__name__)

T = TypeVar("T", bound="ComponentData")

PhaseTuple: TypeAlias = tuple[float, float, float]

DATA_STREAM_BUFFER_SIZE: int = 50


class TransitionalMetric(Enum):
    """An enum representing the metrics we had in v0.15 but are not a metric in v0.17."""

    SOC_LOWER_BOUND = "soc_lower_bound"
    """Lower bound of state of charge."""
    SOC_UPPER_BOUND = "soc_upper_bound"
    """Upper bound of state of charge."""

    POWER_INCLUSION_LOWER_BOUND = "power_inclusion_lower_bound"
    """Power inclusion lower bound."""
    POWER_EXCLUSION_LOWER_BOUND = "power_exclusion_lower_bound"
    """Power exclusion lower bound."""
    POWER_EXCLUSION_UPPER_BOUND = "power_exclusion_upper_bound"
    """Power exclusion upper bound."""
    POWER_INCLUSION_UPPER_BOUND = "power_inclusion_upper_bound"
    """Power inclusion upper bound."""

    ACTIVE_POWER_INCLUSION_LOWER_BOUND = "active_power_inclusion_lower_bound"
    """Active power inclusion lower bound."""
    ACTIVE_POWER_EXCLUSION_LOWER_BOUND = "active_power_exclusion_lower_bound"
    """Active power exclusion lower bound."""
    ACTIVE_POWER_EXCLUSION_UPPER_BOUND = "active_power_exclusion_upper_bound"
    """Active power exclusion upper bound."""
    ACTIVE_POWER_INCLUSION_UPPER_BOUND = "active_power_inclusion_upper_bound"
    """Active power inclusion upper bound."""


@dataclass(kw_only=True)
class ComponentData(ABC):
    """A private base class for strongly typed component data classes."""

    component_id: ComponentId
    """The ID identifying this component in the microgrid."""

    timestamp: datetime
    """The timestamp of when the data was measured."""

    states: Set[ComponentStateCode | int] = frozenset()
    """The states of the component."""

    warnings: Set[ComponentErrorCode | int] = frozenset()
    """The warnings of the component."""

    errors: Set[ComponentErrorCode | int] = frozenset()
    """The errors of the component."""

    CATEGORY: ClassVar[ComponentCategory] = ComponentCategory.UNSPECIFIED
    """The category of this component."""

    METRICS: ClassVar[frozenset[Metric]] = frozenset()
    """The metrics of this component."""

    @abstractmethod
    def to_samples(self: Self) -> ComponentDataSamples:
        """Convert the component data to a component data object."""

    @staticmethod
    def _from_samples(class_: type[T], /, samples: ComponentDataSamples) -> T:
        """Create a new instance from a component data object."""
        if not samples.metric_samples:
            raise ValueError("No metrics in the samples.")

        # FIXME: This might not be true forever, but the service sends all metrics with
        # the same timestamp for now, and it is very convenient to map the received data
        # to the old component data metrics packets, which had only one timestamp.
        # When we move away frome these old_component_data wrappers, we should not
        # assume only one metric sample can come per telemetry message anymore.
        timestamp = samples.metric_samples[-1].sampled_at
        for sample in samples.metric_samples[:-1]:
            if sample.sampled_at != timestamp:
                _logger.warning(
                    "ComponentData has multiple timestamps. Using the last one. Samples: %r",
                    samples,
                )
                break

        if not samples.states:
            return class_(component_id=samples.component_id, timestamp=timestamp)

        # FIXME: Same as with metric samples, for now we get only one, and it is super
        # convenient to map to component data, but we should not assume this when moving
        # away from the legacy component data wrappers.
        if len(samples.states) > 1:
            _logger.warning(
                "ComponentData has more than one state. Using the last one. States: %r",
                samples.states,
            )

        return class_(
            component_id=samples.component_id,
            timestamp=timestamp,
            states=samples.states[-1].states,
            warnings=samples.states[-1].warnings,
            errors=samples.states[-1].errors,
        )

    @classmethod
    @abstractmethod
    def from_samples(cls, samples: ComponentDataSamples) -> Self:
        """Create a new instance from a component data object."""

    @classmethod
    def _check_category(cls, component_id: ComponentId) -> None:
        """Check if the given component_id is of the expected type.

        Args:
            component_id: Component id to check.

        Raises:
            ValueError: if the given id is unknown or has a different type.
        """
        # pylint: disable-next=import-outside-toplevel,cyclic-import
        from .. import microgrid

        components = microgrid.connection_manager.get().component_graph.components(
            matching_ids=component_id
        )
        if not components:
            raise ValueError(f"Unable to find component with {component_id}")
        if len(components) > 1:
            raise ValueError(f"Multiple components with id {component_id}")
        component = components.pop()
        if component.category != cls.CATEGORY:
            raise ValueError(
                f"Component with {component_id} is a {component.category}, "
                f"not a {cls.CATEGORY}."
            )

    @classmethod
    def subscribe(
        cls,
        api_client: MicrogridApiClient,
        component_id: ComponentId,
        *,
        buffer_size: int = DATA_STREAM_BUFFER_SIZE,
    ) -> Receiver[Self]:
        """Subscribe to the component data stream."""
        cls._check_category(component_id)

        def _is_valid(messages: Self | Exception) -> TypeGuard[Self]:
            return not isinstance(messages, Exception)

        receiver = api_client.receive_component_data_samples_stream(
            component_id, cls.METRICS, buffer_size=buffer_size
        )

        return receiver.map(cls._receive_logging_errors).filter(_is_valid)

    # This needs to be a classmethod because otherwise it seems like mypy can't
    # guarantee that the Self returned by this function is the same Self in the
    # subscribe() method.
    @classmethod
    def _receive_logging_errors(
        cls, samples: ComponentDataSamples, /
    ) -> Self | Exception:
        try:
            return cls.from_samples(samples)
        except Exception as exc:  # pylint: disable=broad-except
            _logger.exception(
                "Error while creating %r from samples: %r", cls.__name__, samples
            )
            return exc


@dataclass(kw_only=True)
class MeterData(ComponentData):
    """A wrapper class for holding meter data."""

    # FIXME: All of this have now a default of 0.0 because this is what it was doing when
    # we used the API v0.15, as we accessed the fields without checking if the fields
    # really existed, so the default protobuf value of 0.0 for floats was used.
    # When moving away from these legacy component data wrappers, this should not be
    # a problem anymore, as we will just get the data we get.
    active_power: float = 0.0
    """The total active 3-phase AC power, in Watts (W).

    Represented in the passive sign convention.

    * Positive means consumption from the grid.
    * Negative means supply into the grid.
    """

    active_power_per_phase: PhaseTuple = (0.0, 0.0, 0.0)
    """The per-phase AC active power for phase 1, 2, and 3 respectively, in Watt (W).

    Represented in the passive sign convention.

    * Positive means consumption from the grid.
    * Negative means supply into the grid.
    """

    reactive_power: float = 0.0
    """The total reactive 3-phase AC power, in Volt-Ampere Reactive (VAr).

    * Positive power means capacitive (current leading w.r.t. voltage).
    * Negative power means inductive (current lagging w.r.t. voltage).
    """

    reactive_power_per_phase: PhaseTuple = (0.0, 0.0, 0.0)
    """The per-phase AC reactive power, in Volt-Ampere Reactive (VAr).

    The provided values are for phase 1, 2, and 3 respectively.

    * Positive power means capacitive (current leading w.r.t. voltage).
    * Negative power means inductive (current lagging w.r.t. voltage).
    """

    current_per_phase: PhaseTuple = (0.0, 0.0, 0.0)
    """AC current in Amperes (A) for phase/line 1,2 and 3 respectively.

    Represented in the passive sign convention.

    * Positive means consumption from the grid.
    * Negative means supply into the grid.
    """

    voltage_per_phase: PhaseTuple = (0.0, 0.0, 0.0)
    """The ac voltage in volts (v) between the line and the neutral wire for phase/line
        1,2 and 3 respectively.
    """

    frequency: float = 0.0
    """The AC power frequency in Hertz (Hz)."""

    CATEGORY: ClassVar[ComponentCategory] = ComponentCategory.METER
    """The category of this component."""

    METRICS: ClassVar[frozenset[Metric]] = frozenset(
        [
            Metric.AC_ACTIVE_POWER,
            Metric.AC_ACTIVE_POWER_PHASE_1,
            Metric.AC_ACTIVE_POWER_PHASE_2,
            Metric.AC_ACTIVE_POWER_PHASE_3,
            Metric.AC_REACTIVE_POWER,
            Metric.AC_REACTIVE_POWER_PHASE_1,
            Metric.AC_REACTIVE_POWER_PHASE_2,
            Metric.AC_REACTIVE_POWER_PHASE_3,
            Metric.AC_CURRENT_PHASE_1,
            Metric.AC_CURRENT_PHASE_2,
            Metric.AC_CURRENT_PHASE_3,
            Metric.AC_VOLTAGE_PHASE_1_N,
            Metric.AC_VOLTAGE_PHASE_2_N,
            Metric.AC_VOLTAGE_PHASE_3_N,
            Metric.AC_FREQUENCY,
        ]
    )
    """The metrics of this component."""

    @override
    @classmethod
    # pylint: disable-next=too-many-branches
    def from_samples(cls, samples: ComponentDataSamples) -> Self:
        """Create a new instance from a component data object."""
        if not samples.metric_samples:
            raise ValueError("No metrics in the samples.")

        self = cls._from_samples(cls, samples)

        active_power_per_phase: list[float] = [0.0, 0.0, 0.0]
        reactive_power_per_phase: list[float] = [0.0, 0.0, 0.0]
        current_per_phase: list[float] = [0.0, 0.0, 0.0]
        voltage_per_phase: list[float] = [0.0, 0.0, 0.0]

        for sample in samples.metric_samples:
            match sample.metric:
                case Metric.AC_ACTIVE_POWER:
                    self.active_power = sample.as_single_value() or 0.0
                case Metric.AC_ACTIVE_POWER_PHASE_1:
                    active_power_per_phase[0] = sample.as_single_value() or 0.0
                case Metric.AC_ACTIVE_POWER_PHASE_2:
                    active_power_per_phase[1] = sample.as_single_value() or 0.0
                case Metric.AC_ACTIVE_POWER_PHASE_3:
                    active_power_per_phase[2] = sample.as_single_value() or 0.0
                case Metric.AC_REACTIVE_POWER_PHASE_1:
                    reactive_power_per_phase[0] = sample.as_single_value() or 0.0
                case Metric.AC_REACTIVE_POWER_PHASE_2:
                    reactive_power_per_phase[1] = sample.as_single_value() or 0.0
                case Metric.AC_REACTIVE_POWER_PHASE_3:
                    reactive_power_per_phase[2] = sample.as_single_value() or 0.0
                case Metric.AC_REACTIVE_POWER:
                    self.reactive_power = sample.as_single_value() or 0.0
                case Metric.AC_CURRENT_PHASE_1:
                    current_per_phase[0] = sample.as_single_value() or 0.0
                case Metric.AC_CURRENT_PHASE_2:
                    current_per_phase[1] = sample.as_single_value() or 0.0
                case Metric.AC_CURRENT_PHASE_3:
                    current_per_phase[2] = sample.as_single_value() or 0.0
                case Metric.AC_VOLTAGE_PHASE_1_N:
                    voltage_per_phase[0] = sample.as_single_value() or 0.0
                case Metric.AC_VOLTAGE_PHASE_2_N:
                    voltage_per_phase[1] = sample.as_single_value() or 0.0
                case Metric.AC_VOLTAGE_PHASE_3_N:
                    voltage_per_phase[2] = sample.as_single_value() or 0.0
                case Metric.AC_FREQUENCY:
                    self.frequency = sample.as_single_value() or 0.0
                case unexpected:
                    _logger.warning(
                        "Unexpected metric %s in meter data sample: %r",
                        unexpected,
                        sample,
                    )

        self.active_power_per_phase = cast(PhaseTuple, tuple(active_power_per_phase))
        self.reactive_power_per_phase = cast(
            PhaseTuple, tuple(reactive_power_per_phase)
        )
        self.current_per_phase = cast(PhaseTuple, tuple(current_per_phase))
        self.voltage_per_phase = cast(PhaseTuple, tuple(voltage_per_phase))

        return self

    @override
    def to_samples(self) -> ComponentDataSamples:
        """Convert the component data to a component data object."""
        return ComponentDataSamples(
            component_id=self.component_id,
            metric_samples=[
                MetricSample(
                    sampled_at=self.timestamp, metric=metric, value=value, bounds=[]
                )
                for metric, value in [
                    (Metric.AC_ACTIVE_POWER, self.active_power),
                    (Metric.AC_ACTIVE_POWER_PHASE_1, self.active_power_per_phase[0]),
                    (Metric.AC_ACTIVE_POWER_PHASE_2, self.active_power_per_phase[1]),
                    (Metric.AC_ACTIVE_POWER_PHASE_3, self.active_power_per_phase[2]),
                    (Metric.AC_REACTIVE_POWER, self.reactive_power),
                    (
                        Metric.AC_REACTIVE_POWER_PHASE_1,
                        self.reactive_power_per_phase[0],
                    ),
                    (
                        Metric.AC_REACTIVE_POWER_PHASE_2,
                        self.reactive_power_per_phase[1],
                    ),
                    (
                        Metric.AC_REACTIVE_POWER_PHASE_3,
                        self.reactive_power_per_phase[2],
                    ),
                    (Metric.AC_CURRENT_PHASE_1, self.current_per_phase[0]),
                    (Metric.AC_CURRENT_PHASE_2, self.current_per_phase[1]),
                    (Metric.AC_CURRENT_PHASE_3, self.current_per_phase[2]),
                    (Metric.AC_VOLTAGE_PHASE_1_N, self.voltage_per_phase[0]),
                    (Metric.AC_VOLTAGE_PHASE_2_N, self.voltage_per_phase[1]),
                    (Metric.AC_VOLTAGE_PHASE_3_N, self.voltage_per_phase[2]),
                    (Metric.AC_FREQUENCY, self.frequency),
                ]
            ],
            states=[
                ComponentStateSample(
                    sampled_at=self.timestamp,
                    states=frozenset(self.states),
                    warnings=frozenset(self.warnings),
                    errors=frozenset(self.errors),
                )
            ],
        )


@dataclass(kw_only=True)
class BatteryData(ComponentData):  # pylint: disable=too-many-instance-attributes
    """A wrapper class for holding battery data."""

    soc: float = 0.0
    """Battery's overall SoC in percent (%)."""

    soc_lower_bound: float = 0.0
    """The SoC below which discharge commands will be blocked by the system,
        in percent (%).
    """

    soc_upper_bound: float = 0.0
    """The SoC above which charge commands will be blocked by the system,
        in percent (%).
    """

    capacity: float = 0.0
    """The capacity of the battery in Wh (Watt-hour)."""

    power_inclusion_lower_bound: float = 0.0
    """Lower inclusion bound for battery power in watts.

    This is the lower limit of the range within which power requests are allowed for the
    battery.

    See [`frequenz.api.common.metrics_pb2.Metric.system_inclusion_bounds`][] and
    [`frequenz.api.common.metrics_pb2.Metric.system_exclusion_bounds`][] for more
    details.
    """

    power_exclusion_lower_bound: float = 0.0
    """Lower exclusion bound for battery power in watts.

    This is the lower limit of the range within which power requests are not allowed for
    the battery.

    See [`frequenz.api.common.metrics_pb2.Metric.system_inclusion_bounds`][] and
    [`frequenz.api.common.metrics_pb2.Metric.system_exclusion_bounds`][] for more
    details.
    """

    power_inclusion_upper_bound: float = 0.0
    """Upper inclusion bound for battery power in watts.

    This is the upper limit of the range within which power requests are allowed for the
    battery.

    See [`frequenz.api.common.metrics_pb2.Metric.system_inclusion_bounds`][] and
    [`frequenz.api.common.metrics_pb2.Metric.system_exclusion_bounds`][] for more
    details.
    """

    power_exclusion_upper_bound: float = 0.0
    """Upper exclusion bound for battery power in watts.

    This is the upper limit of the range within which power requests are not allowed for
    the battery.

    See [`frequenz.api.common.metrics_pb2.Metric.system_inclusion_bounds`][] and
    [`frequenz.api.common.metrics_pb2.Metric.system_exclusion_bounds`][] for more
    details.
    """

    temperature: float = 0.0
    """The (average) temperature reported by the battery, in Celsius (°C)."""

    CATEGORY: ClassVar[ComponentCategory] = ComponentCategory.BATTERY

    METRICS: ClassVar[frozenset[Metric]] = frozenset(
        [
            Metric.BATTERY_SOC_PCT,
            Metric.DC_POWER,
            Metric.BATTERY_CAPACITY,
            Metric.BATTERY_TEMPERATURE,
        ]
    )
    """The metrics of this component."""

    @override
    @classmethod
    def from_samples(cls, samples: ComponentDataSamples) -> Self:
        """Create a new instance from a component data object."""
        if not samples.metric_samples:
            raise ValueError("No metrics in the samples.")

        self = cls._from_samples(cls, samples)

        for sample in samples.metric_samples:
            value = sample.as_single_value() or 0.0
            match sample.metric:
                case Metric.BATTERY_SOC_PCT:
                    self.soc = value
                    if sample.bounds:
                        # Update power bounds from the SOC metric bounds,
                        # FIXME: We assume only one range is present
                        # If one bound is None, we assume 0 to match the previous
                        # behavior of v0.15, but this should eventually be fixed when
                        # moving away from these legacy wrappers.
                        if len(sample.bounds) > 1 and (
                            sample.bounds[1].lower != 0.0
                            or sample.bounds[1].upper != 0.0
                        ):
                            _logger.warning(
                                "Too many bounds found in sample, a maximum of 1 is "
                                "supported for SOC, using only the first: %r",
                                sample,
                            )
                        self.soc_lower_bound = sample.bounds[0].lower or 0.0
                        self.soc_upper_bound = sample.bounds[0].upper or 0.0
                case Metric.DC_POWER:
                    (
                        self.power_inclusion_lower_bound,
                        self.power_inclusion_upper_bound,
                        self.power_exclusion_lower_bound,
                        self.power_exclusion_upper_bound,
                    ) = _bound_ranges_to_inclusion_exclusion(
                        sample.bounds, "DC_POWER", sample
                    )
                case Metric.BATTERY_CAPACITY:
                    self.capacity = value
                case Metric.BATTERY_TEMPERATURE:
                    self.temperature = value
                case unexpected:
                    _logger.warning(
                        "Unexpected metric %s in battery data sample: %r",
                        unexpected,
                        sample,
                    )

        return self

    @override
    def to_samples(self) -> ComponentDataSamples:
        """Convert the component data to a component data object."""
        return ComponentDataSamples(
            component_id=self.component_id,
            metric_samples=[
                MetricSample(
                    sampled_at=self.timestamp, metric=metric, value=value, bounds=bounds
                )
                for metric, value, bounds in [
                    (
                        Metric.BATTERY_SOC_PCT,
                        self.soc,
                        _inclusion_exclusion_bounds_to_ranges(
                            self.soc_lower_bound, self.soc_upper_bound, 0.0, 0.0
                        ),
                    ),
                    (
                        Metric.DC_POWER,
                        None,
                        _inclusion_exclusion_bounds_to_ranges(
                            self.power_inclusion_lower_bound,
                            self.power_inclusion_upper_bound,
                            self.power_exclusion_lower_bound,
                            self.power_exclusion_upper_bound,
                        ),
                    ),
                    (Metric.BATTERY_CAPACITY, self.capacity, []),
                    (Metric.BATTERY_TEMPERATURE, self.temperature, []),
                ]
            ],
            states=[
                ComponentStateSample(
                    sampled_at=self.timestamp,
                    states=frozenset(self.states),
                    warnings=frozenset(self.warnings),
                    errors=frozenset(self.errors),
                )
            ],
        )


@dataclass(kw_only=True)
class InverterData(ComponentData):  # pylint: disable=too-many-instance-attributes
    """A wrapper class for holding inverter data."""

    active_power: float = 0.0
    """The total active 3-phase AC power, in Watts (W).

    Represented in the passive sign convention.

    * Positive means consumption from the grid.
    * Negative means supply into the grid.
    """

    active_power_per_phase: PhaseTuple = (0.0, 0.0, 0.0)
    """The per-phase AC active power for phase 1, 2, and 3 respectively, in Watt (W).

    Represented in the passive sign convention.

    * Positive means consumption from the grid.
    * Negative means supply into the grid.
    """

    reactive_power: float = 0.0
    """The total reactive 3-phase AC power, in Volt-Ampere Reactive (VAr).

    * Positive power means capacitive (current leading w.r.t. voltage).
    * Negative power means inductive (current lagging w.r.t. voltage).
    """

    reactive_power_per_phase: PhaseTuple = (0.0, 0.0, 0.0)
    """The per-phase AC reactive power, in Volt-Ampere Reactive (VAr).

    The provided values are for phase 1, 2, and 3 respectively.

    * Positive power means capacitive (current leading w.r.t. voltage).
    * Negative power means inductive (current lagging w.r.t. voltage).
    """

    current_per_phase: PhaseTuple = (0.0, 0.0, 0.0)
    """AC current in Amperes (A) for phase/line 1, 2 and 3 respectively.

    Represented in the passive sign convention.

    * Positive means consumption from the grid.
    * Negative means supply into the grid.
    """

    voltage_per_phase: PhaseTuple = (0.0, 0.0, 0.0)
    """The AC voltage in Volts (V) between the line and the neutral wire for
       phase/line 1, 2 and 3 respectively.
    """

    active_power_inclusion_lower_bound: float = 0.0
    """Lower inclusion bound for inverter power in watts.

    This is the lower limit of the range within which power requests are allowed for the
    inverter.

    See [`frequenz.api.common.metrics_pb2.Metric.system_inclusion_bounds`][] and
    [`frequenz.api.common.metrics_pb2.Metric.system_exclusion_bounds`][] for more
    details.
    """

    active_power_exclusion_lower_bound: float = 0.0
    """Lower exclusion bound for inverter power in watts.

    This is the lower limit of the range within which power requests are not allowed for
    the inverter.

    See [`frequenz.api.common.metrics_pb2.Metric.system_inclusion_bounds`][] and
    [`frequenz.api.common.metrics_pb2.Metric.system_exclusion_bounds`][] for more
    details.
    """

    active_power_inclusion_upper_bound: float = 0.0
    """Upper inclusion bound for inverter power in watts.

    This is the upper limit of the range within which power requests are allowed for the
    inverter.

    See [`frequenz.api.common.metrics_pb2.Metric.system_inclusion_bounds`][] and
    [`frequenz.api.common.metrics_pb2.Metric.system_exclusion_bounds`][] for more
    details.
    """

    active_power_exclusion_upper_bound: float = 0.0
    """Upper exclusion bound for inverter power in watts.

    This is the upper limit of the range within which power requests are not allowed for
    the inverter.

    See [`frequenz.api.common.metrics_pb2.Metric.system_inclusion_bounds`][] and
    [`frequenz.api.common.metrics_pb2.Metric.system_exclusion_bounds`][] for more
    details.
    """

    frequency: float = 0.0
    """AC frequency, in Hertz (Hz)."""

    CATEGORY: ClassVar[ComponentCategory] = ComponentCategory.INVERTER

    METRICS: ClassVar[frozenset[Metric]] = frozenset(
        [
            Metric.AC_ACTIVE_POWER,
            Metric.AC_ACTIVE_POWER_PHASE_1,
            Metric.AC_ACTIVE_POWER_PHASE_2,
            Metric.AC_ACTIVE_POWER_PHASE_3,
            Metric.AC_REACTIVE_POWER,
            Metric.AC_REACTIVE_POWER_PHASE_1,
            Metric.AC_REACTIVE_POWER_PHASE_2,
            Metric.AC_REACTIVE_POWER_PHASE_3,
            Metric.AC_CURRENT_PHASE_1,
            Metric.AC_CURRENT_PHASE_2,
            Metric.AC_CURRENT_PHASE_3,
            Metric.AC_VOLTAGE_PHASE_1_N,
            Metric.AC_VOLTAGE_PHASE_2_N,
            Metric.AC_VOLTAGE_PHASE_3_N,
            Metric.AC_FREQUENCY,
        ]
    )
    """The metrics of this component."""

    @override
    @classmethod
    # pylint: disable-next=too-many-branches
    def from_samples(cls, samples: ComponentDataSamples) -> Self:
        """Create a new instance from a component data object."""
        if not samples.metric_samples:
            raise ValueError("No metrics in the samples.")

        self = cls._from_samples(cls, samples)

        active_power_per_phase: list[float] = [0.0, 0.0, 0.0]
        reactive_power_per_phase: list[float] = [0.0, 0.0, 0.0]
        current_per_phase: list[float] = [0.0, 0.0, 0.0]
        voltage_per_phase: list[float] = [0.0, 0.0, 0.0]

        for sample in samples.metric_samples:
            value = sample.as_single_value() or 0.0
            match sample.metric:
                case Metric.AC_ACTIVE_POWER:
                    self.active_power = value
                    (
                        self.active_power_inclusion_lower_bound,
                        self.active_power_inclusion_upper_bound,
                        self.active_power_exclusion_lower_bound,
                        self.active_power_exclusion_upper_bound,
                    ) = _bound_ranges_to_inclusion_exclusion(
                        sample.bounds, "AC_ACTIVE_POWER", sample
                    )
                case Metric.AC_ACTIVE_POWER_PHASE_1:
                    active_power_per_phase[0] = value
                case Metric.AC_ACTIVE_POWER_PHASE_2:
                    active_power_per_phase[1] = value
                case Metric.AC_ACTIVE_POWER_PHASE_3:
                    active_power_per_phase[2] = value
                case Metric.AC_REACTIVE_POWER:
                    self.reactive_power = value
                case Metric.AC_REACTIVE_POWER_PHASE_1:
                    reactive_power_per_phase[0] = value
                case Metric.AC_REACTIVE_POWER_PHASE_2:
                    reactive_power_per_phase[1] = value
                case Metric.AC_REACTIVE_POWER_PHASE_3:
                    reactive_power_per_phase[2] = value
                case Metric.AC_CURRENT_PHASE_1:
                    current_per_phase[0] = value
                case Metric.AC_CURRENT_PHASE_2:
                    current_per_phase[1] = value
                case Metric.AC_CURRENT_PHASE_3:
                    current_per_phase[2] = value
                case Metric.AC_VOLTAGE_PHASE_1_N:
                    voltage_per_phase[0] = value
                case Metric.AC_VOLTAGE_PHASE_2_N:
                    voltage_per_phase[1] = value
                case Metric.AC_VOLTAGE_PHASE_3_N:
                    voltage_per_phase[2] = value
                case Metric.AC_FREQUENCY:
                    self.frequency = value
                case unexpected:
                    _logger.warning(
                        "Unexpected metric %s in inverter data sample: %r",
                        unexpected,
                        sample,
                    )

        self.active_power_per_phase = cast(PhaseTuple, tuple(active_power_per_phase))
        self.reactive_power_per_phase = cast(
            PhaseTuple, tuple(reactive_power_per_phase)
        )
        self.current_per_phase = cast(PhaseTuple, tuple(current_per_phase))
        self.voltage_per_phase = cast(PhaseTuple, tuple(voltage_per_phase))

        return self

    @override
    def to_samples(self) -> ComponentDataSamples:
        """Convert the component data to a component data object."""
        return ComponentDataSamples(
            component_id=self.component_id,
            metric_samples=[
                MetricSample(
                    sampled_at=self.timestamp,
                    metric=Metric.AC_ACTIVE_POWER,
                    value=self.active_power,
                    bounds=_inclusion_exclusion_bounds_to_ranges(
                        self.active_power_inclusion_lower_bound,
                        self.active_power_inclusion_upper_bound,
                        self.active_power_exclusion_lower_bound,
                        self.active_power_exclusion_upper_bound,
                    ),
                ),
                *(
                    MetricSample(
                        sampled_at=self.timestamp, metric=metric, value=value, bounds=[]
                    )
                    for metric, value in [
                        (
                            Metric.AC_ACTIVE_POWER_PHASE_1,
                            self.active_power_per_phase[0],
                        ),
                        (
                            Metric.AC_ACTIVE_POWER_PHASE_2,
                            self.active_power_per_phase[1],
                        ),
                        (
                            Metric.AC_ACTIVE_POWER_PHASE_3,
                            self.active_power_per_phase[2],
                        ),
                        (Metric.AC_REACTIVE_POWER, self.reactive_power),
                        (
                            Metric.AC_REACTIVE_POWER_PHASE_1,
                            self.reactive_power_per_phase[0],
                        ),
                        (
                            Metric.AC_REACTIVE_POWER_PHASE_2,
                            self.reactive_power_per_phase[1],
                        ),
                        (
                            Metric.AC_REACTIVE_POWER_PHASE_3,
                            self.reactive_power_per_phase[2],
                        ),
                        (Metric.AC_CURRENT_PHASE_1, self.current_per_phase[0]),
                        (Metric.AC_CURRENT_PHASE_2, self.current_per_phase[1]),
                        (Metric.AC_CURRENT_PHASE_3, self.current_per_phase[2]),
                        (Metric.AC_VOLTAGE_PHASE_1_N, self.voltage_per_phase[0]),
                        (Metric.AC_VOLTAGE_PHASE_2_N, self.voltage_per_phase[1]),
                        (Metric.AC_VOLTAGE_PHASE_3_N, self.voltage_per_phase[2]),
                        (Metric.AC_FREQUENCY, self.frequency),
                    ]
                ),
            ],
            states=[
                ComponentStateSample(
                    sampled_at=self.timestamp,
                    states=frozenset(self.states),
                    warnings=frozenset(self.warnings),
                    errors=frozenset(self.errors),
                )
            ],
        )


@dataclass(kw_only=True)
class EVChargerData(ComponentData):  # pylint: disable=too-many-instance-attributes
    """A wrapper class for holding ev_charger data."""

    active_power: float = 0.0
    """The total active 3-phase AC power, in Watts (W).

    Represented in the passive sign convention.

    * Positive means consumption from the grid.
    * Negative means supply into the grid.
    """

    active_power_per_phase: PhaseTuple = (0.0, 0.0, 0.0)
    """The per-phase AC active power for phase 1, 2, and 3 respectively, in Watt (W).

    Represented in the passive sign convention.

    * Positive means consumption from the grid.
    * Negative means supply into the grid.
    """

    current_per_phase: PhaseTuple = (0.0, 0.0, 0.0)
    """AC current in Amperes (A) for phase/line 1,2 and 3 respectively.

    Represented in the passive sign convention.

    * Positive means consumption from the grid.
    * Negative means supply into the grid.
    """

    reactive_power: float = 0.0
    """The total reactive 3-phase AC power, in Volt-Ampere Reactive (VAr).

    * Positive power means capacitive (current leading w.r.t. voltage).
    * Negative power means inductive (current lagging w.r.t. voltage).
    """

    reactive_power_per_phase: PhaseTuple = (0.0, 0.0, 0.0)
    """The per-phase AC reactive power, in Volt-Ampere Reactive (VAr).

    The provided values are for phase 1, 2, and 3 respectively.

    * Positive power means capacitive (current leading w.r.t. voltage).
    * Negative power means inductive (current lagging w.r.t. voltage).
    """

    voltage_per_phase: PhaseTuple = (0.0, 0.0, 0.0)
    """The AC voltage in Volts (V) between the line and the neutral
        wire for phase/line 1,2 and 3 respectively.
    """

    active_power_inclusion_lower_bound: float = 0.0
    """Lower inclusion bound for EV charger power in watts.

    This is the lower limit of the range within which power requests are allowed for the
    EV charger.

    See [`frequenz.api.common.metrics_pb2.Metric.system_inclusion_bounds`][] and
    [`frequenz.api.common.metrics_pb2.Metric.system_exclusion_bounds`][] for more
    details.
    """

    active_power_exclusion_lower_bound: float = 0.0
    """Lower exclusion bound for EV charger power in watts.

    This is the lower limit of the range within which power requests are not allowed for
    the EV charger.

    See [`frequenz.api.common.metrics_pb2.Metric.system_inclusion_bounds`][] and
    [`frequenz.api.common.metrics_pb2.Metric.system_exclusion_bounds`][] for more
    details.
    """

    active_power_inclusion_upper_bound: float = 0.0
    """Upper inclusion bound for EV charger power in watts.

    This is the upper limit of the range within which power requests are allowed for the
    EV charger.

    See [`frequenz.api.common.metrics_pb2.Metric.system_inclusion_bounds`][] and
    [`frequenz.api.common.metrics_pb2.Metric.system_exclusion_bounds`][] for more
    details.
    """

    active_power_exclusion_upper_bound: float = 0.0
    """Upper exclusion bound for EV charger power in watts.

    This is the upper limit of the range within which power requests are not allowed for
    the EV charger.

    See [`frequenz.api.common.metrics_pb2.Metric.system_inclusion_bounds`][] and
    [`frequenz.api.common.metrics_pb2.Metric.system_exclusion_bounds`][] for more
    details.
    """

    frequency: float = 0.0
    """AC frequency, in Hertz (Hz)."""

    CATEGORY: ClassVar[ComponentCategory] = ComponentCategory.EV_CHARGER
    """The category of this component."""

    METRICS: ClassVar[frozenset[Metric]] = frozenset(
        [
            Metric.AC_ACTIVE_POWER,
            Metric.AC_ACTIVE_POWER_PHASE_1,
            Metric.AC_ACTIVE_POWER_PHASE_2,
            Metric.AC_ACTIVE_POWER_PHASE_3,
            Metric.AC_REACTIVE_POWER,
            Metric.AC_REACTIVE_POWER_PHASE_1,
            Metric.AC_REACTIVE_POWER_PHASE_2,
            Metric.AC_REACTIVE_POWER_PHASE_3,
            Metric.AC_CURRENT_PHASE_1,
            Metric.AC_CURRENT_PHASE_2,
            Metric.AC_CURRENT_PHASE_3,
            Metric.AC_VOLTAGE_PHASE_1_N,
            Metric.AC_VOLTAGE_PHASE_2_N,
            Metric.AC_VOLTAGE_PHASE_3_N,
            Metric.AC_FREQUENCY,
        ]
    )
    """The metrics of this component."""

    @override
    @classmethod
    # pylint: disable-next=too-many-branches
    def from_samples(cls, samples: ComponentDataSamples) -> Self:
        """Create a new instance from a component data object."""
        if not samples.metric_samples:
            raise ValueError("No metrics in the samples.")

        self = cls._from_samples(cls, samples)

        active_power_per_phase: list[float] = [0.0, 0.0, 0.0]
        reactive_power_per_phase: list[float] = [0.0, 0.0, 0.0]
        current_per_phase: list[float] = [0.0, 0.0, 0.0]
        voltage_per_phase: list[float] = [0.0, 0.0, 0.0]

        for sample in samples.metric_samples:
            value = sample.as_single_value() or 0.0
            match sample.metric:
                case Metric.AC_ACTIVE_POWER:
                    self.active_power = value
                    (
                        self.active_power_inclusion_lower_bound,
                        self.active_power_inclusion_upper_bound,
                        self.active_power_exclusion_lower_bound,
                        self.active_power_exclusion_upper_bound,
                    ) = _bound_ranges_to_inclusion_exclusion(
                        sample.bounds, "AC_ACTIVE_POWER", sample
                    )
                case Metric.AC_ACTIVE_POWER_PHASE_1:
                    active_power_per_phase[0] = value
                case Metric.AC_ACTIVE_POWER_PHASE_2:
                    active_power_per_phase[1] = value
                case Metric.AC_ACTIVE_POWER_PHASE_3:
                    active_power_per_phase[2] = value
                case Metric.AC_REACTIVE_POWER:
                    self.reactive_power = value
                case Metric.AC_REACTIVE_POWER_PHASE_1:
                    reactive_power_per_phase[0] = value
                case Metric.AC_REACTIVE_POWER_PHASE_2:
                    reactive_power_per_phase[1] = value
                case Metric.AC_REACTIVE_POWER_PHASE_3:
                    reactive_power_per_phase[2] = value
                case Metric.AC_CURRENT_PHASE_1:
                    current_per_phase[0] = value
                case Metric.AC_CURRENT_PHASE_2:
                    current_per_phase[1] = value
                case Metric.AC_CURRENT_PHASE_3:
                    current_per_phase[2] = value
                case Metric.AC_VOLTAGE_PHASE_1_N:
                    voltage_per_phase[0] = value
                case Metric.AC_VOLTAGE_PHASE_2_N:
                    voltage_per_phase[1] = value
                case Metric.AC_VOLTAGE_PHASE_3_N:
                    voltage_per_phase[2] = value
                case Metric.AC_FREQUENCY:
                    self.frequency = value
                case unexpected:
                    _logger.warning(
                        "Unexpected metric %s in ev charger data sample: %r",
                        unexpected,
                        sample,
                    )

        self.active_power_per_phase = cast(PhaseTuple, tuple(active_power_per_phase))
        self.reactive_power_per_phase = cast(
            PhaseTuple, tuple(reactive_power_per_phase)
        )
        self.current_per_phase = cast(PhaseTuple, tuple(current_per_phase))
        self.voltage_per_phase = cast(PhaseTuple, tuple(voltage_per_phase))

        return self

    @override
    def to_samples(self) -> ComponentDataSamples:
        """Convert the component data to a component data object."""
        return ComponentDataSamples(
            component_id=self.component_id,
            metric_samples=[
                MetricSample(
                    sampled_at=self.timestamp,
                    metric=Metric.AC_ACTIVE_POWER,
                    value=self.active_power,
                    bounds=_inclusion_exclusion_bounds_to_ranges(
                        self.active_power_inclusion_lower_bound,
                        self.active_power_inclusion_upper_bound,
                        self.active_power_exclusion_lower_bound,
                        self.active_power_exclusion_upper_bound,
                    ),
                ),
                *(
                    MetricSample(
                        sampled_at=self.timestamp, metric=metric, value=value, bounds=[]
                    )
                    for metric, value in [
                        (
                            Metric.AC_ACTIVE_POWER_PHASE_1,
                            self.active_power_per_phase[0],
                        ),
                        (
                            Metric.AC_ACTIVE_POWER_PHASE_2,
                            self.active_power_per_phase[1],
                        ),
                        (
                            Metric.AC_ACTIVE_POWER_PHASE_3,
                            self.active_power_per_phase[2],
                        ),
                        (Metric.AC_REACTIVE_POWER, self.reactive_power),
                        (
                            Metric.AC_REACTIVE_POWER_PHASE_1,
                            self.reactive_power_per_phase[0],
                        ),
                        (
                            Metric.AC_REACTIVE_POWER_PHASE_2,
                            self.reactive_power_per_phase[1],
                        ),
                        (
                            Metric.AC_REACTIVE_POWER_PHASE_3,
                            self.reactive_power_per_phase[2],
                        ),
                        (Metric.AC_CURRENT_PHASE_1, self.current_per_phase[0]),
                        (Metric.AC_CURRENT_PHASE_2, self.current_per_phase[1]),
                        (Metric.AC_CURRENT_PHASE_3, self.current_per_phase[2]),
                        (Metric.AC_VOLTAGE_PHASE_1_N, self.voltage_per_phase[0]),
                        (Metric.AC_VOLTAGE_PHASE_2_N, self.voltage_per_phase[1]),
                        (Metric.AC_VOLTAGE_PHASE_3_N, self.voltage_per_phase[2]),
                        (Metric.AC_FREQUENCY, self.frequency),
                    ]
                ),
            ],
            states=[
                ComponentStateSample(
                    sampled_at=self.timestamp,
                    states=frozenset(self.states),
                    warnings=frozenset(self.warnings),
                    errors=frozenset(self.errors),
                )
            ],
        )

    def is_ev_connected(self) -> bool:
        """Check whether an EV is connected to the charger.

        Returns:
            When the charger is not in an error state, whether an EV is connected to
                the charger.
        """
        has_error = ComponentStateCode.ERROR in self.states
        is_authorized = (
            ComponentErrorCode.UNAUTHORIZED not in self.errors
            and ComponentErrorCode.UNAUTHORIZED not in self.warnings
        )
        is_connected_at_ev = bool(
            {
                ComponentStateCode.EV_CHARGING_CABLE_LOCKED_AT_EV,
                ComponentStateCode.EV_CHARGING_CABLE_PLUGGED_AT_EV,
            }
            & self.states
        )
        is_connected_at_station = bool(
            {
                ComponentStateCode.EV_CHARGING_CABLE_LOCKED_AT_STATION,
                ComponentStateCode.EV_CHARGING_CABLE_PLUGGED_AT_STATION,
            }
            & self.states
        )

        return (
            not has_error
            and is_authorized
            and is_connected_at_ev
            and is_connected_at_station
        )


def _bound_ranges_to_inclusion_exclusion(
    bounds: list[Bounds], name: str, sample: MetricSample
) -> tuple[float, float, float, float]:
    """Convert a list of bounds to inclusion and exclusion bounds.

        Args:
            bounds: A list of bounds.
    name: The name of the metric (for logging purposes).
            sample: The sample containing the bounds (for logging purposes).

        Returns:
            A tuple containing the inclusion lower bound, inclusion upper bound,
                exclusion lower bound, and exclusion upper bound.
    """
    match bounds:
        case []:
            return (0.0, 0.0, 0.0, 0.0)
        case [inclusion_bound]:
            return (
                inclusion_bound.lower or 0.0,
                inclusion_bound.upper or 0.0,
                0.0,
                0.0,
            )
        case list():
            if len(bounds) > 2:
                _logger.warning(
                    "Too many bounds found in sample, a "
                    "maximum of 2 are supported for %s, "
                    "using only the first 2: %r",
                    name,
                    sample,
                )
            range1, range2 = bounds[:2]
            return (
                range1.lower or 0.0,
                range2.upper or 0.0,
                range1.upper or 0.0,
                range2.lower or 0.0,
            )
        case unexpected:
            assert_never(unexpected)


def _try_create_bounds(lower: float, upper: float, description: str) -> list[Bounds]:
    """Safely create a bounds object, handling any exceptions gracefully.

    Args:
        lower: Lower bound value
        upper: Upper bound value
        description: Description for logging

    Returns:
        List containing the created Bounds object or an empty list if creation failed.
    """
    try:
        return [Bounds(lower=lower, upper=upper)]
    except ValueError as exc:
        _logger.warning(
            "Ignoring invalid %s bounds [%s, %s]: %s",
            description,
            lower,
            upper,
            exc,
            stack_info=True,
        )
        return []


def _inclusion_exclusion_bounds_to_ranges(
    inclusion_lower_bound: float,
    inclusion_upper_bound: float,
    exclusion_lower_bound: float,
    exclusion_upper_bound: float,
) -> list[Bounds]:
    """Convert inclusion and exclusion bounds to ranges.

    Args:
        inclusion_lower_bound: The lower limit of the range within which power requests
            are allowed for the component.
        inclusion_upper_bound: The upper limit of the range within which power requests
            are allowed for the component.
        exclusion_lower_bound: The lower limit of the range within which power requests
            are not allowed for the component.
        exclusion_upper_bound: The upper limit of the range within which power requests
            are not allowed for the component.

    Returns:
        A list of bounds.
    """
    ranges: list[Bounds] = []
    if exclusion_lower_bound == 0.0 and exclusion_upper_bound == 0.0:
        if inclusion_lower_bound == 0.0 and inclusion_upper_bound == 0.0:
            # No bounds are present at all
            return []
        # Only inclusion bounds are present
        return _try_create_bounds(
            inclusion_lower_bound, inclusion_upper_bound, "inclusion"
        )

    if inclusion_lower_bound == 0.0 and inclusion_upper_bound == 0.0:
        # There are exclusion bounds, but no inclusion bounds, we create 2 ranges, one
        # from -inf to exclusion_lower_bound and one from exclusion_upper_bound to +inf
        ranges.extend(
            _try_create_bounds(
                float("-inf"), exclusion_lower_bound, "exclusion lower bound"
            )
        )
        ranges.extend(
            _try_create_bounds(
                exclusion_upper_bound, float("+inf"), "exclusion upper bound"
            )
        )
        return ranges

    # First range: from inclusion_lower_bound to exclusion_lower_bound.
    # If either value is NaN, skip the ordering check. Is not entirely clear what to do
    # with NaN, but this is the old behavior so we are keeping it for now.
    if (
        math.isnan(inclusion_lower_bound)
        or math.isnan(exclusion_lower_bound)
        or inclusion_lower_bound <= exclusion_lower_bound
    ):
        ranges.extend(
            _try_create_bounds(
                inclusion_lower_bound, exclusion_lower_bound, "first range"
            )
        )
    else:
        _logger.warning(
            "Inclusion lower bound (%s) is greater than exclusion lower bound (%s), "
            "skipping this bound in the ranges",
            inclusion_lower_bound,
            exclusion_lower_bound,
        )
    # Second range: from exclusion_upper_bound to inclusion_upper_bound.
    if (
        math.isnan(exclusion_upper_bound)
        or math.isnan(inclusion_upper_bound)
        or exclusion_upper_bound <= inclusion_upper_bound
    ):
        ranges.extend(
            _try_create_bounds(
                exclusion_upper_bound, inclusion_upper_bound, "second range"
            )
        )
    else:
        _logger.warning(
            "Inclusion upper bound (%s) is less than exclusion upper bound (%s), "
            "no second range to add",
            inclusion_upper_bound,
            exclusion_upper_bound,
        )

    return ranges
