"""
Interface for a class that will implement the resampling logic.

Copyright
Copyright © 2022 Frequenz Energy-as-a-Service GmbH

License
MIT
"""
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List

from frequenz.sdk.data_ingestion.resampling.ring_buffer import DataPoint
from frequenz.sdk.microgrid import Component, ComponentCategory

logger = logging.Logger(__name__)


class Resampler(ABC):
    """Resampler interface."""

    def resample_component_data(  # pylint: disable=too-many-arguments
        self,
        component: Component,
        data_points: List[DataPoint],
        resampling_window_start: datetime,
        resampling_frequency: float,
        max_component_data_delay: float = 5.0,
    ) -> DataPoint:
        """Delegate resampling of data points based on the component category.

        Args:
            component: component that the data points belong to
            data_points: data points for a single battery from the ring buffer.
            resampling_window_start: timestamp of the beginning of the resampling window.
            resampling_frequency: what frequency should the original data points be
                converted to
            max_component_data_delay: amount of time in seconds describing how much
                delayed data points in the ring buffer for `component_d` can be to still
                be considered accurate enough for resampling

        Returns:
            Data points for a given battery resampled into a single sample.

        Raises:
            ValueError: if category of any component in the microgrid is not supported
        """
        if component.category == ComponentCategory.BATTERY:
            return self.resample_battery_data(
                data_points=data_points,
                resampling_window_start=resampling_window_start,
                resampling_frequency=resampling_frequency,
                max_component_data_delay=max_component_data_delay,
            )
        if component.category == ComponentCategory.METER:
            return self.resample_meter_data(
                data_points=data_points,
                resampling_window_start=resampling_window_start,
                resampling_frequency=resampling_frequency,
                max_component_data_delay=max_component_data_delay,
            )
        if component.category == ComponentCategory.INVERTER:
            return self.resample_inverter_data(
                data_points=data_points,
                resampling_window_start=resampling_window_start,
                resampling_frequency=resampling_frequency,
                max_component_data_delay=max_component_data_delay,
            )
        if component.category == ComponentCategory.EV_CHARGER:
            return self.resample_ev_charger_data(
                data_points=data_points,
                resampling_window_start=resampling_window_start,
                resampling_frequency=resampling_frequency,
                max_component_data_delay=max_component_data_delay,
            )
        raise ValueError(f"Unsupported component category: {component.category}")

    @abstractmethod
    def resample_battery_data(
        self,
        data_points: List[DataPoint],
        resampling_window_start: datetime,
        resampling_frequency: float,
        max_component_data_delay: float = 5.0,
    ) -> DataPoint:
        """Resample battery data.

        Args:
            data_points: Data points for a single battery from the ring buffer.
            resampling_window_start: timestamp of the beginning of the resampling window.
            resampling_frequency: what frequency should the original data points be
                converted to
            max_component_data_delay: amount of time in seconds describing how much
                delayed data points in the ring buffer for `component_d` can be to still
                be considered accurate enough for resampling

        Returns:
            Data points for a given battery resampled into a single sample.
        """

    @abstractmethod
    def resample_meter_data(
        self,
        data_points: List[DataPoint],
        resampling_window_start: datetime,
        resampling_frequency: float,
        max_component_data_delay: float = 5.0,
    ) -> DataPoint:
        """Resample meter data.

        Args:
            data_points: Data points for a single meter from the ring buffer.
            resampling_window_start: timestamp of the beginning of the resampling window.
            resampling_frequency: what frequency should the original data points be
                converted to
            max_component_data_delay: amount of time in seconds describing how much
                delayed data points in the ring buffer for `component_d` can be to still
                be considered accurate enough for resampling

        Returns:
            Data points for a given meter resampled into a single sample.
        """

    @abstractmethod
    def resample_inverter_data(
        self,
        data_points: List[DataPoint],
        resampling_window_start: datetime,
        resampling_frequency: float,
        max_component_data_delay: float = 5.0,
    ) -> DataPoint:
        """Resample inverter data.

        Args:
            data_points: Data points for a single inverter from the ring buffer.
            resampling_window_start: timestamp of the beginning of the resampling window.
            resampling_frequency: what frequency should the original data points be
                converted to
            max_component_data_delay: amount of time in seconds describing how much
                delayed data points in the ring buffer for `component_d` can be to still
                be considered accurate enough for resampling

        Returns:
            Data points for a given inverter resampled into a single sample.
        """

    @abstractmethod
    def resample_ev_charger_data(
        self,
        data_points: List[DataPoint],
        resampling_window_start: datetime,
        resampling_frequency: float,
        max_component_data_delay: float = 5.0,
    ) -> DataPoint:
        """Resample EV charger data.

        Args:
            data_points: Data points for a single EV charger from the ring buffer.
            resampling_window_start: timestamp of the beginning of the resampling window.
            resampling_frequency: what frequency should the original data points be
                converted to
            max_component_data_delay: amount of time in seconds describing how much
                delayed data points in the ring buffer for `component_d` can be to still
                be considered accurate enough for resampling

        Returns:
            Data points for a given EV charger resampled into a single sample.
        """
