# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""A base class for creating simple composable actors."""

from ._channel_registry import ChannelRegistry
from ._config_managing import ConfigManagingActor
from ._data_sourcing import ComponentMetricRequest, DataSourcingActor
from ._decorator import actor
from ._resampling import ComponentMetricsResamplingActor, ResamplerConfig

__all__ = [
    "ChannelRegistry",
    "ComponentMetricRequest",
    "ComponentMetricsResamplingActor",
    "ConfigManagingActor",
    "DataSourcingActor",
    "ResamplerConfig",
    "actor",
]
