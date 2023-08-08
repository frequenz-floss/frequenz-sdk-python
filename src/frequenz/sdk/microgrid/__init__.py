# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Microgrid monitoring and control system.

This package provides a complete suite of data structures and functionality
for monitoring and adjusting the state of a microgrid.
"""

from ..actor import ResamplerConfig
from . import _data_pipeline, client, component, connection_manager
from ._data_pipeline import battery_pool, ev_charger_pool, logical_meter
from ._graph import ComponentGraph


async def initialize(host: str, port: int, resampler_config: ResamplerConfig) -> None:
    """Initialize the microgrid connection manager and the data pipeline.

    Args:
        host: Host to connect to, to reach the microgrid API.
        port: port to connect to.
        resampler_config: Configuration for the resampling actor.
    """
    await connection_manager.initialize(host, port)
    await _data_pipeline.initialize(resampler_config)


__all__ = [
    "ComponentGraph",
    "initialize",
    "client",
    "component",
    "battery_pool",
    "ev_charger_pool",
    "logical_meter",
]
