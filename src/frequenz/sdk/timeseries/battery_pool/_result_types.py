# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Methods for processing battery-inverter data."""

from dataclasses import dataclass, field
from datetime import datetime

from .. import _base_types
from .._quantities import Power


@dataclass
class PowerMetrics:
    """Power bounds metrics."""

    # compare = False tells the dataclass to not use name for comparison methods
    timestamp: datetime = field(compare=False)
    """Timestamp of the metrics."""

    # pylint: disable=line-too-long
    inclusion_bounds: _base_types.Bounds[Power] | None
    """Inclusion power bounds for all batteries in the battery pool instance.

    This is the range within which power requests are allowed by the battery pool.

    When exclusion bounds are present, they will exclude a subset of the inclusion
    bounds.

    See [`frequenz.api.common.metrics_pb2.Metric.system_inclusion_bounds`][] and
    [`frequenz.api.common.metrics_pb2.Metric.system_exclusion_bounds`][] for more
    details.
    """

    exclusion_bounds: _base_types.Bounds[Power] | None
    """Exclusion power bounds for all batteries in the battery pool instance.

    This is the range within which power requests are NOT allowed by the battery pool.
    If present, they will be a subset of the inclusion bounds.

    See [`frequenz.api.common.metrics_pb2.Metric.system_inclusion_bounds`][] and
    [`frequenz.api.common.metrics_pb2.Metric.system_exclusion_bounds`][] for more
    details.
    """
    # pylint: enable=line-too-long
