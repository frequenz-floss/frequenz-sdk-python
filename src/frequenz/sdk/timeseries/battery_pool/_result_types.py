# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Methods for processing battery-inverter data."""

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Bounds:
    """Lower and upper bound values."""

    lower: float
    """Lower bound."""

    upper: float
    """Upper bound."""


@dataclass
class PowerMetrics:
    """Power bounds metrics."""

    # compare = False tells the dataclass to not use name for comparison methods
    timestamp: datetime = field(compare=False)
    """Timestamp of the metrics."""

    # pylint: disable=line-too-long
    inclusion_bounds: Bounds
    """Inclusion power bounds for all batteries in the battery pool instance.

    This is the range within which power requests are allowed by the battery pool.

    When exclusion bounds are present, they will exclude a subset of the inclusion
    bounds.

    More details [here](https://github.com/frequenz-floss/frequenz-api-common/blob/v0.3.0/proto/frequenz/api/common/metrics.proto#L37-L91).
    """

    exclusion_bounds: Bounds
    """Exclusion power bounds for all batteries in the battery pool instance.

    This is the range within which power requests are NOT allowed by the battery pool.
    If present, they will be a subset of the inclusion bounds.

    More details [here](https://github.com/frequenz-floss/frequenz-api-common/blob/v0.3.0/proto/frequenz/api/common/metrics.proto#L37-L91).
    """
    # pylint: enable=line-too-long
