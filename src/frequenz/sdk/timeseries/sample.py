# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Common types for the Data Pipeline."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class Sample:
    """A measurement taken at a particular point in time.

    The `value` could be `None` if a component is malfunctioning or data is
    lacking for another reason, but a sample still needs to be sent to have a
    coherent view on a group of component metrics for a particular timestamp.
    """

    timestamp: datetime
    value: Optional[float] = None
