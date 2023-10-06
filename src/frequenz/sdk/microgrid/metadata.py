# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Metadata that describes a microgrid."""

from dataclasses import dataclass
from zoneinfo import ZoneInfo

from timezonefinder import TimezoneFinder

_timezone_finder = TimezoneFinder()


@dataclass(frozen=True, kw_only=True)
class Location:
    """Metadata for the location of microgrid."""

    latitude: float | None = None
    """The latitude of the microgrid in degree."""

    longitude: float | None = None
    """The longitude of the microgrid in degree."""

    timezone: ZoneInfo | None = None
    """The timezone of the microgrid.

    The timezone will be set to None if the latitude or longitude points
    are not set or the timezone cannot be found given the location points.
    """

    def __post_init__(self) -> None:
        """Initialize the timezone of the microgrid."""
        if self.latitude is None or self.longitude is None or self.timezone is not None:
            return

        timezone = _timezone_finder.timezone_at(lat=self.latitude, lng=self.longitude)
        if timezone:
            # The dataclass is frozen, so it needs to use __setattr__ to set the timezone.
            object.__setattr__(self, "timezone", ZoneInfo(key=timezone))


@dataclass(frozen=True, kw_only=True)
class Metadata:
    """Metadata for the microgrid."""

    microgrid_id: int | None = None
    """The ID of the microgrid."""

    location: Location | None = None
    """The location of the microgrid."""
