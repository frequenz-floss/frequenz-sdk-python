# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tracking of the blocking status of a component."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


@dataclass(kw_only=True)
class BlockingStatus:
    """Tracking of the blocking status of a component."""

    min_duration: timedelta
    """The minimum blocking duration."""

    max_duration: timedelta
    """The maximum blocking duration."""

    last_blocking_duration: timedelta = timedelta(seconds=0.0)
    """Last blocking duration."""

    blocked_until: datetime | None = None
    """Time until which the component is blocked."""

    def __post_init__(self) -> None:
        """Validate the blocking duration."""
        assert self.min_duration <= self.max_duration, (
            f"Minimum blocking duration ({self.min_duration}) cannot be greater "
            f"than maximum blocking duration ({self.max_duration})"
        )
        self.last_blocking_duration = self.min_duration
        self._timedelta_zero = timedelta(seconds=0.0)

    def block(self) -> timedelta:
        """Set the component as blocked.

        Returns:
            The duration for which the component is blocked.
        """
        now = datetime.now(tz=timezone.utc)

        # If is not blocked
        if self.blocked_until is None:
            self.last_blocking_duration = self.min_duration
            self.blocked_until = now + self.last_blocking_duration
            return self.last_blocking_duration

        # If still blocked, then do nothing
        if self.blocked_until > now:
            return self._timedelta_zero

        # If previous blocking time expired, then blocked it once again.
        # Increase last blocking time, unless it reach the maximum.
        self.last_blocking_duration = min(
            2 * self.last_blocking_duration, self.max_duration
        )
        self.blocked_until = now + self.last_blocking_duration

        return self.last_blocking_duration

    def unblock(self) -> None:
        """Set the component as unblocked."""
        self.blocked_until = None

    def is_blocked(self) -> bool:
        """Check if the component is blocked.

        Returns:
            True if battery is blocked, False otherwise.
        """
        if self.blocked_until is None:
            return False
        return self.blocked_until > datetime.now(tz=timezone.utc)
