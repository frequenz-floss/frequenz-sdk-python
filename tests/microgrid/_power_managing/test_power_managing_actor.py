# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Tests for PowerManagingActor."""

# pylint: disable=protected-access
# We access _should_update_bounds directly (a static helper) rather than
# going through the full async bounds tracker machinery.

from datetime import datetime, timezone

from frequenz.quantities import Power

from frequenz.sdk.microgrid._power_managing._power_managing_actor import (
    PowerManagingActor,
)
from frequenz.sdk.timeseries._base_types import Bounds, SystemBounds


class TestPowerManagingActor:
    """Tests for PowerManagingActor._should_update_bounds."""

    def _bounds(
        self,
        lower_w: float | None,
        upper_w: float | None,
    ) -> SystemBounds:
        """Create a SystemBounds with the given inclusion bounds."""
        now = datetime.now(tz=timezone.utc)
        inc = (
            Bounds(Power.from_watts(lower_w), Power.from_watts(upper_w))
            if lower_w is not None and upper_w is not None
            else None
        )
        return SystemBounds(timestamp=now, inclusion_bounds=inc, exclusion_bounds=None)

    def test_first_bounds_accepted(self) -> None:
        """The very first bounds should always be accepted."""
        b = self._bounds(-1.3e6, 1.3e6)
        assert PowerManagingActor._should_update_bounds(b, None)

    def test_identical_bounds_skipped(self) -> None:
        """No-change updates are a no-op."""
        b = self._bounds(-1.3e6, 1.3e6)
        same = self._bounds(-1.3e6, 1.3e6)
        assert not PowerManagingActor._should_update_bounds(same, b)

    def test_changed_valid_bounds_accepted(self) -> None:
        """A real change in bounds should be propagated."""
        b1 = self._bounds(-1.3e6, 1.3e6)
        b2 = self._bounds(-1.0e6, 1.0e6)
        assert PowerManagingActor._should_update_bounds(b2, b1)

    def test_transient_none_ignored(self) -> None:
        """When data goes stale, inclusion_bounds=None must NOT overwrite valid bounds."""
        valid = self._bounds(-1.3e6, 1.3e6)
        none_bounds = self._bounds(None, None)
        assert not PowerManagingActor._should_update_bounds(
            none_bounds, valid
        ), "transient None must not replace valid bounds"

    def test_none_after_none_irrelevant(self) -> None:
        """Repeated None-bounds are a no-op."""
        none1 = self._bounds(None, None)
        none2 = self._bounds(None, None)
        assert not PowerManagingActor._should_update_bounds(none2, none1)

    def test_valid_after_none_accepted(self) -> None:
        """When data resumes, valid bounds must be accepted."""
        none_bounds = self._bounds(None, None)
        valid = self._bounds(-1.3e6, 1.3e6)
        assert PowerManagingActor._should_update_bounds(valid, none_bounds)
        assert PowerManagingActor._should_update_bounds(valid, None)

    def test_real_zero_bounds_accepted(self) -> None:
        """Bounds that are truly [0, 0] (not stale-data) must be propagated."""
        valid = self._bounds(-1.3e6, 1.3e6)
        zero = self._bounds(0.0, 0.0)
        assert PowerManagingActor._should_update_bounds(
            zero, valid
        ), "real zero bounds must propagate even though they look like a collapse"

    def test_almost_zero_bounds_accepted(self) -> None:
        """Very small but non-zero bounds are still real data."""
        valid = self._bounds(-1.3e6, 1.3e6)
        tiny = self._bounds(-100.0, 100.0)
        assert PowerManagingActor._should_update_bounds(
            tiny, valid
        ), "small-but-non-zero bounds must propagate"
