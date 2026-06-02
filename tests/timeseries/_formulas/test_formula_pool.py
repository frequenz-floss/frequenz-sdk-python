# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Tests for the formula pool's caching and teardown behavior."""

import asyncio
from typing import cast

import pytest
from frequenz.channels import Broadcast
from frequenz.client.microgrid.metrics import Metric
from frequenz.quantities import Quantity

from frequenz.sdk._internal._channels import ChannelRegistry
from frequenz.sdk.microgrid._data_sourcing import ComponentMetricRequest
from frequenz.sdk.timeseries.formulas._formula import Formula
from frequenz.sdk.timeseries.formulas._formula_pool import FormulaPool

# The tests inspect the pool's caches directly to assert eviction.
# pylint: disable=protected-access


def _make_pool() -> FormulaPool:
    """Create a formula pool wired to inert, real channels."""
    request_channel = Broadcast[ComponentMetricRequest](name="resampler-requests")
    return FormulaPool(
        "test-pool",
        ChannelRegistry(name="test-registry"),
        request_channel.new_sender(),
    )


class _StubFormula:
    """A stand-in for a pooled formula that records and can stall/fail `stop()`."""

    def __init__(self) -> None:
        self.stop_count = 0
        self.entered = asyncio.Event()
        """Set when `stop()` is entered, so a test can await mid-teardown."""
        self.release = asyncio.Event()
        """A blocking `stop()` waits on this before returning."""
        self.block = False
        self.error: BaseException | None = None

    async def stop(self) -> None:
        """Record the call, optionally block, then optionally raise."""
        self.stop_count += 1
        self.entered.set()
        if self.block:
            await self.release.wait()
        if self.error is not None:
            raise self.error


def _as_formula(stub: _StubFormula) -> Formula[Quantity]:
    """Cast a stub to the type the string-formula cache stores."""
    return cast(Formula[Quantity], stub)


class TestFormulaPool:
    """Tests for `FormulaPool`."""

    async def test_from_string_distinguishes_colliding_keys(self) -> None:
        """`from_string` keys on `(formula, metric)`, not a lossy concatenation."""
        pool = _make_pool()

        # These two (formula, metric) pairs both collapse to "#123" under the old
        # `formula_str + str(metric.value)` scheme, yet are distinct formulas.
        assert "#12" + str(Metric.DC_POWER.value) == "#1" + str(
            Metric.AC_POWER_APPARENT_PHASE_1.value
        )

        first = pool.from_string("#12", Metric.DC_POWER)
        second = pool.from_string("#1", Metric.AC_POWER_APPARENT_PHASE_1)

        assert first is not second
        assert len(pool._string_formulas) == 2
        # Each pair still resolves to its own cached formula on repeat.
        assert pool.from_string("#12", Metric.DC_POWER) is first
        assert pool.from_string("#1", Metric.AC_POWER_APPARENT_PHASE_1) is second

        await pool.stop()

    async def test_stop_stops_and_evicts_string_formulas(self) -> None:
        """`stop()` stops every string formula and clears the cache."""
        pool = _make_pool()
        stubs = {
            ("a", Metric.AC_POWER_ACTIVE): _StubFormula(),
            ("b", Metric.DC_POWER): _StubFormula(),
        }
        for key, stub in stubs.items():
            pool._string_formulas[key] = _as_formula(stub)

        await pool.stop()

        assert all(stub.stop_count == 1 for stub in stubs.values())
        assert not pool._string_formulas

    async def test_stop_string_formula_stops_only_the_requested_one(self) -> None:
        """`stop_string_formula` stops and evicts just the requested formula."""
        pool = _make_pool()
        keep, drop = _StubFormula(), _StubFormula()
        keep_key = ("keep", Metric.AC_POWER_ACTIVE)
        drop_key = ("drop", Metric.AC_POWER_ACTIVE)
        pool._string_formulas[keep_key] = _as_formula(keep)
        pool._string_formulas[drop_key] = _as_formula(drop)

        await pool.stop_string_formula("drop", Metric.AC_POWER_ACTIVE)

        assert drop.stop_count == 1
        assert drop_key not in pool._string_formulas
        assert keep.stop_count == 0
        assert keep_key in pool._string_formulas

    async def test_stop_string_formula_keeps_handle_when_stop_fails(self) -> None:
        """A failed `stop()` leaves the formula in the pool so cleanup can retry."""
        pool = _make_pool()
        stub = _StubFormula()
        stub.error = RuntimeError("boom")
        key = ("formula", Metric.AC_POWER_ACTIVE)
        pool._string_formulas[key] = _as_formula(stub)

        with pytest.raises(RuntimeError, match="boom"):
            await pool.stop_string_formula("formula", Metric.AC_POWER_ACTIVE)

        assert stub.stop_count == 1
        assert key in pool._string_formulas

    async def test_stop_keeps_formula_added_during_teardown(self) -> None:
        """A formula added while `stop()` awaits is not dropped without stopping."""
        pool = _make_pool()
        blocker = _StubFormula()
        blocker.block = True
        blocker_key = ("blocker", Metric.AC_POWER_ACTIVE)
        pool._string_formulas[blocker_key] = _as_formula(blocker)

        stopping = asyncio.create_task(pool.stop())
        # Wait until stop() is awaiting the blocker's stop(), i.e. mid-teardown.
        await blocker.entered.wait()

        # A concurrent from_string() adds an entry after stop()'s key snapshot.
        added_key = ("#9", Metric.AC_POWER_ACTIVE)
        added = pool.from_string("#9", Metric.AC_POWER_ACTIVE)

        blocker.release.set()
        await stopping

        # The blocker was stopped and evicted; the late addition survived (it must
        # not be cleared without being stopped).
        assert blocker.stop_count == 1
        assert blocker_key not in pool._string_formulas
        assert added_key in pool._string_formulas
        assert pool.from_string("#9", Metric.AC_POWER_ACTIVE) is added

        await pool.stop()

    async def test_from_reactive_power_formula_is_cached(self) -> None:
        """Repeated `from_reactive_power_formula` calls reuse the cached formula."""
        pool = _make_pool()

        first = pool.from_reactive_power_formula("grid_reactive_power", "#1")
        second = pool.from_reactive_power_formula("grid_reactive_power", "#1")

        assert first is second
        assert len(pool._reactive_power_formulas) == 1
        # Regression: reactive formulas must not leak into the active-power cache.
        assert not pool.power_formulas

        await pool.stop()
