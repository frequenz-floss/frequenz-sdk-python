# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Tests for the formula functions."""

from dataclasses import dataclass
from datetime import datetime, timezone

from frequenz.quantities import Quantity
from typing_extensions import override

from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries.formulas._base_ast_node import AstNode
from frequenz.sdk.timeseries.formulas._functions import Coalesce


@dataclass(kw_only=True)
class _ScriptedNode(AstNode[Quantity]):
    """An AST node evaluating to a predefined sequence of values."""

    values: list[Sample[Quantity] | Quantity | None]
    """The values to return, one per evaluation, `None` once exhausted."""

    subscribed: bool = False
    """Whether this node is currently subscribed."""

    @override
    async def evaluate(self) -> Sample[Quantity] | Quantity | None:
        """Return the next scripted value."""
        return self.values.pop(0) if self.values else None

    @override
    def format(self, wrap: bool = False) -> str:
        """Return a string representation of this node."""
        return "scripted"

    @override
    async def subscribe(self) -> None:
        """Mark this node as subscribed."""
        self.subscribed = True

    @override
    async def unsubscribe(self) -> None:
        """Mark this node as unsubscribed."""
        self.subscribed = False


class TestCoalesce:
    """Tests for the `COALESCE()` function."""

    async def test_param_evaluating_to_none(self) -> None:
        """Test a param evaluating to `None` stops being the tracked one.

        A param evaluates to `None`, rather than to a `Sample` with no value, when it
        is an expression that has no value at all yet, like a nested function call.
        Such a param must stop being the tracked one, or the samples produced by
        another param are counted against it, and the coalesce ends up unsubscribing
        from the param actually producing values.
        """
        timestamp = datetime.now(timezone.utc)
        params = [
            _ScriptedNode(values=[None] * 6),
            _ScriptedNode(values=[Sample(timestamp, Quantity(1.0))] + [None] * 4),
            _ScriptedNode(values=[Sample(timestamp, Quantity(2.0))] * 3),
        ]
        coalesce = Coalesce(params=list(params))

        # Params are subscribed to one by one, as the previous ones fail to produce a
        # value: #1 from the start, #2 and #3 after each call returning `None`.
        assert await coalesce() is None
        assert [param.subscribed for param in params] == [True, True, False]
        assert await coalesce() == Sample(timestamp, Quantity(1.0))
        assert await coalesce() is None
        assert [param.subscribed for param in params] == [True, True, True]

        # From here on only #3 produces values, while #2, the tracked param, evaluates
        # to `None`. So #3 must become the tracked param, instead of accumulating
        # stable samples for #2, which would unsubscribe from #3 once it reached the
        # required number of samples.
        for _ in range(3):
            assert await coalesce() == Sample(timestamp, Quantity(2.0))
        assert [param.subscribed for param in params] == [True, True, True]
