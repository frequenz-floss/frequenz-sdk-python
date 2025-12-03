# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""A composable formula represented as an AST."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Generic

from frequenz.channels import Broadcast, Receiver
from typing_extensions import override

from frequenz.sdk.timeseries.formulas._resampled_stream_fetcher import (
    ResampledStreamFetcher,
)

from ...actor import BackgroundService
from .. import ReceiverFetcher, Sample
from .._base_types import QuantityT
from . import _ast
from ._base_ast_node import AstNode
from ._formula_evaluator import FormulaEvaluatingActor
from ._functions import Coalesce, Max, Min

_logger = logging.getLogger(__name__)


class Formula(BackgroundService, ReceiverFetcher[Sample[QuantityT]]):
    """A formula represented as an AST."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        *,
        name: str,
        root: AstNode,
        create_method: Callable[[float], QuantityT],
        streams: list[_ast.TelemetryStream[QuantityT]],
        sub_formulas: list[Formula[QuantityT]] | None = None,
        metric_fetcher: ResampledStreamFetcher | None = None,
    ) -> None:
        """Create a `Formula` instance.

        Args:
            name: The name of the formula.
            root: The root node of the formula AST.
            create_method: A method to generate the output values with.  If the
                formula is for generating power values, this would be
                `Power.from_watts`, for example.
            streams: The telemetry streams that the formula depends on.
            sub_formulas: Any sub-formulas that this formula depends on.
            metric_fetcher: An optional metric fetcher that needs to be started
                before the formula can be evaluated.
        """
        BackgroundService.__init__(self)
        self._name: str = name
        self._root: AstNode = root
        self._components: list[_ast.TelemetryStream[QuantityT]] = streams
        self._create_method: Callable[[float], QuantityT] = create_method
        self._sub_formulas: list[Formula[QuantityT]] = sub_formulas or []

        self._channel: Broadcast[Sample[QuantityT]] = Broadcast(
            name=f"{self}",
            resend_latest=True,
        )
        self._evaluator: FormulaEvaluatingActor[QuantityT] = FormulaEvaluatingActor(
            root=self._root,
            components=self._components,
            create_method=self._create_method,
            output_channel=self._channel,
            metric_fetcher=metric_fetcher,
        )

    @override
    def __str__(self) -> str:
        """Return a string representation of the formula."""
        return f"[{self._name}]({self._root})"

    @override
    def new_receiver(self, *, limit: int = 50) -> Receiver[Sample[QuantityT]]:
        """Subscribe to the formula evaluator to get evaluated samples."""
        if not self._evaluator.is_running:
            # raise RuntimeError(
            #     f"Formula evaluator for '{self._root}' is not running. Please "
            #     + "call `start()` on the formula before using it.",
            # )
            # _logger.warning(
            #     "Formula evaluator for '%s' is not running. Starting it.  "
            #     + "Please call `start()` on the formula before using it."
            #     self._root,
            # )
            self.start()
        return self._channel.new_receiver(limit=limit)

    @override
    def start(self) -> None:
        """Start the formula evaluator."""
        for sub_formula in self._sub_formulas:
            sub_formula.start()
        self._evaluator.start()

    @override
    async def stop(self, msg: str | None = None) -> None:
        """Stop the formula evaluator."""
        await BackgroundService.stop(self, msg)
        for sub_formula in self._sub_formulas:
            await sub_formula.stop(msg)
        await self._evaluator.stop(msg)

    def __add__(
        self, other: FormulaBuilder[QuantityT] | QuantityT | Formula[QuantityT]
    ) -> FormulaBuilder[QuantityT]:
        """Create an addition operation node."""
        return FormulaBuilder(self, self._create_method) + other

    def __sub__(
        self, other: FormulaBuilder[QuantityT] | QuantityT | Formula[QuantityT]
    ) -> FormulaBuilder[QuantityT]:
        """Create a subtraction operation node."""
        return FormulaBuilder(self, self._create_method) - other

    def __mul__(self, other: float) -> FormulaBuilder[QuantityT]:
        """Create a multiplication operation node."""
        return FormulaBuilder(self, self._create_method) * other

    def __truediv__(self, other: float) -> FormulaBuilder[QuantityT]:
        """Create a division operation node."""
        return FormulaBuilder(self, self._create_method) / other

    def coalesce(
        self,
        other: list[FormulaBuilder[QuantityT] | QuantityT | Formula[QuantityT]],
    ) -> FormulaBuilder[QuantityT]:
        """Create a coalesce operation node."""
        return FormulaBuilder(self, self._create_method).coalesce(other)

    def min(
        self,
        other: list[FormulaBuilder[QuantityT] | QuantityT | Formula[QuantityT]],
    ) -> FormulaBuilder[QuantityT]:
        """Create a min operation node."""
        return FormulaBuilder(self, self._create_method).min(other)

    def max(
        self,
        other: list[FormulaBuilder[QuantityT] | QuantityT | Formula[QuantityT]],
    ) -> FormulaBuilder[QuantityT]:
        """Create a max operation node."""
        return FormulaBuilder(self, self._create_method).max(other)


class FormulaBuilder(Generic[QuantityT]):
    """A builder for higher-order formulas represented as ASTs."""

    def __init__(
        self,
        formula: Formula[QuantityT] | AstNode,
        create_method: Callable[[float], QuantityT],
        streams: list[_ast.TelemetryStream[QuantityT]] | None = None,
        sub_formulas: list[Formula[QuantityT]] | None = None,
    ) -> None:
        """Create a `FormulaBuilder` instance.

        Args:
            formula: The initial formula to build upon.
            create_method: A method to generate the output values with.  If the
                formula is for generating power values, this would be
                `Power.from_watts`, for example.
            streams: The telemetry streams that the formula depends on.
            sub_formulas: Any sub-formulas that this formula depends on.
        """
        self._create_method: Callable[[float], QuantityT] = create_method
        self._streams: list[_ast.TelemetryStream[QuantityT]] = streams or []
        """Input streams that need to be synchronized before evaluation."""
        self._sub_formulas: list[Formula[QuantityT]] = sub_formulas or []
        """Sub-formulas whose lifetimes are managed by this formula."""

        if isinstance(formula, Formula):
            self.root: AstNode = _ast.TelemetryStream(
                source=str(formula),
                stream=formula.new_receiver(),
            )
            self._streams.append(self.root)
            self._sub_formulas.append(formula)
        else:
            self.root = formula

    def __add__(
        self,
        other: FormulaBuilder[QuantityT] | QuantityT | Formula[QuantityT],
    ) -> FormulaBuilder[QuantityT]:
        """Create an addition operation node."""
        if isinstance(other, FormulaBuilder):
            right_node = other.root
            self._streams.extend(other._streams)
        elif isinstance(other, Formula):
            right_node = _ast.TelemetryStream(
                source=str(other),
                stream=other.new_receiver(),
            )
            self._streams.append(right_node)
            self._sub_formulas.append(other)
        else:
            right_node = _ast.Constant(value=other.base_value)

        new_root = _ast.Add(left=self.root, right=right_node)
        return FormulaBuilder(
            new_root,
            self._create_method,
            self._streams,
            self._sub_formulas,
        )

    def __sub__(
        self,
        other: FormulaBuilder[QuantityT] | QuantityT | Formula[QuantityT],
    ) -> FormulaBuilder[QuantityT]:
        """Create a subtraction operation node."""
        if isinstance(other, FormulaBuilder):
            right_node = other.root
            self._streams.extend(other._streams)
        elif isinstance(other, Formula):
            right_node = _ast.TelemetryStream(
                source=str(other), stream=other.new_receiver()
            )
            self._streams.append(right_node)
            self._sub_formulas.append(other)
        else:
            right_node = _ast.Constant(value=other.base_value)

        new_root = _ast.Sub(left=self.root, right=right_node)
        return FormulaBuilder(
            new_root,
            self._create_method,
            self._streams,
            self._sub_formulas,
        )

    def __mul__(self, other: float) -> FormulaBuilder[QuantityT]:
        """Create a multiplication operation node."""
        right_node = _ast.Constant(value=other)
        new_root = _ast.Mul(left=self.root, right=right_node)
        return FormulaBuilder(
            new_root,
            self._create_method,
            self._streams,
            self._sub_formulas,
        )

    def __truediv__(
        self,
        other: float,
    ) -> FormulaBuilder[QuantityT]:
        """Create a division operation node."""
        right_node = _ast.Constant(value=other)
        new_root = _ast.Div(left=self.root, right=right_node)
        return FormulaBuilder(
            new_root,
            self._create_method,
            self._streams,
            self._sub_formulas,
        )

    def coalesce(
        self,
        other: list[FormulaBuilder[QuantityT] | QuantityT | Formula[QuantityT]],
    ) -> FormulaBuilder[QuantityT]:
        """Create a coalesce operation node."""
        right_nodes: list[AstNode] = []
        for item in other:
            if isinstance(item, FormulaBuilder):
                right_nodes.append(item.root)
                self._streams.extend(item._streams)  # pylint: disable=protected-access
            elif isinstance(item, Formula):
                right_node = _ast.TelemetryStream(
                    source=str(item),
                    stream=item.new_receiver(),
                )
                right_nodes.append(right_node)
                self._streams.append(right_node)
                self._sub_formulas.append(item)
            else:
                right_nodes.append(_ast.Constant(value=item.base_value))

        new_root = _ast.FunCall(
            function=Coalesce(),
            args=[self.root] + right_nodes,
        )

        return FormulaBuilder(
            new_root,
            self._create_method,
            self._streams,
            self._sub_formulas,
        )

    def min(
        self,
        other: list[FormulaBuilder[QuantityT] | QuantityT | Formula[QuantityT]],
    ) -> FormulaBuilder[QuantityT]:
        """Create a min operation node."""
        right_nodes: list[AstNode] = []
        for item in other:
            if isinstance(item, FormulaBuilder):
                right_nodes.append(item.root)
                self._streams.extend(item._streams)  # pylint: disable=protected-access
            elif isinstance(item, Formula):
                right_node = _ast.TelemetryStream(
                    source=str(item),
                    stream=item.new_receiver(),
                )
                right_nodes.append(right_node)
                self._streams.append(right_node)
                self._sub_formulas.append(item)
            else:
                right_nodes.append(_ast.Constant(value=item.base_value))

        new_root = _ast.FunCall(
            function=Min(),
            args=[self.root] + right_nodes,
        )

        return FormulaBuilder(
            new_root,
            self._create_method,
            self._streams,
            self._sub_formulas,
        )

    def max(
        self,
        other: list[FormulaBuilder[QuantityT] | QuantityT | Formula[QuantityT]],
    ) -> FormulaBuilder[QuantityT]:
        """Create a max operation node."""
        right_nodes: list[AstNode] = []
        for item in other:
            if isinstance(item, FormulaBuilder):
                right_nodes.append(item.root)
                self._streams.extend(item._streams)  # pylint: disable=protected-access
            elif isinstance(item, Formula):
                right_node = _ast.TelemetryStream(
                    source=str(item),
                    stream=item.new_receiver(),
                )
                right_nodes.append(right_node)
                self._streams.append(right_node)
                self._sub_formulas.append(item)
            else:
                right_nodes.append(_ast.Constant(value=item.base_value))

        new_root = _ast.FunCall(
            function=Max(),
            args=[self.root] + right_nodes,
        )

        return FormulaBuilder(
            new_root,
            self._create_method,
            self._streams,
            self._sub_formulas,
        )

    def build(
        self,
        name: str,
    ) -> Formula[QuantityT]:
        """Build a `Formula` instance.

        Args:
            name: The name of the formula.

        Returns:
            A `Formula` instance.
        """
        return Formula(
            name=name,
            root=self.root,
            create_method=self._create_method,
            streams=self._streams,
            sub_formulas=self._sub_formulas,
        )
