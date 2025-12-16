# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""A composite formula for three-phase metrics."""

# Temporary disable strict private usage checking for pyright
# pyright: strict, reportPrivateUsage=false

from __future__ import annotations

from collections.abc import Callable
from typing import Generic

from frequenz.channels import Broadcast, Receiver
from typing_extensions import override

from ...actor import BackgroundService
from .._base_types import QuantityT, Sample3Phase
from ._formula import Formula, FormulaBuilder
from ._formula_3_phase_evaluator import (
    Formula3PhaseEvaluatingActor,
)


class Formula3Phase(BackgroundService, Generic[QuantityT]):
    """A composite formula for three-phase metrics."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        *,
        name: str,
        phase_1: Formula[QuantityT],
        phase_2: Formula[QuantityT],
        phase_3: Formula[QuantityT],
        sub_formulas: list[Formula3Phase[QuantityT]] | None = None,
    ) -> None:
        """Initialize this instance.

        Args:
            name: The name of the formula.
            phase_1: The formula for phase 1.
            phase_2: The formula for phase 2.
            phase_3: The formula for phase 3.
            sub_formulas: Sub-formulas that need to be started before this formula.
        """
        BackgroundService.__init__(self)
        self._formula_p1: Formula[QuantityT] = phase_1
        self._formula_p2: Formula[QuantityT] = phase_2
        self._formula_p3: Formula[QuantityT] = phase_3
        self._create_method: Callable[[float], QuantityT] = phase_1._create_method

        self._channel: Broadcast[Sample3Phase[QuantityT]] = Broadcast(
            name=f"[Formula3Phase:{name}]({phase_1.name})"
        )
        self._sub_formulas: list[Formula3Phase[QuantityT]] = sub_formulas or []
        self._evaluator: Formula3PhaseEvaluatingActor[QuantityT] = (
            Formula3PhaseEvaluatingActor(phase_1, phase_2, phase_3, self._channel)
        )

    def new_receiver(self, *, max_size: int = 50) -> Receiver[Sample3Phase[QuantityT]]:
        """Subscribe to the output of this formula."""
        if not self._evaluator.is_running:
            self.start()
        return self._channel.new_receiver(limit=max_size)

    @override
    def start(self) -> None:
        """Start the per-phase and sub formulas."""
        for sub_formula in self._sub_formulas:
            sub_formula.start()
        self._formula_p1.start()
        self._formula_p2.start()
        self._formula_p3.start()
        self._evaluator.start()

    @override
    async def stop(self, msg: str | None = None) -> None:
        """Stop the formula."""
        await BackgroundService.stop(self, msg)
        for sub_formula in self._sub_formulas:
            await sub_formula.stop(msg)
        await self._formula_p1.stop(msg)
        await self._formula_p2.stop(msg)
        await self._formula_p3.stop(msg)
        await self._evaluator.stop(msg)

    def __add__(
        self,
        other: Formula3PhaseBuilder[QuantityT] | Formula3Phase[QuantityT],
    ) -> Formula3PhaseBuilder[QuantityT]:
        """Add two three-phase formulas."""
        return Formula3PhaseBuilder(self, create_method=self._create_method) + other

    def __sub__(
        self,
        other: Formula3PhaseBuilder[QuantityT] | Formula3Phase[QuantityT],
    ) -> Formula3PhaseBuilder[QuantityT]:
        """Subtract two three-phase formulas."""
        return Formula3PhaseBuilder(self, create_method=self._create_method) - other

    def __mul__(
        self,
        scalar: float,
    ) -> Formula3PhaseBuilder[QuantityT]:
        """Multiply the three-phase formula by a scalar."""
        return Formula3PhaseBuilder(self, create_method=self._create_method) * scalar

    def __truediv__(
        self,
        scalar: float,
    ) -> Formula3PhaseBuilder[QuantityT]:
        """Divide the three-phase formula by a scalar."""
        return Formula3PhaseBuilder(self, create_method=self._create_method) / scalar

    def coalesce(
        self,
        *other: Formula3PhaseBuilder[QuantityT]
        | Formula3Phase[QuantityT]
        | tuple[QuantityT, QuantityT, QuantityT],
    ) -> Formula3PhaseBuilder[QuantityT]:
        """Coalesce the three-phase formula with a default value."""
        return Formula3PhaseBuilder(self, create_method=self._create_method).coalesce(
            *other
        )

    def min(
        self,
        *other: Formula3PhaseBuilder[QuantityT]
        | Formula3Phase[QuantityT]
        | tuple[QuantityT, QuantityT, QuantityT],
    ) -> Formula3PhaseBuilder[QuantityT]:
        """Get the minimum of the three-phase formula with other formulas."""
        return Formula3PhaseBuilder(self, create_method=self._create_method).min(*other)

    def max(
        self,
        *other: Formula3PhaseBuilder[QuantityT]
        | Formula3Phase[QuantityT]
        | tuple[QuantityT, QuantityT, QuantityT],
    ) -> Formula3PhaseBuilder[QuantityT]:
        """Get the maximum of the three-phase formula with other formulas."""
        return Formula3PhaseBuilder(self, create_method=self._create_method).max(*other)


class Formula3PhaseBuilder(Generic[QuantityT]):
    """Builder for three-phase formulas."""

    def __init__(
        self,
        formula: (
            Formula3Phase[QuantityT]
            | tuple[
                FormulaBuilder[QuantityT],
                FormulaBuilder[QuantityT],
                FormulaBuilder[QuantityT],
            ]
        ),
        create_method: Callable[[float], QuantityT],
        sub_formulas: list[Formula3Phase[QuantityT]] | None = None,
    ) -> None:
        """Initialize this instance."""
        self._create_method: Callable[[float], QuantityT] = create_method
        self._sub_formulas: list[Formula3Phase[QuantityT]] = sub_formulas or []

        if isinstance(formula, Formula3Phase):
            self._sub_formulas.append(formula)
            self.root: tuple[
                FormulaBuilder[QuantityT],
                FormulaBuilder[QuantityT],
                FormulaBuilder[QuantityT],
            ] = (
                FormulaBuilder(formula._formula_p1, create_method=self._create_method),
                FormulaBuilder(formula._formula_p2, create_method=self._create_method),
                FormulaBuilder(formula._formula_p3, create_method=self._create_method),
            )
        else:
            self.root = formula

    def __add__(
        self,
        other: Formula3PhaseBuilder[QuantityT] | Formula3Phase[QuantityT],
    ) -> Formula3PhaseBuilder[QuantityT]:
        """Add two three-phase formulas.

        Args:
            other: The other formula to add.

        Returns:
            A new three-phase formula builder representing the sum.
        """
        if isinstance(other, Formula3Phase):
            other = Formula3PhaseBuilder(
                other,
                create_method=self._create_method,
            )
        new_sub_formulas = self._sub_formulas + other._sub_formulas
        return Formula3PhaseBuilder(
            (
                self.root[0] + other.root[0],
                self.root[1] + other.root[1],
                self.root[2] + other.root[2],
            ),
            create_method=self._create_method,
            sub_formulas=new_sub_formulas,
        )

    def __sub__(
        self,
        other: Formula3PhaseBuilder[QuantityT] | Formula3Phase[QuantityT],
    ) -> Formula3PhaseBuilder[QuantityT]:
        """Subtract two three-phase formulas.

        Args:
            other: The other formula to subtract.

        Returns:
            A new three-phase formula builder representing the difference.
        """
        if isinstance(other, Formula3Phase):
            other = Formula3PhaseBuilder(
                other,
                create_method=self._create_method,
            )
        new_sub_formulas = self._sub_formulas + other._sub_formulas
        return Formula3PhaseBuilder(
            (
                self.root[0] - other.root[0],
                self.root[1] - other.root[1],
                self.root[2] - other.root[2],
            ),
            create_method=self._create_method,
            sub_formulas=new_sub_formulas,
        )

    def __mul__(
        self,
        scalar: float,
    ) -> Formula3PhaseBuilder[QuantityT]:
        """Multiply the three-phase formula by a scalar.

        Args:
            scalar: The scalar to multiply by.

        Returns:
            A new three-phase formula builder representing the product.
        """
        return Formula3PhaseBuilder(
            (
                self.root[0] * scalar,
                self.root[1] * scalar,
                self.root[2] * scalar,
            ),
            create_method=self._create_method,
            sub_formulas=self._sub_formulas,
        )

    def __truediv__(
        self,
        scalar: float,
    ) -> Formula3PhaseBuilder[QuantityT]:
        """Divide the three-phase formula by a scalar.

        Args:
            scalar: The scalar to divide by.

        Returns:
            A new three-phase formula builder representing the quotient.
        """
        return Formula3PhaseBuilder(
            (
                self.root[0] / scalar,
                self.root[1] / scalar,
                self.root[2] / scalar,
            ),
            create_method=self._create_method,
            sub_formulas=self._sub_formulas,
        )

    def coalesce(
        self,
        *others: Formula3PhaseBuilder[QuantityT]
        | Formula3Phase[QuantityT]
        | tuple[QuantityT, QuantityT, QuantityT],
    ) -> Formula3PhaseBuilder[QuantityT]:
        """Coalesce the three-phase formula with a default value.

        Args:
            *others: The other formulas or default values to coalesce with.

        Returns:
            A new three-phase formula builder representing the coalesced formula.
        """
        right_nodes_phase_1: list[
            Formula[QuantityT] | QuantityT | FormulaBuilder[QuantityT]
        ] = []
        right_nodes_phase_2: list[
            Formula[QuantityT] | QuantityT | FormulaBuilder[QuantityT]
        ] = []
        right_nodes_phase_3: list[
            Formula[QuantityT] | QuantityT | FormulaBuilder[QuantityT]
        ] = []
        sub_formulas: list[Formula3Phase[QuantityT]] = []
        sub_formulas.extend(self._sub_formulas)

        for item in others:
            if isinstance(item, tuple):
                right_nodes_phase_1.append(item[0])
                right_nodes_phase_2.append(item[1])
                right_nodes_phase_3.append(item[2])
            elif isinstance(item, Formula3Phase):
                right_nodes_phase_1.append(
                    FormulaBuilder(
                        item._formula_p1,  # pylint: disable=protected-access
                        create_method=self._create_method,
                    )
                )
                right_nodes_phase_2.append(
                    FormulaBuilder(
                        item._formula_p2,  # pylint: disable=protected-access
                        create_method=self._create_method,
                    )
                )
                right_nodes_phase_3.append(
                    FormulaBuilder(
                        item._formula_p3,  # pylint: disable=protected-access
                        create_method=self._create_method,
                    )
                )
                sub_formulas.append(item)
            else:
                right_nodes_phase_1.append(item.root[0])
                right_nodes_phase_2.append(item.root[1])
                right_nodes_phase_3.append(item.root[2])
        return Formula3PhaseBuilder(
            (
                self.root[0].coalesce(*right_nodes_phase_1),
                self.root[1].coalesce(*right_nodes_phase_2),
                self.root[2].coalesce(*right_nodes_phase_3),
            ),
            create_method=self._create_method,
            sub_formulas=sub_formulas,
        )

    def min(
        self,
        *others: Formula3PhaseBuilder[QuantityT]
        | Formula3Phase[QuantityT]
        | tuple[QuantityT, QuantityT, QuantityT],
    ) -> Formula3PhaseBuilder[QuantityT]:
        """Get the minimum of the three-phase formula with other formulas.

        Args:
            *others: The other formulas or values to compare with.

        Returns:
            A new three-phase formula builder representing the minimum.
        """
        right_nodes_phase_1: list[
            Formula[QuantityT] | QuantityT | FormulaBuilder[QuantityT]
        ] = []
        right_nodes_phase_2: list[
            Formula[QuantityT] | QuantityT | FormulaBuilder[QuantityT]
        ] = []
        right_nodes_phase_3: list[
            Formula[QuantityT] | QuantityT | FormulaBuilder[QuantityT]
        ] = []
        sub_formulas: list[Formula3Phase[QuantityT]] = []
        sub_formulas.extend(self._sub_formulas)

        for item in others:
            if isinstance(item, tuple):
                right_nodes_phase_1.append(item[0])
                right_nodes_phase_2.append(item[1])
                right_nodes_phase_3.append(item[2])
            elif isinstance(item, Formula3Phase):
                right_nodes_phase_1.append(
                    FormulaBuilder(
                        item._formula_p1,  # pylint: disable=protected-access
                        create_method=self._create_method,
                    )
                )
                right_nodes_phase_2.append(
                    FormulaBuilder(
                        item._formula_p2,  # pylint: disable=protected-access
                        create_method=self._create_method,
                    )
                )
                right_nodes_phase_3.append(
                    FormulaBuilder(
                        item._formula_p3,  # pylint: disable=protected-access
                        create_method=self._create_method,
                    )
                )
                sub_formulas.append(item)
            else:
                right_nodes_phase_1.append(item.root[0])
                right_nodes_phase_2.append(item.root[1])
                right_nodes_phase_3.append(item.root[2])
        return Formula3PhaseBuilder(
            (
                self.root[0].min(*right_nodes_phase_1),
                self.root[1].min(*right_nodes_phase_2),
                self.root[2].min(*right_nodes_phase_3),
            ),
            create_method=self._create_method,
            sub_formulas=sub_formulas,
        )

    def max(
        self,
        *others: Formula3PhaseBuilder[QuantityT]
        | Formula3Phase[QuantityT]
        | tuple[QuantityT, QuantityT, QuantityT],
    ) -> Formula3PhaseBuilder[QuantityT]:
        """Get the maximum of the three-phase formula with other formulas.

        Args:
            *others: The other formulas or values to compare with.

        Returns:
            A new three-phase formula builder representing the maximum.
        """
        right_nodes_phase_1: list[
            Formula[QuantityT] | QuantityT | FormulaBuilder[QuantityT]
        ] = []
        right_nodes_phase_2: list[
            Formula[QuantityT] | QuantityT | FormulaBuilder[QuantityT]
        ] = []
        right_nodes_phase_3: list[
            Formula[QuantityT] | QuantityT | FormulaBuilder[QuantityT]
        ] = []
        sub_formulas: list[Formula3Phase[QuantityT]] = []
        sub_formulas.extend(self._sub_formulas)

        for item in others:
            if isinstance(item, tuple):
                right_nodes_phase_1.append(item[0])
                right_nodes_phase_2.append(item[1])
                right_nodes_phase_3.append(item[2])
            elif isinstance(item, Formula3Phase):
                right_nodes_phase_1.append(
                    FormulaBuilder(
                        item._formula_p1,  # pylint: disable=protected-access
                        create_method=self._create_method,
                    )
                )
                right_nodes_phase_2.append(
                    FormulaBuilder(
                        item._formula_p2,  # pylint: disable=protected-access
                        create_method=self._create_method,
                    )
                )
                right_nodes_phase_3.append(
                    FormulaBuilder(
                        item._formula_p3,  # pylint: disable=protected-access
                        create_method=self._create_method,
                    )
                )
                sub_formulas.append(item)
            else:
                right_nodes_phase_1.append(item.root[0])
                right_nodes_phase_2.append(item.root[1])
                right_nodes_phase_3.append(item.root[2])
        return Formula3PhaseBuilder(
            (
                self.root[0].max(*right_nodes_phase_1),
                self.root[1].max(*right_nodes_phase_2),
                self.root[2].max(*right_nodes_phase_3),
            ),
            create_method=self._create_method,
            sub_formulas=sub_formulas,
        )

    def build(self, name: str) -> Formula3Phase[QuantityT]:
        """Build the three-phase formula.

        Args:
            name: The name of the formula.

        Returns:
            The built three-phase formula.
        """
        phase_1_formula = self.root[0].build(name + "_phase_1")
        phase_2_formula = self.root[1].build(name + "_phase_2")
        phase_3_formula = self.root[2].build(name + "_phase_3")

        return Formula3Phase(
            name=name,
            phase_1=phase_1_formula,
            phase_2=phase_2_formula,
            phase_3=phase_3_formula,
            sub_formulas=self._sub_formulas,
        )
