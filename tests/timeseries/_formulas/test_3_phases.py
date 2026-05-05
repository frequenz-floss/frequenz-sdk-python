# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Tests for three phase formulas."""

import asyncio
from collections import OrderedDict
from collections.abc import Callable
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import async_solipsism
import pytest
from frequenz.channels import Broadcast, Receiver
from frequenz.quantities import Quantity

from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries.formulas._formula_3_phase import (
    Formula3Phase,
    Formula3PhaseBuilder,
)
from frequenz.sdk.timeseries.formulas._parser import parse
from frequenz.sdk.timeseries.formulas._resampled_stream_fetcher import (
    ResampledStreamFetcher,
)


@pytest.fixture
def event_loop_policy() -> async_solipsism.EventLoopPolicy:
    """Event loop policy."""
    return async_solipsism.EventLoopPolicy()


class TestFormula3Phase:
    """Tests for 3-phase formulas."""

    async def run_test(  # pylint: disable=too-many-locals
        self,
        io_pairs: list[
            tuple[
                list[tuple[float | None, float | None, float | None]],
                tuple[float | None, float | None, float | None],
            ]
        ],
        make_builder: Callable[
            [Formula3Phase[Quantity], Formula3Phase[Quantity]],
            Formula3PhaseBuilder[Quantity],
        ],
    ) -> None:
        """Run a test for 3-phase formulas."""
        channels: OrderedDict[int, Broadcast[Sample[Quantity]]] = OrderedDict()

        def stream_recv(comp_id: int) -> Receiver[Sample[Quantity]]:
            comp_id = int(comp_id)
            if comp_id not in channels:
                channels[comp_id] = Broadcast(name=f"chan-#{comp_id}")
            return channels[comp_id].new_receiver()

        telem_fetcher = MagicMock(spec=ResampledStreamFetcher)
        telem_fetcher.fetch_stream = AsyncMock(side_effect=stream_recv)
        l1_formulas = [
            parse(
                name=str(ctr),
                formula=f"#{ctr}",
                create_method=Quantity,
                telemetry_fetcher=telem_fetcher,
            )
            for ctr in range(6)
        ]
        p3_formula_1 = Formula3Phase(
            name="P3-1",
            phase_1=l1_formulas[0],
            phase_2=l1_formulas[1],
            phase_3=l1_formulas[2],
        )
        p3_formula_2 = Formula3Phase(
            name="P3-2",
            phase_1=l1_formulas[3],
            phase_2=l1_formulas[4],
            phase_3=l1_formulas[5],
        )
        builder = make_builder(p3_formula_1, p3_formula_2)
        formula = builder.build("l2")

        receiver = formula.new_receiver()

        await asyncio.sleep(0.1)

        now = datetime.now(timezone.utc)
        for inputs, expected_output in io_pairs:
            _ = await asyncio.gather(
                *[
                    channels[formula_id * 3 + phase_idx]
                    .new_sender()
                    .send(
                        Sample(
                            timestamp=now,
                            value=(
                                None if input_value is None else Quantity(input_value)
                            ),
                        )
                    )
                    for formula_id, input_values in enumerate(inputs)
                    for phase_idx, input_value in enumerate(input_values)
                ]
            )
            output = list(iter(await receiver.receive()))
            for phase_idx, expected_value in enumerate(expected_output):
                if expected_value is None:
                    assert output[phase_idx] is None
                else:
                    phase_value = output[phase_idx]
                    assert phase_value is not None
                    assert phase_value.base_value == expected_value

    async def test_composition(self) -> None:
        """Test a simple 3-phase formula."""
        await self.run_test(
            [
                ([(1.0, 2.0, 3.0), (2.0, 3.0, 4.0)], (6.0, 10.0, 14.0)),
                ([(-5.0, 10.0, 15.0), (10.0, 15.0, -20.0)], (10.0, 50.0, -10.0)),
                ([(None, 2.0, 3.0), (2.0, None, 4.0)], (None, None, 14.0)),
            ],
            lambda f1, f2: (f1 + f2) * 2.0,
        )

        await self.run_test(
            [
                ([(4.0, 9.0, 16.0), (2.0, 3.0, 4.0)], (1.0, 3.0, 6.0)),
                ([(-5.0, 10.0, 15.0), (10.0, 5.0, -20.0)], (-7.5, 2.5, 17.5)),
                ([(None, 2.0, 3.0), (2.0, None, 4.0)], (None, None, -0.5)),
            ],
            lambda f1, f2: (f1 - f2) / 2.0,
        )

        await self.run_test(
            [
                ([(4.0, 9.0, 16.0), (2.0, 13.0, 4.0)], (4.0, 13.0, 16.0)),
                ([(-5.0, 10.0, 15.0), (10.0, 5.0, -20.0)], (10.0, 10.0, 15.0)),
                ([(None, 2.0, 3.0), (2.0, None, 4.0)], (None, None, 4.0)),
            ],
            lambda f1, f2: f1.max(f2),
        )

        await self.run_test(
            [
                ([(4.0, 9.0, 16.0), (2.0, 13.0, 4.0)], (2.0, 9.0, 4.0)),
                ([(-5.0, 10.0, 15.0), (10.0, 5.0, -20.0)], (-5.0, 5.0, -20.0)),
                ([(None, 2.0, 3.0), (2.0, None, 4.0)], (None, None, 3.0)),
            ],
            lambda f1, f2: f1.min(f2),
        )

        await self.run_test(
            [
                ([(4.0, 9.0, 16.0), (2.0, 13.0, 4.0)], (4.0, 9.0, 16.0)),
                ([(-5.0, 10.0, None), (10.0, 5.0, None)], (-5.0, 10.0, None)),
                ([(-5.0, 10.0, None), (10.0, 5.0, None)], (-5.0, 10.0, 0.0)),
                ([(None, 2.0, 3.0), (2.0, None, 4.0)], (None, 2.0, 3.0)),
                ([(None, 2.0, 3.0), (2.0, None, 4.0)], (2.0, 2.0, 3.0)),
            ],
            lambda f1, f2: f1.coalesce(
                f2, (Quantity.zero(), Quantity.zero(), Quantity.zero())
            ),
        )
