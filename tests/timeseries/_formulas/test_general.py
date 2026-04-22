# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Tests for the Formula implementation."""

import asyncio
import logging
from collections import OrderedDict
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import NamedTuple
from unittest.mock import AsyncMock, MagicMock

import async_solipsism
import pytest
from frequenz.channels import Broadcast, Receiver
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.quantities import Quantity

from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries.formulas._formula import Formula, FormulaBuilder
from frequenz.sdk.timeseries.formulas._parser import parse
from frequenz.sdk.timeseries.formulas._resampled_stream_fetcher import (
    ResampledStreamFetcher,
)

_logger = logging.getLogger(__name__)


@pytest.fixture
def event_loop_policy() -> async_solipsism.EventLoopPolicy:
    """Event loop policy."""
    return async_solipsism.EventLoopPolicy()


class TestFormulas:
    """Tests for Formula."""

    async def run_test(  # pylint: disable=too-many-locals
        self,
        formula_str: str,
        expected: str,
        component_ids: list[int],
        io_pairs: list[tuple[list[float | None], float | None]],
    ) -> None:
        """Run a formula test."""
        _logger.debug("TESTING FORMULA: %s", formula_str)
        channels: OrderedDict[ComponentId, Broadcast[Sample[Quantity]]] = OrderedDict()
        for comp_id in component_ids:
            channels[ComponentId(comp_id)] = Broadcast(
                name=f"chan-#{comp_id}", resend_latest=True
            )

        def stream_recv(comp_id: ComponentId) -> Receiver[Sample[Quantity]]:
            return channels[comp_id].new_receiver()

        telem_fetcher = MagicMock(spec=ResampledStreamFetcher)
        telem_fetcher.fetch_stream = AsyncMock(side_effect=stream_recv)
        formula = parse(
            name="f",
            formula=formula_str,
            create_method=Quantity,
            telemetry_fetcher=telem_fetcher,
        )
        assert str(formula) == expected

        async with formula as formula:
            results_rx = formula.new_receiver()
            await asyncio.sleep(0.1)  # Allow time for setup
            now = datetime.now()
            tests_passed = 0

            for io_pair in io_pairs:
                io_input, io_output = io_pair
                now += timedelta(seconds=1)
                _ = await asyncio.gather(
                    *[
                        chan.new_sender().send(
                            Sample(now, None if not value else Quantity(value))
                        )
                        for chan, value in zip(
                            [
                                channels[ComponentId(comp_id)]
                                for comp_id in component_ids
                            ],
                            io_input,
                        )
                    ]
                )
                next_val = await results_rx.receive()
                if io_output is None:
                    assert next_val.value is None
                else:
                    assert (
                        next_val.value is not None
                        and next_val.value.base_value == io_output
                    )
                tests_passed += 1
                _logger.debug("%s: Passed inputs: %s", tests_passed, io_input)
            assert tests_passed == len(io_pairs)

    async def test_simple(self) -> None:
        """Test simple formulas."""
        await self.run_test(
            "#2 - #4 + #5",
            "[f](#2 - #4 + #5)",
            [2, 4, 5],
            [
                ([10.0, 12.0, 15.0], 13.0),
                ([15.0, 17.0, 20.0], 18.0),
            ],
        )
        await self.run_test(
            "#2 + #4 - #5",
            "[f](#2 + #4 - #5)",
            [2, 4, 5],
            [
                ([10.0, 12.0, 15.0], 7.0),
                ([15.0, 17.0, 20.0], 12.0),
            ],
        )
        await self.run_test(
            "#2 * #4 + #5",
            "[f](#2 * #4 + #5)",
            [2, 4, 5],
            [
                ([10.0, 12.0, 15.0], 135.0),
                ([15.0, 17.0, 20.0], 275.0),
            ],
        )
        await self.run_test(
            "#2 * #4 / #5",
            "[f](#2 * #4 / #5)",
            [2, 4, 5],
            [
                ([10.0, 12.0, 15.0], 8.0),
                ([15.0, 17.0, 20.0], 12.75),
            ],
        )
        await self.run_test(
            "#2 / #4 - #5",
            "[f](#2 / #4 - #5)",
            [2, 4, 5],
            [
                ([6.0, 12.0, 15.0], -14.5),
                ([15.0, 20.0, 20.0], -19.25),
            ],
        )
        await self.run_test(
            "#2 - #4 - #5",
            "[f](#2 - #4 - #5)",
            [2, 4, 5],
            [
                ([6.0, 12.0, 15.0], -21.0),
                ([15.0, 20.0, 20.0], -25.0),
            ],
        )
        await self.run_test(
            "#2 + #4 + #5",
            "[f](#2 + #4 + #5)",
            [2, 4, 5],
            [
                ([6.0, 12.0, 15.0], 33.0),
                ([15.0, 20.0, 20.0], 55.0),
            ],
        )
        await self.run_test(
            "#2 / #4 / #5",
            "[f](#2 / #4 / #5)",
            [2, 4, 5],
            [
                ([30.0, 3.0, 5.0], 2.0),
                ([15.0, 3.0, 2.0], 2.5),
            ],
        )
        # Test unary minus: Parser inserts a zero before it.
        await self.run_test(
            "-#2",
            "[f](0.0 - #2)",
            [2],
            [
                ([30.0], -30.0),
                ([15.0], -15.0),
            ],
        )

    async def test_compound(self) -> None:
        """Test compound formulas."""
        await self.run_test(
            "#2 + #4 - #5 * #6",
            "[f](#2 + #4 - #5 * #6)",
            [2, 4, 5, 6],
            [
                ([10.0, 12.0, 15.0, 2.0], -8.0),
                ([15.0, 17.0, 20.0, 1.5], 2.0),
            ],
        )
        await self.run_test(
            "#2 + (#4 - #5) * #6",
            "[f](#2 + (#4 - #5) * #6)",
            [2, 4, 5, 6],
            [
                ([10.0, 12.0, 15.0, 2.0], 4.0),
                ([15.0, 17.0, 20.0, 1.5], 10.5),
            ],
        )
        await self.run_test(
            "#2 + (#4 - #5 * #6)",
            "[f](#2 + #4 - #5 * #6)",
            [2, 4, 5, 6],
            [
                ([10.0, 12.0, 15.0, 2.0], -8.0),
                ([15.0, 17.0, 20.0, 1.5], 2.0),
            ],
        )
        await self.run_test(
            "#2 + (#4 - #5 - #6)",
            "[f](#2 + #4 - #5 - #6)",
            [2, 4, 5, 6],
            [
                ([10.0, 12.0, 15.0, 2.0], 5.0),
                ([15.0, 17.0, 20.0, 1.5], 10.5),
            ],
        )
        await self.run_test(
            "#2 + #4 - #5 - #6",
            "[f](#2 + #4 - #5 - #6)",
            [2, 4, 5, 6],
            [
                ([10.0, 12.0, 15.0, 2.0], 5.0),
                ([15.0, 17.0, 20.0, 1.5], 10.5),
            ],
        )
        await self.run_test(
            "#2 + #4 - (#5 - #6)",
            "[f](#2 + #4 - (#5 - #6))",
            [2, 4, 5, 6],
            [
                ([10.0, 12.0, 15.0, 2.0], 9.0),
                ([15.0, 17.0, 20.0, 1.5], 13.5),
            ],
        )
        await self.run_test(
            "(#2 + #4 - #5) * #6",
            "[f]((#2 + #4 - #5) * #6)",
            [2, 4, 5, 6],
            [
                ([10.0, 12.0, 15.0, 2.0], 14.0),
                ([15.0, 17.0, 20.0, 1.5], 18.0),
            ],
        )
        await self.run_test(
            "(#2 + #4 - #5) / #6",
            "[f]((#2 + #4 - #5) / #6)",
            [2, 4, 5, 6],
            [
                ([10.0, 12.0, 15.0, 2.0], 3.5),
                ([15.0, 17.0, 20.0, 1.5], 8.0),
            ],
        )
        await self.run_test(
            "#2 + #4 - (#5 / #6)",
            "[f](#2 + #4 - #5 / #6)",
            [2, 4, 5, 6],
            [
                ([10.0, 12.0, 15.0, 2.0], 14.5),
                ([15.0, 17.0, 20.0, 5.0], 28.0),
            ],
        )

        await self.run_test(
            "#2 - #4 + #5",
            "[f](#2 - #4 + #5)",
            [2, 4, 5],
            [
                ([10.0, 12.0, 15.0], 13.0),
                ([None, 12.0, 15.0], None),
                ([10.0, None, 15.0], None),
                ([15.0, 17.0, 20.0], 18.0),
                ([15.0, None, None], None),
            ],
        )

        await self.run_test(
            "#2 + #4 - (#5 * #6)",
            "[f](#2 + #4 - #5 * #6)",
            [2, 4, 5, 6],
            [
                ([10.0, 12.0, 15.0, 2.0], -8.0),
                ([10.0, 12.0, 15.0, None], None),
                ([10.0, None, 15.0, 2.0], None),
                ([15.0, 17.0, 20.0, 5.0], -68.0),
                ([15.0, 17.0, None, 5.0], None),
            ],
        )

    async def test_max_min_coalesce(self) -> None:
        """Test max, min and coalesce functions."""
        await self.run_test(
            "#2 + MAX(#4, #5)",
            "[f](#2 + MAX(#4, #5))",
            [2, 4, 5],
            [
                ([10.0, 12.0, 15.0], 25.0),
            ],
        )
        await self.run_test(
            "#2 + COALESCE(#4, #5, 0.0)",
            "[f](#2 + COALESCE(#4, #5, 0.0))",
            [2, 4, 5],
            [
                ([10.0, 12.0, 15.0], 22.0),
                ([10.0, None, 15.0], None),
                ([10.0, None, 15.0], 25.0),
                ([10.0, None, None], 10.0),
            ],
        )
        await self.run_test(
            "MIN(#2, #4) + COALESCE(#5, 0.0)",
            "[f](MIN(#2, #4) + COALESCE(#5, 0.0))",
            [2, 4, 5],
            [
                ([4.0, 6.0, 5.0], 9.0),
                ([-2.0, 1.0, 5.0], 3.0),
                ([-2.0, 15.0, None], -2.0),
                ([None, 15.0, None], None),
            ],
        )
        await self.run_test(
            "MIN(#23, 0.0) + COALESCE(MAX(#24 - #25, 0.0), 0.0)",
            "[f](MIN(#23, 0.0) + COALESCE(MAX(#24 - #25, 0.0), 0.0))",
            [23, 24, 25],
            [
                ([4.0, 6.0, 5.0], 1.0),
                ([-2.0, 1.0, 5.0], -2.0),
                ([-2.0, 15.0, 1.0], 12.0),
                ([None, 15.0, 1.0], None),
                ([None, None, 1.0], None),
                ([None, None, None], None),
                ([-2.0, None, None], -2.0),
                ([-2.0, None, 5.0], -2.0),
                ([-2.0, 15.0, None], -2.0),
            ],
        )


class TestFormulaComposition:
    """Tests for formula channels."""

    async def run_test(  # pylint: disable=too-many-locals
        self,
        num_items: int,
        make_builder: (
            Callable[[Formula[Quantity]], FormulaBuilder[Quantity]]
            | Callable[[Formula[Quantity], Formula[Quantity]], FormulaBuilder[Quantity]]
            | Callable[
                [Formula[Quantity], Formula[Quantity], Formula[Quantity]],
                FormulaBuilder[Quantity],
            ]
            | Callable[
                [
                    Formula[Quantity],
                    Formula[Quantity],
                    Formula[Quantity],
                    Formula[Quantity],
                ],
                FormulaBuilder[Quantity],
            ]
        ),
        expected: str,
        io_pairs: list[tuple[list[float | None], float | None]],
    ) -> None:
        """Run a test with the specs provided."""
        channels: OrderedDict[int, Broadcast[Sample[Quantity]]] = OrderedDict()

        for ctr in range(num_items):
            channels[ctr] = Broadcast(name=f"chan-#{ctr}", resend_latest=True)

        def stream_recv(comp_id: int) -> Receiver[Sample[Quantity]]:
            comp_id = int(comp_id)
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
            for ctr in range(num_items)
        ]
        builder = make_builder(*l1_formulas)
        formula = builder.build("l2")

        assert str(formula) == expected

        _logger.debug("TESTING FORMULA: %s", expected)

        result_chan = formula.new_receiver()
        await asyncio.sleep(0.1)
        now = datetime.now()
        tests_passed = 0
        for io_pair in io_pairs:
            io_input, io_output = io_pair
            now += timedelta(seconds=1)
            _ = await asyncio.gather(
                *[
                    channels[comp_id]
                    .new_sender()
                    .send(Sample(now, None if not value else Quantity(value)))
                    for comp_id, value in enumerate(io_input)
                ]
            )
            next_val = await result_chan.receive()
            if io_output is None:
                assert next_val.value is None
            else:
                assert (
                    next_val.value is not None
                    and next_val.value.base_value == io_output
                )
            tests_passed += 1
            _logger.debug("%s: Passed inputs: %s", tests_passed, io_input)
        await formula.stop()
        assert tests_passed == len(io_pairs)

    async def test_simple(self) -> None:
        """Test simple formulas."""
        await self.run_test(
            3,
            lambda c2, c4, c5: c2 - c4 + c5,
            "[l2]([0](#0) - [1](#1) + [2](#2))",
            [
                ([10.0, 12.0, 15.0], 13.0),
                ([15.0, 17.0, 20.0], 18.0),
            ],
        )
        await self.run_test(
            3,
            lambda c2, c4, c5: c2 + c4 - c5,
            "[l2]([0](#0) + [1](#1) - [2](#2))",
            [
                ([10.0, 12.0, 15.0], 7.0),
                ([15.0, 17.0, 20.0], 12.0),
            ],
        )
        await self.run_test(
            2,
            lambda c4, c5: (c4 + c5) * 2.0,
            "[l2](([0](#0) + [1](#1)) * 2.0)",
            [
                ([10.0, 15.0], 50.0),
                ([15.0, 20.0], 70.0),
            ],
        )
        await self.run_test(
            2,
            lambda c4, c5: ((c4 - c5) / 2.0) * 3.0,
            "[l2](([0](#0) - [1](#1)) / 2.0 * 3.0)",
            [
                ([10.0, 15.0], -7.5),
                ([15.0, 25.0], -15.0),
            ],
        )
        await self.run_test(
            2,
            lambda c2, c5: c2 / 2.0 - c5,
            "[l2]([0](#0) / 2.0 - [1](#1))",
            [
                ([6.0, 15.0], -12.0),
                ([15.0, 20.0], -12.5),
            ],
        )
        await self.run_test(
            3,
            lambda c2, c4, c5: c2 - c4 - c5,
            "[l2]([0](#0) - [1](#1) - [2](#2))",
            [
                ([6.0, 12.0, 15.0], -21.0),
                ([15.0, 20.0, 20.0], -25.0),
            ],
        )
        await self.run_test(
            3,
            lambda c2, c4, c5: c2 + c4 + c5,
            "[l2]([0](#0) + [1](#1) + [2](#2))",
            [
                ([6.0, 12.0, 15.0], 33.0),
                ([15.0, 20.0, 20.0], 55.0),
            ],
        )
        await self.run_test(
            1,
            lambda c2: c2 / 2.0 / 5.0,
            "[l2]([0](#0) / 2.0 / 5.0)",
            [
                ([30.0], 3.0),
                ([150.0], 15.0),
            ],
        )

        await self.run_test(
            3,
            lambda c2, c4, c5: c2 - c4 + c5,
            "[l2]([0](#0) - [1](#1) + [2](#2))",
            [
                ([10.0, 12.0, 15.0], 13.0),
                ([None, 12.0, 15.0], None),
                ([10.0, None, 15.0], None),
                ([15.0, 17.0, 20.0], 18.0),
                ([15.0, None, None], None),
            ],
        )

    async def test_max(self) -> None:
        """Test the max function."""
        await self.run_test(
            3,
            lambda c2, c4, c5: c2 + c4.max(c5),
            "[l2]([0](#0) + MAX([1](#1), [2](#2)))",
            [
                ([10.0, 12.0, 15.0], 25.0),
            ],
        )
        await self.run_test(
            3,
            lambda c2, c4, c5: (c2 + c4).max(c5),
            "[l2](MAX([0](#0) + [1](#1), [2](#2)))",
            [
                ([10.0, 12.0, 15.0], 22.0),
            ],
        )
        await self.run_test(
            3,
            lambda c2, c4, c5: (c2 + c4).max(c5),
            "[l2](MAX([0](#0) + [1](#1), [2](#2)))",
            [
                ([10.0, 12.0, 15.0], 22.0),
            ],
        )
        await self.run_test(
            3,
            lambda c2, c4, c5: c2 + c4.max(c5),
            "[l2]([0](#0) + MAX([1](#1), [2](#2)))",
            [
                ([10.0, 12.0, 15.0], 25.0),
            ],
        )

    async def test_min(self) -> None:
        """Test the min function."""
        await self.run_test(
            3,
            lambda c2, c4, c5: (c2 + c4).min(c5),
            "[l2](MIN([0](#0) + [1](#1), [2](#2)))",
            [
                ([10.0, 12.0, 15.0], 15.0),
            ],
        )
        await self.run_test(
            3,
            lambda c2, c4, c5: c2 + c4.min(c5),
            "[l2]([0](#0) + MIN([1](#1), [2](#2)))",
            [
                ([10.0, 12.0, 15.0], 22.0),
            ],
        )
        await self.run_test(
            3,
            lambda c2, c4, c5: (c2 + c4).min(c5),
            "[l2](MIN([0](#0) + [1](#1), [2](#2)))",
            [
                ([10.0, 2.0, 15.0], 12.0),
            ],
        )
        await self.run_test(
            3,
            lambda c2, c4, c5: c2 + c4.min(c5),
            "[l2]([0](#0) + MIN([1](#1), [2](#2)))",
            [
                ([10.0, 12.0, 15.0], 22.0),
            ],
        )

    async def test_coalesce(self) -> None:
        """Test the coalesce function."""
        await self.run_test(
            3,
            lambda c2, c4, c5: c2.coalesce(c4, c5),
            "[l2](COALESCE([0](#0), [1](#1), [2](#2)))",
            [
                ([None, 12.0, 15.0], None),
                ([None, 12.0, 15.0], 12.0),
                ([None, None, 15.0], None),
                ([None, None, 15.0], 15.0),
                ([10.0, None, 15.0], 10.0),
                ([None, None, None], None),
            ],
        )

        await self.run_test(
            3,
            lambda c2, c4, c5: (c2 * 5.0).coalesce(c4 / 2.0, c5),
            "[l2](COALESCE([0](#0) * 5.0, [1](#1) / 2.0, [2](#2)))",
            [
                ([None, 12.0, 15.0], None),
                ([None, 12.0, 15.0], 6.0),
                ([None, None, 15.0], None),
                ([None, None, 15.0], 15.0),
                ([10.0, None, 15.0], 50.0),
                # Subscription to c5 was kept because we only unsubscribe after 3 samples
                ([None, None, 15.0], 15.0),
                ([None, None, 15.0], 15.0),
                ([None, None, 15.0], 15.0),
                ([None, None, None], None),
            ],
        )

    async def test_min_max_coalesce(self) -> None:
        """Test min and max functions in combination."""
        await self.run_test(
            3,
            lambda c2, c4, c5: c2.min(c4).max(c5),
            "[l2](MAX(MIN([0](#0), [1](#1)), [2](#2)))",
            [
                ([4.0, 6.0, 5.0], 5.0),
            ],
        )

        await self.run_test(
            3,
            lambda c2, c4, c5: c2.min(Quantity(0.0))
            + (c4 - c5).max(Quantity(0.0)).coalesce(Quantity(0.0)),
            "[l2](MIN([0](#0), 0.0) + COALESCE(MAX([1](#1) - [2](#2), 0.0), 0.0))",
            [
                ([4.0, 6.0, 5.0], 1.0),
                ([-2.0, 1.0, 5.0], -2.0),
                ([-2.0, 15.0, 1.0], 12.0),
                ([None, 15.0, 1.0], None),
                ([None, None, 1.0], None),
                ([None, None, None], None),
                ([-2.0, None, None], -2.0),
                ([-2.0, None, 5.0], -2.0),
                ([-2.0, 15.0, None], -2.0),
            ],
        )

    async def test_compound(self) -> None:
        """Test compound formulas."""
        await self.run_test(
            4,
            lambda c2, c4, c5, c6: c2 + c4 - c5 + c6 * 2.0,
            "[l2]([0](#0) + [1](#1) - [2](#2) + [3](#3) * 2.0)",
            [
                ([10.0, 12.0, 15.0, 2.0], 11.0),
                ([15.0, 17.0, 20.0, 1.5], 15.0),
            ],
        )
        await self.run_test(
            4,
            lambda c2, c4, c5, c6: c2 + (c4 - c5) - c6 * 2.0,
            "[l2]([0](#0) + [1](#1) - [2](#2) - [3](#3) * 2.0)",
            [
                ([10.0, 12.0, 15.0, 2.0], 3.0),
                ([15.0, 17.0, 20.0, 1.5], 9.0),
            ],
        )
        await self.run_test(
            4,
            lambda c2, c4, c5, c6: c2 + (c4 - c5 + c6) * 2.0,
            "[l2]([0](#0) + ([1](#1) - [2](#2) + [3](#3)) * 2.0)",
            [
                ([10.0, 12.0, 15.0, 2.0], 8.0),
                ([15.0, 17.0, 20.0, 1.5], 12.0),
            ],
        )
        await self.run_test(
            4,
            lambda c2, c4, c5, c6: c2 + (c4 - c5 - c6),
            "[l2]([0](#0) + [1](#1) - [2](#2) - [3](#3))",
            [
                ([10.0, 12.0, 15.0, 2.0], 5.0),
                ([15.0, 17.0, 20.0, 1.5], 10.5),
            ],
        )
        await self.run_test(
            4,
            lambda c2, c4, c5, c6: c2 + c4 - c5 - c6,
            "[l2]([0](#0) + [1](#1) - [2](#2) - [3](#3))",
            [
                ([10.0, 12.0, 15.0, 2.0], 5.0),
                ([15.0, 17.0, 20.0, 1.5], 10.5),
            ],
        )
        await self.run_test(
            4,
            lambda c2, c4, c5, c6: c2 + c4 - (c5 - c6),
            "[l2]([0](#0) + [1](#1) - ([2](#2) - [3](#3)))",
            [
                ([10.0, 12.0, 15.0, 2.0], 9.0),
                ([15.0, 17.0, 20.0, 1.5], 13.5),
            ],
        )
        await self.run_test(
            4,
            lambda c2, c4, c5, c6: (c2 + c4 - c5) - c6 * 2.0,
            "[l2]([0](#0) + [1](#1) - [2](#2) - [3](#3) * 2.0)",
            [
                ([10.0, 12.0, 15.0, 2.0], 3.0),
                ([15.0, 17.0, 20.0, 1.5], 9.0),
            ],
        )
        await self.run_test(
            3,
            lambda c2, c4, c5: (c2 + c4 - c5) / 2.0,
            "[l2](([0](#0) + [1](#1) - [2](#2)) / 2.0)",
            [
                ([10.0, 15.0, 2.0], 11.5),
                ([15.0, 20.0, 1.5], 16.75),
            ],
        )

        await self.run_test(
            4,
            lambda c2, c4, c5, c6: c2 + c4 - (c5 - c6 / 2.0),
            "[l2]([0](#0) + [1](#1) - ([2](#2) - [3](#3) / 2.0))",
            [
                ([10.0, 12.0, 15.0, 2.0], 8.0),
                ([10.0, 12.0, 15.0, None], None),
                ([10.0, None, 15.0, 2.0], None),
                ([15.0, 17.0, 20.0, 5.0], 14.5),
                ([15.0, 17.0, None, 5.0], None),
            ],
        )


class TestCoalesceFunction:
    """Test coalesce function subscribe/unsubscribe behavior."""

    class CoalesceSample(NamedTuple):
        """Helper class to represent expected behavior of coalesce function."""

        values: list[float | None]
        expected_subscriptions: list[bool]

    async def run_test(  # pylint: disable=too-many-locals
        self,
        formula_str: str,
        samples: list[CoalesceSample],
    ) -> None:
        """Run a test with the specs provided."""
        # Component IDs are 0, 1, 2 for convenience.
        channels: list[Broadcast[Sample[Quantity]]] = [
            Broadcast(name=str(num)) for num in range(3)
        ]
        senders = [channel.new_sender() for channel in channels]
        receivers: list[Receiver[Sample[Quantity]] | None] = [None, None, None]

        def new_receiver(component_id: ComponentId) -> Receiver[Sample[Quantity]]:
            """Create a new receiver, overwriting any existing one.

            When Coalesce unsubscribes, it closes its receiver.
            """
            comp_id = int(component_id)
            receiver = channels[comp_id].new_receiver()
            receivers[comp_id] = receiver
            return receiver

        telem_fetcher = MagicMock(spec=ResampledStreamFetcher)
        telem_fetcher.fetch_stream = AsyncMock(side_effect=new_receiver)
        formula = parse(
            name="f2",
            formula=formula_str,
            create_method=Quantity,
            telemetry_fetcher=telem_fetcher,
        )

        result_chan = formula.new_receiver()
        await asyncio.sleep(0.1)
        now = datetime.now()

        async def send_sample(values: list[float | None]) -> None:
            nonlocal now
            now += timedelta(seconds=1)
            _ = await asyncio.gather(
                *[
                    senders[comp_id].send(
                        Sample(now, None if not value else Quantity(value))
                    )
                    for comp_id, value in enumerate(values)
                ]
            )
            _ = await result_chan.receive()

        for sample in samples:
            await send_sample(sample.values)
            active_subscriptions = [
                receiver is not None and not getattr(receiver, "_closed", True)
                for receiver in receivers
            ]
            assert active_subscriptions == sample.expected_subscriptions

        await formula.stop()

    async def test_coalesce_subscribe(self) -> None:
        """Test coalesce subscribes when None values are encountered."""
        await self.run_test(
            "COALESCE(#0, #1, #2, 0.0)",
            [
                self.CoalesceSample(
                    values=[10.0, None, None],
                    expected_subscriptions=[True, False, False],
                ),
                # No need to subscribe unless stream #1 gives None
                self.CoalesceSample(
                    values=[10.0, 12.0, 15.0],
                    expected_subscriptions=[True, False, False],
                ),
                # If None is encountered, one subscription is added per sample
                self.CoalesceSample(
                    values=[None, None, 15.0],
                    expected_subscriptions=[True, True, False],
                ),
                self.CoalesceSample(
                    values=[None, None, 15.0],
                    expected_subscriptions=[True, True, True],
                ),
            ],
        )

    async def test_coalesce_unsubscribe(self) -> None:
        """Test coalesce only unsubscribes after 3 samples."""
        await self.run_test(
            "COALESCE(#0, #1, #2, 0.0)",
            [
                # First subscription is added before the first sample.
                # Every sample can add one subscription.
                self.CoalesceSample(
                    values=[None, None, 15.0],
                    expected_subscriptions=[True, True, False],
                ),
                self.CoalesceSample(
                    values=[None, None, 15.0],
                    expected_subscriptions=[True, True, True],
                ),
                # After 3 samples, the last subscription is dropped.
                self.CoalesceSample(
                    values=[None, 12.0, 15.0],
                    expected_subscriptions=[True, True, True],
                ),
                self.CoalesceSample(
                    values=[None, 12.0, 15.0],
                    expected_subscriptions=[True, True, True],
                ),
                self.CoalesceSample(
                    values=[None, 12.0, 15.0],
                    expected_subscriptions=[True, True, False],
                ),
            ],
        )
