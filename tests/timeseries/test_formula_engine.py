# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for the FormulaEngine and the Tokenizer."""

import asyncio
from collections.abc import Callable
from datetime import datetime

from frequenz.channels import Broadcast, Receiver

from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries._quantities import Power
from frequenz.sdk.timeseries.formula_engine._formula_engine import (
    FormulaBuilder,
    FormulaEngine,
    HigherOrderFormulaBuilder,
)
from frequenz.sdk.timeseries.formula_engine._tokenizer import (
    Token,
    Tokenizer,
    TokenType,
)


class TestTokenizer:
    """Tests for the Tokenizer."""

    def test_1(self) -> None:
        """Test the tokenization of the formula: "#10 + #20 - (#5 * #4)"."""
        tokens = []
        lexer = Tokenizer("#10 + #20 - (#5 * #4)")
        for token in lexer:
            tokens.append(token)
        assert tokens == [
            Token(TokenType.COMPONENT_METRIC, "10"),
            Token(TokenType.OPER, "+"),
            Token(TokenType.COMPONENT_METRIC, "20"),
            Token(TokenType.OPER, "-"),
            Token(TokenType.OPER, "("),
            Token(TokenType.COMPONENT_METRIC, "5"),
            Token(TokenType.OPER, "*"),
            Token(TokenType.COMPONENT_METRIC, "4"),
            Token(TokenType.OPER, ")"),
        ]


class TestFormulaEngine:
    """Tests for the FormulaEngine."""

    async def run_test(  # pylint: disable=too-many-locals
        self,
        formula: str,
        postfix: str,
        io_pairs: list[tuple[list[float | None], float | None]],
        nones_are_zeros: bool = False,
    ) -> None:
        """Run a formula test."""
        channels: dict[str, Broadcast[Sample[float]]] = {}
        builder = FormulaBuilder("test_formula", Power.from_watts)
        for token in Tokenizer(formula):
            if token.type == TokenType.COMPONENT_METRIC:
                if token.value not in channels:
                    channels[token.value] = Broadcast(token.value)
                builder.push_metric(
                    f"#{token.value}",
                    channels[token.value].new_receiver(),
                    nones_are_zeros=nones_are_zeros,
                )
            elif token.type == TokenType.OPER:
                builder.push_oper(token.value)
        engine = builder.build()
        results_rx = engine.new_receiver()

        assert repr(builder._steps) == postfix  # pylint: disable=protected-access

        now = datetime.now()
        tests_passed = 0
        for io_pair in io_pairs:
            io_input, io_output = io_pair
            await asyncio.gather(
                *[
                    chan.new_sender().send(Sample(now, None if not value else value))
                    for chan, value in zip(channels.values(), io_input)
                ]
            )
            next_val = await results_rx.receive()
            if io_output is None:
                assert next_val.value is None
            else:
                assert (
                    next_val.value is not None
                    and Power.as_watts(next_val.value) == io_output
                )
            tests_passed += 1
        await engine._stop()  # pylint: disable=protected-access
        assert tests_passed == len(io_pairs)

    async def test_simple(self) -> None:
        """Test simple formulas."""
        await self.run_test(
            "#2 - #4 + #5",
            "[#2, #4, -, #5, +]",
            [
                ([10.0, 12.0, 15.0], 13.0),
                ([15.0, 17.0, 20.0], 18.0),
            ],
        )
        await self.run_test(
            "#2 + #4 - #5",
            "[#2, #4, #5, -, +]",
            [
                ([10.0, 12.0, 15.0], 7.0),
                ([15.0, 17.0, 20.0], 12.0),
            ],
        )
        await self.run_test(
            "#2 * #4 + #5",
            "[#2, #4, *, #5, +]",
            [
                ([10.0, 12.0, 15.0], 135.0),
                ([15.0, 17.0, 20.0], 275.0),
            ],
        )
        await self.run_test(
            "#2 * #4 / #5",
            "[#2, #4, #5, /, *]",
            [
                ([10.0, 12.0, 15.0], 8.0),
                ([15.0, 17.0, 20.0], 12.75),
            ],
        )
        await self.run_test(
            "#2 / #4 - #5",
            "[#2, #4, /, #5, -]",
            [
                ([6.0, 12.0, 15.0], -14.5),
                ([15.0, 20.0, 20.0], -19.25),
            ],
        )
        await self.run_test(
            "#2 - #4 - #5",
            "[#2, #4, -, #5, -]",
            [
                ([6.0, 12.0, 15.0], -21.0),
                ([15.0, 20.0, 20.0], -25.0),
            ],
        )
        await self.run_test(
            "#2 + #4 + #5",
            "[#2, #4, +, #5, +]",
            [
                ([6.0, 12.0, 15.0], 33.0),
                ([15.0, 20.0, 20.0], 55.0),
            ],
        )
        await self.run_test(
            "#2 / #4 / #5",
            "[#2, #4, /, #5, /]",
            [
                ([30.0, 3.0, 5.0], 2.0),
                ([15.0, 3.0, 2.0], 2.5),
            ],
        )

    async def test_compound(self) -> None:
        """Test compound formulas."""
        await self.run_test(
            "#2 + #4 - #5 * #6",
            "[#2, #4, #5, #6, *, -, +]",
            [
                ([10.0, 12.0, 15.0, 2.0], -8.0),
                ([15.0, 17.0, 20.0, 1.5], 2.0),
            ],
        )
        await self.run_test(
            "#2 + (#4 - #5) * #6",
            "[#2, #4, #5, -, #6, *, +]",
            [
                ([10.0, 12.0, 15.0, 2.0], 4.0),
                ([15.0, 17.0, 20.0, 1.5], 10.5),
            ],
        )
        await self.run_test(
            "#2 + (#4 - #5 * #6)",
            "[#2, #4, #5, #6, *, -, +]",
            [
                ([10.0, 12.0, 15.0, 2.0], -8.0),
                ([15.0, 17.0, 20.0, 1.5], 2.0),
            ],
        )
        await self.run_test(
            "#2 + (#4 - #5 - #6)",
            "[#2, #4, #5, -, #6, -, +]",
            [
                ([10.0, 12.0, 15.0, 2.0], 5.0),
                ([15.0, 17.0, 20.0, 1.5], 10.5),
            ],
        )
        await self.run_test(
            "#2 + #4 - #5 - #6",
            "[#2, #4, #5, -, #6, -, +]",
            [
                ([10.0, 12.0, 15.0, 2.0], 5.0),
                ([15.0, 17.0, 20.0, 1.5], 10.5),
            ],
        )
        await self.run_test(
            "#2 + #4 - (#5 - #6)",
            "[#2, #4, #5, #6, -, -, +]",
            [
                ([10.0, 12.0, 15.0, 2.0], 9.0),
                ([15.0, 17.0, 20.0, 1.5], 13.5),
            ],
        )
        await self.run_test(
            "(#2 + #4 - #5) * #6",
            "[#2, #4, #5, -, +, #6, *]",
            [
                ([10.0, 12.0, 15.0, 2.0], 14.0),
                ([15.0, 17.0, 20.0, 1.5], 18.0),
            ],
        )
        await self.run_test(
            "(#2 + #4 - #5) / #6",
            "[#2, #4, #5, -, +, #6, /]",
            [
                ([10.0, 12.0, 15.0, 2.0], 3.5),
                ([15.0, 17.0, 20.0, 1.5], 8.0),
            ],
        )
        await self.run_test(
            "#2 + #4 - (#5 / #6)",
            "[#2, #4, #5, #6, /, -, +]",
            [
                ([10.0, 12.0, 15.0, 2.0], 14.5),
                ([15.0, 17.0, 20.0, 5.0], 28.0),
            ],
        )

    async def test_nones_are_zeros(self) -> None:
        """Test that `None`s are treated as zeros when configured."""
        await self.run_test(
            "#2 - #4 + #5",
            "[#2, #4, -, #5, +]",
            [
                ([10.0, 12.0, 15.0], 13.0),
                ([None, 12.0, 15.0], 3.0),
                ([10.0, None, 15.0], 25.0),
                ([15.0, 17.0, 20.0], 18.0),
                ([15.0, None, None], 15.0),
            ],
            True,
        )

        await self.run_test(
            "#2 + #4 - (#5 * #6)",
            "[#2, #4, #5, #6, *, -, +]",
            [
                ([10.0, 12.0, 15.0, 2.0], -8.0),
                ([10.0, 12.0, 15.0, None], 22.0),
                ([10.0, None, 15.0, 2.0], -20.0),
                ([15.0, 17.0, 20.0, 5.0], -68.0),
                ([15.0, 17.0, None, 5.0], 32.0),
            ],
            True,
        )

    async def test_nones_are_not_zeros(self) -> None:
        """Test that calculated values are `None` on input `None`s."""
        await self.run_test(
            "#2 - #4 + #5",
            "[#2, #4, -, #5, +]",
            [
                ([10.0, 12.0, 15.0], 13.0),
                ([None, 12.0, 15.0], None),
                ([10.0, None, 15.0], None),
                ([15.0, 17.0, 20.0], 18.0),
                ([15.0, None, None], None),
            ],
            False,
        )

        await self.run_test(
            "#2 + #4 - (#5 * #6)",
            "[#2, #4, #5, #6, *, -, +]",
            [
                ([10.0, 12.0, 15.0, 2.0], -8.0),
                ([10.0, 12.0, 15.0, None], None),
                ([10.0, None, 15.0, 2.0], None),
                ([15.0, 17.0, 20.0, 5.0], -68.0),
                ([15.0, 17.0, None, 5.0], None),
            ],
            False,
        )


class TestFormulaEngineComposition:
    """Tests for formula channels."""

    def make_engine(
        self, stream_id: int, data: Receiver[Sample[float]]
    ) -> FormulaEngine[Power]:
        """Make a basic FormulaEngine."""
        name = f"#{stream_id}"
        builder = FormulaBuilder(name, create_method=Power.from_watts)
        builder.push_metric(
            name,
            data,
            nones_are_zeros=False,
        )
        return FormulaEngine(builder, create_method=Power.from_watts)

    async def run_test(  # pylint: disable=too-many-locals
        self,
        num_items: int,
        make_builder: (
            Callable[
                [
                    FormulaEngine[Power],
                ],
                HigherOrderFormulaBuilder[Power],
            ]
            | Callable[
                [
                    FormulaEngine[Power],
                    FormulaEngine[Power],
                ],
                HigherOrderFormulaBuilder[Power],
            ]
            | Callable[
                [
                    FormulaEngine[Power],
                    FormulaEngine[Power],
                    FormulaEngine[Power],
                ],
                HigherOrderFormulaBuilder[Power],
            ]
            | Callable[
                [
                    FormulaEngine[Power],
                    FormulaEngine[Power],
                    FormulaEngine[Power],
                    FormulaEngine[Power],
                ],
                HigherOrderFormulaBuilder[Power],
            ]
        ),
        io_pairs: list[tuple[list[float | None], float | None]],
        nones_are_zeros: bool = False,
    ) -> None:
        """Run a test with the specs provided."""
        channels = [Broadcast[Sample[float]](str(ctr)) for ctr in range(num_items)]
        l1_engines = [
            self.make_engine(ctr, channels[ctr].new_receiver())
            for ctr in range(num_items)
        ]
        builder = make_builder(*l1_engines)
        engine = builder.build("l2 formula", nones_are_zeros=nones_are_zeros)
        result_chan = engine.new_receiver()

        now = datetime.now()
        tests_passed = 0
        for io_pair in io_pairs:
            io_input, io_output = io_pair
            await asyncio.gather(
                *[
                    chan.new_sender().send(Sample(now, None if not value else value))
                    for chan, value in zip(channels, io_input)
                ]
            )
            next_val = await result_chan.receive()
            if io_output is None:
                assert next_val.value is None
            else:
                assert (
                    next_val.value is not None
                    and Power.as_watts(next_val.value) == io_output
                )
            tests_passed += 1
        await engine._stop()  # pylint: disable=protected-access
        assert tests_passed == len(io_pairs)

    async def test_simple(self) -> None:
        """Test simple formulas."""
        await self.run_test(
            3,
            lambda c2, c4, c5: c2 - c4 + c5,
            [
                ([10.0, 12.0, 15.0], 13.0),
                ([15.0, 17.0, 20.0], 18.0),
            ],
        )
        await self.run_test(
            3,
            lambda c2, c4, c5: c2 + c4 - c5,
            [
                ([10.0, 12.0, 15.0], 7.0),
                ([15.0, 17.0, 20.0], 12.0),
            ],
        )
        await self.run_test(
            3,
            lambda c2, c4, c5: c2 * c4 + c5,
            [
                ([10.0, 12.0, 15.0], 135.0),
                ([15.0, 17.0, 20.0], 275.0),
            ],
        )
        await self.run_test(
            3,
            lambda c2, c4, c5: c2 * c4 / c5,
            [
                ([10.0, 12.0, 15.0], 8.0),
                ([15.0, 17.0, 20.0], 12.75),
            ],
        )
        await self.run_test(
            3,
            lambda c2, c4, c5: c2 / c4 - c5,
            [
                ([6.0, 12.0, 15.0], -14.5),
                ([15.0, 20.0, 20.0], -19.25),
            ],
        )
        await self.run_test(
            3,
            lambda c2, c4, c5: c2 - c4 - c5,
            [
                ([6.0, 12.0, 15.0], -21.0),
                ([15.0, 20.0, 20.0], -25.0),
            ],
        )
        await self.run_test(
            3,
            lambda c2, c4, c5: c2 + c4 + c5,
            [
                ([6.0, 12.0, 15.0], 33.0),
                ([15.0, 20.0, 20.0], 55.0),
            ],
        )
        await self.run_test(
            3,
            lambda c2, c4, c5: c2 / c4 / c5,
            [
                ([30.0, 3.0, 5.0], 2.0),
                ([15.0, 3.0, 2.0], 2.5),
            ],
        )

    async def test_min_max(self) -> None:
        """Test min and max functions in combination."""
        await self.run_test(
            3,
            lambda c2, c4, c5: c2.min(c4).max(c5),
            [
                ([4.0, 6.0, 5.0], 5.0),
            ],
        )

    async def test_max(self) -> None:
        """Test the max function."""
        await self.run_test(
            3,
            lambda c2, c4, c5: c2 * c4.max(c5),
            [
                ([10.0, 12.0, 15.0], 150.0),
            ],
        )
        await self.run_test(
            3,
            lambda c2, c4, c5: (c2 * c4).max(c5),
            [
                ([10.0, 12.0, 15.0], 120.0),
            ],
        )
        await self.run_test(
            3,
            lambda c2, c4, c5: (c2 + c4).max(c5),
            [
                ([10.0, 12.0, 15.0], 22.0),
            ],
        )
        await self.run_test(
            3,
            lambda c2, c4, c5: c2 + c4.max(c5),
            [
                ([10.0, 12.0, 15.0], 25.0),
            ],
        )

    async def test_min(self) -> None:
        """Test the min function."""
        await self.run_test(
            3,
            lambda c2, c4, c5: (c2 * c4).min(c5),
            [
                ([10.0, 12.0, 15.0], 15.0),
            ],
        )
        await self.run_test(
            3,
            lambda c2, c4, c5: c2 * c4.min(c5),
            [
                ([10.0, 12.0, 15.0], 120.0),
            ],
        )
        await self.run_test(
            3,
            lambda c2, c4, c5: (c2 + c4).min(c5),
            [
                ([10.0, 2.0, 15.0], 12.0),
            ],
        )
        await self.run_test(
            3,
            lambda c2, c4, c5: c2 + c4.min(c5),
            [
                ([10.0, 12.0, 15.0], 22.0),
            ],
        )

    async def test_compound(self) -> None:
        """Test compound formulas."""
        await self.run_test(
            4,
            lambda c2, c4, c5, c6: c2 + c4 - c5 * c6,
            [
                ([10.0, 12.0, 15.0, 2.0], -8.0),
                ([15.0, 17.0, 20.0, 1.5], 2.0),
            ],
        )
        await self.run_test(
            4,
            lambda c2, c4, c5, c6: c2 + (c4 - c5) * c6,
            [
                ([10.0, 12.0, 15.0, 2.0], 4.0),
                ([15.0, 17.0, 20.0, 1.5], 10.5),
            ],
        )
        await self.run_test(
            4,
            lambda c2, c4, c5, c6: c2 + (c4 - c5 * c6),
            [
                ([10.0, 12.0, 15.0, 2.0], -8.0),
                ([15.0, 17.0, 20.0, 1.5], 2.0),
            ],
        )
        await self.run_test(
            4,
            lambda c2, c4, c5, c6: c2 + (c4 - c5 - c6),
            [
                ([10.0, 12.0, 15.0, 2.0], 5.0),
                ([15.0, 17.0, 20.0, 1.5], 10.5),
            ],
        )
        await self.run_test(
            4,
            lambda c2, c4, c5, c6: c2 + c4 - c5 - c6,
            [
                ([10.0, 12.0, 15.0, 2.0], 5.0),
                ([15.0, 17.0, 20.0, 1.5], 10.5),
            ],
        )
        await self.run_test(
            4,
            lambda c2, c4, c5, c6: c2 + c4 - (c5 - c6),
            [
                ([10.0, 12.0, 15.0, 2.0], 9.0),
                ([15.0, 17.0, 20.0, 1.5], 13.5),
            ],
        )
        await self.run_test(
            4,
            lambda c2, c4, c5, c6: (c2 + c4 - c5) * c6,
            [
                ([10.0, 12.0, 15.0, 2.0], 14.0),
                ([15.0, 17.0, 20.0, 1.5], 18.0),
            ],
        )
        await self.run_test(
            4,
            lambda c2, c4, c5, c6: (c2 + c4 - c5) / c6,
            [
                ([10.0, 12.0, 15.0, 2.0], 3.5),
                ([15.0, 17.0, 20.0, 1.5], 8.0),
            ],
        )
        await self.run_test(
            4,
            lambda c2, c4, c5, c6: c2 + c4 - (c5 / c6),
            [
                ([10.0, 12.0, 15.0, 2.0], 14.5),
                ([15.0, 17.0, 20.0, 5.0], 28.0),
            ],
        )

    async def test_consumption(self) -> None:
        """Test the consumption operator."""
        await self.run_test(
            1,
            lambda c1: c1.consumption(),
            [
                ([10.0], 10.0),
                ([-10.0], 0.0),
            ],
        )

    async def test_production(self) -> None:
        """Test the production operator."""
        await self.run_test(
            1,
            lambda c1: c1.production(),
            [
                ([10.0], 0.0),
                ([-10.0], 10.0),
            ],
        )

    async def test_consumption_production(self) -> None:
        """Test the consumption and production operator combined."""
        await self.run_test(
            2,
            lambda c1, c2: c1.consumption() + c2.production(),
            [
                ([10.0, 12.0], 10.0),
                ([-12.0, -10.0], 10.0),
            ],
        )
        await self.run_test(
            2,
            lambda c1, c2: c1.consumption() + c2.consumption(),
            [
                ([10.0, -12.0], 10.0),
                ([-10.0, 12.0], 12.0),
            ],
        )
        await self.run_test(
            2,
            lambda c1, c2: c1.production() + c2.production(),
            [
                ([10.0, -12.0], 12.0),
                ([-10.0, 12.0], 10.0),
            ],
        )
        await self.run_test(
            2,
            lambda c1, c2: c1.min(c2).consumption(),
            [
                ([10.0, -12.0], 0.0),
                ([10.0, 12.0], 10.0),
            ],
        )
        await self.run_test(
            2,
            lambda c1, c2: c1.max(c2).consumption(),
            [
                ([10.0, -12.0], 10.0),
                ([-10.0, -12.0], 0.0),
            ],
        )
        await self.run_test(
            2,
            lambda c1, c2: c1.min(c2).production(),
            [
                ([10.0, -12.0], 12.0),
                ([10.0, 12.0], 0.0),
            ],
        )
        await self.run_test(
            2,
            lambda c1, c2: c1.max(c2).production(),
            [
                ([10.0, -12.0], 0.0),
                ([-10.0, -12.0], 10.0),
            ],
        )
        await self.run_test(
            2,
            lambda c1, c2: c1.production() + c2,
            [
                ([10.0, -12.0], -12.0),
                ([-10.0, -12.0], -2.0),
            ],
        )

    async def test_nones_are_zeros(self) -> None:
        """Test that `None`s are treated as zeros when configured."""
        await self.run_test(
            3,
            lambda c2, c4, c5: c2 - c4 + c5,
            [
                ([10.0, 12.0, 15.0], 13.0),
                ([None, 12.0, 15.0], 3.0),
                ([10.0, None, 15.0], 25.0),
                ([15.0, 17.0, 20.0], 18.0),
                ([15.0, None, None], 15.0),
            ],
            True,
        )

        await self.run_test(
            4,
            lambda c2, c4, c5, c6: c2 + c4 - (c5 * c6),
            [
                ([10.0, 12.0, 15.0, 2.0], -8.0),
                ([10.0, 12.0, 15.0, None], 22.0),
                ([10.0, None, 15.0, 2.0], -20.0),
                ([15.0, 17.0, 20.0, 5.0], -68.0),
                ([15.0, 17.0, None, 5.0], 32.0),
            ],
            True,
        )

    async def test_nones_are_not_zeros(self) -> None:
        """Test that calculated values are `None` on input `None`s."""
        await self.run_test(
            3,
            lambda c2, c4, c5: c2 - c4 + c5,
            [
                ([10.0, 12.0, 15.0], 13.0),
                ([None, 12.0, 15.0], None),
                ([10.0, None, 15.0], None),
                ([15.0, 17.0, 20.0], 18.0),
                ([15.0, None, None], None),
            ],
            False,
        )

        await self.run_test(
            4,
            lambda c2, c4, c5, c6: c2 + c4 - (c5 * c6),
            [
                ([10.0, 12.0, 15.0, 2.0], -8.0),
                ([10.0, 12.0, 15.0, None], None),
                ([10.0, None, 15.0, 2.0], None),
                ([15.0, 17.0, 20.0, 5.0], -68.0),
                ([15.0, 17.0, None, 5.0], None),
            ],
            False,
        )


class TestConstantValue:
    """Tests for the constant value step."""

    async def test_constant_value(self) -> None:
        """Test using constant values in formulas."""
        channel_1 = Broadcast[Sample[Quantity]]("channel_1")
        channel_2 = Broadcast[Sample[Quantity]]("channel_2")

        sender_1 = channel_1.new_sender()
        sender_2 = channel_2.new_sender()

        builder = FormulaBuilder("test_constant_value", create_method=Quantity)
        builder.push_metric(
            "channel_1", channel_1.new_receiver(), nones_are_zeros=False
        )
        builder.push_oper("+")
        builder.push_constant(2.0)
        builder.push_oper("*")
        builder.push_metric(
            "channel_2", channel_2.new_receiver(), nones_are_zeros=False
        )

        engine = builder.build()

        results_rx = engine.new_receiver()

        now = datetime.now()
        await sender_1.send(Sample(now, Quantity(10.0)))
        await sender_2.send(Sample(now, Quantity(15.0)))
        assert (await results_rx.receive()).value == Quantity(40.0)

        await sender_1.send(Sample(now, Quantity(-10.0)))
        await sender_2.send(Sample(now, Quantity(15.0)))
        assert (await results_rx.receive()).value == Quantity(20.0)

        builder = FormulaBuilder("test_constant_value", create_method=Quantity)
        builder.push_oper("(")
        builder.push_metric(
            "channel_1", channel_1.new_receiver(), nones_are_zeros=False
        )
        builder.push_oper("+")
        builder.push_constant(2.0)
        builder.push_oper(")")
        builder.push_oper("*")
        builder.push_metric(
            "channel_2", channel_2.new_receiver(), nones_are_zeros=False
        )

        engine = builder.build()

        results_rx = engine.new_receiver()

        now = datetime.now()
        await sender_1.send(Sample(now, Quantity(10.0)))
        await sender_2.send(Sample(now, Quantity(15.0)))
        assert (await results_rx.receive()).value == Quantity(180.0)

        await sender_1.send(Sample(now, Quantity(-10.0)))
        await sender_2.send(Sample(now, Quantity(15.0)))
        assert (await results_rx.receive()).value == Quantity(-120.0)


class TestClipper:
    """Tests for the clipper step."""

    async def test_clipper(self) -> None:
        """Test the usage of clipper in formulas."""
        channel_1 = Broadcast[Sample[Quantity]]("channel_1")
        channel_2 = Broadcast[Sample[Quantity]]("channel_2")

        sender_1 = channel_1.new_sender()
        sender_2 = channel_2.new_sender()

        builder = FormulaBuilder("test_clipper", create_method=Quantity)
        builder.push_metric(
            "channel_1", channel_1.new_receiver(), nones_are_zeros=False
        )
        builder.push_oper("+")
        builder.push_metric(
            "channel_2", channel_2.new_receiver(), nones_are_zeros=False
        )
        builder.push_clipper(0.0, 100.0)
        engine = builder.build()

        results_rx = engine.new_receiver()

        now = datetime.now()
        await sender_1.send(Sample(now, Quantity(10.0)))
        await sender_2.send(Sample(now, Quantity(150.0)))
        assert (await results_rx.receive()).value == Quantity(110.0)

        await sender_1.send(Sample(now, Quantity(200.0)))
        await sender_2.send(Sample(now, Quantity(-10.0)))
        assert (await results_rx.receive()).value == Quantity(200.0)

        await sender_1.send(Sample(now, Quantity(200.0)))
        await sender_2.send(Sample(now, Quantity(10.0)))
        assert (await results_rx.receive()).value == Quantity(210.0)

        builder = FormulaBuilder("test_clipper", create_method=Quantity)
        builder.push_oper("(")
        builder.push_metric(
            "channel_1", channel_1.new_receiver(), nones_are_zeros=False
        )
        builder.push_oper("+")
        builder.push_metric(
            "channel_2", channel_2.new_receiver(), nones_are_zeros=False
        )
        builder.push_oper(")")
        builder.push_clipper(0.0, 100.0)
        engine = builder.build()

        results_rx = engine.new_receiver()

        now = datetime.now()
        await sender_1.send(Sample(now, Quantity(10.0)))
        await sender_2.send(Sample(now, Quantity(150.0)))
        assert (await results_rx.receive()).value == Quantity(100.0)

        await sender_1.send(Sample(now, Quantity(200.0)))
        await sender_2.send(Sample(now, Quantity(-10.0)))
        assert (await results_rx.receive()).value == Quantity(100.0)

        await sender_1.send(Sample(now, Quantity(25.0)))
        await sender_2.send(Sample(now, Quantity(-10.0)))
        assert (await results_rx.receive()).value == Quantity(15.0)


class TestFormulaOutputTyping:
    """Tests for the typing of the output of formulas."""

    async def test_types(self) -> None:
        """Test the typing of the output of formulas."""
        channel_1 = Broadcast[Sample[Power]]("channel_1")
        channel_2 = Broadcast[Sample[Power]]("channel_2")

        sender_1 = channel_1.new_sender()
        sender_2 = channel_2.new_sender()

        builder = FormulaBuilder("test_typing", create_method=Power.from_watts)
        builder.push_metric(
            "channel_1", channel_1.new_receiver(), nones_are_zeros=False
        )
        builder.push_oper("+")
        builder.push_metric(
            "channel_2", channel_2.new_receiver(), nones_are_zeros=False
        )
        engine = builder.build()

        results_rx = engine.new_receiver()

        now = datetime.now()
        await sender_1.send(Sample(now, Power.from_watts(10.0)))
        await sender_2.send(Sample(now, Power.from_watts(150.0)))
        result = await results_rx.receive()
        assert result is not None and result.value is not None
        assert result.value.as_watts() == 160.0


class TestFromReceiver:
    """Test creating a formula engine from a receiver."""

    async def test_from_receiver(self) -> None:
        """Test creating a formula engine from a receiver."""
        channel = Broadcast[Sample[Power]]("channel_1")
        sender = channel.new_sender()

        builder = FormulaBuilder("test_from_receiver", create_method=Power.from_watts)
        builder.push_metric("channel_1", channel.new_receiver(), nones_are_zeros=False)
        engine = builder.build()

        engine_from_receiver = FormulaEngine.from_receiver(
            "test_from_receiver", engine.new_receiver(), create_method=Power.from_watts
        )

        results_rx = engine_from_receiver.new_receiver()

        await sender.send(Sample(datetime.now(), Power.from_watts(10.0)))
        result = await results_rx.receive()
        assert result is not None and result.value is not None
        assert result.value.as_watts() == 10.0
