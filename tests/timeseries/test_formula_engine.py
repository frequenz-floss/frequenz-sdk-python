# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for the FormulaEngine and the Tokenizer."""

import asyncio
from datetime import datetime
from typing import Callable, Dict, List, Optional, Tuple, Union

from frequenz.channels import Broadcast, Receiver

from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries.logical_meter._formula_engine import (
    FormulaBuilder,
    FormulaEngine,
    FormulaReceiver,
    HigherOrderFormulaBuilder,
)
from frequenz.sdk.timeseries.logical_meter._tokenizer import Token, Tokenizer, TokenType


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

    async def run_test(
        self,
        formula: str,
        postfix: str,
        io_pairs: List[Tuple[List[Optional[float]], Optional[float]]],
        nones_are_zeros: bool = False,
    ) -> None:
        """Run a formula test."""
        channels: Dict[str, Broadcast[Sample]] = {}
        builder = FormulaBuilder("test_formula")
        for token in Tokenizer(formula):
            if token.type == TokenType.COMPONENT_METRIC:
                if token.value not in channels:
                    channels[token.value] = Broadcast(token.value)
                builder.push_metric(
                    f"#{token.value}",
                    channels[token.value].new_receiver(),
                    nones_are_zeros,
                )
            elif token.type == TokenType.OPER:
                builder.push_oper(token.value)
        engine = builder.build()

        assert repr(engine._steps) == postfix  # pylint: disable=protected-access

        now = datetime.now()
        tests_passed = 0
        for io_pair in io_pairs:
            io_input, io_output = io_pair
            assert all(
                await asyncio.gather(
                    *[
                        chan.new_sender().send(Sample(now, value))
                        for chan, value in zip(channels.values(), io_input)
                    ]
                )
            )
            next_val = await engine._apply()  # pylint: disable=protected-access
            assert (next_val).value == io_output
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


class TestFormulaChannel:
    """Tests for formula channels."""

    def make_engine(self, stream_id: int, data: Receiver[Sample]) -> FormulaEngine:
        """Make a basic FormulaEngine."""
        name = f"#{stream_id}"
        builder = FormulaBuilder(name)
        builder.push_metric(
            name,
            data,
            nones_are_zeros=False,
        )
        return builder.build()

    async def run_test(  # pylint: disable=too-many-locals
        self,
        num_items: int,
        make_builder: Union[
            Callable[
                [FormulaReceiver, FormulaReceiver, FormulaReceiver],
                HigherOrderFormulaBuilder,
            ],
            Callable[
                [FormulaReceiver, FormulaReceiver, FormulaReceiver, FormulaReceiver],
                HigherOrderFormulaBuilder,
            ],
        ],
        io_pairs: List[Tuple[List[Optional[float]], Optional[float]]],
        nones_are_zeros: bool = False,
    ) -> None:
        """Run a test with the specs provided."""
        channels = [Broadcast[Sample](str(ctr)) for ctr in range(num_items)]
        l1_engines = [
            self.make_engine(ctr, channels[ctr].new_receiver())
            for ctr in range(num_items)
        ]
        builder = make_builder(*[e.new_receiver() for e in l1_engines])
        engine = builder.build("l2 formula", nones_are_zeros)
        result_chan = engine.new_receiver()

        now = datetime.now()
        tests_passed = 0
        for io_pair in io_pairs:
            io_input, io_output = io_pair
            assert all(
                await asyncio.gather(
                    *[
                        chan.new_sender().send(Sample(now, value))
                        for chan, value in zip(channels, io_input)
                    ]
                )
            )
            next_val = await result_chan.receive()
            assert next_val.value == io_output
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
