# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for the FormulaEngine and the Tokenizer."""

import asyncio
from datetime import datetime
from typing import Callable, Dict, List, Optional, Tuple, Union

from frequenz.channels import Broadcast, Receiver

from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries._formula_engine._formula_engine import (
    FormulaBuilder,
    FormulaEngine,
    HigherOrderFormulaBuilder,
)
from frequenz.sdk.timeseries._formula_engine._tokenizer import (
    Token,
    Tokenizer,
    TokenType,
)
from frequenz.sdk.timeseries._quantities import Quantity


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
        io_pairs: List[Tuple[List[Optional[float]], Optional[float]]],
        nones_are_zeros: bool = False,
    ) -> None:
        """Run a formula test."""
        channels: Dict[str, Broadcast[Sample[Quantity]]] = {}
        builder = FormulaBuilder("test_formula", Quantity)
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
        results_rx = engine.new_receiver()

        assert repr(builder._steps) == postfix  # pylint: disable=protected-access

        now = datetime.now()
        tests_passed = 0
        for io_pair in io_pairs:
            io_input, io_output = io_pair
            await asyncio.gather(
                *[
                    chan.new_sender().send(
                        Sample(now, None if not value else Quantity(value))
                    )
                    for chan, value in zip(channels.values(), io_input)
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
        self, stream_id: int, data: Receiver[Sample[Quantity]]
    ) -> FormulaEngine:
        """Make a basic FormulaEngine."""
        name = f"#{stream_id}"
        builder = FormulaBuilder(name, output_type=Quantity)
        builder.push_metric(
            name,
            data,
            nones_are_zeros=False,
        )
        return FormulaEngine(builder, output_type=Quantity)

    async def run_test(  # pylint: disable=too-many-locals
        self,
        num_items: int,
        make_builder: Union[
            Callable[
                [FormulaEngine, FormulaEngine, FormulaEngine],
                HigherOrderFormulaBuilder,
            ],
            Callable[
                [FormulaEngine, FormulaEngine, FormulaEngine, FormulaEngine],
                HigherOrderFormulaBuilder,
            ],
        ],
        io_pairs: List[Tuple[List[Optional[float]], Optional[float]]],
        nones_are_zeros: bool = False,
    ) -> None:
        """Run a test with the specs provided."""
        channels = [Broadcast[Sample[Quantity]](str(ctr)) for ctr in range(num_items)]
        l1_engines = [
            self.make_engine(ctr, channels[ctr].new_receiver())
            for ctr in range(num_items)
        ]
        builder = make_builder(*l1_engines)
        engine = builder.build("l2 formula", nones_are_zeros)
        result_chan = engine.new_receiver()

        now = datetime.now()
        tests_passed = 0
        for io_pair in io_pairs:
            io_input, io_output = io_pair
            await asyncio.gather(
                *[
                    chan.new_sender().send(
                        Sample(now, None if not value else Quantity(value))
                    )
                    for chan, value in zip(channels, io_input)
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


class TestFormulaAverager:
    """Tests for the formula step for calculating average."""

    async def run_test(
        self,
        components: List[str],
        io_pairs: List[Tuple[List[Optional[float]], Optional[float]]],
    ) -> None:
        """Run a formula test."""
        channels: Dict[str, Broadcast[Sample[Quantity]]] = {}
        streams: List[Tuple[str, Receiver[Sample[Quantity]], bool]] = []
        builder = FormulaBuilder("test_averager", output_type=Quantity)
        for comp_id in components:
            if comp_id not in channels:
                channels[comp_id] = Broadcast(comp_id)
                streams.append((f"{comp_id}", channels[comp_id].new_receiver(), False))

        builder.push_average(streams)
        engine = builder.build()
        results_rx = engine.new_receiver()
        now = datetime.now()
        tests_passed = 0
        for io_pair in io_pairs:
            io_input, io_output = io_pair
            await asyncio.gather(
                *[
                    chan.new_sender().send(
                        Sample(now, None if not value else Quantity(value))
                    )
                    for chan, value in zip(channels.values(), io_input)
                ]
            )
            next_val = await results_rx.receive()
            if io_output is None:
                assert next_val.value is None
            else:
                assert next_val.value and next_val.value.base_value == io_output
            tests_passed += 1
        await engine._stop()  # pylint: disable=protected-access
        assert tests_passed == len(io_pairs)

    async def test_simple(self) -> None:
        """Test simple formulas."""
        await self.run_test(
            ["#2", "#4", "#5"],
            [
                ([10.0, 12.0, 14.0], 12.0),
                ([15.0, 17.0, 19.0], 17.0),
                ([11.1, 11.1, 11.1], 11.1),
            ],
        )

    async def test_nones_are_skipped(self) -> None:
        """Test that `None`s are skipped for computing the average."""
        await self.run_test(
            ["#2", "#4", "#5"],
            [
                ([11.0, 13.0, 15.0], 13.0),
                ([None, 13.0, 19.0], 16.0),
                ([12.2, None, 22.2], 17.2),
                ([16.5, 19.5, None], 18.0),
                ([None, 13.0, None], 13.0),
                ([None, None, None], 0.0),
            ],
        )


class TestConstantValue:
    """Tests for the constant value step."""

    async def test_constant_value(self) -> None:
        """Test using constant values in formulas."""

        channel_1 = Broadcast[Sample[Quantity]]("channel_1")
        channel_2 = Broadcast[Sample[Quantity]]("channel_2")

        sender_1 = channel_1.new_sender()
        sender_2 = channel_2.new_sender()

        builder = FormulaBuilder("test_constant_value", output_type=Quantity)
        builder.push_metric("channel_1", channel_1.new_receiver(), False)
        builder.push_oper("+")
        builder.push_constant(2.0)
        builder.push_oper("*")
        builder.push_metric("channel_2", channel_2.new_receiver(), False)

        engine = builder.build()

        results_rx = engine.new_receiver()

        now = datetime.now()
        await sender_1.send(Sample(now, Quantity(10.0)))
        await sender_2.send(Sample(now, Quantity(15.0)))
        assert (await results_rx.receive()).value == Quantity(40.0)

        await sender_1.send(Sample(now, Quantity(-10.0)))
        await sender_2.send(Sample(now, Quantity(15.0)))
        assert (await results_rx.receive()).value == Quantity(20.0)

        builder = FormulaBuilder("test_constant_value", output_type=Quantity)
        builder.push_oper("(")
        builder.push_metric("channel_1", channel_1.new_receiver(), False)
        builder.push_oper("+")
        builder.push_constant(2.0)
        builder.push_oper(")")
        builder.push_oper("*")
        builder.push_metric("channel_2", channel_2.new_receiver(), False)

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

        builder = FormulaBuilder("test_clipper", output_type=Quantity)
        builder.push_metric("channel_1", channel_1.new_receiver(), False)
        builder.push_oper("+")
        builder.push_metric("channel_2", channel_2.new_receiver(), False)
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

        builder = FormulaBuilder("test_clipper", output_type=Quantity)
        builder.push_oper("(")
        builder.push_metric("channel_1", channel_1.new_receiver(), False)
        builder.push_oper("+")
        builder.push_metric("channel_2", channel_2.new_receiver(), False)
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
