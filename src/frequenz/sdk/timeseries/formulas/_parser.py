# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Parser for formulas."""

from __future__ import annotations

import logging
from collections.abc import Callable, Coroutine
from typing import Generic

from frequenz.channels import Receiver
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.quantities import Quantity
from more_itertools import peekable

from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries._base_types import QuantityT

from . import _ast, _token
from ._base_ast_node import AstNode
from ._exceptions import FormulaSyntaxError
from ._formula import Formula
from ._functions import FunCall, Function
from ._lexer import Lexer
from ._resampled_stream_fetcher import ResampledStreamFetcher

_logger = logging.getLogger(__name__)


def parse(
    *,
    name: str,
    formula: str,
    telemetry_fetcher: ResampledStreamFetcher,
    create_method: Callable[[float], QuantityT],
) -> Formula[QuantityT]:
    """Parse a formula string into an AST.

    Args:
        name: The name of the formula.
        formula: The formula string to parse.
        telemetry_fetcher: The telemetry fetcher to get component streams.
        create_method: A method to create the corresponding QuantityT from a
            float, based on the metric.

    Returns:
        The parsed formula AST.
    """
    return _Parser(
        name=name,
        formula=formula,
        telemetry_fetcher=telemetry_fetcher,
        create_method=create_method,
    ).parse()


class _Parser(Generic[QuantityT]):
    def __init__(
        self,
        *,
        name: str,
        formula: str,
        telemetry_fetcher: ResampledStreamFetcher,
        create_method: Callable[[float], QuantityT],
    ):
        """Initialize the parser."""
        self._name: str = name
        self._formula: str = formula
        self._lexer: peekable[_token.Token] = peekable(Lexer(formula))
        self._telemetry_fetcher: ResampledStreamFetcher = telemetry_fetcher
        self._create_method: Callable[[float], QuantityT] = create_method

    def _peek_next_token(self) -> _token.Token | None:
        """Get the next token from the lexer, or None if there is None."""
        try:
            return self._lexer.peek()
        except StopIteration:
            return None

    def _parse_term(self) -> AstNode[QuantityT] | None:
        """Parse a term.

        A term is any expression. It may contain binary operators,
        bracketed expressions, function calls, literals and component IDs.
        """
        factor = self._parse_factor()
        if factor is None:
            return None

        token: _token.Token | None = self._peek_next_token()
        while token is not None and isinstance(token, (_token.Plus, _token.Minus)):
            token = next(self._lexer)
            next_factor = self._parse_factor()

            if next_factor is None:
                raise FormulaSyntaxError(
                    formula=self._formula,
                    span=(token.span[1], token.span[1] + 1),
                    message="Expected expression",
                )

            if isinstance(token, _token.Plus):
                factor = _ast.Add(left=factor, right=next_factor)
            elif isinstance(token, _token.Minus):
                factor = _ast.Sub(left=factor, right=next_factor)

            token = self._peek_next_token()

        return factor

    def _parse_factor(self) -> AstNode[QuantityT] | None:
        """Parse a factor.

        A factor is any expression that does not contain
        addition or subtraction outside of parentheses.
        """
        unary = self._parse_unary()

        if unary is None:
            return None

        token: _token.Token | None = self._peek_next_token()
        while token is not None and isinstance(token, (_token.Mul, _token.Div)):
            token = next(self._lexer)
            next_unary = self._parse_unary()
            if next_unary is None:
                raise FormulaSyntaxError(
                    formula=self._formula,
                    span=(token.span[1], token.span[1] + 1),
                    message="Expected expression",
                )

            if isinstance(token, _token.Mul):
                unary = _ast.Mul(left=unary, right=next_unary)
            elif isinstance(token, _token.Div):
                unary = _ast.Div(left=unary, right=next_unary)

            token = self._peek_next_token()

        return unary

    def _parse_unary(self) -> AstNode[QuantityT] | None:
        """Parse unary.

        A unary is any expression that does not contain any binary
        operators outside of parentheses.
        """
        token: _token.Token | None = self._peek_next_token()
        if token is not None and isinstance(token, _token.Minus):
            token = next(self._lexer)
            primary: AstNode[QuantityT] | None = self._parse_primary()
            if primary is None:
                raise FormulaSyntaxError(
                    formula=self._formula,
                    span=(token.span[1], token.span[1] + 1),
                    message="Expected expression",
                )

            zero_const = _ast.Constant(value=self._create_method(0.0))
            return _ast.Sub(left=zero_const, right=primary)

        return self._parse_primary()

    def _parse_bracketed(self) -> AstNode[QuantityT] | None:
        oparen = next(self._lexer)  # consume '('
        assert isinstance(oparen, _token.OpenParen)

        expr: AstNode[QuantityT] | None = self._parse_term()
        if expr is None:
            raise FormulaSyntaxError(
                formula=self._formula,
                span=(oparen.span[1], oparen.span[1] + 1),
                message="Expected expression",
            )

        token: _token.Token | None = self._peek_next_token()
        if token is None or not isinstance(token, _token.CloseParen):
            raise FormulaSyntaxError(
                formula=self._formula,
                span=oparen.span,
                message="Unmatched parenthesis",
            )

        _ = next(self._lexer)  # consume ')'

        return expr

    def _parse_function_call(self) -> AstNode[QuantityT] | None:
        fn_name: _token.Token = next(self._lexer)
        function_class: type[Function[QuantityT]] | None = (
            Function.function_class_by_name(fn_name.value)
        )

        if function_class is None:
            raise FormulaSyntaxError(
                formula=self._formula,
                span=fn_name.span,
                message="Unknown function name",
            )

        params: list[AstNode[QuantityT]] = []

        token: _token.Token | None = self._peek_next_token()
        if token is None or not isinstance(token, _token.OpenParen):
            raise FormulaSyntaxError(
                formula=self._formula,
                span=(fn_name.span[1], fn_name.span[1] + 1),
                message="Expected '(' after function name",
            )
        oparen = next(self._lexer)  # consume '('

        while True:
            param = self._parse_term()
            if param is None:
                raise FormulaSyntaxError(
                    formula=self._formula,
                    span=(token.span[1], token.span[1] + 1),
                    message="Expected argument",
                )
            params.append(param)

            token = self._peek_next_token()
            if token is None:
                raise FormulaSyntaxError(
                    formula=self._formula,
                    span=oparen.span,
                    message="Unmatched parenthesis",
                )

            if isinstance(token, _token.Comma):
                _ = next(self._lexer)  # consume ','
                continue
            if isinstance(token, _token.CloseParen):
                _ = next(self._lexer)  # consume ')'
                break

            raise FormulaSyntaxError(
                formula=self._formula,
                span=token.span,
                message="Expected ',' or ')'",
            )

        return FunCall(function=function_class(params))

    def _parse_primary(self) -> AstNode[QuantityT] | None:
        """Parse a primary.

        A primary(expression) can be any of the following:
        - A number literal
        - A component ID
        - A function call
        - A bracketed expression
        """
        token: _token.Token | None = self._peek_next_token()
        if token is None:
            return None

        def make_component_stream_fetcher(
            f: ResampledStreamFetcher, cid: ComponentId
        ) -> Callable[[], Coroutine[None, None, Receiver[Sample[Quantity]]]]:
            return lambda: f.fetch_stream(cid)

        if isinstance(token, _token.Component):
            _ = next(self._lexer)  # consume token
            comp = _ast.TelemetryStream(
                source=f"#{token.id}",
                metric_fetcher=make_component_stream_fetcher(
                    self._telemetry_fetcher, ComponentId(int(token.id))
                ),
                create_method=self._create_method,
            )
            return comp

        if isinstance(token, _token.Number):
            _ = next(self._lexer)
            return _ast.Constant(value=self._create_method(float(token.value)))

        if isinstance(token, _token.OpenParen):
            return self._parse_bracketed()

        if isinstance(token, _token.Symbol):
            return self._parse_function_call()

        return None

    def parse(self) -> Formula[QuantityT]:
        expr = self._parse_term()
        if expr is None:
            raise FormulaSyntaxError(
                formula=self._formula,
                span=None,
                message="Empty formula",
            )
        # There should not be any tokens left
        token = self._peek_next_token()
        if token is not None:
            raise FormulaSyntaxError(
                formula=self._formula,
                span=token.span,
                message="Unexpected token",
            )

        return Formula(
            name=self._name,
            root=expr,
            create_method=self._create_method,
            metric_fetcher=self._telemetry_fetcher,
        )
