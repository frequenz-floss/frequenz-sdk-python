# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for quantity types."""

import pytest

from frequenz.sdk.timeseries._quantities import Quantity


class Fz1(
    Quantity,
    exponent_unit_map={
        0: "Hz",
        3: "kHz",
    },
):
    """Frequency quantity with narrow exponent unit map."""


class Fz2(
    Quantity,
    exponent_unit_map={
        -6: "uHz",
        -3: "mHz",
        0: "Hz",
        3: "kHz",
        6: "MHz",
        9: "GHz",
    },
):
    """Frequency quantity with broad exponent unit map."""


def test_string_representation() -> None:
    """Test the string representation of the quantities."""
    assert f"{Quantity(1.024445, exponent=0)}" == "1.024"
    assert f"{Quantity(1.024445, exponent=0):.0}" == "1"
    assert f"{Quantity(1.024445, exponent=0):.6}" == "1.024445"

    assert f"{Quantity(1.024445, exponent=3)}" == "1024.445"

    assert f"{Fz1(1.024445, exponent=0)}" == "1.024 Hz"
    assert f"{Fz1(1.024445, exponent=0):.0}" == "1 Hz"
    assert f"{Fz1(1.024445, exponent=0):.1}" == "1 Hz"
    assert f"{Fz1(1.024445, exponent=0):.2}" == "1.02 Hz"
    assert f"{Fz1(1.024445, exponent=0):.9}" == "1.024445 Hz"
    assert f"{Fz1(1.024445, exponent=0):0.0}" == "1 Hz"
    assert f"{Fz1(1.024445, exponent=0):0.1}" == "1.0 Hz"
    assert f"{Fz1(1.024445, exponent=0):0.2}" == "1.02 Hz"
    assert f"{Fz1(1.024445, exponent=0):0.9}" == "1.024445000 Hz"

    assert f"{Fz1(1.024445, exponent=3)}" == "1.024 kHz"
    assert f"{Fz2(1.024445, exponent=3)}" == "1.024 kHz"

    assert f"{Fz1(1.024445, exponent=6)}" == "1024.445 kHz"
    assert f"{Fz2(1.024445, exponent=6)}" == "1.024 MHz"

    assert f"{Fz1(1.024445, exponent=9)}" == "1024445 kHz"
    assert f"{Fz2(1.024445, exponent=9)}" == "1.024 GHz"

    assert f"{Fz1(1.024445, exponent=-3)}" == "0.001 Hz"
    assert f"{Fz2(1.024445, exponent=-3)}" == "1.024 mHz"

    assert f"{Fz1(1.024445, exponent=-6)}" == "0 Hz"
    assert f"{Fz1(1.024445, exponent=-6):.6}" == "0.000001 Hz"
    assert f"{Fz2(1.024445, exponent=-6)}" == "1.024 uHz"

    assert f"{Fz1(1.024445, exponent=-12)}" == "0 Hz"
    assert f"{Fz2(1.024445, exponent=-12)}" == "0 Hz"


def test_addition_subtraction() -> None:
    """Test the addition and subtraction of the quantities."""
    assert Quantity(1) + Quantity(1, exponent=0) == Quantity(2, exponent=0)
    assert Quantity(1) + Quantity(1, exponent=3) == Quantity(1001, exponent=0)
    assert Quantity(1) - Quantity(1, exponent=0) == Quantity(0, exponent=0)

    assert Fz1(1) + Fz1(1) == Fz1(2)
    with pytest.raises(TypeError) as excinfo:
        assert Fz1(1) + Fz2(1)  # type: ignore
    assert excinfo.value.args[0] == "unsupported operand type(s) for +: 'Fz1' and 'Fz2'"
    with pytest.raises(TypeError) as excinfo:
        assert Fz1(1) - Fz2(1)  # type: ignore
    assert excinfo.value.args[0] == "unsupported operand type(s) for -: 'Fz1' and 'Fz2'"


def test_comparison() -> None:
    """Test the comparison of the quantities."""
    assert Quantity(1.024445, exponent=0) == Quantity(1.024445, exponent=0)
    assert Quantity(1.024445, exponent=0) != Quantity(1.024445, exponent=3)
    assert Quantity(1.024445, exponent=0) < Quantity(1.024445, exponent=3)
    assert Quantity(1.024445, exponent=0) <= Quantity(1.024445, exponent=3)
    assert Quantity(1.024445, exponent=0) <= Quantity(1.024445, exponent=0)
    assert Quantity(1.024445, exponent=0) > Quantity(1.024445, exponent=-3)
    assert Quantity(1.024445, exponent=0) >= Quantity(1.024445, exponent=-3)
    assert Quantity(1.024445, exponent=0) >= Quantity(1.024445, exponent=0)

    assert Fz1(1.024445, exponent=0) == Fz1(1.024445, exponent=0)
    assert Fz1(1.024445, exponent=0) != Fz1(1.024445, exponent=3)
    assert Fz1(1.024445, exponent=0) < Fz1(1.024445, exponent=3)
    assert Fz1(1.024445, exponent=0) <= Fz1(1.024445, exponent=3)
    assert Fz1(1.024445, exponent=0) <= Fz1(1.024445, exponent=0)
    assert Fz1(1.024445, exponent=0) > Fz1(1.024445, exponent=-3)
    assert Fz1(1.024445, exponent=0) >= Fz1(1.024445, exponent=-3)
    assert Fz1(1.024445, exponent=0) >= Fz1(1.024445, exponent=0)

    assert Fz1(1.024445, exponent=0) != Fz2(1.024445, exponent=0)
    with pytest.raises(TypeError) as excinfo:
        # unfortunately, mypy does not identify this as an error, when comparing a child
        # type against a base type, but they should still fail, because base-type
        # instances are being used as dimension-less quantities, whereas all child types
        # have dimensions/units.
        assert Fz1(1.024445, exponent=0) <= Quantity(1.024445, exponent=0)
    assert (
        excinfo.value.args[0]
        == "'<=' not supported between instances of 'Fz1' and 'Quantity'"
    )
    with pytest.raises(TypeError) as excinfo:
        assert Quantity(1.024445, exponent=0) <= Fz1(1.024445, exponent=0)
    assert (
        excinfo.value.args[0]
        == "'<=' not supported between instances of 'Quantity' and 'Fz1'"
    )
    with pytest.raises(TypeError) as excinfo:
        assert Fz1(1.024445, exponent=0) < Fz2(1.024445, exponent=3)  # type: ignore
    assert (
        excinfo.value.args[0]
        == "'<' not supported between instances of 'Fz1' and 'Fz2'"
    )
    with pytest.raises(TypeError) as excinfo:
        assert Fz1(1.024445, exponent=0) <= Fz2(1.024445, exponent=3)  # type: ignore
    assert (
        excinfo.value.args[0]
        == "'<=' not supported between instances of 'Fz1' and 'Fz2'"
    )
    with pytest.raises(TypeError) as excinfo:
        assert Fz1(1.024445, exponent=0) > Fz2(1.024445, exponent=-3)  # type: ignore
    assert (
        excinfo.value.args[0]
        == "'>' not supported between instances of 'Fz1' and 'Fz2'"
    )
    with pytest.raises(TypeError) as excinfo:
        assert Fz1(1.024445, exponent=0) >= Fz2(1.024445, exponent=-3)  # type: ignore
    assert (
        excinfo.value.args[0]
        == "'>=' not supported between instances of 'Fz1' and 'Fz2'"
    )
