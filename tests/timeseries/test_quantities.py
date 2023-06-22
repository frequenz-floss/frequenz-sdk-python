# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for quantity types."""

from datetime import timedelta

import pytest

from frequenz.sdk.timeseries._quantities import (
    Current,
    Energy,
    Power,
    Quantity,
    Voltage,
)


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
    assert str(Quantity(1.024445, exponent=0)) == "1.024"
    assert (
        repr(Quantity(1.024445, exponent=0)) == "Quantity(value=1.024445, exponent=0)"
    )
    assert f"{Quantity(1.024445, exponent=0)}" == "1.024"
    assert f"{Quantity(1.024445, exponent=0):.0}" == "1"
    assert f"{Quantity(1.024445, exponent=0):.6}" == "1.024445"

    assert f"{Quantity(1.024445, exponent=3)}" == "1024.445"

    assert str(Fz1(1.024445, exponent=0)) == "1.024 Hz"
    assert repr(Fz1(1.024445, exponent=0)) == "Fz1(value=1.024445, exponent=0)"
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


def test_power() -> None:
    """Test the power class."""
    power = Power.from_milliwatts(0.0000002)
    assert f"{power:.9}" == "0.0000002 mW"
    power = Power.from_kilowatts(10000000.2)
    assert f"{power}" == "10000 MW"

    power = Power.from_kilowatts(1.2)
    assert power.as_watts() == 1200.0
    assert power.as_megawatts() == 0.0012
    assert power.as_kilowatts() == 1.2
    assert power == Power.from_milliwatts(1200000.0)
    assert power == Power.from_megawatts(0.0012)
    assert power != Power.from_watts(1000.0)


def test_current() -> None:
    """Test the current class."""
    current = Current.from_milliamperes(0.0000002)
    assert f"{current:.9}" == "0.0000002 mA"
    current = Current.from_amperes(600000.0)
    assert f"{current}" == "600000 A"

    current = Current.from_amperes(6.0)
    assert current.as_amperes() == 6.0
    assert current.as_milliamperes() == 6000.0
    assert current == Current.from_milliamperes(6000.0)
    assert current == Current.from_amperes(6.0)
    assert current != Current.from_amperes(5.0)


def test_voltage() -> None:
    """Test the voltage class."""
    voltage = Voltage.from_millivolts(0.0000002)
    assert f"{voltage:.9}" == "0.0000002 mV"
    voltage = Voltage.from_kilovolts(600000.0)
    assert f"{voltage}" == "600000 kV"

    voltage = Voltage.from_volts(6.0)
    assert voltage.as_volts() == 6.0
    assert voltage.as_millivolts() == 6000.0
    assert voltage.as_kilovolts() == 0.006
    assert voltage == Voltage.from_millivolts(6000.0)
    assert voltage == Voltage.from_kilovolts(0.006)
    assert voltage == Voltage.from_volts(6.0)
    assert voltage != Voltage.from_volts(5.0)


def test_energy() -> None:
    """Test the energy class."""
    energy = Energy.from_watt_hours(0.0000002)
    assert f"{energy:.9}" == "0.0000002 Wh"
    energy = Energy.from_megawatt_hours(600000.0)
    assert f"{energy}" == "600000 MWh"

    energy = Energy.from_kilowatt_hours(6.0)
    assert energy.as_watt_hours() == 6000.0
    assert energy.as_kilowatt_hours() == 6.0
    assert energy.as_megawatt_hours() == 0.006
    assert energy == Energy.from_megawatt_hours(0.006)
    assert energy == Energy.from_kilowatt_hours(6.0)
    assert energy != Energy.from_kilowatt_hours(5.0)


def test_quantity_compositions() -> None:
    """Test the composition of quantities."""
    power = Power.from_watts(1000.0)
    voltage = Voltage.from_volts(230.0)
    current = Current.from_amperes(4.3478260869565215)
    energy = Energy.from_kilowatt_hours(6.2)

    assert power / voltage == current
    assert power / current == voltage
    assert power == voltage * current
    assert power == current * voltage

    assert energy / power == timedelta(hours=6.2)
    assert energy / timedelta(hours=6.2) == power
    assert energy == power * timedelta(hours=6.2)
