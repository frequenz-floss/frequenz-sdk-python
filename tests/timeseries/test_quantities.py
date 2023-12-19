# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for quantity types."""

from datetime import timedelta

import pytest

from frequenz.sdk.timeseries._quantities import (
    Current,
    Energy,
    Frequency,
    Percentage,
    Power,
    Quantity,
    Temperature,
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


def test_zero() -> None:
    """Test the zero value for quantity."""
    assert Quantity(0.0) == Quantity.zero()
    assert Quantity(0.0, exponent=100) == Quantity.zero()
    assert Quantity.zero() is Quantity.zero()  # It is a "singleton"
    assert Quantity.zero().base_value == 0.0

    # Test the singleton is immutable
    one = Quantity.zero()
    one += Quantity(1.0)
    assert one != Quantity.zero()
    assert Quantity.zero() == Quantity(0.0)

    assert Power.from_watts(0.0) == Power.zero()
    assert Power.from_kilowatts(0.0) == Power.zero()
    assert isinstance(Power.zero(), Power)
    assert Power.zero().as_watts() == 0.0
    assert Power.zero().as_kilowatts() == 0.0
    assert Power.zero() is Power.zero()  # It is a "singleton"

    assert Current.from_amperes(0.0) == Current.zero()
    assert Current.from_milliamperes(0.0) == Current.zero()
    assert isinstance(Current.zero(), Current)
    assert Current.zero().as_amperes() == 0.0
    assert Current.zero().as_milliamperes() == 0.0
    assert Current.zero() is Current.zero()  # It is a "singleton"

    assert Voltage.from_volts(0.0) == Voltage.zero()
    assert Voltage.from_kilovolts(0.0) == Voltage.zero()
    assert isinstance(Voltage.zero(), Voltage)
    assert Voltage.zero().as_volts() == 0.0
    assert Voltage.zero().as_kilovolts() == 0.0
    assert Voltage.zero() is Voltage.zero()  # It is a "singleton"

    assert Energy.from_kilowatt_hours(0.0) == Energy.zero()
    assert Energy.from_megawatt_hours(0.0) == Energy.zero()
    assert isinstance(Energy.zero(), Energy)
    assert Energy.zero().as_kilowatt_hours() == 0.0
    assert Energy.zero().as_megawatt_hours() == 0.0
    assert Energy.zero() is Energy.zero()  # It is a "singleton"

    assert Frequency.from_hertz(0.0) == Frequency.zero()
    assert Frequency.from_megahertz(0.0) == Frequency.zero()
    assert isinstance(Frequency.zero(), Frequency)
    assert Frequency.zero().as_hertz() == 0.0
    assert Frequency.zero().as_megahertz() == 0.0
    assert Frequency.zero() is Frequency.zero()  # It is a "singleton"

    assert Percentage.from_percent(0.0) == Percentage.zero()
    assert Percentage.from_fraction(0.0) == Percentage.zero()
    assert isinstance(Percentage.zero(), Percentage)
    assert Percentage.zero().as_percent() == 0.0
    assert Percentage.zero().as_fraction() == 0.0
    assert Percentage.zero() is Percentage.zero()  # It is a "singleton"


def test_string_representation() -> None:
    """Test the string representation of the quantities."""
    assert str(Quantity(1.024445, exponent=0)) == "1.024"
    assert (
        repr(Quantity(1.024445, exponent=0)) == "Quantity(value=1.024445, exponent=0)"
    )
    assert f"{Quantity(1.024445, exponent=0)}" == "1.024"
    assert f"{Quantity(1.024445, exponent=0):.0}" == "1"
    assert f"{Quantity(0.124445, exponent=0):.0}" == "0"
    assert f"{Quantity(0.50001, exponent=0):.0}" == "1"
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

    assert f"{Fz1(0)}" == "0 Hz"

    assert f"{Fz1(-20)}" == "-20 Hz"
    assert f"{Fz1(-20000)}" == "-20 kHz"

    assert f"{Power.from_watts(0.000124445):.0}" == "0 W"
    assert f"{Energy.from_watt_hours(0.124445):.0}" == "0 Wh"


def test_isclose() -> None:
    """Test the isclose method of the quantities."""
    assert Fz1(1.024445).isclose(Fz1(1.024445))
    assert not Fz1(1.024445).isclose(Fz1(1.0))


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

    fz1 = Fz1(1.0)
    fz1 += Fz1(4.0)
    assert fz1 == Fz1(5.0)
    fz1 -= Fz1(9.0)
    assert fz1 == Fz1(-4.0)

    with pytest.raises(TypeError) as excinfo:
        fz1 += Fz2(1.0)  # type: ignore


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

    with pytest.raises(TypeError):
        # using the default constructor should raise.
        Power(1.0, exponent=0)


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

    with pytest.raises(TypeError):
        # using the default constructor should raise.
        Current(1.0, exponent=0)


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

    with pytest.raises(TypeError):
        # using the default constructor should raise.
        Voltage(1.0, exponent=0)


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

    with pytest.raises(TypeError):
        # using the default constructor should raise.
        Energy(1.0, exponent=0)


def test_temperature() -> None:
    """Test the temperature class."""
    temp = Temperature.from_celsius(30.4)
    assert f"{temp}" == "30.4 °C"

    assert temp.as_celsius() == 30.4
    assert temp != Temperature.from_celsius(5.0)

    with pytest.raises(TypeError):
        # using the default constructor should raise.
        Temperature(1.0, exponent=0)


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


def test_frequency() -> None:
    """Test the frequency class."""
    freq = Frequency.from_hertz(0.0000002)
    assert f"{freq:.9}" == "0.0000002 Hz"
    freq = Frequency.from_kilohertz(600000.0)
    assert f"{freq}" == "600 MHz"

    freq = Frequency.from_hertz(6.0)
    assert freq.as_hertz() == 6.0
    assert freq.as_kilohertz() == 0.006
    assert freq == Frequency.from_kilohertz(0.006)
    assert freq == Frequency.from_hertz(6.0)
    assert freq != Frequency.from_hertz(5.0)

    with pytest.raises(TypeError):
        # using the default constructor should raise.
        Frequency(1.0, exponent=0)


def test_percentage() -> None:
    """Test the percentage class."""
    pct = Percentage.from_fraction(0.204)
    assert f"{pct}" == "20.4 %"
    pct = Percentage.from_percent(20.4)
    assert f"{pct}" == "20.4 %"
    assert pct.as_percent() == 20.4
    assert pct.as_fraction() == 0.204


def test_neg() -> None:
    """Test the negation of quantities."""
    power = Power.from_watts(1000.0)
    assert -power == Power.from_watts(-1000.0)
    assert -(-power) == power

    voltage = Voltage.from_volts(230.0)
    assert -voltage == Voltage.from_volts(-230.0)
    assert -(-voltage) == voltage

    current = Current.from_amperes(2)
    assert -current == Current.from_amperes(-2)
    assert -(-current) == current

    energy = Energy.from_kilowatt_hours(6.2)
    assert -energy == Energy.from_kilowatt_hours(-6.2)

    freq = Frequency.from_hertz(50)
    assert -freq == Frequency.from_hertz(-50)
    assert -(-freq) == freq

    pct = Percentage.from_fraction(30)
    assert -pct == Percentage.from_fraction(-30)
    assert -(-pct) == pct


def test_inf() -> None:
    """Test proper formating when using inf in quantities."""
    assert f"{Power.from_watts(float('inf'))}" == "inf W"
    assert f"{Power.from_watts(float('-inf'))}" == "-inf W"

    assert f"{Voltage.from_volts(float('inf'))}" == "inf V"
    assert f"{Voltage.from_volts(float('-inf'))}" == "-inf V"

    assert f"{Current.from_amperes(float('inf'))}" == "inf A"
    assert f"{Current.from_amperes(float('-inf'))}" == "-inf A"

    assert f"{Energy.from_watt_hours(float('inf'))}" == "inf Wh"
    assert f"{Energy.from_watt_hours(float('-inf'))}" == "-inf Wh"

    assert f"{Frequency.from_hertz(float('inf'))}" == "inf Hz"
    assert f"{Frequency.from_hertz(float('-inf'))}" == "-inf Hz"

    assert f"{Percentage.from_fraction(float('inf'))}" == "inf %"
    assert f"{Percentage.from_fraction(float('-inf'))}" == "-inf %"


def test_nan() -> None:
    """Test proper formating when using nan in quantities."""
    assert f"{Power.from_watts(float('nan'))}" == "nan W"
    assert f"{Voltage.from_volts(float('nan'))}" == "nan V"
    assert f"{Current.from_amperes(float('nan'))}" == "nan A"
    assert f"{Energy.from_watt_hours(float('nan'))}" == "nan Wh"
    assert f"{Frequency.from_hertz(float('nan'))}" == "nan Hz"
    assert f"{Percentage.from_fraction(float('nan'))}" == "nan %"


def test_abs() -> None:
    """Test the absolute value of quantities."""
    power = Power.from_watts(1000.0)
    assert abs(power) == Power.from_watts(1000.0)
    assert abs(-power) == Power.from_watts(1000.0)

    voltage = Voltage.from_volts(230.0)
    assert abs(voltage) == Voltage.from_volts(230.0)
    assert abs(-voltage) == Voltage.from_volts(230.0)

    current = Current.from_amperes(2)
    assert abs(current) == Current.from_amperes(2)
    assert abs(-current) == Current.from_amperes(2)

    energy = Energy.from_kilowatt_hours(6.2)
    assert abs(energy) == Energy.from_kilowatt_hours(6.2)
    assert abs(-energy) == Energy.from_kilowatt_hours(6.2)

    freq = Frequency.from_hertz(50)
    assert abs(freq) == Frequency.from_hertz(50)
    assert abs(-freq) == Frequency.from_hertz(50)

    pct = Percentage.from_fraction(30)
    assert abs(pct) == Percentage.from_fraction(30)
    assert abs(-pct) == Percentage.from_fraction(30)


def test_quantity_multiplied_with_precentage() -> None:
    """Test the multiplication of all quantities with percentage."""
    percentage = Percentage.from_percent(50)
    power = Power.from_watts(1000.0)
    voltage = Voltage.from_volts(230.0)
    current = Current.from_amperes(2)
    energy = Energy.from_kilowatt_hours(12)
    percentage_ = Percentage.from_percent(50)

    assert power * percentage == Power.from_watts(500.0)
    assert voltage * percentage == Voltage.from_volts(115.0)
    assert current * percentage == Current.from_amperes(1)
    assert energy * percentage == Energy.from_kilowatt_hours(6)
    assert percentage_ * percentage == Percentage.from_percent(25)

    power *= percentage
    assert power == Power.from_watts(500.0)
    voltage *= percentage
    assert voltage == Voltage.from_volts(115.0)
    current *= percentage
    assert current == Current.from_amperes(1)
    energy *= percentage
    assert energy == Energy.from_kilowatt_hours(6)
    percentage_ *= percentage
    assert percentage_ == Percentage.from_percent(25)


def test_invalid_multiplications() -> None:
    """Test the multiplication of quantities with invalid quantities."""
    power = Power.from_watts(1000.0)
    voltage = Voltage.from_volts(230.0)
    current = Current.from_amperes(2)
    energy = Energy.from_kilowatt_hours(12)

    for quantity in [power, voltage, current, energy]:
        with pytest.raises(TypeError):
            _ = power * quantity  # type: ignore
        with pytest.raises(TypeError):
            power *= quantity  # type: ignore

    for quantity in [voltage, power, energy]:
        with pytest.raises(TypeError):
            _ = voltage * quantity  # type: ignore
        with pytest.raises(TypeError):
            voltage *= quantity  # type: ignore

    for quantity in [current, power, energy]:
        with pytest.raises(TypeError):
            _ = current * quantity  # type: ignore
        with pytest.raises(TypeError):
            current *= quantity  # type: ignore

    for quantity in [energy, power, voltage, current]:
        with pytest.raises(TypeError):
            _ = energy * quantity  # type: ignore
        with pytest.raises(TypeError):
            energy *= quantity  # type: ignore

    for quantity in [power, voltage, current, energy, Percentage.from_percent(50)]:
        with pytest.raises(TypeError):
            _ = quantity * 200.0  # type: ignore
        with pytest.raises(TypeError):
            quantity *= 200.0  # type: ignore
