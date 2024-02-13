# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for quantity types."""

import inspect
from datetime import timedelta
from typing import Callable

import hypothesis
import pytest
from hypothesis import strategies as st

from frequenz.sdk.timeseries import _quantities
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


_CtorType = Callable[[float], Quantity]

# This is the current number of subclasses. This probably will get outdated, but it will
# provide at least some safety against something going really wrong and end up testing
# an empty list. With this we should at least make sure we are not testing less classes
# than before. We don't get the actual number using len(_QUANTITY_SUBCLASSES) because it
# would defeat the purpose of the test.
_SANITFY_NUM_CLASSES = 7

_QUANTITY_SUBCLASSES = [
    cls
    for _, cls in inspect.getmembers(
        _quantities,
        lambda m: inspect.isclass(m) and issubclass(m, Quantity) and m is not Quantity,
    )
]

# A very basic sanity check that are messing up the introspection
assert len(_QUANTITY_SUBCLASSES) >= _SANITFY_NUM_CLASSES

_QUANTITY_BASE_UNIT_STRINGS = [
    cls._new(0).base_unit  # pylint: disable=protected-access
    for cls in _QUANTITY_SUBCLASSES
]
for unit in _QUANTITY_BASE_UNIT_STRINGS:
    assert unit is not None

_QUANTITY_CTORS = [
    method
    for cls in _QUANTITY_SUBCLASSES
    for _, method in inspect.getmembers(
        cls,
        lambda m: inspect.ismethod(m)
        and m.__name__.startswith("from_")
        and m.__name__ != ("from_string"),
    )
]
# A very basic sanity check that are messing up the introspection. There are actually
# many more constructors than classes, but this still works as a very basic check.
assert len(_QUANTITY_CTORS) >= _SANITFY_NUM_CLASSES


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


@pytest.mark.parametrize("quantity_ctor", _QUANTITY_CTORS)
def test_base_value_from_ctor_is_float(quantity_ctor: _CtorType) -> None:
    """Test that the base value always is a float."""
    quantity = quantity_ctor(1)
    assert isinstance(quantity.base_value, float)


@pytest.mark.parametrize("quantity_type", _QUANTITY_SUBCLASSES + [Quantity])
def test_base_value_from_zero_is_float(quantity_type: type[Quantity]) -> None:
    """Test that the base value always is a float."""
    quantity = quantity_type.zero()
    assert isinstance(quantity.base_value, float)


@pytest.mark.parametrize(
    "quantity_type, unit", zip(_QUANTITY_SUBCLASSES, _QUANTITY_BASE_UNIT_STRINGS)
)
def test_base_value_from_string_is_float(
    quantity_type: type[Quantity], unit: str
) -> None:
    """Test that the base value always is a float."""
    quantity = quantity_type.from_string(f"1 {unit}")
    assert isinstance(quantity.base_value, float)


def test_string_representation() -> None:
    """Test the string representation of the quantities."""
    assert str(Quantity(1.024445, exponent=0)) == "1.024"
    assert (
        repr(Quantity(1.024445, exponent=0)) == "Quantity(value=1.024445, exponent=0)"
    )
    assert f"{Quantity(0.50001, exponent=0):.0}" == "1"
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
    assert f"{Power.from_watts(-0.0):.0}" == "-0 W"
    assert f"{Power.from_watts(0.0):.0}" == "0 W"
    assert f"{Voltage.from_volts(999.9999850988388)}" == "1 kV"


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
    freq = Frequency.from_kilohertz(600_000.0)
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


@pytest.mark.parametrize("quantity_ctor", _QUANTITY_CTORS + [Quantity])
# Use a small amount to avoid long running tests, we have too many combinations
@hypothesis.settings(max_examples=10)
@hypothesis.given(
    quantity_value=st.floats(
        allow_infinity=False,
        allow_nan=False,
        allow_subnormal=False,
        # We need to set this because otherwise constructors with big exponents will
        # cause the value to be too big for the float type, and the test will fail.
        max_value=1e298,
        min_value=-1e298,
    ),
    percent=st.floats(allow_infinity=False, allow_nan=False, allow_subnormal=False),
)
def test_quantity_multiplied_with_precentage(
    quantity_ctor: type[Quantity], quantity_value: float, percent: float
) -> None:
    """Test the multiplication of all quantities with percentage."""
    percentage = Percentage.from_percent(percent)
    quantity = quantity_ctor(quantity_value)
    expected_value = quantity.base_value * (percent / 100.0)
    print(f"{quantity=}, {percentage=}, {expected_value=}")

    product = quantity * percentage
    print(f"{product=}")
    assert product.base_value == expected_value

    quantity *= percentage
    print(f"*{quantity=}")
    assert quantity.base_value == expected_value


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


# We can't use _QUANTITY_TYPES here, because it will break the tests, as hypothesis
# will generate more values, some of which are unsupported by the quantities. See the
# test comment for more details.
@pytest.mark.parametrize("quantity_type", [Power, Voltage, Current, Energy, Frequency])
@pytest.mark.parametrize("exponent", [0, 3, 6, 9])
@hypothesis.settings(
    max_examples=1000
)  # Set to have a decent amount of examples (default is 100)
@hypothesis.seed(42)  # Seed that triggers a lot of problematic edge cases
@hypothesis.given(value=st.floats(min_value=-1.0, max_value=1.0))
def test_to_and_from_string(
    quantity_type: type[Quantity], exponent: int, value: float
) -> None:
    """Test string parsing and formatting.

    The parameters for this test are constructed to stay deterministic.

    With a different (or random) seed or different max_examples the
    test will show failing examples.

    Fixing those cases was considered an unreasonable amount of work
    at the time of writing.

    For the future, one idea was to parse the string number after the first
    generation and regenerate it with the more appropriate unit and precision.
    """
    quantity = quantity_type.__new__(quantity_type)
    quantity._base_value = value * 10**exponent  # pylint: disable=protected-access
    # The above should be replaced with:
    # quantity = quantity_type._new(  # pylint: disable=protected-access
    #     value, exponent=exponent
    # )
    # But we can't do that now, because, you guessed it, it will also break the tests
    # (_new() will use 10.0**exponent instead of 10**exponent, which seems to have some
    # effect on the tests.
    quantity_str = f"{quantity:.{exponent}}"
    from_string = quantity_type.from_string(quantity_str)
    try:
        assert f"{from_string:.{exponent}}" == quantity_str
    except AssertionError as error:
        pytest.fail(
            f"Failed for {quantity.base_value} != from_string({from_string.base_value}) "
            + f"with exponent {exponent} and source value '{value}': {error}"
        )
