# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Types for holding quantities with units."""

from __future__ import annotations

import math
from datetime import timedelta
from typing import Any, NoReturn, Self, TypeVar, overload

QuantityT = TypeVar(
    "QuantityT", "Quantity", "Power", "Current", "Voltage", "Energy", "Percentage"
)


class Quantity:
    """A quantity with a unit."""

    _base_value: float
    """The value of this quantity in the base unit."""

    _exponent_unit_map: dict[int, str] | None = None
    """A mapping from the exponent of the base unit to the unit symbol.

    If None, this quantity has no unit.  None is possible only when using the base
    class.  Sub-classes must define this.
    """

    def __init__(self, value: float, exponent: int = 0) -> None:
        """Initialize a new quantity.

        Args:
            value: The value of this quantity in a given exponent of the base unit.
            exponent: The exponent of the base unit the given value is in.
        """
        self._base_value = value * 10**exponent

    def __init_subclass__(cls, exponent_unit_map: dict[int, str]) -> None:
        """Initialize a new subclass of Quantity.

        Args:
            exponent_unit_map: A mapping from the exponent of the base unit to the unit
                symbol.

        Raises:
            TypeError: If the given exponent_unit_map is not a dict.
            ValueError: If the given exponent_unit_map does not contain a base unit
                (exponent 0).
        """
        if not 0 in exponent_unit_map:
            raise ValueError("Expected a base unit for the type (for exponent 0)")
        cls._exponent_unit_map = exponent_unit_map
        super().__init_subclass__()

    @property
    def base_value(self) -> float:
        """Return the value of this quantity in the base unit.

        Returns:
            The value of this quantity in the base unit.
        """
        return self._base_value

    @property
    def base_unit(self) -> str | None:
        """Return the base unit of this quantity.

        None if this quantity has no unit.

        Returns:
            The base unit of this quantity.
        """
        if not self._exponent_unit_map:
            return None
        return self._exponent_unit_map[0]

    def isnan(self) -> bool:
        """Return whether this quantity is NaN.

        Returns:
            Whether this quantity is NaN.
        """
        return math.isnan(self._base_value)

    def isinf(self) -> bool:
        """Return whether this quantity is infinite.

        Returns:
            Whether this quantity is infinite.
        """
        return math.isinf(self._base_value)

    def __hash__(self) -> int:
        """Return a hash of this object.

        Returns:
            A hash of this object.
        """
        return hash((type(self), self._base_value))

    def __repr__(self) -> str:
        """Return a representation of this quantity.

        Returns:
            A representation of this quantity.
        """
        return f"{type(self).__name__}(value={self._base_value}, exponent=0)"

    def __str__(self) -> str:
        """Return a string representation of this quantity.

        Returns:
            A string representation of this quantity.
        """
        return self.__format__("")

    def __format__(self, __format_spec: str) -> str:
        """Return a formatted string representation of this quantity.

        If specified, must be of this form: `[0].{precision}`.  If a 0 is not given, the
        trailing zeros will be omitted.  If no precision is given, the default is 3.

        The returned string will use the unit that will result in the maxium precision,
        based on the magnitude of the value.

        Example:
            ```python
            from frequenz.sdk.timeseries import Current
            c = Current.from_amperes(0.2345)
            assert f"{c:.2}" == "234.5 mA"
            c = Current.from_amperes(1.2345)
            assert f"{c:.2}" == "1.23 A"
            c = Current.from_milliamperes(1.2345)
            assert f"{c:.6}" == "1.2345 mA"
            ```

        Args:
            __format_spec: The format specifier.

        Returns:
            A string representation of this quantity.

        Raises:
            ValueError: If the given format specifier is invalid.
        """
        keep_trailing_zeros = False
        if __format_spec != "":
            fspec_parts = __format_spec.split(".")
            if (
                len(fspec_parts) != 2
                or fspec_parts[0] not in ("", "0")
                or not fspec_parts[1].isdigit()
            ):
                raise ValueError(
                    "Invalid format specifier. Must be empty or `[0].{precision}`"
                )
            if fspec_parts[0] == "0":
                keep_trailing_zeros = True
            precision = int(fspec_parts[1])
        else:
            precision = 3
        if not self._exponent_unit_map:
            return f"{self._base_value:.{precision}f}"

        abs_value = abs(self._base_value)
        exponent = math.floor(math.log10(abs_value)) if abs_value else 0
        unit_place = exponent - exponent % 3
        if unit_place < min(self._exponent_unit_map):
            unit = self._exponent_unit_map[min(self._exponent_unit_map.keys())]
            unit_place = min(self._exponent_unit_map)
        elif unit_place > max(self._exponent_unit_map):
            unit = self._exponent_unit_map[max(self._exponent_unit_map.keys())]
            unit_place = max(self._exponent_unit_map)
        else:
            unit = self._exponent_unit_map[unit_place]
        value_str = f"{self._base_value / 10 ** unit_place:.{precision}f}"
        stripped = value_str.rstrip("0").rstrip(".")
        if not keep_trailing_zeros:
            value_str = stripped
        unit_str = unit if stripped != "0" else self._exponent_unit_map[0]
        return f"{value_str} {unit_str}"

    def __add__(self, other: Self) -> Self:
        """Return the sum of this quantity and another.

        Args:
            other: The other quantity.

        Returns:
            The sum of this quantity and another.
        """
        if not type(other) is type(self):
            return NotImplemented
        summe = type(self).__new__(type(self))
        summe._base_value = self._base_value + other._base_value
        return summe

    def __sub__(self, other: Self) -> Self:
        """Return the difference of this quantity and another.

        Args:
            other: The other quantity.

        Returns:
            The difference of this quantity and another.
        """
        if not type(other) is type(self):
            return NotImplemented
        difference = type(self).__new__(type(self))
        difference._base_value = self._base_value - other._base_value
        return difference

    def __gt__(self, other: Self) -> bool:
        """Return whether this quantity is greater than another.

        Args:
            other: The other quantity.

        Returns:
            Whether this quantity is greater than another.
        """
        if not type(other) is type(self):
            return NotImplemented
        return self._base_value > other._base_value

    def __ge__(self, other: Self) -> bool:
        """Return whether this quantity is greater than or equal to another.

        Args:
            other: The other quantity.

        Returns:
            Whether this quantity is greater than or equal to another.
        """
        if not type(other) is type(self):
            return NotImplemented
        return self._base_value >= other._base_value

    def __lt__(self, other: Self) -> bool:
        """Return whether this quantity is less than another.

        Args:
            other: The other quantity.

        Returns:
            Whether this quantity is less than another.
        """
        if not type(other) is type(self):
            return NotImplemented
        return self._base_value < other._base_value

    def __le__(self, other: Self) -> bool:
        """Return whether this quantity is less than or equal to another.

        Args:
            other: The other quantity.

        Returns:
            Whether this quantity is less than or equal to another.
        """
        if not type(other) is type(self):
            return NotImplemented
        return self._base_value <= other._base_value

    def __eq__(self, other: object) -> bool:
        """Return whether this quantity is equal to another.

        Args:
            other: The other quantity.

        Returns:
            Whether this quantity is equal to another.
        """
        if not type(other) is type(self):
            return NotImplemented
        # The above check ensures that both quantities are the exact same type, because
        # `isinstance` returns true for subclasses and superclasses.  But the above check
        # doesn't help mypy identify the type of other,  so the below line is necessary.
        assert isinstance(other, self.__class__)
        return self._base_value == other._base_value


class _NoDefaultConstructible(type):
    """A metaclass that disables the default constructor."""

    def __call__(cls, *_args: Any, **_kwargs: Any) -> NoReturn:
        """Raise a TypeError when the default constructor is called.

        Args:
            _args: ignored positional arguments.
            _kwargs: ignored keyword arguments.

        Raises:
            TypeError: Always.
        """
        raise TypeError(
            "Use of default constructor NOT allowed for "
            f"{cls.__module__}.{cls.__qualname__}, "
            f"use one of the `{cls.__name__}.from_*()` methods instead."
        )


class Power(
    Quantity,
    metaclass=_NoDefaultConstructible,
    exponent_unit_map={
        -3: "mW",
        0: "W",
        3: "kW",
        6: "MW",
    },
):
    """A power quantity."""

    @classmethod
    def from_watts(cls, watts: float) -> Self:
        """Initialize a new power quantity.

        Args:
            watts: The power in watts.

        Returns:
            A new power quantity.
        """
        power = cls.__new__(cls)
        power._base_value = watts
        return power

    @classmethod
    def from_milliwatts(cls, milliwatts: float) -> Self:
        """Initialize a new power quantity.

        Args:
            milliwatts: The power in milliwatts.

        Returns:
            A new power quantity.
        """
        power = cls.__new__(cls)
        power._base_value = milliwatts * 10**-3
        return power

    @classmethod
    def from_kilowatts(cls, kilowatts: float) -> Self:
        """Initialize a new power quantity.

        Args:
            kilowatts: The power in kilowatts.

        Returns:
            A new power quantity.
        """
        power = cls.__new__(cls)
        power._base_value = kilowatts * 10**3
        return power

    @classmethod
    def from_megawatts(cls, megawatts: float) -> Self:
        """Initialize a new power quantity.

        Args:
            megawatts: The power in megawatts.

        Returns:
            A new power quantity.
        """
        power = cls.__new__(cls)
        power._base_value = megawatts * 10**6
        return power

    def as_watts(self) -> float:
        """Return the power in watts.

        Returns:
            The power in watts.
        """
        return self._base_value

    def as_kilowatts(self) -> float:
        """Return the power in kilowatts.

        Returns:
            The power in kilowatts.
        """
        return self._base_value / 1e3

    def as_megawatts(self) -> float:
        """Return the power in megawatts.

        Returns:
            The power in megawatts.
        """
        return self._base_value / 1e6

    def __mul__(self, duration: timedelta) -> Energy:
        """Return an energy from multiplying this power by the given duration.

        Args:
            duration: The duration to multiply by.

        Returns:
            An energy from multiplying this power by the given duration.
        """
        return Energy.from_watt_hours(
            self._base_value * duration.total_seconds() / 3600.0
        )

    @overload
    def __truediv__(self, other: Current) -> Voltage:
        """Return a voltage from dividing this power by the given current.

        Args:
            other: The current to divide by.
        """

    @overload
    def __truediv__(self, other: Voltage) -> Current:
        """Return a current from dividing this power by the given voltage.

        Args:
            other: The voltage to divide by.
        """

    def __truediv__(self, other: Current | Voltage) -> Voltage | Current:
        """Return a current or voltage from dividing this power by the given value.

        Args:
            other: The current or voltage to divide by.

        Returns:
            A current or voltage from dividing this power by the given value.

        Raises:
            TypeError: If the given value is not a current or voltage.
        """
        if isinstance(other, Current):
            return Voltage.from_volts(self._base_value / other._base_value)
        if isinstance(other, Voltage):
            return Current.from_amperes(self._base_value / other._base_value)
        raise TypeError(
            f"unsupported operand type(s) for /: '{type(self)}' and '{type(other)}'"
        )


class Current(
    Quantity,
    metaclass=_NoDefaultConstructible,
    exponent_unit_map={
        -3: "mA",
        0: "A",
    },
):
    """A current quantity."""

    @classmethod
    def from_amperes(cls, amperes: float) -> Self:
        """Initialize a new current quantity.

        Args:
            amperes: The current in amperes.

        Returns:
            A new current quantity.
        """
        current = cls.__new__(cls)
        current._base_value = amperes
        return current

    @classmethod
    def from_milliamperes(cls, milliamperes: float) -> Self:
        """Initialize a new current quantity.

        Args:
            milliamperes: The current in milliamperes.

        Returns:
            A new current quantity.
        """
        current = cls.__new__(cls)
        current._base_value = milliamperes * 10**-3
        return current

    def as_amperes(self) -> float:
        """Return the current in amperes.

        Returns:
            The current in amperes.
        """
        return self._base_value

    def as_milliamperes(self) -> float:
        """Return the current in milliamperes.

        Returns:
            The current in milliamperes.
        """
        return self._base_value * 1e3

    def __mul__(self, voltage: Voltage) -> Power:
        """Multiply the current by a voltage to get a power.

        Args:
            voltage: The voltage.

        Returns:
            The power.
        """
        return Power.from_watts(self._base_value * voltage._base_value)


class Voltage(
    Quantity,
    metaclass=_NoDefaultConstructible,
    exponent_unit_map={0: "V", -3: "mV", 3: "kV"},
):
    """A voltage quantity."""

    @classmethod
    def from_volts(cls, volts: float) -> Self:
        """Initialize a new voltage quantity.

        Args:
            volts: The voltage in volts.

        Returns:
            A new voltage quantity.
        """
        voltage = cls.__new__(cls)
        voltage._base_value = volts
        return voltage

    @classmethod
    def from_millivolts(cls, millivolts: float) -> Self:
        """Initialize a new voltage quantity.

        Args:
            millivolts: The voltage in millivolts.

        Returns:
            A new voltage quantity.
        """
        voltage = cls.__new__(cls)
        voltage._base_value = millivolts * 10**-3
        return voltage

    @classmethod
    def from_kilovolts(cls, kilovolts: float) -> Self:
        """Initialize a new voltage quantity.

        Args:
            kilovolts: The voltage in kilovolts.

        Returns:
            A new voltage quantity.
        """
        voltage = cls.__new__(cls)
        voltage._base_value = kilovolts * 10**3
        return voltage

    def as_volts(self) -> float:
        """Return the voltage in volts.

        Returns:
            The voltage in volts.
        """
        return self._base_value

    def as_millivolts(self) -> float:
        """Return the voltage in millivolts.

        Returns:
            The voltage in millivolts.
        """
        return self._base_value * 1e3

    def as_kilovolts(self) -> float:
        """Return the voltage in kilovolts.

        Returns:
            The voltage in kilovolts.
        """
        return self._base_value / 1e3

    def __mul__(self, current: Current) -> Power:
        """Multiply the voltage by the current to get the power.

        Args:
            current: The current to multiply the voltage with.

        Returns:
            The calculated power.
        """
        return Power.from_watts(self._base_value * current._base_value)


class Energy(
    Quantity,
    metaclass=_NoDefaultConstructible,
    exponent_unit_map={
        0: "Wh",
        3: "kWh",
        6: "MWh",
    },
):
    """An energy quantity."""

    @classmethod
    def from_watt_hours(cls, watt_hours: float) -> Self:
        """Initialize a new energy quantity.

        Args:
            watt_hours: The energy in watt hours.

        Returns:
            A new energy quantity.
        """
        energy = cls.__new__(cls)
        energy._base_value = watt_hours
        return energy

    @classmethod
    def from_kilowatt_hours(cls, kilowatt_hours: float) -> Self:
        """Initialize a new energy quantity.

        Args:
            kilowatt_hours: The energy in kilowatt hours.

        Returns:
            A new energy quantity.
        """
        energy = cls.__new__(cls)
        energy._base_value = kilowatt_hours * 10**3
        return energy

    @classmethod
    def from_megawatt_hours(cls, megawatt_hours: float) -> Self:
        """Initialize a new energy quantity.

        Args:
            megawatt_hours: The energy in megawatt hours.

        Returns:
            A new energy quantity.
        """
        energy = cls.__new__(cls)
        energy._base_value = megawatt_hours * 10**6
        return energy

    def as_watt_hours(self) -> float:
        """Return the energy in watt hours.

        Returns:
            The energy in watt hours.
        """
        return self._base_value

    def as_kilowatt_hours(self) -> float:
        """Return the energy in kilowatt hours.

        Returns:
            The energy in kilowatt hours.
        """
        return self._base_value / 1e3

    def as_megawatt_hours(self) -> float:
        """Return the energy in megawatt hours.

        Returns:
            The energy in megawatt hours.
        """
        return self._base_value / 1e6

    @overload
    def __truediv__(self, other: timedelta) -> Power:
        """Return a power from dividing this energy by the given duration.

        Args:
            other: The duration to divide by.
        """

    @overload
    def __truediv__(self, other: Power) -> timedelta:
        """Return a duration from dividing this energy by the given power.

        Args:
            other: The power to divide by.
        """

    def __truediv__(self, other: timedelta | Power) -> Power | timedelta:
        """Return a power or duration from dividing this energy by the given value.

        Args:
            other: The power or duration to divide by.

        Returns:
            A power or duration from dividing this energy by the given value.

        Raises:
            TypeError: If the given value is not a power or duration.
        """
        if isinstance(other, timedelta):
            return Power.from_watts(self._base_value / (other.total_seconds() / 3600.0))
        if isinstance(other, Power):
            return timedelta(seconds=(self._base_value / other._base_value) * 3600.0)
        raise TypeError(
            f"unsupported operand type(s) for /: '{type(self)}' and '{type(other)}'"
        )


class Percentage(
    Quantity,
    metaclass=_NoDefaultConstructible,
    exponent_unit_map={0: "%"},
):
    """A percentage quantity."""

    @classmethod
    def from_percent(cls, percent: float) -> Self:
        """Initialize a new percentage quantity from a percent value.

        Args:
            percent: The percent value, normally in the 0.0-100.0 range.

        Returns:
            A new percentage quantity.
        """
        percentage = cls.__new__(cls)
        percentage._base_value = percent
        return percentage

    @classmethod
    def from_fraction(cls, fraction: float) -> Self:
        """Initialize a new percentage quantity from a fraction.

        Args:
            fraction: The fraction, normally in the 0.0-1.0 range.

        Returns:
            A new percentage quantity.
        """
        percentage = cls.__new__(cls)
        percentage._base_value = fraction * 100
        return percentage

    def as_percent(self) -> float:
        """Return this quantity as a percentage.

        Returns:
            This quantity as a percentage.
        """
        return self._base_value

    def as_fraction(self) -> float:
        """Return this quantity as a fraction.

        Returns:
            This quantity as a fraction.
        """
        return self._base_value / 100
