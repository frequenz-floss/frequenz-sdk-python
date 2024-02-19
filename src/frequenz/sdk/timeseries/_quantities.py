# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Types for holding quantities with units."""

# pylint: disable=too-many-lines

from __future__ import annotations

import math
from datetime import timedelta
from typing import Any, NoReturn, Self, TypeVar, overload

QuantityT = TypeVar(
    "QuantityT",
    "Quantity",
    "Power",
    "Current",
    "Voltage",
    "Energy",
    "Frequency",
    "Percentage",
    "Temperature",
)
"""Type variable for representing various quantity types."""


class Quantity:
    """A quantity with a unit.

    Quantities try to behave like float and are also immutable.
    """

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
        self._base_value = value * 10.0**exponent

    @classmethod
    def _new(cls, value: float, *, exponent: int = 0) -> Self:
        """Instantiate a new quantity subclass instance.

        Args:
            value: The value of this quantity in a given exponent of the base unit.
            exponent: The exponent of the base unit the given value is in.

        Returns:
            A new quantity subclass instance.
        """
        self = cls.__new__(cls)
        self._base_value = value * 10.0**exponent
        return self

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
        if 0 not in exponent_unit_map:
            raise ValueError("Expected a base unit for the type (for exponent 0)")
        cls._exponent_unit_map = exponent_unit_map
        super().__init_subclass__()

    _zero_cache: dict[type, Quantity] = {}
    """Cache for zero singletons.

    This is a workaround for mypy getting confused when using @functools.cache and
    @classmethod combined with returning Self. It believes the resulting type of this
    method is Self and complains that members of the actual class don't exist in Self,
    so we need to implement the cache ourselves.
    """

    @classmethod
    def zero(cls) -> Self:
        """Return a quantity with value 0.0.

        Returns:
            A quantity with value 0.0.
        """
        _zero = cls._zero_cache.get(cls, None)
        if _zero is None:
            _zero = cls.__new__(cls)
            _zero._base_value = 0.0
            cls._zero_cache[cls] = _zero
        assert isinstance(_zero, cls)
        return _zero

    @classmethod
    def from_string(cls, string: str) -> Self:
        """Return a quantity from a string representation.

        Args:
            string: The string representation of the quantity.

        Returns:
            A quantity object with the value given in the string.

        Raises:
            ValueError: If the string does not match the expected format.

        """
        split_string = string.split(" ")

        if len(split_string) != 2:
            raise ValueError(
                f"Expected a string of the form 'value unit', got {string}"
            )

        assert cls._exponent_unit_map is not None
        exp_map = cls._exponent_unit_map

        for exponent, unit in exp_map.items():
            if unit == split_string[1]:
                instance = cls.__new__(cls)
                try:
                    instance._base_value = float(split_string[0]) * 10**exponent
                except ValueError as error:
                    raise ValueError(f"Failed to parse string '{string}'.") from error

                return instance

        raise ValueError(f"Unknown unit {split_string[1]}")

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

    def isclose(self, other: Self, rel_tol: float = 1e-9, abs_tol: float = 0.0) -> bool:
        """Return whether this quantity is close to another.

        Args:
            other: The quantity to compare to.
            rel_tol: The relative tolerance.
            abs_tol: The absolute tolerance.

        Returns:
            Whether this quantity is close to another.
        """
        return math.isclose(
            self._base_value,
            other._base_value,  # pylint: disable=protected-access
            rel_tol=rel_tol,
            abs_tol=abs_tol,
        )

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

    # pylint: disable=too-many-branches
    def __format__(self, __format_spec: str) -> str:
        """Return a formatted string representation of this quantity.

        If specified, must be of this form: `[0].{precision}`.  If a 0 is not given, the
        trailing zeros will be omitted.  If no precision is given, the default is 3.

        The returned string will use the unit that will result in the maximum precision,
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

        if math.isinf(self._base_value) or math.isnan(self._base_value):
            return f"{self._base_value} {self._exponent_unit_map[0]}"

        if abs_value := abs(self._base_value):
            precision_pow = 10 ** (precision)
            # Prevent numbers like 999.999999 being rendered as 1000 V
            # instead of 1 kV.
            # This could happen because the str formatting function does
            # rounding as well.
            # This is an imperfect solution that works for _most_ cases.
            # isclose parameters were chosen according to the observed cases
            if math.isclose(abs_value, precision_pow, abs_tol=1e-4, rel_tol=0.01):
                # If the value is close to the precision, round it
                exponent = math.ceil(math.log10(precision_pow))
            else:
                exponent = math.floor(math.log10(abs_value))
        else:
            exponent = 0

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

        if value_str in ("-0", "0"):
            stripped = value_str
        else:
            stripped = value_str.rstrip("0").rstrip(".")

        if not keep_trailing_zeros:
            value_str = stripped
        unit_str = unit if stripped not in ("-0", "0") else self._exponent_unit_map[0]
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

    @overload
    def __mul__(self, scalar: float, /) -> Self:
        """Scale this quantity by a scalar.

        Args:
            scalar: The scalar by which to scale this quantity.

        Returns:
            The scaled quantity.
        """

    @overload
    def __mul__(self, percent: Percentage, /) -> Self:
        """Scale this quantity by a percentage.

        Args:
            percent: The percentage by which to scale this quantity.

        Returns:
            The scaled quantity.
        """

    def __mul__(self, value: float | Percentage, /) -> Self:
        """Scale this quantity by a scalar or percentage.

        Args:
            value: The scalar or percentage by which to scale this quantity.

        Returns:
            The scaled quantity.
        """
        match value:
            case float():
                return type(self)._new(self._base_value * value)
            case Percentage():
                return type(self)._new(self._base_value * value.as_fraction())
            case _:
                return NotImplemented

    @overload
    def __truediv__(self, other: float, /) -> Self:
        """Divide this quantity by a scalar.

        Args:
            other: The scalar or percentage to divide this quantity by.

        Returns:
            The divided quantity.
        """

    @overload
    def __truediv__(self, other: Self, /) -> float:
        """Return the ratio of this quantity to another.

        Args:
            other: The other quantity.

        Returns:
            The ratio of this quantity to another.
        """

    def __truediv__(self, value: float | Self, /) -> Self | float:
        """Divide this quantity by a scalar or another quantity.

        Args:
            value: The scalar or quantity to divide this quantity by.

        Returns:
            The divided quantity or the ratio of this quantity to another.
        """
        match value:
            case float():
                return type(self)._new(self._base_value / value)
            case Quantity() if type(value) is type(self):
                return self._base_value / value._base_value
            case _:
                return NotImplemented

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

    def __neg__(self) -> Self:
        """Return the negation of this quantity.

        Returns:
            The negation of this quantity.
        """
        negation = type(self).__new__(type(self))
        negation._base_value = -self._base_value
        return negation

    def __abs__(self) -> Self:
        """Return the absolute value of this quantity.

        Returns:
            The absolute value of this quantity.
        """
        absolute = type(self).__new__(type(self))
        absolute._base_value = abs(self._base_value)
        return absolute


class _NoDefaultConstructible(type):
    """A metaclass that disables the default constructor."""

    def __call__(cls, *_args: Any, **_kwargs: Any) -> NoReturn:
        """Raise a TypeError when the default constructor is called.

        Args:
            *_args: ignored positional arguments.
            **_kwargs: ignored keyword arguments.

        Raises:
            TypeError: Always.
        """
        raise TypeError(
            "Use of default constructor NOT allowed for "
            f"{cls.__module__}.{cls.__qualname__}, "
            f"use one of the `{cls.__name__}.from_*()` methods instead."
        )


class Temperature(
    Quantity,
    metaclass=_NoDefaultConstructible,
    exponent_unit_map={
        0: "°C",
    },
):
    """A temperature quantity (in degrees Celsius)."""

    @classmethod
    def from_celsius(cls, value: float) -> Self:
        """Initialize a new temperature quantity.

        Args:
            value: The temperature in degrees Celsius.

        Returns:
            A new temperature quantity.
        """
        return cls._new(value)

    def as_celsius(self) -> float:
        """Return the temperature in degrees Celsius.

        Returns:
            The temperature in degrees Celsius.
        """
        return self._base_value


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
    """A power quantity.

    Objects of this type are wrappers around `float` values and are immutable.

    The constructors accept a single `float` value, the `as_*()` methods return a
    `float` value, and each of the arithmetic operators supported by this type are
    actually implemented using floating-point arithmetic.

    So all considerations about floating-point arithmetic apply to this type as well.
    """

    @classmethod
    def from_watts(cls, watts: float) -> Self:
        """Initialize a new power quantity.

        Args:
            watts: The power in watts.

        Returns:
            A new power quantity.
        """
        return cls._new(watts)

    @classmethod
    def from_milliwatts(cls, milliwatts: float) -> Self:
        """Initialize a new power quantity.

        Args:
            milliwatts: The power in milliwatts.

        Returns:
            A new power quantity.
        """
        return cls._new(milliwatts, exponent=-3)

    @classmethod
    def from_kilowatts(cls, kilowatts: float) -> Self:
        """Initialize a new power quantity.

        Args:
            kilowatts: The power in kilowatts.

        Returns:
            A new power quantity.
        """
        return cls._new(kilowatts, exponent=3)

    @classmethod
    def from_megawatts(cls, megawatts: float) -> Self:
        """Initialize a new power quantity.

        Args:
            megawatts: The power in megawatts.

        Returns:
            A new power quantity.
        """
        return cls._new(megawatts, exponent=6)

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

    # We need the ignore here because otherwise mypy will give this error:
    # > Overloaded operator methods can't have wider argument types in overrides
    # The problem seems to be when the other type implements an **incompatible**
    # __rmul__ method, which is not the case here, so we should be safe.
    # Please see this example:
    # https://github.com/python/mypy/blob/c26f1297d4f19d2d1124a30efc97caebb8c28616/test-data/unit/check-overloading.test#L4738C1-L4769C55
    # And a discussion in a mypy issue here:
    # https://github.com/python/mypy/issues/4985#issuecomment-389692396
    @overload  # type: ignore[override]
    def __mul__(self, scalar: float, /) -> Self:
        """Scale this power by a scalar.

        Args:
            scalar: The scalar by which to scale this power.

        Returns:
            The scaled power.
        """

    @overload
    def __mul__(self, percent: Percentage, /) -> Self:
        """Scale this power by a percentage.

        Args:
            percent: The percentage by which to scale this power.

        Returns:
            The scaled power.
        """

    @overload
    def __mul__(self, other: timedelta, /) -> Energy:
        """Return an energy from multiplying this power by the given duration.

        Args:
            other: The duration to multiply by.

        Returns:
            The calculated energy.
        """

    def __mul__(self, other: float | Percentage | timedelta, /) -> Self | Energy:
        """Return a power or energy from multiplying this power by the given value.

        Args:
            other: The scalar, percentage or duration to multiply by.

        Returns:
            A power or energy.
        """
        match other:
            case float() | Percentage():
                return super().__mul__(other)
            case timedelta():
                return Energy._new(self._base_value * other.total_seconds() / 3600.0)
            case _:
                return NotImplemented

    # See the comment for Power.__mul__ for why we need the ignore here.
    @overload  # type: ignore[override]
    def __truediv__(self, other: float, /) -> Self:
        """Divide this power by a scalar.

        Args:
            other: The scalar to divide this power by.

        Returns:
            The divided power.
        """

    @overload
    def __truediv__(self, other: Self, /) -> float:
        """Return the ratio of this power to another.

        Args:
            other: The other power.

        Returns:
            The ratio of this power to another.
        """

    @overload
    def __truediv__(self, current: Current, /) -> Voltage:
        """Return a voltage from dividing this power by the given current.

        Args:
            current: The current to divide by.

        Returns:
            A voltage from dividing this power by the a current.
        """

    @overload
    def __truediv__(self, voltage: Voltage, /) -> Current:
        """Return a current from dividing this power by the given voltage.

        Args:
            voltage: The voltage to divide by.

        Returns:
            A current from dividing this power by a voltage.
        """

    def __truediv__(
        self, other: float | Self | Current | Voltage, /
    ) -> Self | float | Voltage | Current:
        """Return a current or voltage from dividing this power by the given value.

        Args:
            other: The scalar, power, current or voltage to divide by.

        Returns:
            A current or voltage from dividing this power by the given value.
        """
        match other:
            case float():
                return super().__truediv__(other)
            case Power():
                return self._base_value / other._base_value
            case Current():
                return Voltage._new(self._base_value / other._base_value)
            case Voltage():
                return Current._new(self._base_value / other._base_value)
            case _:
                return NotImplemented


class Current(
    Quantity,
    metaclass=_NoDefaultConstructible,
    exponent_unit_map={
        -3: "mA",
        0: "A",
    },
):
    """A current quantity.

    Objects of this type are wrappers around `float` values and are immutable.

    The constructors accept a single `float` value, the `as_*()` methods return a
    `float` value, and each of the arithmetic operators supported by this type are
    actually implemented using floating-point arithmetic.

    So all considerations about floating-point arithmetic apply to this type as well.
    """

    @classmethod
    def from_amperes(cls, amperes: float) -> Self:
        """Initialize a new current quantity.

        Args:
            amperes: The current in amperes.

        Returns:
            A new current quantity.
        """
        return cls._new(amperes)

    @classmethod
    def from_milliamperes(cls, milliamperes: float) -> Self:
        """Initialize a new current quantity.

        Args:
            milliamperes: The current in milliamperes.

        Returns:
            A new current quantity.
        """
        return cls._new(milliamperes, exponent=-3)

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

    # See comment for Power.__mul__ for why we need the ignore here.
    @overload  # type: ignore[override]
    def __mul__(self, scalar: float, /) -> Self:
        """Scale this current by a scalar.

        Args:
            scalar: The scalar by which to scale this current.

        Returns:
            The scaled current.
        """

    @overload
    def __mul__(self, percent: Percentage, /) -> Self:
        """Scale this current by a percentage.

        Args:
            percent: The percentage by which to scale this current.

        Returns:
            The scaled current.
        """

    @overload
    def __mul__(self, other: Voltage, /) -> Power:
        """Multiply the current by a voltage to get a power.

        Args:
            other: The voltage.

        Returns:
            The calculated power.
        """

    def __mul__(self, other: float | Percentage | Voltage, /) -> Self | Power:
        """Return a current or power from multiplying this current by the given value.

        Args:
            other: The scalar, percentage or voltage to multiply by.

        Returns:
            A current or power.
        """
        match other:
            case float() | Percentage():
                return super().__mul__(other)
            case Voltage():
                return Power._new(self._base_value * other._base_value)
            case _:
                return NotImplemented


class Voltage(
    Quantity,
    metaclass=_NoDefaultConstructible,
    exponent_unit_map={0: "V", -3: "mV", 3: "kV"},
):
    """A voltage quantity.

    Objects of this type are wrappers around `float` values and are immutable.

    The constructors accept a single `float` value, the `as_*()` methods return a
    `float` value, and each of the arithmetic operators supported by this type are
    actually implemented using floating-point arithmetic.

    So all considerations about floating-point arithmetic apply to this type as well.
    """

    @classmethod
    def from_volts(cls, volts: float) -> Self:
        """Initialize a new voltage quantity.

        Args:
            volts: The voltage in volts.

        Returns:
            A new voltage quantity.
        """
        return cls._new(volts)

    @classmethod
    def from_millivolts(cls, millivolts: float) -> Self:
        """Initialize a new voltage quantity.

        Args:
            millivolts: The voltage in millivolts.

        Returns:
            A new voltage quantity.
        """
        return cls._new(millivolts, exponent=-3)

    @classmethod
    def from_kilovolts(cls, kilovolts: float) -> Self:
        """Initialize a new voltage quantity.

        Args:
            kilovolts: The voltage in kilovolts.

        Returns:
            A new voltage quantity.
        """
        return cls._new(kilovolts, exponent=3)

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

    # See comment for Power.__mul__ for why we need the ignore here.
    @overload  # type: ignore[override]
    def __mul__(self, scalar: float, /) -> Self:
        """Scale this voltage by a scalar.

        Args:
            scalar: The scalar by which to scale this voltage.

        Returns:
            The scaled voltage.
        """

    @overload
    def __mul__(self, percent: Percentage, /) -> Self:
        """Scale this voltage by a percentage.

        Args:
            percent: The percentage by which to scale this voltage.

        Returns:
            The scaled voltage.
        """

    @overload
    def __mul__(self, other: Current, /) -> Power:
        """Multiply the voltage by the current to get the power.

        Args:
            other: The current to multiply the voltage with.

        Returns:
            The calculated power.
        """

    def __mul__(self, other: float | Percentage | Current, /) -> Self | Power:
        """Return a voltage or power from multiplying this voltage by the given value.

        Args:
            other: The scalar, percentage or current to multiply by.

        Returns:
            The calculated voltage or power.
        """
        match other:
            case float() | Percentage():
                return super().__mul__(other)
            case Current():
                return Power._new(self._base_value * other._base_value)
            case _:
                return NotImplemented


class Energy(
    Quantity,
    metaclass=_NoDefaultConstructible,
    exponent_unit_map={
        0: "Wh",
        3: "kWh",
        6: "MWh",
    },
):
    """An energy quantity.

    Objects of this type are wrappers around `float` values and are immutable.

    The constructors accept a single `float` value, the `as_*()` methods return a
    `float` value, and each of the arithmetic operators supported by this type are
    actually implemented using floating-point arithmetic.

    So all considerations about floating-point arithmetic apply to this type as well.
    """

    @classmethod
    def from_watt_hours(cls, watt_hours: float) -> Self:
        """Initialize a new energy quantity.

        Args:
            watt_hours: The energy in watt hours.

        Returns:
            A new energy quantity.
        """
        return cls._new(watt_hours)

    @classmethod
    def from_kilowatt_hours(cls, kilowatt_hours: float) -> Self:
        """Initialize a new energy quantity.

        Args:
            kilowatt_hours: The energy in kilowatt hours.

        Returns:
            A new energy quantity.
        """
        return cls._new(kilowatt_hours, exponent=3)

    @classmethod
    def from_megawatt_hours(cls, megawatt_hours: float) -> Self:
        """Initialize a new energy quantity.

        Args:
            megawatt_hours: The energy in megawatt hours.

        Returns:
            A new energy quantity.
        """
        return cls._new(megawatt_hours, exponent=6)

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

    def __mul__(self, other: float | Percentage) -> Self:
        """Scale this energy by a percentage.

        Args:
            other: The percentage by which to scale this energy.

        Returns:
            The scaled energy.
        """
        match other:
            case float():
                return self._new(self._base_value * other)
            case Percentage():
                return self._new(self._base_value * other.as_fraction())
            case _:
                return NotImplemented

    # See the comment for Power.__mul__ for why we need the ignore here.
    @overload  # type: ignore[override]
    def __truediv__(self, other: float, /) -> Self:
        """Divide this energy by a scalar.

        Args:
            other: The scalar to divide this energy by.

        Returns:
            The divided energy.
        """

    @overload
    def __truediv__(self, other: Self, /) -> float:
        """Return the ratio of this energy to another.

        Args:
            other: The other energy.

        Returns:
            The ratio of this energy to another.
        """

    @overload
    def __truediv__(self, duration: timedelta, /) -> Power:
        """Return a power from dividing this energy by the given duration.

        Args:
            duration: The duration to divide by.

        Returns:
            A power from dividing this energy by the given duration.
        """

    @overload
    def __truediv__(self, power: Power, /) -> timedelta:
        """Return a duration from dividing this energy by the given power.

        Args:
            power: The power to divide by.

        Returns:
            A duration from dividing this energy by the given power.
        """

    def __truediv__(
        self, other: float | Self | timedelta | Power, /
    ) -> Self | float | Power | timedelta:
        """Return a power or duration from dividing this energy by the given value.

        Args:
            other: The scalar, energy, power or duration to divide by.

        Returns:
            A power or duration from dividing this energy by the given value.
        """
        match other:
            case float():
                return super().__truediv__(other)
            case Energy():
                return self._base_value / other._base_value
            case timedelta():
                return Power._new(self._base_value / (other.total_seconds() / 3600.0))
            case Power():
                return timedelta(
                    seconds=(self._base_value / other._base_value) * 3600.0
                )
            case _:
                return NotImplemented


class Frequency(
    Quantity,
    metaclass=_NoDefaultConstructible,
    exponent_unit_map={0: "Hz", 3: "kHz", 6: "MHz", 9: "GHz"},
):
    """A frequency quantity.

    Objects of this type are wrappers around `float` values and are immutable.

    The constructors accept a single `float` value, the `as_*()` methods return a
    `float` value, and each of the arithmetic operators supported by this type are
    actually implemented using floating-point arithmetic.

    So all considerations about floating-point arithmetic apply to this type as well.
    """

    @classmethod
    def from_hertz(cls, hertz: float) -> Self:
        """Initialize a new frequency quantity.

        Args:
            hertz: The frequency in hertz.

        Returns:
            A new frequency quantity.
        """
        return cls._new(hertz)

    @classmethod
    def from_kilohertz(cls, kilohertz: float) -> Self:
        """Initialize a new frequency quantity.

        Args:
            kilohertz: The frequency in kilohertz.

        Returns:
            A new frequency quantity.
        """
        return cls._new(kilohertz, exponent=3)

    @classmethod
    def from_megahertz(cls, megahertz: float) -> Self:
        """Initialize a new frequency quantity.

        Args:
            megahertz: The frequency in megahertz.

        Returns:
            A new frequency quantity.
        """
        return cls._new(megahertz, exponent=6)

    @classmethod
    def from_gigahertz(cls, gigahertz: float) -> Self:
        """Initialize a new frequency quantity.

        Args:
            gigahertz: The frequency in gigahertz.

        Returns:
            A new frequency quantity.
        """
        return cls._new(gigahertz, exponent=9)

    def as_hertz(self) -> float:
        """Return the frequency in hertz.

        Returns:
            The frequency in hertz.
        """
        return self._base_value

    def as_kilohertz(self) -> float:
        """Return the frequency in kilohertz.

        Returns:
            The frequency in kilohertz.
        """
        return self._base_value / 1e3

    def as_megahertz(self) -> float:
        """Return the frequency in megahertz.

        Returns:
            The frequency in megahertz.
        """
        return self._base_value / 1e6

    def as_gigahertz(self) -> float:
        """Return the frequency in gigahertz.

        Returns:
            The frequency in gigahertz.
        """
        return self._base_value / 1e9

    def period(self) -> timedelta:
        """Return the period of the frequency.

        Returns:
            The period of the frequency.
        """
        return timedelta(seconds=1.0 / self._base_value)


class Percentage(
    Quantity,
    metaclass=_NoDefaultConstructible,
    exponent_unit_map={0: "%"},
):
    """A percentage quantity.

    Objects of this type are wrappers around `float` values and are immutable.

    The constructors accept a single `float` value, the `as_*()` methods return a
    `float` value, and each of the arithmetic operators supported by this type are
    actually implemented using floating-point arithmetic.

    So all considerations about floating-point arithmetic apply to this type as well.
    """

    @classmethod
    def from_percent(cls, percent: float) -> Self:
        """Initialize a new percentage quantity from a percent value.

        Args:
            percent: The percent value, normally in the 0.0-100.0 range.

        Returns:
            A new percentage quantity.
        """
        return cls._new(percent)

    @classmethod
    def from_fraction(cls, fraction: float) -> Self:
        """Initialize a new percentage quantity from a fraction.

        Args:
            fraction: The fraction, normally in the 0.0-1.0 range.

        Returns:
            A new percentage quantity.
        """
        return cls._new(fraction * 100)

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
