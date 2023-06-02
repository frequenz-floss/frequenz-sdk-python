# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Types for holding quantities with units."""

import math
from typing import Self


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
        exponent = math.floor(math.log10(abs_value))
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
        return type(self)(self._base_value + other._base_value)

    def __sub__(self, other: Self) -> Self:
        """Return the difference of this quantity and another.

        Args:
            other: The other quantity.

        Returns:
            The difference of this quantity and another.
        """
        if not type(other) is type(self):
            return NotImplemented
        return type(self)(self._base_value - other._base_value)

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
