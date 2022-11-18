# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Provides support for processing power measurements from different microgrid components."""

from __future__ import annotations

from numbers import Real

from frequenz.api.microgrid.common_pb2 import AC


class ComplexPower:
    """Describes the flow of AC power through a microgrid component or connection.

    The complex power represents both active and reactive power in a single complex
    number of the form

        S = P + jQ

    where P is the active power and Q the reactive power.  It is expected that these
    values follow the passive sign convention, where positive and negative values of
    P correspond to consumption and supply respectively, while positive and negative
    values of Q correspond to inductive and capacitive loads.  For details, see:
    https://en.wikipedia.org/wiki/Passive_sign_convention
    """

    def __init__(self, complex_power: complex) -> None:
        """Instantiate a ComplexPower value from a built-in Python complex number.

        Args:
            complex_power: complex number from which to instantiate the power
                value, following the passive sign convention that a
                positive/negative real part corresponds to consumption/supply
                respectively, and a positive/negative imaginary part similarly
                corresponds to inductive/capacitive load
        """
        self._complex_power = complex_power

    @classmethod
    def from_protobuf(cls, ac_message: AC) -> ComplexPower:
        """Create a ComplexPower value from the AC type of the microgrid gRPC API.

        Args:
            ac_message: protobuf message describing the AC power of a component

        Returns:
            Complex power value whose real (active) part is equal to the difference
                between consumption and supply in the `AC` message, and whose imaginary
                (reactive) part is equal to the difference between the message's
                inductive and capacitive power.
        """
        active = ac_message.power_active.value
        reactive = ac_message.power_reactive.value

        return cls(complex(active, reactive))

    @classmethod
    def from_active_power(cls, active_power: float) -> ComplexPower:
        """Create a ComplexPower value from a numerical active power value.

        Args:
            active_power: value of active power, following the passive sign convention
                (positive => consumption, negative => supply)

        Returns:
            Value with real (active) part equal to the provided active power value,
                and zero imaginary (reactive) part.
        """
        return cls(complex(active_power, 0))

    @classmethod
    def from_reactive_power(cls, reactive_power: float) -> ComplexPower:
        """Create a ComplexPower value from a numerical reactive power value.

        Args:
            reactive_power: value of reactive power, following the passive sign
                convention (positive => inductive, negative => capacitive)

        Returns:
            value with zero real (active) part and imaginary (reactive) part equal
                to the provided reactive power value
        """
        return cls(complex(0, reactive_power))

    @property
    def real(self) -> float:
        """Get the active component of the complex power value.

        Returns:
            Value of the real component, following the passive sign convention
                (positive => consumption, negative => supply)
        """
        return self._complex_power.real

    @property
    def active(self) -> float:
        """Get the real component of the complex power value.

        Returns:
            Value of the real component, following the passive sign convention
                (positive => consumption, negative => supply)
        """
        return self.real

    @property
    def consumption(self) -> float:
        """Get the power consumption.

        Returns:
            Value of the real (active) component of the complex power value if it is
                positive, or zero otherwise
        """
        return max(self.real, 0)

    @property
    def supply(self) -> float:
        """Get the power supply.

        Returns:
            Absolute value of the real (active) component of the complex power value
                if the latter is negative, or zero otherwise
        """
        return max(-self.real, 0)

    @property
    def imag(self) -> float:
        """Get the reactive component of the complex power value.

        Returns:
            Value of the imaginary component, following the passive sign convention
                (positive => inductive, negative => capacitive)
        """
        return self._complex_power.imag

    @property
    def reactive(self) -> float:
        """Get the imaginary component of the complex power value.

        Returns:
            Value of the imaginary component, following the passive sign convention
                (positive => inductive, negative => capacitive)
        """
        return self.imag

    @property
    def inductive(self) -> float:
        """Get the inductive power.

        Returns:
            Value of the imaginary (reactive) component of the complex power value
                if it is positive, or zero otherwise
        """
        return max(self.imag, 0)

    @property
    def capacitive(self) -> float:
        """Get the capacitive power.

        Returns:
            Absolute value of the imaginary (reactive) component of the complex
                power value if the latter is negative, or zero otherwise
        """
        return max(-self.imag, 0)

    def __neg__(self) -> ComplexPower:
        """Generate the negative of this value.

        Returns:
            Value whose real and imaginary parts are the negative of this instance's
        """
        return ComplexPower(-self._complex_power)

    def __add__(self, other: object) -> ComplexPower:
        """Add this complex power value to the provided `other`.

        Args:
            other: `ComplexPower` value to add

        Returns:
            Sum of the this value and the provided `other`
        """
        if not isinstance(other, ComplexPower):
            return NotImplemented

        return ComplexPower(self._complex_power + other._complex_power)

    def __sub__(self, other: object) -> ComplexPower:
        """Subtract the provided `other` from this complex power value.

        Args:
            other: `ComplexPower` value to subtract

        Returns:
            Difference between this value and the provided `other`
        """
        if not isinstance(other, ComplexPower):
            return NotImplemented

        return ComplexPower(self._complex_power - other._complex_power)

    def __mul__(self, other: object) -> ComplexPower:
        """Multiply this complex power value by the provided scalar.

        Args:
            other: `Real` value by which to multiply

        Returns:
            Product of this value and the provided `other`
        """
        if not isinstance(other, Real):
            return NotImplemented

        return ComplexPower(self._complex_power * other)

    __rmul__ = __mul__

    def __truediv__(self, other: object) -> ComplexPower:
        """Divide this complex power value by the provided scalar.

        Args:
            other: `Real` value by which to divide

        Returns:
            This value divided by the provided `other`
        """
        if not isinstance(other, Real):
            return NotImplemented

        return ComplexPower(self._complex_power / other)

    def __eq__(self, other: object) -> bool:
        """Check if this complex power value is equal to the provided `other`.

        Args:
            other: `ComplexPower` value to compare this one to.

        Returns:
            `True` if the underlying complex numbers are equal, `False` otherwise
        """
        if not isinstance(other, ComplexPower):
            return NotImplemented

        return self._complex_power == other._complex_power
