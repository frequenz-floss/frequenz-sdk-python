# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for the frequenz.sdk.data_handling.power module."""

import pytest
from frequenz.api.microgrid.common_pb2 import AC, Metric

from frequenz.sdk.data_handling import power

# pylint: disable=invalid-name,too-many-statements,missing-function-docstring
# pylint: disable=too-many-locals,no-member


def test_ComplexPower_init() -> None:
    S = power.ComplexPower

    z0 = S(0.0)
    assert z0.real == 0.0
    assert z0.imag == 0.0

    assert z0.active == 0.0
    assert z0.reactive == 0.0

    assert z0.consumption == 0.0
    assert z0.supply == 0.0
    assert z0.inductive == 0.0
    assert z0.capacitive == 0.0

    z1a = S(1.0)
    assert z1a.real == 1.0
    assert z1a.imag == 0.0

    assert z1a.active == 1.0
    assert z1a.reactive == 0.0

    assert z1a.consumption == 1.0
    assert z1a.supply == 0.0
    assert z1a.inductive == 0.0
    assert z1a.capacitive == 0.0

    z1b = S(-1.0)
    assert z1b.real == -1.0
    assert z1b.imag == 0.0

    assert z1b.active == -1.0
    assert z1b.reactive == 0.0

    assert z1b.consumption == 0.0
    assert z1b.supply == 1.0
    assert z1b.inductive == 0.0
    assert z1b.capacitive == 0.0

    z1c = S(1.0j)
    assert z1c.real == 0.0
    assert z1c.imag == 1.0

    assert z1c.active == 0.0
    assert z1c.reactive == 1.0

    assert z1c.consumption == 0.0
    assert z1c.supply == 0.0
    assert z1c.inductive == 1.0
    assert z1c.capacitive == 0.0

    z1b = S(-1.0j)
    assert z1b.real == 0.0
    assert z1b.imag == -1.0

    assert z1b.active == 0.0
    assert z1b.reactive == -1.0

    assert z1b.consumption == 0.0
    assert z1b.supply == 0.0
    assert z1b.inductive == 0.0
    assert z1b.capacitive == 1.0

    z2 = S(3.5 + 4.0j)
    assert z2.real == 3.5
    assert z2.imag == 4.0

    assert z2.active == 3.5
    assert z2.reactive == 4.0

    assert z2.consumption == 3.5
    assert z2.supply == 0.0
    assert z2.inductive == 4.0
    assert z2.capacitive == 0.0

    z3 = S(-6.0 + 7.75j)
    assert z3.real == -6.0
    assert z3.imag == 7.75

    assert z3.active == -6.0
    assert z3.reactive == 7.75

    assert z3.consumption == 0.0
    assert z3.supply == 6.0
    assert z3.inductive == 7.75
    assert z3.capacitive == 0.0

    z4 = S(5.75 - 1.25j)
    assert z4.real == 5.75
    assert z4.imag == -1.25

    assert z4.active == 5.75
    assert z4.reactive == -1.25

    assert z4.consumption == 5.75
    assert z4.supply == 0.0
    assert z4.inductive == 0.0
    assert z4.capacitive == 1.25

    z5 = S(-3.0 - 9.5j)
    assert z5.real == -3.0
    assert z5.imag == -9.5

    assert z5.active == -3.0
    assert z5.reactive == -9.5

    assert z5.consumption == 0.0
    assert z5.supply == 3.0
    assert z5.inductive == 0.0
    assert z5.capacitive == 9.5


def test_ComplexPower_from_active_power() -> None:
    S = power.ComplexPower

    assert S.from_active_power(0.0) == S(0.0)
    assert S.from_active_power(1.0) == S(1.0)
    assert S.from_active_power(-1.0) == S(-1.0)


def test_ComplexPower_from_reactive_power() -> None:
    S = power.ComplexPower

    assert S.from_reactive_power(0.0) == S(0.0)
    assert S.from_reactive_power(1.0) == S(1.0j)
    assert S.from_reactive_power(-1.0) == S(-1.0j)


def test_ComplexPower_from_protobuf() -> None:
    def active_power(consumption: float, supply: float) -> Metric:
        return Metric(value=consumption - supply)

    def reactive_power(inductive: float, capacitive: float) -> Metric:
        return Metric(value=inductive - capacitive)

    S = power.ComplexPower

    ac1 = AC(
        power_active=active_power(1.0, 0.0),
        power_reactive=reactive_power(1.0, 0.0),
    )

    assert S.from_protobuf(ac1) == S(1.0 + 1.0j)

    ac2 = AC(
        power_active=active_power(0.0, 2.0),
        power_reactive=reactive_power(1.0, 0.0),
    )

    assert S.from_protobuf(ac2) == S(-2.0 + 1.0j)

    ac3 = AC(
        power_active=active_power(3.0, 0.0),
        power_reactive=reactive_power(0.0, 0.25),
    )

    assert S.from_protobuf(ac3) == S(3.0 - 0.25j)

    ac4 = AC(
        power_active=active_power(0.0, 0.75),
        power_reactive=reactive_power(0.0, 4.0),
    )

    assert S.from_protobuf(ac4) == S(-0.75 - 4.0j)

    # in theory `AC` instances should never have both
    # consumption and supply non-zero, and similarly
    # should not have inductive and capacitive both
    # non-zero, but `from_protobuf` will take the net
    # values if this happens

    ac5 = AC(
        power_active=active_power(5.0, 4.0),
        power_reactive=reactive_power(3.0, 7.0),
    )

    assert S.from_protobuf(ac5) == S(1.0 - 4.0j)

    ac6 = AC(
        power_active=active_power(-4.0, -3.0),
        power_reactive=reactive_power(3.0, -2.0),
    )

    assert S.from_protobuf(ac6) == S(-1.0 + 5.0j)

    ac7 = AC(
        power_active=active_power(4.0, 7.0),
        power_reactive=reactive_power(-9.0, 2.5),
    )

    assert S.from_protobuf(ac7) == S(-3.0 - 11.5j)

    ac8 = AC(
        power_active=active_power(-6.0, -8.75),
        power_reactive=reactive_power(-6.5, 1.5),
    )

    assert S.from_protobuf(ac8) == S(2.75 - 8.0j)

    ac9 = AC(
        power_active=active_power(5.0, -4.0),
        power_reactive=reactive_power(6.0, 5.0),
    )

    assert S.from_protobuf(ac9) == S(9.0 + 1.0j)


# pylint: disable=expression-not-assigned
def test_ComplexPower_ops() -> None:
    S = power.ComplexPower

    # __neg__
    assert -S(1.0) == S(-1.0)
    assert -S(1.0j) == S(-1.0j)
    assert -S(1.0 + 2.0j) == S(-1.0 - 2.0j)
    assert -S(1.5 - 3.0j) == S(-1.5 + 3.0j)
    assert -S(-6.25 + 4.5j) == S(6.25 - 4.5j)
    assert -S(-7.5 - 10.25j) == S(7.5 + 10.25j)

    # __add__
    assert S(0.0) + S(1.0 + 2.0j) == S(1.0 + 2.0j)
    assert S(-3 - 4j) + S(0.0) == S(-3 - 4j)
    assert S(4.5) + S(9.5j) == S(4.5 + 9.5j)
    assert S(-3.5 + 1.25j) + S(2.5 - 0.5j) == S(-1.0 + 0.75j)

    with pytest.raises(TypeError):
        S(0.0) + 7.5

    with pytest.raises(TypeError):
        S(0.0) + complex(6, 5)

    # __sub__
    assert S(1.0 + 2.0j) - S(0.0) == S(1.0 + 2.0j)
    assert S(0.0) - S(3.0 + 4.0j) == S(-3.0 - 4.0j)
    assert S(4.5) - S(1.0 + 3.5j) == S(3.5 - 3.5j)
    assert S(2.5j) - S(1.5 + 1.0j) == S(-1.5 + 1.5j)
    assert S(-3.5 - 6.5j) - S(-4.5 - 8.5j) == S(1.0 + 2.0j)

    with pytest.raises(TypeError):
        S(0.0) - 7.5

    with pytest.raises(TypeError):
        S(0.0) - complex(6, 5)

    # __mul__
    assert S(0.0) * 7 == S(0.0)
    assert 7 * S(0.0) == S(0.0)
    assert S(1.0) * 7 == S(7.0)
    assert 7 * S(1.0) == S(7.0)
    assert S(1.0j) * 4 == S(4.0j)
    assert 4 * S(1.0j) == S(4.0j)
    assert S(2.0 + 3.0j) * 9 == S(18.0 + 27.0j)
    assert 7 * S(0.5 + 1.5j) == S(3.5 + 10.5j)
    assert -3.5 * S(2.0 - 4.0j) == S(-7.0 + 14.0j)

    with pytest.raises(TypeError):
        S(1.0) * S(1.0)

    with pytest.raises(TypeError):
        S(1.0) * complex(3, 2)

    # __truediv__
    assert S(0.0) / 42.0 == S(0.0)
    assert S(0.0) / 1.0 == S(0.0)
    assert S(1.0 + 2.0j) / 1 == S(1.0 + 2.0j)
    assert S(1.0 + 2.0j) / 2 == S(0.5 + 1.0j)
    assert S(1.0 + 2.0j) / -0.5 == S(-2.0 - 4.0j)
    assert S(-6 + 7.0j) / -2.0 == S(3.0 - 3.5j)

    with pytest.raises(ZeroDivisionError):
        S(1.0) / 0.0

    with pytest.raises(TypeError):
        S(1.0) / S(1.0)

    with pytest.raises(TypeError):
        S(1.0) / complex(1, 1)

    # __eq__
    # comparison to numerically equivalent values resolve as
    # `False` when the values are not ComplexPower instances
    assert (S(1.0) == 1.0) is False
    assert (S(1.0 + 2.0j) == 1.0 + 2.0j) is False
    assert (-3.0j == S(-3.0j)) is False
    assert (-6.5 - 3.25j == S(-6.5 - 3.25j)) is False
