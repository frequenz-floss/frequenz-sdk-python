# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""
Tests for the microgrid Connection type.
"""

from frequenz.sdk.microgrid import client


# pylint: disable=invalid-name
def test_Connection() -> None:
    """Test the microgrid Connection type."""
    c00 = client.Connection(0, 0)
    assert not c00.is_valid()

    c01 = client.Connection(0, 1)
    assert c01.is_valid()

    c10 = client.Connection(1, 0)
    assert not c10.is_valid()

    c11 = client.Connection(1, 1)
    assert not c11.is_valid()

    c12 = client.Connection(1, 2)
    assert c12.is_valid()

    c21 = client.Connection(2, 1)
    assert c21.is_valid()
