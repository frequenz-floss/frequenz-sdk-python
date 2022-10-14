"""
Tests for the microgrid component data.

Copyright
Copyright © 2022 Frequenz Energy-as-a-Service GmbH

License
MIT
"""
import datetime

import pytest

from frequenz.sdk.microgrid.component_data import ComponentData


def test_component_data_abstract_class() -> None:
    """Verify the base class ComponentData may not be instantiated."""
    with pytest.raises(TypeError):
        # pylint: disable=abstract-class-instantiated
        ComponentData(0, datetime.datetime.utcnow())  # type: ignore
