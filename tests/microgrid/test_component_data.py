# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""
Tests for the microgrid component data.
"""

from datetime import datetime, timezone

import pytest

from frequenz.sdk.microgrid.component import ComponentData


def test_component_data_abstract_class() -> None:
    """Verify the base class ComponentData may not be instantiated."""
    with pytest.raises(TypeError):
        # pylint: disable=abstract-class-instantiated
        ComponentData(0, datetime.now(timezone.utc))  # type: ignore
