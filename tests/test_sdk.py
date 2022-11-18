# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""
Top level tests for the `frequenz.sdk` pacakge
"""

import frequenz.sdk
import frequenz.sdk.configs as config
from frequenz.sdk.configs import ConfigManager


def test_sdk_import() -> None:
    """Checks that `import frequenz.sdk` works"""
    assert frequenz.sdk is not None


def test_sdk_import_config() -> None:
    """Checks that `import frequenz.sdk` works"""
    assert config is not None


def test_sdk_import_config_manager() -> None:
    """Checks that `import frequenz.sdk` works"""
    assert ConfigManager is not None
