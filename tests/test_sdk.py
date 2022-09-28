"""
Top level tests for the `frequenz.sdk` pacakge

Copyright
Copyright © 2021 Frequenz Energy-as-a-Service GmbH

License
MIT
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
