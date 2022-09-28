"""
Config interface.

Copyright
Copyright © 2022 Frequenz Energy-as-a-Service GmbH

License
MIT
"""

from .config import Config
from .config_manager import ConfigManager

# Explicitly declare the public API.
__all__ = [
    "Config",
    "ConfigManager",
]
