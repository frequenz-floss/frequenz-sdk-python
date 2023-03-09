# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Module with constants shared between instances of the sdk.

To be replaced by ConfigManager.
"""

RECEIVER_MAX_SIZE = 50
"""Default buffer size of the receiver."""

WAIT_FOR_COMPONENT_DATA_SEC: float = 2
"""Delay the start of the application to wait for the data."""

MAX_BATTERY_DATA_AGE_SEC: float = 2
"""Max time difference for the battery or inverter data to be considered as reliable.

If battery or inverter stopped sending data, then this is the maximum time when its
last message should be considered as valid. After that time, component data
should not be used.
"""
