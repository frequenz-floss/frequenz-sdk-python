# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""A base class for creating simple composable actors."""

from .channel_registry import ChannelRegistry
from .decorator import actor

__all__ = ["actor", "ChannelRegistry"]
