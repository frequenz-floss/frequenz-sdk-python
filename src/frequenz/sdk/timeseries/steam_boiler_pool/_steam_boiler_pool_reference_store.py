# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Manages shared state/tasks for a set of steam boilers."""

import uuid
from typing import Type

from frequenz.client.microgrid.component import Component, SteamBoiler
from typing_extensions import override

from ..component_pool._component_pool_reference_store import ComponentPoolReferenceStore
from ._system_bounds_tracker import SteamBoilerSystemBoundsTracker


class SteamBoilerPoolReferenceStore(ComponentPoolReferenceStore):
    """A class for maintaining the shared state/tasks for a set of pools of steam boilers.

    This includes ownership of
    - the formula pool and metric calculators.
    - the tasks for calculating system bounds for the steam boilers.

    These are independent of the priority of the actors and can be shared between
    multiple users of the same set of steam boilers.

    They are exposed through the SteamBoilerPool class.
    """

    @staticmethod
    def get_component_class() -> Type[Component]:
        """Class of the component type."""
        return SteamBoiler

    @staticmethod
    def get_pool_type_name() -> str:
        """Name of the pool type, for display purposes."""
        return "SteamBoilerPool"

    @staticmethod
    def get_component_type_name_plural() -> str:
        """Name of the component type, for display purposes."""
        return "steam boilers"

    @override
    def get_namespace(self) -> str:
        """Namespace to use with the data pipeline."""
        return f"steam-boiler-pool-{uuid.uuid4()}"

    @override
    def create_bounds_tracker(self) -> None:
        """Create the bounds tracker for the pool."""
        # In locations without steam boilers, the bounds tracker will not be started.
        if self.component_ids:
            self.bounds_tracker = SteamBoilerSystemBoundsTracker(
                self.component_ids,
                self.status_receiver,
                self.bounds_channel.new_sender(),
            )
            self.bounds_tracker.start()
