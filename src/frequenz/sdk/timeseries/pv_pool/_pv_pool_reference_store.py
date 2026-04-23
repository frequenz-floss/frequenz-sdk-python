# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Manages shared state/tasks for a set of PV inverters."""

import uuid
from typing import Type

from frequenz.client.microgrid.component import Component, SolarInverter
from typing_extensions import override

from ..abstract_pool import AbstractPoolReferenceStore
from ._system_bounds_tracker import PVSystemBoundsTracker


class PVPoolReferenceStore(AbstractPoolReferenceStore):
    """A class for maintaining the shared state/tasks for a set of pool of PV inverters.

    This includes ownership of
    - the formula pool and metric calculators.
    - the tasks for calculating system bounds for the PV inverters.

    These are independent of the priority of the actors and can be shared between
    multiple users of the same set of PV inverters.

    They are exposed through the PVPool class.
    """

    @staticmethod
    def get_component_class() -> Type[Component]:
        """Class of the component type."""
        return SolarInverter

    @staticmethod
    def get_pool_type_name() -> str:
        """Name of the pool type, for display purposes."""
        return "PVPool"

    @staticmethod
    def get_component_type_name_plural() -> str:
        """Name of the component type, for display purposes."""
        return "PV inverters"

    @override
    def get_namespace(self) -> str:
        """Namespace to use with the data pipeline."""
        return f"pv-pool-{uuid.uuid4()}"

    @override
    def create_bounds_tracker(self) -> None:
        """Create the bounds tracker for the pool."""
        # In locations without PV inverters, the bounds tracker will not be started.
        if self.component_ids:
            self.bounds_tracker = PVSystemBoundsTracker(
                self.component_ids,
                self.status_receiver,
                self.bounds_channel.new_sender(),
            )
            self.bounds_tracker.start()
