# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Component graph representation for a microgrid."""

from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.component import (
    BatteryInverter,
    Chp,
    Component,
    ComponentConnection,
    EvCharger,
    SolarInverter,
)
from frequenz.microgrid_component_graph import ComponentGraph as BaseComponentGraph
from typing_extensions import override


class ComponentGraph(BaseComponentGraph[Component, ComponentConnection, ComponentId]):
    """A representation of a microgrid's component graph."""

    def is_pv_inverter(self, component: Component) -> bool:
        """Check if the specified component is a PV inverter.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is a PV inverter.
        """
        return isinstance(component, SolarInverter)

    def is_pv_chain(self, component: Component) -> bool:
        """Check if the specified component is part of a PV chain.

        A component is part of a PV chain if it is either a PV inverter or a PV
        meter.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is part of a PV chain.
        """
        return self.is_pv_inverter(component) or self.is_pv_meter(component)

    @override
    def is_pv_meter(self, component: Component | ComponentId) -> bool:
        """Check if the specified component is a PV meter.

        Args:
            component: The component or component ID to check.

        Returns:
            Whether the specified component is a PV meter.
        """
        if isinstance(component, Component):
            return super().is_pv_meter(component.id)
        return super().is_pv_meter(component)

    def is_ev_charger(self, component: Component) -> bool:
        """Check if the specified component is an EV charger.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is an EV charger.
        """
        return isinstance(component, EvCharger)

    def is_ev_charger_chain(self, component: Component) -> bool:
        """Check if the specified component is part of an EV charger chain.

        A component is part of an EV charger chain if it is either an EV charger or an
        EV charger meter.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is part of an EV charger chain.
        """
        return self.is_ev_charger(component) or self.is_ev_charger_meter(component)

    @override
    def is_ev_charger_meter(self, component: Component | ComponentId) -> bool:
        """Check if the specified component is an EV charger meter.

        Args:
            component: The component or component ID to check.

        Returns:
            Whether the specified component is an EV charger meter.
        """
        if isinstance(component, Component):
            return super().is_ev_charger_meter(component.id)
        return super().is_ev_charger_meter(component)

    def is_battery_inverter(self, component: Component) -> bool:
        """Check if the specified component is a battery inverter.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is a battery inverter.
        """
        return isinstance(component, BatteryInverter)

    def is_battery_chain(self, component: Component) -> bool:
        """Check if the specified component is part of a battery chain.

        A component is part of a battery chain if it is either a battery inverter or a
        battery meter.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is part of a battery chain.
        """
        return self.is_battery_inverter(component) or self.is_battery_meter(component)

    @override
    def is_battery_meter(self, component: Component | ComponentId) -> bool:
        """Check if the specified component is a battery meter.

        Args:
            component: The component or component ID to check.

        Returns:
            Whether the specified component is a battery meter.
        """
        if isinstance(component, Component):
            return super().is_battery_meter(component.id)
        return super().is_battery_meter(component)

    def is_chp(self, component: Component) -> bool:
        """Check if the specified component is a CHP.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is a CHP.
        """
        return isinstance(component, Chp)

    def is_chp_chain(self, component: Component) -> bool:
        """Check if the specified component is part of a CHP chain.

        A component is part of a CHP chain if it is either a CHP or a CHP meter.

        Args:
            component: The component to check.

        Returns:
            Whether the specified component is part of a CHP chain.
        """
        return self.is_chp(component) or self.is_chp_meter(component)

    @override
    def is_chp_meter(self, component: Component | ComponentId) -> bool:
        """Check if the specified component is a CHP meter.

        Args:
            component: The component or component ID to check.

        Returns:
            Whether the specified component is a CHP meter.
        """
        if isinstance(component, Component):
            return super().is_chp_meter(component.id)
        return super().is_chp_meter(component)
