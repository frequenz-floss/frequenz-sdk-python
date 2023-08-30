# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Generate graphs from component data structures."""

from dataclasses import replace
from typing import Any, overload

from frequenz.sdk.microgrid._graph import _MicrogridComponentGraph
from frequenz.sdk.microgrid.client import Connection
from frequenz.sdk.microgrid.component import Component, ComponentCategory, InverterType
from frequenz.sdk.microgrid.component._component import ComponentType


class GraphGenerator:
    """Utilities to generate graphs from component data structures."""

    SUFFIXES = {
        ComponentCategory.CHP: 5,
        ComponentCategory.EV_CHARGER: 6,
        ComponentCategory.METER: 7,
        ComponentCategory.INVERTER: 8,
        ComponentCategory.BATTERY: 9,
    }

    def __init__(self) -> None:
        """Create a new instance."""
        self._id_increment = 0

    def new_id(self) -> dict[ComponentCategory, int]:
        """Get the next available component id.

        Usage example:

        ```python

        gen = GraphGenerator()

        base_id = gen.id()

        # You can use the same base_id for multiple categories.
        # It will be the same id with the according suffix.
        meter_id = base_id[ComponentCategory.METER]
        bat_id   = base_id[ComponentCategory.BATTERY]
        # ...

        # If you just want to get the next available id for a category,
        # you can use:
        meter2_id = gen.id()[ComponentCategory.METER]
        ```

        Returns:
            a dict containing the next available id for each component category.
        """
        id_per_category = {
            category: self._id_increment * 10 + suffix
            for category, suffix in self.SUFFIXES.items()
        }
        self._id_increment += 1
        return id_per_category

    @overload
    def component(self, other: Component) -> Component:
        """Just return the given component.

        Args:
            other: the component to return.

        Returns:
            the given component.
        """

    @overload
    def component(
        self, other: ComponentCategory, comp_type: ComponentType | None = None
    ) -> Component:
        """Create a new component with the next available id for the given category.

        Args:
            other: the component category to get the id for.
            comp_type: the component type to set.

        Returns:
            the next available component id for the given category.
        """

    def component(
        self,
        other: ComponentCategory | Component,
        comp_type: ComponentType | None = None,
    ) -> Component:
        """Make or return a new component.

        Args:
            other: a component or the component category to get the id for.
            comp_type: the component type to set.

        Returns:
            the next available component id for the given category.
        """
        if isinstance(other, Component):
            return other

        assert isinstance(other, ComponentCategory)
        category = other

        return Component(self.new_id()[category], category, comp_type)

    @staticmethod
    def grid() -> Component:
        """Get a new grid component with default id.

        Returns:
            a new grid component with default id.
        """
        return Component(1, ComponentCategory.GRID)

    def to_graph(self, components: Any) -> _MicrogridComponentGraph:
        """Convert a list of components to a graph.

        GRID will be added and connected as the first component.

        Input can be a graph made of tuples, lists, components or component
        categories. Inverter Types will be set automatically based on the
        successors category.

        Definition of possible inputs and input types:

        - ComponentLike: A component object or a component category.
        - AnyTuple: A tuple of any tuple type explained here.
        - AnyList: A list of AnyTuple or ComponentLike, or a mix of both.
        - tuple[ComponentLike, ComponentLike]: A tuple of two components. The first
            member is connected to the second member.
        - tuple[ComponentLike, AnyTuple]: A tuple of a ComponentLike and an AnyTuple.
            The first member is connected to the first member of the tuple.
        - tuple[ComponentLike, AnyList]: A tuple of a ComponentLike and an AnyList.
            The first member is connected to all members of the list.

        ```python
        gen = GraphGenerator()

        # Pre-create a battery component to refer to it later or to use it in multiple
        # places in the graph.
        special_bat = gen.component(ComponentCategory.BATTERY)

        graph = gen.to_graph(
            (
                ComponentCategory.METER, # grid side meter
                [ # list of components connected to grid side meter
                    (
                        ComponentCategory.METER, # Meter in front of battery->inverter
                        ( # A tuple, first is connected to parent, second is the child of the first
                            # Inverter in front of battery, type will be
                            # set to InverterType.BATTERY
                            ComponentCategory.INVERTER,
                            ComponentCategory.BATTERY,  # Battery
                        ),
                    ),
                    (
                        ComponentCategory.METER,
                        (
                            # Inverter in front of battery, type will be
                            # set to InverterType.BATTERY
                            ComponentCategory.INVERTER,
                            special_bat,  # Pre-created battery
                        ),
                    ),
                ],
            )
        )
        ```

        Args:
            components: the components to convert to a graph.

        Returns:
            a tuple containing the components and connections of the graph.
        """
        graph = self._to_graph(self.grid(), components)
        return _MicrogridComponentGraph(set(graph[0]), set(graph[1]))

    def _to_graph(
        self, parent: Component, children: Any
    ) -> tuple[list[Component], list[Connection]]:
        """Convert a list of components to a graph.

        Args:
            parent: the parent component.
            children: the children components.

        Returns:
            a tuple containing the components and connections of the graph.
        """

        def inverter_type(category: ComponentCategory) -> InverterType | None:
            if category == ComponentCategory.BATTERY:
                return InverterType.BATTERY
            return None

        def update_inverter_type(successor: Component) -> None:
            nonlocal parent
            if parent.category == ComponentCategory.INVERTER:
                if comp_type := inverter_type(successor.category):
                    parent = replace(parent, type=comp_type)

        if isinstance(children, (Component, ComponentCategory)):
            rhs = self.component(children)
            update_inverter_type(rhs)
            return [parent, rhs], [Connection(parent.component_id, rhs.component_id)]
        if isinstance(children, tuple):
            assert len(children) == 2
            comp, con = self._to_graph(self.component(children[0]), children[1])
            update_inverter_type(comp[0])
            return [parent] + comp, con + [
                Connection(parent.component_id, comp[0].component_id)
            ]
        if isinstance(children, list):
            comp = []
            con = []
            for _component in children:
                sub_components: list[Component]
                sub_con: list[Connection]

                if isinstance(_component, tuple):
                    sub_parent = self.component(_component[0])
                    sub_children = _component[1]

                    sub_components, sub_con = self._to_graph(sub_parent, sub_children)
                else:
                    sub_components = [self.component(_component)]
                    sub_con = []

                update_inverter_type(sub_components[0])
                comp += sub_components
                con += sub_con + [
                    Connection(parent.component_id, sub_components[0].component_id)
                ]
            return [parent] + comp, con

        raise ValueError("Invalid component list")


def test_graph_generator_simple() -> None:
    """Test a simple graph."""
    gen = GraphGenerator()
    graph = gen.to_graph(
        (
            ComponentCategory.METER,  # grid side meter
            [  # list of components connected to grid side meter
                (
                    ComponentCategory.METER,  # Meter in front of battery
                    (
                        # Inverter in front of battery, type will be
                        # set to InverterType.BATTERY
                        ComponentCategory.INVERTER,
                        ComponentCategory.BATTERY,  # Battery
                    ),
                ),
                (
                    ComponentCategory.METER,
                    # Inverter, type is explicitly set to InverterType.SOLAR
                    gen.component(ComponentCategory.INVERTER, InverterType.SOLAR),
                ),
                (ComponentCategory.METER, ComponentCategory.EV_CHARGER),
                (ComponentCategory.INVERTER, ComponentCategory.BATTERY),
            ],
        )
    )

    meters = list(graph.components(component_category={ComponentCategory.METER}))
    meters.sort(key=lambda x: x.component_id)
    assert len(meters) == 4
    assert len(graph.successors(meters[0].component_id)) == 4
    assert graph.predecessors(meters[1].component_id) == {meters[0]}
    assert graph.predecessors(meters[2].component_id) == {meters[0]}
    assert graph.predecessors(meters[3].component_id) == {meters[0]}

    inverters = list(graph.components(component_category={ComponentCategory.INVERTER}))
    inverters.sort(key=lambda x: x.component_id)
    assert len(inverters) == 3

    assert len(graph.successors(inverters[0].component_id)) == 0
    assert inverters[0].type == InverterType.SOLAR

    assert len(graph.successors(inverters[1].component_id)) == 1
    assert inverters[1].type == InverterType.BATTERY

    assert len(graph.successors(inverters[2].component_id)) == 1
    assert inverters[2].type == InverterType.BATTERY

    assert len(graph.components(component_category={ComponentCategory.BATTERY})) == 2
    assert len(graph.components(component_category={ComponentCategory.EV_CHARGER})) == 1

    graph.validate()


def test_graph_generator_no_grid_meter() -> None:
    """Test a graph without a grid side meter and a list of components at the top."""
    gen = GraphGenerator()
    graph = gen.to_graph(
        [
            (
                ComponentCategory.INVERTER,
                ComponentCategory.BATTERY,
            ),
            (
                ComponentCategory.METER,
                (
                    ComponentCategory.INVERTER,
                    ComponentCategory.BATTERY,
                ),
            ),
        ]
    )

    meters = list(graph.components(component_category={ComponentCategory.METER}))
    assert len(meters) == 1
    assert len(graph.successors(meters[0].component_id)) == 1

    inverters = list(graph.components(component_category={ComponentCategory.INVERTER}))
    assert len(inverters) == 2

    assert len(graph.successors(inverters[0].component_id)) == 1
    assert len(graph.successors(inverters[1].component_id)) == 1

    assert len(graph.components(component_category={ComponentCategory.BATTERY})) == 2

    graph.validate()
