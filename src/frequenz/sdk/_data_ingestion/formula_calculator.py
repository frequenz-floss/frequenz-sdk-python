# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Class for evaluating formulas."""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import chain
from typing import Any, Dict, List, Optional, Set, Tuple

import sympy

from .._data_handling.time_series import (
    BatteryField,
    EVChargerField,
    InverterField,
    LatestEntryCache,
    MeterField,
    SymbolComponentCategory,
    SymbolMapping,
    TimeSeriesEntry,
    TimeSeriesFormula,
)
from ..microgrid import ComponentGraph
from ..microgrid.component import ComponentCategory
from .component_info import infer_microgrid_config
from .constants import (
    METRIC_BATTERIES_ACTIVE_POWER,
    METRIC_BATTERIES_ACTIVE_POWER_BOUNDS,
    METRIC_BATTERIES_CAPACITY,
    METRIC_BATTERIES_REMAINING_ENERGY,
    METRIC_CLIENT_LOAD,
    METRIC_EV_CHARGERS_CONSUMPTION,
    METRIC_GRID_LOAD,
    METRIC_PV_PROD,
)

logger = logging.Logger(__name__)


@dataclass(frozen=True)
class ComponentGroupInfo:
    """Component ids and meter connections for components of a specific type."""

    ids: List[int]
    connections: List[Optional[ComponentCategory]]


class FormulaCalculator:  # pylint: disable=too-many-instance-attributes
    """Class for evaluating formulas based on streamed component data.

    Default formulas are hardcoded in code and it is not possible to change them.
    Default formulas use always all possible components.
    If one component is broken, then 0 will be put in place of missing component data.

    For example, lets say we have 2 batteries: B1, B2
    B1:energy =100 B2:energy=200
    If B2 suddenly stops working then total_energy should be 100.
    The total energy will be back 300 as soon as broken component will start
    sending data.

    The default set of formulas includes:
        `client_load`: site consumption
        `grid_load`: grid consumption
        `pv_prod`: total PV production (if one or more pv arrays exist)
        `ev_chargers_consumption`: total ev charging rate (if one or more ev chargers exist)
        `total_energy`: total energy (in Wh) in all batteries
        `batteries_active_power`: active_power of all battery inverters
        `batteries_active_power_bounds`: active_power_bounds of all battery inverters
        `batteries_capacity`: total capacity of all batteries
    """

    def __init__(
        self,
        component_graph: ComponentGraph,
        additional_formulas: Optional[Dict[str, TimeSeriesFormula[Any]]] = None,
        battery_ids_overrides: Optional[Dict[str, Set[int]]] = None,
        symbol_mappings: Optional[List[SymbolMapping]] = None,
    ) -> None:
        """Initialize FormulaCalculator.

        Args:
            component_graph: component graph of microgrid
            additional_formulas: a dictionary of additional formulas not included
                in the default set, with key specifying names for these formulas
            battery_ids_overrides: mapping of formula names to list of battery
                ids to be taken into account when evaluating the formula
            symbol_mappings: list of additional symbol definitions, necessary if
                the additional user-defined formulas contains extra symbols that
                are not included in the default set

        Raises:
            KeyError: if there are missing symbols or duplicated formula names
        """
        component_infos, battery_inverter_mappings = infer_microgrid_config(
            component_graph
        )
        self.component_infos = component_infos
        self.battery_inverter_mappings = battery_inverter_mappings
        self.battery_ids_overrides = battery_ids_overrides
        self.microgrid_formulas: Dict[str, TimeSeriesFormula[Any]] = (
            {} if additional_formulas is None else additional_formulas
        )
        custom_symbol_mappings: List[SymbolMapping] = symbol_mappings or []
        default_symbol_mappings = self._create_default_symbol_mappings()

        self.symbol_mappings: Dict[str, SymbolMapping] = {
            symbol_mapping.symbol: symbol_mapping
            for symbol_mapping in custom_symbol_mappings + default_symbol_mappings
        }

        self.set_microgrid_formulas()
        self._check_symbol_definitions_exist()
        self.symbol_values: LatestEntryCache[str, Any] = LatestEntryCache()
        self.results: Dict[str, TimeSeriesEntry[Any]] = {}
        # Max permitted time when the component should send any information.
        # After that timeout the component will be treated as not existing.
        # Formulas will put 0 in place of data from this components.
        # This will happen until component starts sending data.
        self.component_data_timeout_sec = 60

    def _create_client_load_formula(
        self,
        meter_group_info: ComponentGroupInfo,
        ev_charger_group_info: ComponentGroupInfo,
        inverter_group_info: ComponentGroupInfo,
    ) -> TimeSeriesFormula[float]:
        """Create formula for client_load.

        Args:
            meter_group_info: ids and meter connections of all meters
            ev_charger_group_info: ids of all ev chargers
            inverter_group_info: ids of all inverters

        Returns:
            Formula for calculating client_load.

        Raises:
            ValueError: when there is neither market nor grid meters
        """
        if None in meter_group_info.connections:
            return TimeSeriesFormula(
                " + ".join(
                    [
                        f"meter_{meter_group_info.ids[i]}_active_power"
                        for i in range(len(meter_group_info.ids))
                        if meter_group_info.connections[i] is None
                    ]
                )
            )

        if ComponentCategory.GRID in meter_group_info.connections:
            return TimeSeriesFormula(
                " + ".join(
                    [
                        f"meter_{meter_group_info.ids[i]}_active_power"
                        for i in range(len(meter_group_info.ids))
                        if meter_group_info.connections[i] == ComponentCategory.GRID
                    ]
                    + [
                        f"meter_{meter_group_info.ids[i]}_active_power"
                        for i in range(len(meter_group_info.ids))
                        if meter_group_info.connections[i] == ComponentCategory.PV_ARRAY
                    ]
                    + [
                        f"meter_{meter_group_info.ids[i]}_active_power"
                        for i in range(len(meter_group_info.ids))
                        if meter_group_info.connections[i] == ComponentCategory.CHP
                    ]
                    + [
                        f"ev_charger_{ev_charger_group_info.ids[i]}_active_power"
                        for i in range(len(ev_charger_group_info.ids))
                    ]
                    + [
                        f"inverter_{inverter_id}_active_power"
                        for inverter_id in inverter_group_info.ids
                    ]
                )
            )

        raise ValueError("Either `market` or `grid` component must be present")

    def _create_grid_load_formula(
        self,
        meter_group_info: ComponentGroupInfo,
        ev_charger_group_info: ComponentGroupInfo,
        inverter_group_info: ComponentGroupInfo,
    ) -> TimeSeriesFormula[float]:
        """Create formula for grid_load.

        Args:
            meter_group_info: ids and meter connections of all meters
            ev_charger_group_info: ids of all ev chargers
            inverter_group_info: ids of all inverters

        Returns:
            Formula for calculating grid_load.

        Raises:
            ValueError: when there is neither market nor grid meters
        """
        if None in meter_group_info.connections:
            return TimeSeriesFormula(
                " + ".join(
                    [
                        f"meter_{meter_group_info.ids[i]}_active_power"
                        for i in range(len(meter_group_info.ids))
                        if meter_group_info.connections[i] is None
                    ]
                    + [
                        f"meter_{meter_group_info.ids[i]}_active_power"
                        for i in range(len(meter_group_info.ids))
                        if meter_group_info.connections[i] == ComponentCategory.PV_ARRAY
                    ]
                    + [
                        f"meter_{meter_group_info.ids[i]}_active_power"
                        for i in range(len(meter_group_info.ids))
                        if meter_group_info.connections[i] == ComponentCategory.CHP
                    ]
                    + [
                        f"ev_charger_{ev_charger_group_info.ids[i]}_active_power"
                        for i in range(len(ev_charger_group_info.ids))
                    ]
                    + [
                        f"inverter_{inverter_id}_active_power"
                        for inverter_id in inverter_group_info.ids
                    ]
                )
            )

        if ComponentCategory.GRID in meter_group_info.connections:
            return TimeSeriesFormula(
                " + ".join(
                    [
                        f"meter_{meter_group_info.ids[i]}_active_power"
                        for i in range(len(meter_group_info.ids))
                        if meter_group_info.connections[i] == ComponentCategory.GRID
                    ]
                )
            )

        raise ValueError("Either `market` or `grid` component must be present")

    def _create_batteries_capacity_formula(
        self, battery_ids: List[int]
    ) -> TimeSeriesFormula[float]:
        """Create formula for total capacity of all batteries.

        Args:
            battery_ids: battery ids

        Returns:
            Formula for calculating total capacity of all batteries.
        """
        if len(battery_ids) > 0:
            return TimeSeriesFormula(
                " + ".join(
                    [f"battery_{battery_id}_capacity" for battery_id in battery_ids]
                )
            )
        return TimeSeriesFormula("0")

    def _create_pv_prod_formula(
        self, meter_group_info: ComponentGroupInfo
    ) -> TimeSeriesFormula[float]:
        """Create formula for pv_prod.

        Computes total active power of all meters that measures pv systems.

        Args:
            meter_group_info: ids and meter connections of all meters

        Returns:
            Formula for calculating pv_prod.
        """
        if ComponentCategory.PV_ARRAY in meter_group_info.connections:
            return TimeSeriesFormula(
                " + ".join(
                    [
                        f"meter_{meter_group_info.ids[i]}_active_power"
                        for i in range(len(meter_group_info.ids))
                        if meter_group_info.connections[i] == ComponentCategory.PV_ARRAY
                    ]
                )
            )
        return TimeSeriesFormula("0")

    def _create_ev_active_power_consumption_formula(
        self, ev_charger_ids: List[int]
    ) -> TimeSeriesFormula[float]:
        """Create formula for ev_active_power_consumption.

        SUM(current_consumption_power[i] for i in all_ev_chargers)
        where current_consumption_power = METRIC_EV_ACTIVE_POWER

        Args:
            ev_charger_ids: ev charger ids

        Returns:
            Formula for calculating ev_active_power_consumption.
        """
        if len(ev_charger_ids) > 0:
            return TimeSeriesFormula(
                " + ".join(
                    [
                        f"ev_charger_{ev_charger_id}_active_power"
                        for ev_charger_id in ev_charger_ids
                    ]
                )
            )
        return TimeSeriesFormula("0")

    def _create_active_power_formula(
        self, inverter_ids: List[int]
    ) -> TimeSeriesFormula[float]:
        """Create formula for active_power.

        SUM(active_power[i] for i in all_inverters)
        where active_power = METRIC_ACTIVE_POWER

        Args:
            inverter_ids: inverter ids

        Returns:
            Formula for calculating active_power.
        """
        return TimeSeriesFormula(
            " + ".join(
                [f"inverter_{inverter_id}_active_power" for inverter_id in inverter_ids]
            )
        )

    def _create_batteries_active_power_bounds_formula(
        self, battery_ids: List[int], battery_inverter_mappings: Dict[int, int]
    ) -> TimeSeriesFormula[Tuple[float, float]]:
        """Create formula for batteries_active_power_bounds.

        batteries_active_power_bounds is total upper and lower bound for active power
        read from battery-inverter pairs. Battery-inverter pair is pair of battery and
        adjacent inverter.

        It returns a tuple with upper and lower bound, where:
        lower -
            sum of lower bound for each battery-inverter pair.
            Lower bound of battery-inverter pair is
            max(battery.active_power_lower_bound, inverter.active_power_lower_bound)
        upper -
            sum of upper bound for each battery-inverter pair.
            Upper bound of battery-inverter pair is
            min(battery.power_upper_bound, inverter.active_power_upper_bound),

        Args:
            battery_ids: battery ids
            battery_inverter_mappings: mappings between batteries and battery inverters

        Returns:
            Formula for calculating batteries_active_power_bounds.
        """
        lower_bound_formula = " + ".join(
            [
                f"max(battery_{bid}_power_lower_bound, "
                f"inverter_{battery_inverter_mappings[bid]}_active_power_lower_bound)"
                for bid in battery_ids
            ]
        )

        upper_bound_formula = " + ".join(
            [
                f"min(battery_{bid}_power_upper_bound, "
                f"inverter_{battery_inverter_mappings[bid]}_active_power_upper_bound)"
                for bid in battery_ids
            ]
        )
        str_formula = sympy.Tuple(lower_bound_formula, upper_bound_formula)  # type: ignore
        expression = sympy.sympify(str_formula)  # type: ignore
        return TimeSeriesFormula(expression)

    def _create_total_energy_formula(
        self, battery_ids: List[int]
    ) -> TimeSeriesFormula[float]:
        """Create formula for total_energy.

        SUM(Soc[i]*capacity[i] for in in all_batteries)

        Args:
            battery_ids: battery ids

        Returns:
            Formula for calculating total_energy.
        """
        return TimeSeriesFormula(
            " + ".join(
                [f"battery_{bid}_soc * battery_{bid}_capacity" for bid in battery_ids]
            )
        )

    def filter_selected_batteries(
        self,
        battery_ids: List[int],
        formula_name: str,
    ) -> List[int]:
        """Filter selected batteries from all batteries in the component graph.

        Batteries to be used for an individual formula can be overridden via config
            file in the UI.

        Args:
            battery_ids: list of ids of all the batteries in the component graph
            formula_name: name of the formula to apply the override for

        Returns:
            list of battery ids selected by the user in the UI for a given formula
        """
        if (
            self.battery_ids_overrides is None
            or formula_name not in self.battery_ids_overrides
        ):
            return battery_ids

        selected_battery_ids = self.battery_ids_overrides[formula_name]
        return list(set(battery_ids) & selected_battery_ids)

    def set_microgrid_formulas(
        self,
    ) -> None:
        """Set formulas associated with the components specified for the microgrid.

        Raises:
            ValueError: when there is neither market nor grid meters
        """
        components_by_type = self._group_components()

        client_load_formula = self._create_client_load_formula(
            meter_group_info=components_by_type[ComponentCategory.METER],
            ev_charger_group_info=components_by_type[ComponentCategory.EV_CHARGER],
            inverter_group_info=components_by_type[ComponentCategory.INVERTER],
        )
        self._add_formula(METRIC_CLIENT_LOAD, client_load_formula)

        grid_load_formula = self._create_grid_load_formula(
            meter_group_info=components_by_type[ComponentCategory.METER],
            ev_charger_group_info=components_by_type[ComponentCategory.EV_CHARGER],
            inverter_group_info=components_by_type[ComponentCategory.INVERTER],
        )
        self._add_formula(METRIC_GRID_LOAD, grid_load_formula)

        pv_prod_formula = self._create_pv_prod_formula(
            meter_group_info=components_by_type[ComponentCategory.METER]
        )
        self._add_formula(METRIC_PV_PROD, pv_prod_formula)

        ev_active_power_consumption_formula = (
            self._create_ev_active_power_consumption_formula(
                ev_charger_ids=components_by_type[ComponentCategory.EV_CHARGER].ids
            )
        )
        self._add_formula(
            METRIC_EV_CHARGERS_CONSUMPTION, ev_active_power_consumption_formula
        )

        active_power_formula = self._create_active_power_formula(
            inverter_ids=components_by_type[ComponentCategory.INVERTER].ids
        )
        self._add_formula(METRIC_BATTERIES_ACTIVE_POWER, active_power_formula)

        batteries_active_power_bounds_formula = (
            self._create_batteries_active_power_bounds_formula(
                battery_ids=self.filter_selected_batteries(
                    battery_ids=components_by_type[ComponentCategory.BATTERY].ids,
                    formula_name=METRIC_BATTERIES_ACTIVE_POWER_BOUNDS,
                ),
                battery_inverter_mappings=self.battery_inverter_mappings,
            )
        )
        self._add_formula(
            METRIC_BATTERIES_ACTIVE_POWER_BOUNDS, batteries_active_power_bounds_formula
        )

        total_energy_formula = self._create_total_energy_formula(
            battery_ids=self.filter_selected_batteries(
                battery_ids=components_by_type[ComponentCategory.BATTERY].ids,
                formula_name=METRIC_BATTERIES_REMAINING_ENERGY,
            ),
        )
        self._add_formula(METRIC_BATTERIES_REMAINING_ENERGY, total_energy_formula)

        batteries_capacity_formula = self._create_batteries_capacity_formula(
            battery_ids=self.filter_selected_batteries(
                battery_ids=components_by_type[ComponentCategory.BATTERY].ids,
                formula_name=METRIC_BATTERIES_CAPACITY,
            ),
        )
        self._add_formula(METRIC_BATTERIES_CAPACITY, batteries_capacity_formula)

    def _make_group(
        self, component_category: ComponentCategory, attr: str
    ) -> List[Any]:
        """Filter components by type and return a single attribute for all of them.

        Args:
            component_category: components of which type to include in the
                result.
            attr: attribute to return for all components of type
                `component_category`.

        Returns:
            Single attribute of all components of type `component_category`.
        """
        return [
            getattr(info, attr)
            for info in self.component_infos
            if info.category == component_category
        ]

    def _group_components(self) -> Dict[ComponentCategory, ComponentGroupInfo]:
        """Group components by component category.

        Returns:
            Components grouped by type, where groups hold information about
                component IDs and meter connections.
        """
        meter_connections = self._make_group(
            ComponentCategory.METER, "meter_connection"
        )

        meter_ids = self._make_group(ComponentCategory.METER, "component_id")
        battery_ids = self._make_group(ComponentCategory.BATTERY, "component_id")
        inverter_ids = self._make_group(ComponentCategory.INVERTER, "component_id")
        ev_charger_ids = self._make_group(ComponentCategory.EV_CHARGER, "component_id")

        components_by_type = {
            ComponentCategory.METER: ComponentGroupInfo(
                ids=meter_ids, connections=meter_connections
            ),
            ComponentCategory.INVERTER: ComponentGroupInfo(
                ids=inverter_ids, connections=[]
            ),
            ComponentCategory.BATTERY: ComponentGroupInfo(
                ids=battery_ids, connections=[]
            ),
            ComponentCategory.EV_CHARGER: ComponentGroupInfo(
                ids=ev_charger_ids, connections=[]
            ),
        }
        return components_by_type

    def _create_default_inverter_symbol_mappings(
        self, inverter_ids: List[int]
    ) -> List[SymbolMapping]:
        """Create default symbol mappings for inverters.

        Args:
            inverter_ids: list of ids of inverters

        Returns:
            list of default symbol mappings for inverters
        """
        symbol_mappings: List[SymbolMapping] = []
        for iid in inverter_ids:
            symbol_mappings.append(
                SymbolMapping(
                    SymbolComponentCategory.INVERTER, iid, InverterField.ACTIVE_POWER
                )
            )
            symbol_mappings.append(
                SymbolMapping(
                    SymbolComponentCategory.INVERTER,
                    iid,
                    InverterField.ACTIVE_POWER_UPPER_BOUND,
                )
            )
            symbol_mappings.append(
                SymbolMapping(
                    SymbolComponentCategory.INVERTER,
                    iid,
                    InverterField.ACTIVE_POWER_LOWER_BOUND,
                )
            )
        return symbol_mappings

    def _create_default_battery_symbol_mappings(
        self, battery_ids: List[int]
    ) -> List[SymbolMapping]:
        """Create default symbol mappings for batteries.

        Args:
            battery_ids: list of ids of batteries

        Returns:
            list of default symbol mappings for batteries
        """
        symbol_mappings: List[SymbolMapping] = []
        for bid in battery_ids:
            symbol_mappings.append(
                SymbolMapping(SymbolComponentCategory.BATTERY, bid, BatteryField.SOC)
            )
            symbol_mappings.append(
                SymbolMapping(
                    SymbolComponentCategory.BATTERY,
                    bid,
                    BatteryField.POWER_UPPER_BOUND,
                )
            )
            symbol_mappings.append(
                SymbolMapping(
                    SymbolComponentCategory.BATTERY,
                    bid,
                    BatteryField.POWER_LOWER_BOUND,
                )
            )
            symbol_mappings.append(
                SymbolMapping(
                    SymbolComponentCategory.BATTERY, bid, BatteryField.CAPACITY
                )
            )
        return symbol_mappings

    def _create_default_ev_charger_symbol_mappings(
        self, ev_charger_ids: List[int]
    ) -> List[SymbolMapping]:
        """Create default symbol mappings for EV chargers.

        Args:
            ev_charger_ids: list of ids of EV chargers

        Returns:
            list of default symbol mappings for EV chargers
        """
        symbol_mappings: List[SymbolMapping] = []
        for evid in ev_charger_ids:
            symbol_mappings.append(
                SymbolMapping(
                    SymbolComponentCategory.EV_CHARGER,
                    evid,
                    EVChargerField.ACTIVE_POWER,
                )
            )
        return symbol_mappings

    def _create_default_meter_symbol_mappings(
        self, meter_ids: List[int]
    ) -> List[SymbolMapping]:
        """Create default symbol mappings for meters.

        Args:
            meter_ids: list of ids of meters

        Returns:
            list of default symbol mappings for meters
        """
        symbol_mappings: List[SymbolMapping] = []
        for mid in meter_ids:
            symbol_mappings.append(
                SymbolMapping(
                    SymbolComponentCategory.METER, mid, MeterField.ACTIVE_POWER
                )
            )
        return symbol_mappings

    def _create_default_symbol_mappings(self) -> List[SymbolMapping]:
        """Create default symbol mappings for all component categories.

        Returns:
            list of default symbol mappings for all component categories
        """
        component_groups = self._group_components()
        symbol_mappings: List[SymbolMapping] = []

        meter_symbol_mappings = self._create_default_meter_symbol_mappings(
            meter_ids=component_groups[ComponentCategory.METER].ids,
        )
        symbol_mappings.extend(meter_symbol_mappings)

        inverter_symbol_mappings = self._create_default_inverter_symbol_mappings(
            inverter_ids=component_groups[ComponentCategory.INVERTER].ids
        )
        symbol_mappings.extend(inverter_symbol_mappings)

        ev_charger_symbol_mappings = self._create_default_ev_charger_symbol_mappings(
            ev_charger_ids=component_groups[ComponentCategory.EV_CHARGER].ids
        )
        symbol_mappings.extend(ev_charger_symbol_mappings)

        battery_symbol_mappings = self._create_default_battery_symbol_mappings(
            battery_ids=component_groups[ComponentCategory.BATTERY].ids
        )
        symbol_mappings.extend(battery_symbol_mappings)
        return symbol_mappings

    def _add_formula(self, formula_name: str, formula: TimeSeriesFormula[Any]) -> None:
        """Add a default formula.

        If the formula is not already supplied by user upon initialization of the actor
        and that an output channel is provided.

        Args:
            formula_name: name of formula
            formula: definition of the time series formula

        Raises:
            KeyError: when formula is already defined
        """
        if formula_name in self.microgrid_formulas:
            raise KeyError(f"Formula {formula_name} is already defined!")

        self.microgrid_formulas[formula_name] = formula

    def _check_symbol_definitions_exist(
        self,
    ) -> None:
        """Check that all the symbols used in `microgrid_formulas` are defined.

        Raises:
            KeyError: when used symbols do not exist
        """
        required_symbols = [
            self.microgrid_formulas[formula_name].symbols
            for formula_name in self.microgrid_formulas
        ]
        unique_required_symbols = set(chain(*required_symbols))
        defined_symbols = list(self.symbol_mappings.keys())
        missing_symbols = []
        for symbol in unique_required_symbols:
            if symbol not in defined_symbols:
                missing_symbols.append(symbol)
        if len(missing_symbols) > 0:
            raise KeyError(
                f"Symbol(s) {missing_symbols} used in `microgrid_formulas` not defined."
            )

    def update_symbol_values(self, component_data: Dict[str, Any]) -> Set[str]:
        """Update all the symbol values with latest streamed component data.

        In order to have the formulas computed, the user has to provide
        symbol mappings, which map symbols to attributes from protobuf messages.

        When a single component sends data, the values of the corresponding symbols
        will be updated, e.g. when an inverter with id=8 sends the following data:

        {
            'active_power': 255.0,
            'active_power_lower_bound': -999.0,
            'active_power_upper_bound': 1000.0,
            'id': 8,
            'timestamp': datetime.datetime(2022, 9, 1, 16, 36, 42, 575424, tzinfo=<UTC>)
        }

        values of the declared symbols related to that component id will be
        updated as follows:

        TimeSeriesEntry(
            'inverter_8_active_power',
            datetime.datetime(2022, 9, 1, 16, 36, 42, 575424, tzinfo=<UTC>),
            255.0
        )
        TimeSeriesEntry(
            'inverter_8_active_power_lower_bound',
            datetime.datetime(2022, 9, 1, 16, 36, 42, 575424, tzinfo=<UTC>),
            -999.0
        )
        TimeSeriesEntry(
            'inverter_8_active_power_upper_bound',
            datetime.datetime(2022, 9, 1, 16, 36, 42, 575424, tzinfo=<UTC>),
            1000.0
        )

        If a symbol for a given component hasn't been declared, its value will not be
        updated, even if that component sends some data.

        Args:
            component_data: latest streamed component data from a single device

        Returns:
            The set of updated symbols.
        """
        updated_symbol_mappings = [
            symbol_mapping
            for symbol_mapping in self.symbol_mappings.values()
            if symbol_mapping.component_id == component_data["id"]
        ]
        for symbol_mapping in updated_symbol_mappings:
            self.symbol_values.update(
                key=symbol_mapping.symbol,
                entry=TimeSeriesEntry(
                    timestamp=component_data["timestamp"],
                    value=component_data[symbol_mapping.field.value],
                ),
            )
        return set(symbol_mapping.symbol for symbol_mapping in updated_symbol_mappings)

    def compute(
        self, symbols: Set[str], only_formula_names: Optional[Set[str]] = None
    ) -> List[str]:
        """Compute all the formulas that depend on the provided symbols.

        Whenever Microgrid devices send data, it is then mapped to symbols that are
        used by the formulas. The provided `symbols` will cause the formulas relying
        on them to be computed.

        If a formula doesn't depend on the symbols that were recently updated, it will
        not be computed and if the provided `symbols` aren't used for any formula, no
        computation will be done.

        Some formulas might rely on multiple symbols, out of which not all might be
        available at a given point of time. In this case, the values for missing
        symbols will be replaced with a default value (0.0), which is passed to
        the `evaluate` method as the `default_entry` parameter.

        Args:
            symbols: the set of recently updated symbols from a single device
            only_formula_names: optional set of formula names that need to be
                evaluated. If no names are provided, all formulas relying on
                the provided `symbols` will be evaluated.

        Returns:
            Names of the computed formulas.
        """
        formulas_to_compute: Set[str] = {
            formula_name
            for formula_name in self.microgrid_formulas
            if len(symbols & set(self.microgrid_formulas[formula_name].symbols)) > 0
        }
        if only_formula_names is not None:
            formulas_to_compute = formulas_to_compute.intersection(only_formula_names)

        computed_formula_names = []
        for formula_name in formulas_to_compute:
            res = self.microgrid_formulas[formula_name].evaluate(
                cache=self.symbol_values,
                formula_name=formula_name,
                symbol_to_symbol_mapping=self.symbol_mappings,
                timedelta_tolerance=timedelta(seconds=self.component_data_timeout_sec),
                default_entry=TimeSeriesEntry[Any](
                    timestamp=datetime.now(timezone.utc), value=0.0
                ),
            )
            if res is not None:
                self.results[formula_name] = res
                computed_formula_names.append(formula_name)
        return computed_formula_names
