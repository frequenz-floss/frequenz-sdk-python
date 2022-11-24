# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""
Actor for combining stream data from different components using TimeSeriesFormula.

Including default standard formulas for client load, grid load, total pv production,
ev charging rate, battery SoC, active power, max consume and supply rate.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from frequenz.channels import Broadcast, Receiver, Sender
from frequenz.channels.util import Merge, Select

from .._data_handling.time_series import TimeSeriesEntry
from ..actor import actor
from ..config import Config
from ..microgrid import ComponentGraph
from ..microgrid.client import MicrogridApiClient
from .component_info import infer_microgrid_config
from .formula_calculator import FormulaCalculator
from .gen_component_receivers import gen_component_receivers

logger = logging.Logger(__name__)

CONFIG_FILE_FORMULA_PREFIX = "formula_"


@actor
class MicrogridData:  # pylint: disable=too-many-instance-attributes
    """Actor for receiving component data, calculating formulas and sending results."""

    # current number of arguments already minimum for purpose of MicrogridData
    # (client, component graph, output channels, additional formula and symbols)
    def __init__(  # pylint: disable=too-many-arguments
        self,
        microgrid_client: MicrogridApiClient,
        component_graph: ComponentGraph,
        outputs: Dict[str, Sender[TimeSeriesEntry[Any]]],
        formula_calculator: FormulaCalculator,
        config_update_receiver: Optional[Receiver[Config]] = None,
        wait_for_data_sec: float = 2,
        formula_update_interval_sec: float = 0.5,
    ) -> None:
        """Initialise MicrogridData actor.

        Args:
            microgrid_client: microgrid client
            component_graph: component graph of microgrid
            outputs: senders for output to channel
            formula_calculator: an instance of FormulaCalculator responsible for
                managing symbols and evaluating formulas
            config_update_receiver: receiver for receiving config file updates
            wait_for_data_sec: how long actor should wait before processing first
                request. It is a time needed to collect first components data.
            formula_update_interval_sec: how frequently send formula results. With this
                frequency all results from formulas will be send.

        Raises:
            ValueError: if a sender corresponding to any of the microgrid_formulas
                is not provided
        """
        component_infos, battery_inverter_mappings = infer_microgrid_config(
            component_graph
        )
        self.component_graph = component_graph
        self.component_infos = component_infos
        self.battery_inverter_mappings = battery_inverter_mappings
        self.microgrid_client = microgrid_client
        self._outputs = outputs
        # Fallback to an empty receiver so that `Select` doesn't raise `KeyError`s
        if config_update_receiver is not None:
            self._config_update_receiver = config_update_receiver
        else:
            self._config_update_receiver = Broadcast[Config](
                "microgrid_data_config_update_channel"
            ).new_receiver()

        self._formula_update_interval_sec: float = formula_update_interval_sec
        self._wait_for_data_sec = wait_for_data_sec
        self.formula_calculator = formula_calculator
        self.microgrid_formula_overrides: Dict[str, Set[int]] = {}

        for output_channel_name in outputs.keys():
            if output_channel_name not in self.formula_calculator.microgrid_formulas:
                raise ValueError(
                    f"No formula defined for output channel {output_channel_name}."
                )

    async def resend_formulas(self) -> None:
        """Send formulas results with some period."""
        # Sleep first to collect initial data from all component
        await asyncio.sleep(self._wait_for_data_sec)
        tasks: List[asyncio.Task[bool]] = []
        while True:
            start_time = datetime.now(timezone.utc)
            # For every formula that was updated at least once, send that formula.
            for name, formula_result in self.formula_calculator.results.items():
                if name not in self._outputs:
                    # If formula was computed but there is not output channel in
                    # MicrogridData then it is bug in MicrogridData.
                    logger.error(
                        "MicrogridData: No channel for formula %s.",
                        name,
                    )
                else:
                    task = asyncio.create_task(self._outputs[name].send(formula_result))
                    tasks.append(task)

            await asyncio.gather(*tasks, return_exceptions=True)
            diff: float = (datetime.now(timezone.utc) - start_time).total_seconds()
            if diff >= self._formula_update_interval_sec:
                logger.error(
                    "Sending results of formulas took too long: %f, "
                    "expected sending interval: %f",
                    diff,
                    self._formula_update_interval_sec,
                )
            else:
                await asyncio.sleep(self._formula_update_interval_sec - diff)

    def parse_formula_overrides(self, config: Config) -> Dict[str, Set[int]]:
        """Parse formula overrides from the config file.

        The following format of the config file is expected:

        ...
        formula_grid_load = "[1,3,4]"
        formula_batteries_remaining_power = "[75,123,1]"
        ...

        "formula_" is a configurable prefix defined in `CONFIG_FILE_FORMULA_PREFIX`.

        Args:
            config: contents of the recently updated config file.

        Returns:
            mapping of formula names to list of battery ids to be taken into account
                when evaluating the formula

        Raises:
            ValueError: if value provided refers to a formula that wasn't defined in
                formula calculator
            AttributeError: if value provided for a formula name doesn't have `split`
                method
            TypeError: if battery ids to be taken into account for a formula are not
                integers
        """
        formula_overrides: Dict[str, Set[int]] = {}
        try:
            formula_config = config.get_dict(
                CONFIG_FILE_FORMULA_PREFIX, expected_values_type=Set[int]
            )
        except ValueError as err:
            logger.error(
                "Formula overrides (config variables starting with %s "
                " are expected to be str -> Set[int] mappings: %s",
                CONFIG_FILE_FORMULA_PREFIX,
                err,
            )
            raise

        for formula_name, battery_ids in formula_config.items():
            if formula_name not in self.formula_calculator.microgrid_formulas:
                logger.error(
                    "Formula %s cannot be updated, because it does not exist.",
                    formula_name,
                )
                continue

            formula_overrides[formula_name] = battery_ids
        return formula_overrides

    # pylint: disable=unused-argument
    async def _reinitialize(
        self, config: Config, resend_formulas_task: asyncio.Task[None]
    ) -> None:
        """Reinitialize MicrogridData with updated config.

        To be implemented once the config file scheme for updating formulas is known.

        This function will:
        - reinitialize formula calculator based on the new config
        - change and apply settings like `formula_update_interval_sec` or
            `wait_for_data_sec`

        Args:
            config: contents of the recently updated config file.
            resend_formulas_task: task for sending formula results


        Raises:
            Exception: when an error occurred either while parsing the new config or
                when creating a new instance of FormulaCalculator
        """
        try:
            new_overrides = self.parse_formula_overrides(config)
        except Exception as err:  # pylint: disable=broad-except
            # This is an error, because it shouldn't happen, but in general we don't need
            # to log a stack trace too because it should be just bad user input
            logger.error(
                "An error occurred while applying the new configuration: %s", err
            )
            raise

        try:
            new_formula_calculator = FormulaCalculator(
                self.component_graph,
                battery_ids_overrides=new_overrides,
            )
        except Exception:  # pylint: disable=broad-except
            # If this fails, there is a bug for sure, so we print the stack track.
            logger.exception(
                "An error occurred while creating the new formula calculator"
            )
            raise
        resend_formulas_task.cancel()
        self.microgrid_formula_overrides = new_overrides
        self.formula_calculator = new_formula_calculator

    async def run(self) -> None:
        """Run the actor."""
        while True:
            receivers = await gen_component_receivers(
                self.component_infos, self.microgrid_client
            )

            # Create a task that will periodically send formula results.
            resend_formulas_task = asyncio.create_task(self.resend_formulas())
            select = Select(
                component_data_receiver=Merge(*receivers.values()),
                config_update_receiver=self._config_update_receiver,
            )
            while await select.ready():
                if msg := select.component_data_receiver:
                    # Update symbols and recalculate formulas.
                    updated_symbols = self.formula_calculator.update_symbol_values(
                        msg.inner
                    )
                    self.formula_calculator.compute(
                        updated_symbols, only_formula_names=set(self._outputs.keys())
                    )
                elif msg := select.config_update_receiver:
                    try:
                        await self._reinitialize(msg.inner, resend_formulas_task)
                    except Exception as err:  # pylint: disable=broad-except
                        logger.error(
                            "Reinitialization failed, because an error occurred "
                            "while applying the new configuration: %s",
                            err,
                        )
                    else:
                        # Break from the inner loop to regenerate component receivers
                        break
