# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Functions for generating DataColletor with necessary fields from ComponentInfo."""
# PROTOBUF METRICS:

METRIC_COMPONENT_ID = "id"
METRIC_TIMESTAMP = "timestamp"

# Capacity of the battery (0, +inf)
METRIC_CAPACITY = "capacity"

# SoC of the battery
METRIC_SOC = "soc"


# PROTOBUF AND FORMULA METRICS:

# Consumption power upper bound (0, +inf) for inverter
# And name for the formula that computes sum of upper bound for each inverter-battery
# pairs
METRIC_ACTIVE_POWER_UPPER_BOUND = "active_power_upper_bound"

# Supply power upper bound (-inf , 0) for inverter
# And name for the formula that computes sum of lower bound for all inverter-battery
# pairs
METRIC_ACTIVE_POWER_LOWER_BOUND = "active_power_lower_bound"

# Current consume power from EvCharger (0, +inf)
# And name for the formula that computes sum of active power for each ev charger.
METRIC_EV_ACTIVE_POWER = "active_power"

# Active power now for inverter, battery or meter
# And name for the formula that computes sum of active power for all inverters.
METRIC_ACTIVE_POWER = "active_power"

# Consumption power upper bound (0, +inf) for battery
METRIC_POWER_UPPER_BOUND = "power_upper_bound"

# Supply power upper bound (-inf , 0) for battery
METRIC_POWER_LOWER_BOUND = "power_lower_bound"

# FORMULA METRICS:

# Name of the formula that computes batteries_active_power_bounds for battery inverters
METRIC_BATTERIES_ACTIVE_POWER_BOUNDS = "batteries_active_power_bounds"

# Name of the formula that computes active_power for battery inverters
METRIC_BATTERIES_ACTIVE_POWER = "batteries_active_power"

# Name of the formula that computes  batteries remaining energy (SoC * capacity)
METRIC_BATTERIES_REMAINING_ENERGY = "batteries_remaining_energy"

# Name of the formula that computes grid load
METRIC_GRID_LOAD = "grid_load"

# Name of the formula that computes client load
METRIC_CLIENT_LOAD = "client_load"

# Name of the formula that computes pv active_power for all pv_meters
METRIC_PV_PROD = "pv_prod"

# Name of the formula that computes active_power for all ev_chargers
METRIC_EV_CHARGERS_CONSUMPTION = "ev_chargers_consumption"

# Name of the formula that computes total capacity of all batteries
METRIC_BATTERIES_CAPACITY = "batteries_capacity"
