"""Distribute power between many batteries.

When charge/discharge method is called the power should be distributed so that
the SoC in batteries stays at the same level. That way of distribution
prevents using only one battery, increasing temperature, and maximize the total
amount power to charge/discharge.

Copyright
Copyright © 2022 Frequenz Energy-as-a-Service GmbH

License
MIT
"""

from .power_distributor import PowerDistributor
from .utils import Request, Result

__all__ = ["Result", "Request", "PowerDistributor"]
