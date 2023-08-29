# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Configuration file for nox."""

from frequenz.repo.config import nox
from frequenz.repo.config.nox import default

config = default.lib_config.copy()
config.opts.mypy = []  # Set in pyproject.toml

nox.configure(config)
