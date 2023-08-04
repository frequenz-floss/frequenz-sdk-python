# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Read and update config variables."""

import logging
import os
import tomllib
from collections import abc
from typing import Any

from frequenz.channels import Sender
from frequenz.channels.util import FileWatcher

from ..actor._decorator import actor
from ..config import Config

_logger = logging.getLogger(__name__)


@actor
class ConfigManagingActor:
    """An actor that monitors a TOML configuration file for updates.

    When the file is updated, the new configuration is sent, as a [`dict`][], to the
    `output` sender.

    When the actor is started, if a configuration file already exists, then it will be
    read and sent to the `output` sender before the actor starts monitoring the file
    for updates. This way users can rely on the actor to do the initial configuration
    reading too.
    """

    def __init__(
        self,
        conf_file: str,
        output: Sender[Config],
        event_types: abc.Set[FileWatcher.EventType] = frozenset(FileWatcher.EventType),
    ) -> None:
        """Initialize this instance.

        Args:
            conf_file: The path to the TOML file with the configuration.
            output: The sender to send the config to.
            event_types: The set of event types to monitor.
        """
        self._conf_file: str = conf_file
        self._conf_dir: str = os.path.dirname(conf_file)
        self._file_watcher = FileWatcher(
            paths=[self._conf_dir], event_types=event_types
        )
        self._output = output

    def _read_config(self) -> dict[str, Any]:
        """Read the contents of the configuration file.

        Returns:
            A dictionary containing configuration variables.

        Raises:
            ValueError: If config file cannot be read.
        """
        try:
            with open(self._conf_file, "rb") as toml_file:
                return tomllib.load(toml_file)
        except ValueError as err:
            logging.error("%s: Can't read config file, err: %s", self, err)
            raise

    async def send_config(self) -> None:
        """Send the configuration to the output sender."""
        conf_vars = self._read_config()
        config = Config(conf_vars)
        await self._output.send(config)

    async def run(self) -> None:
        """Monitor for and send configuration file updates."""
        await self.send_config()

        async for event in self._file_watcher:
            if event.type != FileWatcher.EventType.DELETE:
                if str(event.path) == self._conf_file:
                    _logger.info(
                        "%s: Update configs, because file %s was modified.",
                        self,
                        self._conf_file,
                    )
                    await self.send_config()
