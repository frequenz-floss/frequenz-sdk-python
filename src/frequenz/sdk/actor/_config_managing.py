# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Read and update config variables."""

import logging
import os
from typing import Any, Dict, Optional, Set

import toml
from frequenz.channels import Sender
from frequenz.channels.util import FileWatcher

from ..actor._decorator import actor
from ..config import Config

logger = logging.getLogger(__name__)


@actor
class ConfigManagingActor:
    """
    Manages config variables.

    Config variables are read from file.
    Only single file can be read.
    If new file is read, then previous configs will be forgotten.
    """

    def __init__(
        self,
        conf_file: str,
        output: Sender[Config],
        event_types: Optional[Set[FileWatcher.EventType]] = None,
    ) -> None:
        """Read config variables from the file.

        Args:
            conf_file: Path to file with config variables.
            output: Channel to publish updates to.
            event_types: Which types of events should update the config and
                trigger a notification.
        """
        self._conf_file: str = conf_file
        self._conf_dir: str = os.path.dirname(conf_file)
        self._file_watcher = FileWatcher(
            paths=[self._conf_dir], event_types=event_types
        )
        self._output = output

    def _read_config(self) -> Dict[str, Any]:
        """Read the contents of the config file.

        Raises:
            ValueError: if config file cannot be read.

        Returns:
            A dictionary containing configuration variables.
        """
        try:
            return toml.load(self._conf_file)
        except ValueError as err:
            logging.error("Can't read config file, err: %s", err)
            raise

    async def send_config(self) -> None:
        """Send config file using a broadcast channel."""
        conf_vars = self._read_config()
        config = Config(conf_vars)
        await self._output.send(config)

    async def run(self) -> None:
        """Watch config file and update when modified.

        At startup, the Config Manager sends the current config so that it
        can be cache in the Broadcast channel and served to receivers even if
        there hasn't been any change to the config file itself.
        """
        await self.send_config()

        async for path in self._file_watcher:
            if str(path) == self._conf_file:
                logger.info(
                    "Update configs, because file %s was modified.",
                    self._conf_file,
                )
                await self.send_config()

        logger.debug("ConfigManager stopped.")
