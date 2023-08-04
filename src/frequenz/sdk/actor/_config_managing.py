# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Read and update config variables."""

import logging
import pathlib
import tomllib
from collections import abc
from typing import Any, assert_never

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
        config_path: pathlib.Path | str,
        output: Sender[Config],
        event_types: abc.Set[FileWatcher.EventType] = frozenset(FileWatcher.EventType),
    ) -> None:
        """Initialize this instance.

        Args:
            config_path: The path to the TOML file with the configuration.
            output: The sender to send the config to.
            event_types: The set of event types to monitor.
        """
        self._config_path: pathlib.Path = (
            config_path
            if isinstance(config_path, pathlib.Path)
            else pathlib.Path(config_path)
        )
        # FileWatcher can't watch for non-existing files, so we need to watch for the
        # parent directory instead just in case a configuration file doesn't exist yet
        # or it is deleted and recreated again.
        self._file_watcher: FileWatcher = FileWatcher(
            paths=[self._config_path.parent], event_types=event_types
        )
        self._output: Sender[Config] = output

    def _read_config(self) -> dict[str, Any]:
        """Read the contents of the configuration file.

        Returns:
            A dictionary containing configuration variables.

        Raises:
            ValueError: If config file cannot be read.
        """
        try:
            with self._config_path.open("rb") as toml_file:
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
            # Since we are watching the whole parent directory, we need to make sure
            # we only react to events related to the configuration file.
            if event.path != self._config_path:
                continue

            match event.type:
                case FileWatcher.EventType.CREATE:
                    _logger.info(
                        "%s: The configuration file %s was created, sending new config...",
                        self,
                        self._config_path,
                    )
                    await self.send_config()
                case FileWatcher.EventType.MODIFY:
                    _logger.info(
                        "%s: The configuration file %s was modified, sending update...",
                        self,
                        self._config_path,
                    )
                    await self.send_config()
                case FileWatcher.EventType.DELETE:
                    _logger.info(
                        "%s: The configuration file %s was deleted, ignoring...",
                        self,
                        self._config_path,
                    )
                case _:
                    assert_never(event.type)
