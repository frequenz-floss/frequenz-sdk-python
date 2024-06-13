# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Read and update config variables."""

import logging
import pathlib
import tomllib
from collections import abc
from typing import Any, assert_never

from frequenz.channels import Sender
from frequenz.channels.file_watcher import EventType, FileWatcher

from ..actor._actor import Actor

_logger = logging.getLogger(__name__)


class ConfigManagingActor(Actor):
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
        output: Sender[dict[str, Any]],
        event_types: abc.Set[EventType] = frozenset(EventType),
        *,
        name: str | None = None,
    ) -> None:
        """Initialize this instance.

        Args:
            config_path: The path to the TOML file with the configuration.
            output: The sender to send the configuration to.
            event_types: The set of event types to monitor.
            name: The name of the actor. If `None`, `str(id(self))` will
                be used. This is used mostly for debugging purposes.
        """
        super().__init__(name=name)
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
        self._output: Sender[dict[str, Any]] = output

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
            _logger.error("%s: Can't read config file, err: %s", self, err)
            raise

    async def send_config(self) -> None:
        """Send the configuration to the output sender."""
        config = self._read_config()
        await self._output.send(config)

    async def _run(self) -> None:
        """Monitor for and send configuration file updates.

        At startup, the Config Manager sends the current config so that it
        can be cache in the Broadcast channel and served to receivers even if
        there hasn't been any change to the config file itself.
        """
        await self.send_config()

        async for event in self._file_watcher:
            # Since we are watching the whole parent directory, we need to make sure
            # we only react to events related to the configuration file.
            if event.path != self._config_path:
                continue

            match event.type:
                case EventType.CREATE:
                    _logger.info(
                        "%s: The configuration file %s was created, sending new config...",
                        self,
                        self._config_path,
                    )
                    await self.send_config()
                case EventType.MODIFY:
                    _logger.info(
                        "%s: The configuration file %s was modified, sending update...",
                        self,
                        self._config_path,
                    )
                    await self.send_config()
                case EventType.DELETE:
                    _logger.info(
                        "%s: The configuration file %s was deleted, ignoring...",
                        self,
                        self._config_path,
                    )
                case _:
                    assert_never(event.type)
