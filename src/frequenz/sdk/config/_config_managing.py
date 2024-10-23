# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Read and update config variables."""

import logging
import pathlib
import tomllib
from collections import abc
from collections.abc import Mapping, MutableMapping
from datetime import timedelta
from typing import Any, assert_never

from frequenz.channels import Sender
from frequenz.channels.file_watcher import EventType, FileWatcher

from ..actor._actor import Actor

_logger = logging.getLogger(__name__)


class ConfigManagingActor(Actor):
    """An actor that monitors a TOML configuration files for updates.

    When the actor is started the configuration files will be read and sent to the
    output sender. Then the actor will start monitoring the files for updates.

    If no configuration file could be read, the actor will raise a exception.

    The configuration files are read in the order of the paths, so the last path will
    override the configuration set by the previous paths. Dict keys will be merged
    recursively, but other objects (like lists) will be replaced by the value in the
    last path.

    Example:
        If `config1.toml` contains:

        ```toml
        var1 = 1
        var2 = 2
        ```

        And `config2.toml` contains:

        ```toml
        var2 = 3
        var3 = 4
        ```

        Then the final configuration will be:

        ```py
        {
            "var1": 1,
            "var2": 3,
            "var3": 4,
        }
        ```
    """

    # pylint: disable-next=too-many-arguments
    def __init__(
        self,
        config_paths: abc.Iterable[pathlib.Path | str],
        output: Sender[abc.Mapping[str, Any]],
        event_types: abc.Set[EventType] = frozenset(EventType),
        *,
        name: str | None = None,
        force_polling: bool = True,
        polling_interval: timedelta = timedelta(seconds=1),
    ) -> None:
        """Initialize this instance.

        Args:
            config_paths: The paths to the TOML files with the configuration. Order
                matters, as the configuration will be read and updated in the order
                of the paths, so the last path will override the configuration set by
                the previous paths. Dict keys will be merged recursively, but other
                objects (like lists) will be replaced by the value in the last path.
            output: The sender to send the configuration to.
            event_types: The set of event types to monitor.
            name: The name of the actor. If `None`, `str(id(self))` will
                be used. This is used mostly for debugging purposes.
            force_polling: Whether to force file polling to check for changes.
            polling_interval: The interval to poll for changes. Only relevant if
                polling is enabled.
        """
        super().__init__(name=name)
        self._config_paths: list[pathlib.Path] = [
            (
                config_path
                if isinstance(config_path, pathlib.Path)
                else pathlib.Path(config_path)
            )
            for config_path in config_paths
        ]
        self._output: Sender[abc.Mapping[str, Any]] = output
        self._event_types: abc.Set[EventType] = event_types
        self._force_polling: bool = force_polling
        self._polling_interval: timedelta = polling_interval

    def _read_config(self) -> abc.Mapping[str, Any]:
        """Read the contents of the configuration file.

        Returns:
            A dictionary containing configuration variables.

        Raises:
            ValueError: If config file cannot be read.
        """
        error_count = 0
        config: dict[str, Any] = {}

        for config_path in self._config_paths:
            try:
                with config_path.open("rb") as toml_file:
                    data = tomllib.load(toml_file)
                    config = _recursive_update(config, data)
            except ValueError as err:
                _logger.error("%s: Can't read config file, err: %s", self, err)
                error_count += 1

        if error_count == len(self._config_paths):
            raise ValueError(f"{self}: Can't read any of the config files")

        return config

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

        parent_paths = {p.parent for p in self._config_paths}

        # FileWatcher can't watch for non-existing files, so we need to watch for the
        # parent directories instead just in case a configuration file doesn't exist yet
        # or it is deleted and recreated again.
        file_watcher = FileWatcher(
            paths=list(parent_paths),
            event_types=self._event_types,
            force_polling=self._force_polling,
            polling_interval=self._polling_interval,
        )

        try:
            async for event in file_watcher:
                # Since we are watching the whole parent directories, we need to make
                # sure we only react to events related to the configuration files we
                # are interested in.
                if not any(event.path.samefile(p) for p in self._config_paths):
                    continue

                match event.type:
                    case EventType.CREATE:
                        _logger.info(
                            "%s: The configuration file %s was created, sending new config...",
                            self,
                            event.path,
                        )
                        await self.send_config()
                    case EventType.MODIFY:
                        _logger.info(
                            "%s: The configuration file %s was modified, sending update...",
                            self,
                            event.path,
                        )
                        await self.send_config()
                    case EventType.DELETE:
                        _logger.info(
                            "%s: The configuration file %s was deleted, ignoring...",
                            self,
                            event.path,
                        )
                    case _:
                        assert_never(event.type)
        finally:
            del file_watcher


def _recursive_update(
    target: dict[str, Any], overrides: Mapping[str, Any]
) -> dict[str, Any]:
    """Recursively updates dictionary d1 with values from dictionary d2.

    Args:
        target: The original dictionary to be updated.
        overrides: The dictionary with updates.

    Returns:
        The updated dictionary.
    """
    for key, value in overrides.items():
        if (
            key in target
            and isinstance(target[key], MutableMapping)
            and isinstance(value, MutableMapping)
        ):
            _recursive_update(target[key], value)
        else:
            target[key] = value
    return target
