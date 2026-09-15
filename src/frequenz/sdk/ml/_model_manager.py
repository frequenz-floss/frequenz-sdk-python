# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Load, update, monitor and retrieve machine learning models."""

import asyncio
import logging
import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Generic, TypeVar, cast

from frequenz.channels.file_watcher import EventType, FileWatcher
from typing_extensions import override

from frequenz.sdk.actor import BackgroundService

_logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class _Model(Generic[T]):
    """Represent a machine learning model."""

    data: T
    path: Path


class ModelNotFoundError(Exception):
    """Exception raised when a model is not found."""

    def __init__(self, key: str) -> None:
        """Initialize the exception with the specified model key.

        Args:
            key: The key of the model that was not found.
        """
        super().__init__(f"Model with key '{key}' is not found.")


class ModelManager(BackgroundService, Generic[T]):
    """Load, update, monitor and retrieve machine learning models."""

    def __init__(self, model_paths: dict[str, Path], *, name: str | None = None):
        """Initialize the model manager with the specified model paths.

        Args:
            model_paths: A dictionary of model keys and their corresponding file paths.
            name: The name of the model manager service.
        """
        super().__init__(name=name)
        self._models: dict[str, _Model[T]] = {}
        self.model_paths = model_paths
        self.load_models()

    def load_models(self) -> None:
        """Load the models from the specified paths."""
        for key, path in self.model_paths.items():
            self._models[key] = _Model(data=self._load(path), path=path)

    @staticmethod
    def _load(path: Path) -> T:
        """Load the model from the specified path.

        Args:
            path: The path to the model file.

        Returns:
            T: The loaded model data.

        Raises:
            ModelNotFoundError: If the model file does not exist.
        """
        try:
            with path.open("rb") as file:
                return cast(T, pickle.load(file))
        except FileNotFoundError as exc:
            raise ModelNotFoundError(str(path)) from exc

    @override
    def start(self) -> None:
        """Start the model monitoring service by creating a background task."""
        if not self.is_running:
            task = asyncio.create_task(self._monitor_paths())
            self._tasks.add(task)
            _logger.info(
                "%s: Started ModelManager service with task %s",
                self.name,
                task,
            )

    async def _monitor_paths(self) -> None:
        """Monitor model file paths and reload models as necessary."""
        model_paths = [model.path for model in self._models.values()]
        file_watcher = FileWatcher(
            paths=list(model_paths), event_types=[EventType.CREATE, EventType.MODIFY]
        )
        _logger.info("%s: Monitoring model paths for changes.", self.name)
        async for event in file_watcher:
            _logger.info(
                "%s: Reloading model from file %s due to a %s event...",
                self.name,
                event.path,
                event.type.name,
            )
            self.reload_model(Path(event.path))

    def reload_model(self, path: Path) -> None:
        """Reload the model from the specified path.

        Args:
            path: The path to the model file.
        """
        for key, model in self._models.items():
            if model.path == path:
                try:
                    model.data = self._load(path)
                    _logger.info(
                        "%s: Successfully reloaded model from %s",
                        self.name,
                        path,
                    )
                except Exception:  # pylint: disable=broad-except
                    _logger.exception("Failed to reload model from %s", path)

    def get_model(self, key: str) -> T:
        """Retrieve a loaded model by key.

        Args:
            key: The key of the model to retrieve.

        Returns:
            The loaded model data.

        Raises:
            KeyError: If the model with the specified key is not found.
        """
        try:
            return self._models[key].data
        except KeyError as exc:
            raise KeyError(f"Model with key '{key}' is not found.") from exc
