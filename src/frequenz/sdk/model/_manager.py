# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Load, update, monitor and retrieve machine learning models."""

import abc
import asyncio
import logging
import os
import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generic, TypeVar

from frequenz.channels.util import FileWatcher

from frequenz.sdk._internal._asyncio import cancel_and_await

_logger = logging.getLogger(__name__)

T = TypeVar("T")
"""The model type."""


class ModelRepository(abc.ABC):
    """Provide an interface for building the model repository configuration.

    The ModelRepository is meant to be subclassed to provide specific
    implementations based on the type of models being managed.
    """

    @abc.abstractmethod
    def _get_allowed_keys(self) -> list[Any]:
        """Get the list of allowed keys for the model repository.

        Returns:
            a list of allowed keys.  # noqa: DAR202

        Raises:
            NotImplementedError: if the method is not implemented in the subclass.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _build_path(self, key: Any) -> str:
        """Build the model path for the given key.

        Args:
            key: the key for which to build the model path.

        Returns:
            the model path.  # noqa: DAR202

        Raises:
            NotImplementedError: if the method is not implemented in the subclass.
        """
        raise NotImplementedError

    def get_models_config(self) -> dict[Any, str]:
        """Get the models configuration.

        Returns:
            the model keys to model paths mapping.
        """
        allowed_keys = self._get_allowed_keys()
        assert allowed_keys, "Allowed keys must be set."
        return {key: self._build_path(key) for key in allowed_keys}


@dataclass
class SingleModelRepository(ModelRepository):
    """Repository to manage a single model."""

    model_path: str
    """The path to a single model file."""

    def _get_allowed_keys(self) -> list[Any]:
        """Get the allowed keys for the single model repository.

        Returns:
            the allowed single key.
        """
        return ["Single"]

    def _build_path(self, key: Any) -> str:
        """Build the model path for the given key.

        Args:
            key: the key for which to build the model path.

        Returns:
            the model path.
        """
        return self.model_path


@dataclass
class WeekdayModelRepository(ModelRepository):
    """Repository to manage weekday-based models."""

    directory: str
    """The base directory where the weekday-based models are stored."""

    file_name: str
    """The name of the file containing the model."""

    def _get_allowed_keys(self) -> list[Any]:
        """Get the allowed keys for the weekday-based models repository.

        Returns:
            the allowed keys for the weekday-based models repository.
        """
        return ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    def _build_path(self, key: str) -> str:
        """Build the model path for the given key.

        The model path is constructed by combining the directory, key and
        file name as follow: directory/key/file_name.

        Args:
            key: the key for which to build the model path.

        Returns:
            The model path.

        Raises:
            AssertionError: if the directory or file name is not set.
        """
        assert self.directory, "Directory must be set."
        assert self.file_name, "File name must be set."
        return f"{self.directory}/{key}/{self.file_name}"


@dataclass
class EnumeratedModelRepository(ModelRepository):
    """Repository to manage interval-based models."""

    directory: str
    """The base directory where the interval-based models are stored."""

    file_name: str
    """The name of the file containing the model."""

    num_models: int
    """The number of interval-based models to load and manage."""

    def _get_allowed_keys(self) -> list[Any]:
        """Get the allowed keys for the interval-based models repository.

        Returns:
            the allowed keys for the interval-based models repository.
        """
        return list(range(self.num_models))

    def _build_path(self, key: str) -> str:
        """Build the model path for the given key.

        The model path is constructed by combining the directory, key and
        file name as follow: directory/key/file_name.

        Args:
            key: the key for which to build the model path.

        Returns:
            the model path.

        Raises:
            AssertionError: if the directory or file name is not set.
        """
        assert self.directory, "Directory must be set."
        assert self.file_name, "File name must be set."
        return f"{self.directory}/{key}/{self.file_name}"


@dataclass
class _Model(Generic[T]):
    """Represent a machine learning model."""

    data: T
    """The machine learning model."""

    path: str
    """The path to the model file."""


class ModelManager(Generic[T]):
    """Manage machine learning models.

    The model manager acts as a central hub for retrieving models, reloading
    models, and monitoring model paths for changes.
    """

    def __init__(self, model_repository: ModelRepository) -> None:
        """Initialize the model manager.

        Args:
            model_repository: a model repository that defines the mapping
                between model keys and paths.
        """
        self._models: dict[Any, _Model[T]] = {}

        models_config = model_repository.get_models_config()
        for key, model_path in models_config.items():
            self._models[key] = _Model(data=self._load(model_path), path=model_path)

        self._monitoring_task: asyncio.Task[None] = asyncio.create_task(
            self._start_model_monitor()
        )

    def __getitem__(self, key: Any) -> T:
        """Get a specific loaded model using the subscript operator.

        Args:
            key: the key to identify the model.

        Returns:
            the model instance corresponding to the key.
        """
        return self.get_model(key)

    def get_model(self, key: Any = None) -> T:
        """Get a specific loaded model.

        Args:
            key: the key to identify the model.

        Raises:
            KeyError: if the key is invalid or it is not provided for
                multi-model retrieval.

        Returns:
            the model instance corresponding to the key.
        """
        if key is None:
            if len(self._models) != 1:
                raise KeyError("No key provided for multi-model retrieval")
            key = list(self._models.keys())[0]

        if key not in self._models:
            raise KeyError("Invalid key")

        return self._models[key].data

    def get_paths(self) -> list[Path | str]:
        """Get the paths of all loaded models.

        Returns:
            the paths of all loaded models.
        """
        return [model.path for model in self._models.values()]

    def reload_model(self, model_path: str) -> None:
        """Reload a model from the given path.

        Args:
            model_path: the path to the model file.

        Raises:
            ValueError: if the model with the specified path is not found.
        """
        if model_path not in self.get_paths():
            raise ValueError(f"No model found with path: {model_path}")

        for model in self._models.values():
            if model.path == model_path:
                model.data = self._load(model_path)

    async def join(self) -> None:
        """Await the monitoring task, and return when the task completes."""
        if self._monitoring_task and not self._monitoring_task.done():
            await self._monitoring_task

    async def _start_model_monitor(self) -> None:
        """Start monitoring the model paths for changes."""
        # model_paths = self._model_loader.get_paths()
        model_paths = self.get_paths()
        file_watcher = FileWatcher(paths=model_paths)

        _logger.info("Monitoring the paths of the models for changes: %s", model_paths)

        async for event in file_watcher:
            # The model could be deleted and created again.
            if event.type in (
                FileWatcher.EventType.CREATE,
                FileWatcher.EventType.MODIFY,
            ):
                _logger.info(
                    "Model file %s has been modified. Reloading model...",
                    str(event.path),
                )
                self.reload_model(str(event.path))

        _logger.debug("ModelManager stopped.")

    async def _stop_model_monitor(self) -> None:
        """Stop monitoring the model paths for changes."""
        if self._monitoring_task:
            await cancel_and_await(self._monitoring_task)

    def _load(self, model_path: str) -> T:
        """Load the model file.

        Args:
            model_path: the path of the model to be loaded.

        Raises:
            FileNotFoundError: if the model path does not exist.

        Returns:
            the model instance.
        """
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"The model path {model_path} does not exist.")

        with open(model_path, "rb") as file:
            model: T = pickle.load(file)
            return model
