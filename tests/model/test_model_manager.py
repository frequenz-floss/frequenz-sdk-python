# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Tests for machine learning model manager."""

import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, mock_open, patch

import pytest

from frequenz.sdk.ml import ModelManager


@dataclass
class MockModel:
    """Mock model for unit testing purposes."""

    data: int | str

    def predict(self) -> int | str:
        """Make a prediction based on the model data."""
        return self.data


async def test_model_manager_loading() -> None:
    """Test loading models using ModelManager with direct configuration."""
    model1 = MockModel("Model 1 Data")
    model2 = MockModel("Model 2 Data")
    pickled_model1 = pickle.dumps(model1)
    pickled_model2 = pickle.dumps(model2)

    model_paths = {
        "model1": Path("path/to/model1.pkl"),
        "model2": Path("path/to/model2.pkl"),
    }

    mock_files = {
        "path/to/model1.pkl": mock_open(read_data=pickled_model1)(),
        "path/to/model2.pkl": mock_open(read_data=pickled_model2)(),
    }

    def mock_open_func(file_path: Path, *__args: Any, **__kwargs: Any) -> Any:
        """Mock open function to return the correct mock file object.

        Args:
            file_path: The path to the file to open.
            *__args: Variable length argument list. This can be used to pass additional
                    positional parameters typically used in file opening operations,
                    such as `mode` or `buffering`.
            **__kwargs: Arbitrary keyword arguments. This can include parameters like
                    `encoding` and `errors`, common in file opening operations.

        Returns:
            Any: The mock file object.

        Raises:
            FileNotFoundError: If the file path is not in the mock files dictionary.
        """
        file_path_str = str(file_path)
        if file_path_str in mock_files:
            file_handle = MagicMock()
            file_handle.__enter__.return_value = mock_files[file_path_str]
            return file_handle
        raise FileNotFoundError(f"No mock setup for {file_path_str}")

    with patch("pathlib.Path.open", new=mock_open_func):
        with patch.object(Path, "exists", return_value=True):
            model_manager: ModelManager[MockModel] = ModelManager(
                model_paths=model_paths
            )

        with patch(
            "frequenz.channels.file_watcher.FileWatcher", new_callable=AsyncMock
        ):
            model_manager.start()  # Start the service

            assert isinstance(model_manager.get_model("model1"), MockModel)
            assert model_manager.get_model("model1").data == "Model 1 Data"
            assert model_manager.get_model("model2").data == "Model 2 Data"

            with pytest.raises(KeyError):
                model_manager.get_model("key3")

            await model_manager.stop()  # Stop the service to clean up


async def test_model_manager_update() -> None:
    """Test updating a model in ModelManager."""
    original_model = MockModel("Original Data")
    updated_model = MockModel("Updated Data")
    pickled_original_model = pickle.dumps(original_model)
    pickled_updated_model = pickle.dumps(updated_model)

    model_paths = {"model1": Path("path/to/model1.pkl")}

    mock_file = mock_open(read_data=pickled_original_model)
    with (
        patch("pathlib.Path.open", mock_file),
        patch.object(Path, "exists", return_value=True),
    ):
        model_manager = ModelManager[MockModel](model_paths=model_paths)
        with patch(
            "frequenz.channels.file_watcher.FileWatcher", new_callable=AsyncMock
        ):
            model_manager.start()  # Start the service

            assert model_manager.get_model("model1").data == "Original Data"

            # Simulate updating the model file
            mock_file.return_value.read.return_value = pickled_updated_model
            with patch("pathlib.Path.open", mock_file):
                model_manager.reload_model(Path("path/to/model1.pkl"))
                assert model_manager.get_model("model1").data == "Updated Data"

            await model_manager.stop()  # Stop the service to clean up
