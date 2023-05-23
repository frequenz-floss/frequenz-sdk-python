# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Integration tests for machine learning model manager."""

import asyncio
import os
import pathlib
import pickle
from dataclasses import dataclass
from typing import Any

import pytest

from frequenz.sdk.model import ModelManager, ModelRepository


@pytest.mark.integration
async def test_weekend_model(tmp_path: pathlib.Path) -> None:
    """Test model with custom day periods are updated on model files changes.

    Args:
        tmp_path: a temporary directory created by pytest to load models from
            and to monitor them.
    """
    weekend = ["sat", "sun"]

    @dataclass
    class WeekendModelRepository(ModelRepository):
        """Repository to manage a custom weekend-based models."""

        directory: str
        file_name: str

        def _get_allowed_keys(self) -> list[Any]:
            return weekend

        def _build_path(self, key: str) -> str:
            assert key in self._get_allowed_keys(), f"Key {key} is not allowed."
            return f"{self.directory}/{key}/{self.file_name}"

    weekend_models: dict[str, str] = {day: f"{day} Model" for day in weekend}

    for day in weekend:
        model_path = f"{str(tmp_path)}/{day}/model.pkl"
        os.makedirs(os.path.dirname(model_path), exist_ok=True)
        with open(model_path, "wb") as file:
            pickle.dump(weekend_models[day], file)

    model_repository = WeekendModelRepository(
        directory=str(tmp_path), file_name="model.pkl"
    )
    model_manager: ModelManager[str] = ModelManager(model_repository=model_repository)

    sat_model = model_manager.get_model("sat")
    assert sat_model == weekend_models["sat"]

    sun_model = model_manager.get_model("sun")
    assert sun_model == weekend_models["sun"]

    await asyncio.sleep(0.1)  # Allow the monitoring task to start

    # Update the content of the mock files
    updated_models: dict[str, str] = {day: f"{day} Model UPDATED" for day in weekend}

    for day in weekend:
        model_path = f"{str(tmp_path)}/{day}/model.pkl"
        with open(model_path, "wb") as file:
            pickle.dump(updated_models[day], file)

    await asyncio.sleep(0.1)  # Allow the monitoring task to process the updated models

    sat_model = model_manager["sat"]
    assert sat_model == updated_models["sat"]

    sun_model = model_manager["sun"]
    assert sun_model == updated_models["sun"]

    await model_manager._stop_model_monitor()  # pylint: disable=protected-access
