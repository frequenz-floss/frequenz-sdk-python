# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for machine learning model manager."""

import bisect
import calendar
import pickle
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from unittest.mock import mock_open, patch

import pytest

from frequenz.sdk.model import (
    EnumeratedModelRepository,
    ModelManager,
    ModelRepository,
    SingleModelRepository,
    WeekdayModelRepository,
)


@dataclass
class MockModel:
    """Mock model for unit testing purposes."""

    data: int | str

    def predict(self) -> int | str:
        """Make a prediction based on the model data."""
        return self.data


async def test_single_model() -> None:
    """Test single model."""
    test_model = MockModel("Single Model")
    pickled_model = pickle.dumps(test_model)
    mock_file = mock_open(read_data=pickled_model)

    with patch("builtins.open", side_effect=mock_file), patch(
        "os.path.exists", return_value=True
    ):
        model_repository = SingleModelRepository(
            model_path="models/consumption/single/model.pkl"
        )
        model_manager: ModelManager[MockModel] = ModelManager(
            model_repository=model_repository
        )

        model = model_manager.get_model()

        assert isinstance(model, MockModel)
        assert model.data == test_model.data

        with pytest.raises(KeyError):
            model = model_manager.get_model(-1)
            model = model_manager.get_model(0)
            model = model_manager.get_model(1)
            model = model_manager.get_model("single")
            model = model_manager.get_model("Single")
            model = model_manager[0]
            model = model_manager[""]

    await model_manager._stop_model_monitor()  # pylint: disable=protected-access


async def test_weekday_models_name() -> None:
    """Test weekday-based models using weekday names as keys."""
    day_name_models: dict[str, MockModel] = {
        day: MockModel(f"{day} Model") for day in calendar.day_abbr
    }

    pickled_models = [pickle.dumps(model) for model in day_name_models.values()]
    mock_files = [
        mock_open(read_data=pickled_model)() for pickled_model in pickled_models
    ]

    with patch("builtins.open", side_effect=mock_files), patch(
        "os.path.exists", return_value=True
    ):
        model_repository = WeekdayModelRepository(
            directory="models/consumption/day", file_name="model.pkl"
        )
        model_manager: ModelManager[MockModel] = ModelManager(
            model_repository=model_repository
        )

        monday_model = model_manager.get_model("Mon")
        assert monday_model.data == day_name_models["Mon"].data

        wednesday_model = model_manager["Wed"]
        assert wednesday_model.data == day_name_models["Wed"].data

        friday_model = model_manager["Fri"]
        assert friday_model.data == day_name_models["Fri"].data

        sunday_model = model_manager.get_model("Sun")
        assert sunday_model.data == day_name_models["Sun"].data

        with pytest.raises(KeyError):
            model_manager.get_model()
            model_manager.get_model(10)
            model_manager.get_model("TUE")
            model_manager.get_model("sunday")
            model_manager.get_model("Thursday")
            model_manager.get_model(0)

    await model_manager._stop_model_monitor()  # pylint: disable=protected-access


async def test_weekday_models_number() -> None:
    """Test weekday-based models using the weekday numbers as keys."""

    @dataclass
    class WeekdayCustomModelRepository(ModelRepository):
        """Repository to manage a custom weekday-based models."""

        directory: str
        file_name: str

        def _get_allowed_keys(self) -> list[Any]:
            return [num for num, _ in enumerate(calendar.day_name)]

        def _build_path(self, key: str) -> str:
            return f"{self.directory}/{key}/{self.file_name}"

    day_num_models: dict[int, MockModel] = {
        num: MockModel(f"{day} Model") for num, day in enumerate(calendar.day_name)
    }

    pickled_models = [pickle.dumps(model) for model in day_num_models.values()]
    mock_files = [
        mock_open(read_data=pickled_model)() for pickled_model in pickled_models
    ]

    with patch("builtins.open", side_effect=mock_files), patch(
        "os.path.exists", return_value=True
    ):
        model_repository = WeekdayCustomModelRepository(
            directory="models/consumption/day", file_name="model.pkl"
        )
        model_manager: ModelManager[MockModel] = ModelManager(
            model_repository=model_repository
        )

        monday_model = model_manager.get_model(0)
        assert monday_model.data == day_num_models[0].data
        timestamp = datetime(2023, 6, 12, tzinfo=timezone.utc)
        assert model_manager.get_model(timestamp.weekday()) == monday_model

        timestamp = datetime(2023, 6, 11, tzinfo=timezone.utc)
        sunday_model = model_manager[timestamp.weekday()]
        assert sunday_model.data == day_num_models[6].data

        with pytest.raises(KeyError):
            model_manager.get_model()
            model_manager.get_model(10)
            model_manager.get_model("Monday")

    await model_manager._stop_model_monitor()  # pylint: disable=protected-access


async def test_enumerated_models() -> None:  # pylint: disable=too-many-locals
    """Test 15m ahead current time interval-based models using the interval numbers as keys."""

    def timestamp_to_interval_num(
        timestamp: datetime, interval_to_num: dict[str, int]
    ) -> int:
        interval_key = f"{timestamp.hour:02d}{timestamp.minute:02d}"
        position = bisect.bisect(sorted(interval_to_num.keys()), interval_key)
        return sorted(interval_to_num.values())[position - 1]

    interval_num_models: dict[int, MockModel] = {}
    interval_to_num: dict[str, int] = {}

    interval_min = 15
    num_intervals_in_day = 24 * 60 // interval_min

    for index in range(0, num_intervals_in_day):
        ahead_min = index * interval_min + interval_min
        key = f"{(ahead_min // 60) % 24:02d}{ahead_min % 60:02d}"
        interval_num_models[index] = MockModel(key)
        interval_to_num[key] = index

    pickled_models = [pickle.dumps(model) for model in interval_num_models.values()]
    mock_files = [
        mock_open(read_data=pickled_model)() for pickled_model in pickled_models
    ]

    with patch("builtins.open", side_effect=mock_files), patch(
        "os.path.exists", return_value=True
    ):
        model_repository = EnumeratedModelRepository(
            directory="models/production/interval",
            file_name="model.pkl",
            num_models=num_intervals_in_day,
        )
        model_manager: ModelManager[MockModel] = ModelManager(
            model_repository=model_repository
        )

        first_model = model_manager.get_model(0)
        assert first_model.data == interval_num_models[0].data
        assert first_model.data == "0015"
        timestamp = datetime(2023, 6, 12, 00, 10, tzinfo=timezone.utc)
        interval_num = timestamp_to_interval_num(timestamp, interval_to_num)
        assert model_manager.get_model(interval_num) == first_model

        mid_model = model_manager.get_model(47)
        assert mid_model.data == interval_num_models[47].data
        assert mid_model.data == "1200"
        timestamp = datetime(2023, 6, 12, 11, 48, tzinfo=timezone.utc)
        interval_num = timestamp_to_interval_num(timestamp, interval_to_num)
        assert model_manager.get_model(interval_num) == mid_model

        last_model = model_manager.get_model(95)
        assert last_model.data == interval_num_models[95].data
        assert last_model.data == "0000"
        timestamp = datetime(2023, 6, 12, 23, 45, tzinfo=timezone.utc)
        interval_num = timestamp_to_interval_num(timestamp, interval_to_num)
        assert model_manager.get_model(interval_num) == last_model

    await model_manager._stop_model_monitor()  # pylint: disable=protected-access


async def test_interval_models_name() -> None:
    """Test 15m interval-based models using the interval names as keys."""

    def format_key() -> list[str]:
        return [
            f"{hour:02d}{minute:02d}"
            for hour in range(0, 24)
            for minute in range(0, 60, 15)
        ]

    interval_name_models: dict[str, MockModel] = {
        key: MockModel(f"{key} Model") for key in format_key()
    }

    @dataclass
    class IntervalCustomModelRepository(ModelRepository):
        """Repository to manage a custom interval-based models."""

        directory: str
        file_name: str

        def _get_allowed_keys(self) -> list[Any]:
            return format_key()

        def _build_path(self, key: str) -> str:
            return f"{self.directory}/{key}/{self.file_name}"

    pickled_models = [pickle.dumps(model) for model in interval_name_models.values()]
    mock_files = [
        mock_open(read_data=pickled_model)() for pickled_model in pickled_models
    ]

    with patch("builtins.open", side_effect=mock_files), patch(
        "os.path.exists", return_value=True
    ):
        model_repository = IntervalCustomModelRepository(
            directory="models/consumption/interval", file_name="model.pkl"
        )
        model_manager: ModelManager[MockModel] = ModelManager(
            model_repository=model_repository
        )

        midnight_model = model_manager.get_model("0000")
        assert midnight_model.data == interval_name_models["0000"].data

        midday_model = model_manager["1200"]
        assert midday_model.data == interval_name_models["1200"].data

        sunrise_model = model_manager["0615"]
        assert sunrise_model.data == interval_name_models["0615"].data

        sunset_model = model_manager.get_model("2045")
        assert sunset_model.data == interval_name_models["2045"].data

    await model_manager._stop_model_monitor()  # pylint: disable=protected-access
