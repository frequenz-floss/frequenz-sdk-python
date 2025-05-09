# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Tests for logging config updater."""

import asyncio
import logging
from typing import Any

import pytest
from frequenz.channels import Broadcast
from marshmallow import ValidationError
from pytest_mock import MockerFixture

from frequenz.sdk.config import (
    LoggerConfig,
    LoggingConfig,
    LoggingConfigUpdatingActor,
    RootLoggerConfig,
    load_config,
)


def test_logging_config() -> None:
    """Test if logging config is correctly loaded."""
    config_raw = {
        "root_logger": {"level": "DEBUG"},
        "loggers": {
            "actor": {"name": "frequenz.sdk.actor", "level": "INFO"},
            "timeseries": {"name": "frequenz.sdk.timeseries", "level": "ERROR"},
        },
    }
    config = LoggingConfig(
        root_logger=RootLoggerConfig(level="DEBUG"),
        loggers={
            "actor": LoggerConfig(name="frequenz.sdk.actor", level="INFO"),
            "timeseries": LoggerConfig(name="frequenz.sdk.timeseries", level="ERROR"),
        },
    )

    assert load_config(LoggingConfig, config_raw) == config

    config_raw = {}
    config = LoggingConfig()
    assert load_config(LoggingConfig, config_raw) == config

    config_raw = {"root_logger": {"level": "UNKNOWN"}}
    with pytest.raises(ValidationError):
        load_config(LoggingConfig, config_raw)

    config_raw = {"unknown": {"frequenz.sdk.actor": {"level": "DEBUG"}}}
    assert load_config(LoggingConfig, config_raw) == config


@pytest.fixture
def cleanup_logs() -> Any:
    """Reset logging to default.

    Python doesn't cleanup logging configuration after tests, so we need to do it manually.
    """
    yield

    logging.getLogger("frequenz.sdk.actor").setLevel(logging.NOTSET)
    logging.getLogger("frequenz.sdk.timeseries").setLevel(logging.NOTSET)


async def test_logging_config_updating_actor(
    mocker: MockerFixture,
    cleanup_logs: Any,
) -> None:
    """Test if logging is configured and updated correctly."""
    # Mock method that sets root level logging.
    # Python doesn't cleanup logging configuration after tests.
    # Overriding logging.basicConfig would mess up other tests, so we mock it.
    # This is just for extra safety because changing root logging level in unit tests
    # is not working anyway - python ignores it.
    mocker.patch("frequenz.sdk.config._logging_actor.logging.basicConfig")

    # Mock ConfigManager
    mock_config_manager = mocker.Mock()
    mock_config_manager.config_channel = Broadcast[LoggingConfig | Exception | None](
        name="config"
    )
    mock_config_manager.new_receiver = mocker.Mock(
        return_value=mock_config_manager.config_channel.new_receiver()
    )

    async with LoggingConfigUpdatingActor(mock_config_manager) as actor:
        assert logging.getLogger("frequenz.sdk.actor").level == logging.NOTSET
        assert logging.getLogger("frequenz.sdk.timeseries").level == logging.NOTSET

        update_logging_spy = mocker.spy(actor, "_update_logging")

        # Send first config
        expected_config = LoggingConfig(
            root_logger=RootLoggerConfig(level="ERROR"),
            loggers={
                "actor": LoggerConfig(name="frequenz.sdk.actor", level="DEBUG"),
                "timeseries": LoggerConfig(
                    name="frequenz.sdk.timeseries", level="ERROR"
                ),
            },
        )
        await mock_config_manager.config_channel.new_sender().send(expected_config)
        await asyncio.sleep(0.01)
        update_logging_spy.assert_called_once_with(expected_config)
        assert logging.getLogger("frequenz.sdk.actor").level == logging.DEBUG
        assert logging.getLogger("frequenz.sdk.timeseries").level == logging.ERROR
        update_logging_spy.reset_mock()

        # Send an exception and verify the previous config is maintained
        await mock_config_manager.config_channel.new_sender().send(
            ValueError("Test error")
        )
        await asyncio.sleep(0.01)
        update_logging_spy.assert_not_called()  # Should not try to update logging
        # Previous config should be maintained
        assert logging.getLogger("frequenz.sdk.actor").level == logging.DEBUG
        assert logging.getLogger("frequenz.sdk.timeseries").level == logging.ERROR
        assert (
            actor._current_config == expected_config  # pylint: disable=protected-access
        )  # pylint: disable=protected-access
        update_logging_spy.reset_mock()

        # Update config
        expected_config = LoggingConfig(
            root_logger=RootLoggerConfig(level="WARNING"),
            loggers={
                "actor": LoggerConfig(name="frequenz.sdk.actor", level="INFO"),
            },
        )
        await mock_config_manager.config_channel.new_sender().send(expected_config)
        await asyncio.sleep(0.01)
        update_logging_spy.assert_called_once_with(expected_config)
        assert logging.getLogger("frequenz.sdk.actor").level == logging.INFO
        assert logging.getLogger("frequenz.sdk.timeseries").level == logging.NOTSET
        update_logging_spy.reset_mock()

        # Send a None config to make sure actor doesn't crash and configures a default logging
        await mock_config_manager.config_channel.new_sender().send(None)
        await asyncio.sleep(0.01)
        update_logging_spy.assert_called_once_with(LoggingConfig())
        assert (
            actor._current_config == LoggingConfig()  # pylint: disable=protected-access
        )
        update_logging_spy.reset_mock()
