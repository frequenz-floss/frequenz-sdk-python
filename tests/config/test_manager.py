# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Tests for the config manager module."""


import asyncio
import dataclasses
import logging
import pathlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, assert_never

import marshmallow
import pytest
import pytest_mock

from frequenz.sdk.config import ConfigManager, InvalidValueForKeyError, wait_for_first
from frequenz.sdk.config._manager import _get_key


@dataclass
class SimpleConfig:
    """A simple configuration class for testing."""

    name: str = dataclasses.field(metadata={"validate": lambda s: s.startswith("test")})
    value: int


@dataclass(frozen=True, kw_only=True)
class ReceiverTestCase:
    """A test case for testing new_receiver configurations."""

    title: str
    key: str | tuple[str, ...]
    config_class: type[SimpleConfig]
    input_config: dict[str, Any]
    expected_output: Any | None
    base_schema: type[marshmallow.Schema] | None = None
    marshmallow_load_kwargs: dict[str, Any] | None = None


# Test cases for new_receiver
receiver_test_cases = [
    ReceiverTestCase(
        title="Basic Config",
        key="test",
        config_class=SimpleConfig,
        input_config={"test": {"name": "test1", "value": 42}},
        expected_output=SimpleConfig(name="test1", value=42),
    ),
    ReceiverTestCase(
        title="Nested Key Config",
        key=("nested", "config"),
        config_class=SimpleConfig,
        input_config={"nested": {"config": {"name": "test2", "value": 43}}},
        expected_output=SimpleConfig(name="test2", value=43),
    ),
    ReceiverTestCase(
        title="Validation Error",
        key="test",
        config_class=SimpleConfig,
        input_config={"test": {"name": "no-test1", "value": 42}},
        expected_output="{'name': ['Invalid value.']}",
    ),
    ReceiverTestCase(
        title="Invalid Value Type",
        key="test",
        config_class=SimpleConfig,
        input_config={"test": "not a mapping"},
        expected_output="Value for key ['test'] is not a mapping: 'not a mapping'",
    ),
    ReceiverTestCase(
        title="Raise on unknown",
        key="test",
        config_class=SimpleConfig,
        marshmallow_load_kwargs={"unknown": marshmallow.RAISE},
        input_config={"test": {"name": "test3", "value": 44, "not_allowed": 42}},
        expected_output="{'not_allowed': ['Unknown field.']}",
    ),
    ReceiverTestCase(
        title="Missing Key",
        key="missing",
        config_class=SimpleConfig,
        input_config={"test": {"name": "test3", "value": 44}},
        expected_output=None,
    ),
]


@pytest.mark.parametrize("test_case", receiver_test_cases, ids=lambda tc: tc.title)
async def test_new_receiver_configurations(
    test_case: ReceiverTestCase, mocker: pytest_mock.MockFixture
) -> None:
    """Test different configurations for new_receiver."""
    mocker.patch("frequenz.sdk.config._manager.ConfigManagingActor")
    config_manager = ConfigManager([pathlib.Path("dummy.toml")])
    await config_manager.config_channel.new_sender().send(test_case.input_config)
    receiver = config_manager.new_receiver(
        test_case.key,
        test_case.config_class,
        base_schema=test_case.base_schema,
        marshmallow_load_kwargs=test_case.marshmallow_load_kwargs,
    )

    async with asyncio.timeout(1):
        result = await receiver.receive()
    match result:
        case SimpleConfig() | None:
            assert result == test_case.expected_output
        case Exception():
            assert str(result) == str(test_case.expected_output)
        case unexpected:
            assert_never(unexpected)


async def test_warn_on_unknown_key(
    mocker: pytest_mock.MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that a warning is logged when an unknown key is received."""
    mocker.patch("frequenz.sdk.config._manager.ConfigManagingActor")
    config_manager = ConfigManager([pathlib.Path("dummy.toml")])
    await config_manager.config_channel.new_sender().send(
        {"test": {"name": "test3", "value": 44, "not_allowed": 42}}
    )
    receiver = config_manager.new_receiver("test", SimpleConfig)

    async with asyncio.timeout(1):
        await receiver.receive()

    expected_log_entry = (
        "frequenz.sdk.config._manager",
        logging.WARNING,
        "The configuration for key 'test' has extra fields that will be ignored: "
        "{'not_allowed': ['Unknown field.']}",
    )
    assert expected_log_entry in caplog.record_tuples


async def test_skip_config_update_bursts(mocker: pytest_mock.MockerFixture) -> None:
    """Test that a burst of updates will only send the last update."""
    mocker.patch("frequenz.sdk.config._manager.ConfigManagingActor")
    config_manager = ConfigManager([pathlib.Path("dummy.toml")])
    sender = config_manager.config_channel.new_sender()
    receiver = config_manager.new_receiver(
        "test",
        SimpleConfig,
        skip_unchanged=True,
    )

    await sender.send({"test": {"name": "test1", "value": 42}})
    await sender.send({"test": {"name": "test2", "value": 43}})
    await sender.send({"test": {"name": "test3", "value": 44}})

    # Should only receive one orig_config and then the changed_config
    async with asyncio.timeout(1):
        result = await receiver.receive()
    assert result == SimpleConfig(name="test3", value=44)

    # There should be no more messages
    with pytest.raises(asyncio.TimeoutError):
        async with asyncio.timeout(0.1):
            await receiver.receive()


async def test_skip_unchanged_config(mocker: pytest_mock.MockerFixture) -> None:
    """Test that unchanged configurations are skipped when skip_unchanged is True."""
    mocker.patch("frequenz.sdk.config._manager.ConfigManagingActor")
    config_manager = ConfigManager([pathlib.Path("dummy.toml")])
    sender = config_manager.config_channel.new_sender()
    receiver = config_manager.new_receiver(
        "test",
        SimpleConfig,
        skip_unchanged=True,
    )

    # A first config should be received
    orig_config = {"test": {"name": "test1", "value": 42}}
    await sender.send(orig_config)
    async with asyncio.timeout(1):
        result = await receiver.receive()
    assert result == SimpleConfig(name="test1", value=42)

    # An unchanged config should be skipped (no message received)
    await sender.send(orig_config)
    with pytest.raises(asyncio.TimeoutError):
        async with asyncio.timeout(0.1):
            await receiver.receive()

    # A changed config should be received
    changed_config = {"test": {"name": "test2", "value": 43}}
    await sender.send(changed_config)
    async with asyncio.timeout(1):
        result = await receiver.receive()
    assert result == SimpleConfig(name="test2", value=43)

    # There should be no more messages
    with pytest.raises(asyncio.TimeoutError):
        async with asyncio.timeout(0.1):
            await receiver.receive()


async def test_wait_for_first(mocker: pytest_mock.MockerFixture) -> None:
    """Test wait_for_first function."""
    mocker.patch("frequenz.sdk.config._manager.ConfigManagingActor")
    config_manager = ConfigManager([pathlib.Path("dummy.toml")])

    receiver = config_manager.new_receiver(
        "test",
        SimpleConfig,
    )

    async with asyncio.timeout(0.2):
        with pytest.raises(asyncio.TimeoutError):
            await wait_for_first(receiver, timeout=timedelta(seconds=0.1))

    # Test successful wait
    await config_manager.config_channel.new_sender().send(
        {"test": {"name": "test1", "value": 42}}
    )
    async with asyncio.timeout(0.2):
        result = await wait_for_first(receiver, timeout=timedelta(seconds=0.1))
    assert result == SimpleConfig(name="test1", value=42)


def test_unknown_include_not_supported() -> None:
    """Test that unknown marshmallow load kwargs are not supported."""
    with pytest.raises(ValueError):
        ConfigManager([pathlib.Path("dummy.toml")]).new_receiver(
            "test",
            SimpleConfig,
            marshmallow_load_kwargs={"unknown": marshmallow.INCLUDE},
        )


@pytest.mark.integration
class TestConfigManagerIntegration:
    """Integration tests for ConfigManager."""

    @pytest.fixture
    def config_file(self, tmp_path: pathlib.Path) -> pathlib.Path:
        """Create a temporary config file for testing."""
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            """
            [test]
            name = "test1"
            value = 42

            [logging.loggers.test]
            name = "test"
            level = "DEBUG"
            """
        )
        return config_file

    async def test_full_config_flow(self, config_file: pathlib.Path) -> None:
        """Test the complete flow of configuration management."""
        async with (
            # Disabling force_polling is a hack because of a bug in watchfiles not
            # detecting sub-second changes when using polling.
            ConfigManager([config_file], force_polling=False) as config_manager,
            asyncio.timeout(1),
        ):
            receiver = config_manager.new_receiver("test", SimpleConfig)
            first_config = await wait_for_first(receiver)
            assert first_config == SimpleConfig(name="test1", value=42)
            assert logging.getLogger("test").level == logging.DEBUG

            # Update config file
            config_file.write_text(
                """
                [test]
                name = "test2"
                value = 43

                [logging.loggers.test]
                name = "test"
                level = "INFO"
                """
            )

            # Check updated config
            config = await receiver.receive()
            assert config == SimpleConfig(name="test2", value=43)

            # Check updated logging config
            assert logging.getLogger("test").level == logging.INFO

    async def test_full_config_flow_without_logging(
        self, config_file: pathlib.Path
    ) -> None:
        """Test the complete flow of configuration management without logging."""
        logging.getLogger("test").setLevel(logging.WARNING)
        async with (
            # Disabling force_polling is a hack because of a bug in watchfiles not
            # detecting sub-second changes when using polling.
            ConfigManager(
                [config_file], logging_config_key=None, force_polling=False
            ) as config_manager,
            asyncio.timeout(1),
        ):
            receiver = config_manager.new_receiver("test", SimpleConfig)
            first_config = await wait_for_first(receiver)
            assert first_config == SimpleConfig(name="test1", value=42)
            assert logging.getLogger("test").level == logging.WARNING

            # Update config file
            config_file.write_text(
                """
                [test]
                name = "test2"
                value = 43

                [logging.loggers.test]
                name = "test"
                level = "DEBUG"
                """
            )

            # Check updated config
            config = await receiver.receive()
            assert config == SimpleConfig(name="test2", value=43)

            # Check updated logging config
            assert logging.getLogger("test").level == logging.WARNING


@dataclass(frozen=True)
class GetKeyTestCase:
    """Test case for _get_key function."""

    title: str
    config: dict[str, Any]
    key: str | Sequence[str]
    expected_result: Mapping[str, Any] | None | type[InvalidValueForKeyError]
    expected_error_key: list[str] | None = None
    expected_error_value: Any | None = None


_get_key_test_cases = [
    # Simple string key tests
    GetKeyTestCase(
        title="Simple string key - exists",
        config={"a": {"b": 1}},
        key="a",
        expected_result={"b": 1},
    ),
    GetKeyTestCase(
        title="Simple string key - doesn't exist",
        config={"a": {"b": 1}},
        key="x",
        expected_result=None,
    ),
    GetKeyTestCase(
        title="Simple string key - invalid value type",
        config={"a": 42},
        key="a",
        expected_result=InvalidValueForKeyError,
        expected_error_key=["a"],
        expected_error_value=42,
    ),
    # Sequence key tests
    GetKeyTestCase(
        title="Sequence key - all exist",
        config={"a": {"b": {"c": {"d": 1}}}},
        key=["a", "b", "c"],
        expected_result={"d": 1},
    ),
    GetKeyTestCase(
        title="Sequence key - middle doesn't exist",
        config={"a": {"b": {"c": 1}}},
        key=["a", "x", "c"],
        expected_result=None,
    ),
    GetKeyTestCase(
        title="Sequence key - invalid value in middle",
        config={"a": {"b": 42, "c": 1}},
        key=["a", "b", "c"],
        expected_result=InvalidValueForKeyError,
        expected_error_key=["a", "b"],
        expected_error_value=42,
    ),
    GetKeyTestCase(
        title="Empty sequence key",
        config={"a": 1},
        key=[],
        expected_result={"a": 1},
    ),
    # Empty string tests
    GetKeyTestCase(
        title="Empty string key",
        config={"": {"a": 1}},
        key="",
        expected_result={"a": 1},
    ),
    GetKeyTestCase(
        title="Empty string in sequence",
        config={"": {"": {"a": 1}}},
        key=["", ""],
        expected_result={"a": 1},
    ),
    # None value tests
    GetKeyTestCase(
        title="Value is None",
        config={"a": None},
        key="a",
        expected_result=None,
    ),
    GetKeyTestCase(
        title="Nested None value",
        config={"a": {"b": None}},
        key=["a", "b", "c"],
        expected_result=None,
    ),
    # Special string key tests to verify string handling
    GetKeyTestCase(
        title="String that looks like a sequence",
        config={"key": {"e": 1}},
        key="key",
        expected_result={"e": 1},
    ),
    GetKeyTestCase(
        title="String with special characters",
        config={"a.b": {"c": 1}},
        key="a.b",
        expected_result={"c": 1},
    ),
    # Nested mapping tests
    GetKeyTestCase(
        title="Deeply nested valid path",
        config={"a": {"b": {"c": {"d": {"e": {"f": 1}}}}}},
        key=["a", "b", "c", "d", "e"],
        expected_result={"f": 1},
    ),
    GetKeyTestCase(
        title="Mixed type nested invalid",
        config={"a": {"b": [1, 2, 3]}},
        key=["a", "b"],
        expected_result=InvalidValueForKeyError,
        expected_error_key=["a", "b"],
        expected_error_value=[1, 2, 3],
    ),
]


@pytest.mark.parametrize("test_case", _get_key_test_cases, ids=lambda tc: tc.title)
def test_get_key(test_case: GetKeyTestCase) -> None:
    """Test the _get_key function with various inputs.

    Args:
        test_case: The test case to run.
    """
    if test_case.expected_result is InvalidValueForKeyError:
        with pytest.raises(InvalidValueForKeyError) as exc_info:
            _get_key(test_case.config, test_case.key)

        # Verify the error details
        assert exc_info.value.key == test_case.expected_error_key
        assert exc_info.value.value == test_case.expected_error_value
    else:
        result = _get_key(test_case.config, test_case.key)
        assert result == test_case.expected_result
