# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Test for ConfigManager."""
import os
import pathlib
from collections import defaultdict
from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass
from typing import Any

import pytest
from frequenz.channels import Broadcast

from frequenz.sdk.config import ConfigManagingActor
from frequenz.sdk.config._config_managing import _recursive_update


class Item:
    """Test item."""

    item_id: int
    name: str


def create_content(number: int) -> str:
    """Create content to be written to a config file."""
    return f"""
    logging_lvl = "ERROR"
    var1 = "0"
    var2 = "{number}"
    """


class TestActorConfigManager:
    """Test for ConfigManager."""

    conf_path = "sdk/config.toml"
    conf_content = """
    logging_lvl = 'DEBUG'
    var1 = "1"
    var_int = "5"
    var_float = "3.14"
    var_bool = "true"
    list_int = "[1,2,3]"
    list_float = "[1,2.0,3.5]"
    var_off = "off"
    list_non_strict_bool = ["false", "0", "true", "1"]
    item_data = '[{"item_id": 1, "name": "My Item"}]'
    var_none = 'null'
    [dict_str_int]
    a = 1
    b = 2
    c = 3
    """

    @pytest.fixture()
    def config_file(self, tmp_path: pathlib.Path) -> pathlib.Path:
        """Create a test config file."""
        file_path = tmp_path / TestActorConfigManager.conf_path
        if not file_path.exists():
            file_path.parent.mkdir()
            file_path.touch()
        file_path.write_text(TestActorConfigManager.conf_content)
        return file_path

    async def test_update(self, config_file: pathlib.Path) -> None:
        """Test ConfigManager.

        Check if:

        - the initial content of the content file is correct
        - the config file modifications are picked up and the new content is correct
        """
        config_channel: Broadcast[Mapping[str, Any]] = Broadcast(
            name="Config Channel", resend_latest=True
        )
        config_receiver = config_channel.new_receiver()

        async with ConfigManagingActor(
            [config_file], config_channel.new_sender(), force_polling=False
        ):
            config = await config_receiver.receive()
            assert config is not None
            assert config.get("logging_lvl") == "DEBUG"
            assert config.get("var1") == "1"
            assert config.get("var2") is None
            assert config.get("var3") is None

            number = 5
            config_file.write_text(create_content(number=number))

            config = await config_receiver.receive()
            assert config is not None
            assert config.get("logging_lvl") == "ERROR"
            assert config.get("var1") == "0"
            assert config.get("var2") == str(number)
            assert config.get("var3") is None
            assert config_file.read_text() == create_content(number=number)

    async def test_update_relative_path(self, config_file: pathlib.Path) -> None:
        """Test ConfigManagingActor with a relative path."""
        config_channel: Broadcast[Mapping[str, Any]] = Broadcast(
            name="Config Channel", resend_latest=True
        )
        config_receiver = config_channel.new_receiver()

        current_dir = pathlib.Path.cwd()
        relative_path = os.path.relpath(config_file, current_dir)

        async with ConfigManagingActor(
            [relative_path], config_channel.new_sender(), force_polling=False
        ):
            config = await config_receiver.receive()
            assert config is not None
            assert config.get("var2") is None

            number = 8
            config_file.write_text(create_content(number=number))

            config = await config_receiver.receive()
            assert config is not None
            assert config.get("var2") == str(number)
            assert config_file.read_text() == create_content(number=number)

    async def test_update_multiple_files(self, config_file: pathlib.Path) -> None:
        """Test ConfigManagingActor with multiple files."""
        config_channel: Broadcast[Mapping[str, Any]] = Broadcast(
            name="Config Channel", resend_latest=True
        )
        config_receiver = config_channel.new_receiver()

        config_file2 = config_file.parent / "config2.toml"
        config_file2.write_text(
            """
            logging_lvl = 'ERROR'
            var1 = "0"
            var2 = "15"
            var_float = "3.14"
            list_int = "[1,3]"
            list_float = "[1,2.0,3.8,12.3]"
            var_off = "off"
            item_data = '[{"item_id": 1, "name": "My Item 2"}]'
            [dict_str_int]
            a = 1
            b = 2
            c = 4
            d = 3
            """
        )

        async with ConfigManagingActor(
            [config_file, config_file2],
            config_channel.new_sender(),
            force_polling=False,
        ):
            config = await config_receiver.receive()
            assert config is not None
            # Both config files are read and merged, and config_file2 overrides
            # the variables present in config_file.
            assert config == {
                "logging_lvl": "ERROR",
                "var1": "0",
                "var2": "15",
                "var_int": "5",
                "var_float": "3.14",
                "var_bool": "true",
                "list_int": "[1,3]",
                "list_float": "[1,2.0,3.8,12.3]",
                "var_off": "off",
                "list_non_strict_bool": ["false", "0", "true", "1"],
                "item_data": '[{"item_id": 1, "name": "My Item 2"}]',
                "dict_str_int": {"a": 1, "b": 2, "c": 4, "d": 3},
                "var_none": "null",
            }

            # We overwrite config_file with just two variables
            config_file.write_text(
                """
                logging_lvl = 'INFO'
                list_non_strict_bool = ["false", "0", "true"]
                """
            )

            config = await config_receiver.receive()
            assert config is not None
            # The config now doesn't have the old variables only present in config_file,
            # config_file2 still takes precedence, so no changes in the result other
            # than having less variables (the variables only present in the old
            # config_file are gone)
            assert config == {
                "logging_lvl": "ERROR",
                "var1": "0",
                "var2": "15",
                "var_float": "3.14",
                "list_int": "[1,3]",
                "list_float": "[1,2.0,3.8,12.3]",
                "var_off": "off",
                "item_data": '[{"item_id": 1, "name": "My Item 2"}]',
                "list_non_strict_bool": ["false", "0", "true"],
                "dict_str_int": {"a": 1, "b": 2, "c": 4, "d": 3},
            }

            # Now we only update logging_lvl in config_file2, it still takes precedence
            config_file2.write_text(
                """
                    logging_lvl = 'DEBUG'
                    var1 = "0"
                    var2 = "15"
                    var_float = "3.14"
                    list_int = "[1,3]"
                    list_float = "[1,2.0,3.8,12.3]"
                    var_off = "off"
                    item_data = '[{"item_id": 1, "name": "My Item 2"}]'
                    [dict_str_int]
                    a = 1
                    b = 2
                    c = 4
                    d = 3
                    """
            )

            config = await config_receiver.receive()
            assert config is not None
            assert config == {
                "logging_lvl": "DEBUG",
                "var1": "0",
                "var2": "15",
                "var_float": "3.14",
                "list_int": "[1,3]",
                "list_float": "[1,2.0,3.8,12.3]",
                "var_off": "off",
                "item_data": '[{"item_id": 1, "name": "My Item 2"}]',
                "list_non_strict_bool": ["false", "0", "true"],
                "dict_str_int": {"a": 1, "b": 2, "c": 4, "d": 3},
            }

            # Now add one variable to config_file not present in config_file2 and remove
            # a bunch of variables from config_file2 too, and update a few
            config_file.write_text(
                """
                logging_lvl = 'INFO'
                var10 = "10"
                """
            )
            config_file2.write_text(
                """
                    logging_lvl = 'DEBUG'
                    var1 = "3"
                    var_off = "on"
                    [dict_str_int]
                    a = 1
                    b = 2
                    c = 4
                    """
            )

            config = await config_receiver.receive()
            assert config is not None
            assert config == {
                "logging_lvl": "DEBUG",
                "var1": "3",
                "var10": "10",
                "var_off": "on",
                "dict_str_int": {"a": 1, "b": 2, "c": 4},
            }


@dataclass(frozen=True, kw_only=True)
class RecursiveUpdateTestCase:
    """A test case for the recursive_update function."""

    title: str
    d1: dict[str, Any]
    d2: MutableMapping[str, Any]
    expected: dict[str, Any]


# Define all test cases as instances of TestCase
recursive_update_test_cases = [
    RecursiveUpdateTestCase(
        title="Basic Update",
        d1={"a": 1, "b": 2, "c": {"d": 3, "e": 4}},
        d2={"b": 5, "h": 10, "c": {"d": 30}},
        expected={"a": 1, "b": 5, "h": 10, "c": {"d": 30, "e": 4}},
    ),
    RecursiveUpdateTestCase(
        title="Nested Update",
        d1={"a": {"b": 1, "c": {"d": 2}}, "e": 3},
        d2={"a": {"c": {"d": 20, "f": 4}, "g": 5}, "h": 6},
        expected={"a": {"b": 1, "c": {"d": 20, "f": 4}, "g": 5}, "e": 3, "h": 6},
    ),
    RecursiveUpdateTestCase(
        title="Non-Dict Overwrite",
        d1={"a": {"b": 1}, "c": 2},
        d2={"a": 10, "c": {"d": 3}},
        expected={"a": 10, "c": {"d": 3}},
    ),
    RecursiveUpdateTestCase(
        title="Empty d2",
        d1={"a": 1, "b": {"c": 2}},
        d2={},
        expected={"a": 1, "b": {"c": 2}},
    ),
    RecursiveUpdateTestCase(
        title="Empty d1",
        d1={},
        d2={"a": 1, "b": {"c": 2}},
        expected={"a": 1, "b": {"c": 2}},
    ),
    RecursiveUpdateTestCase(
        title="Non-Mapping Values Overwrite",
        d1={"a": {"b": [1, 2, 3]}, "c": "hello"},
        d2={"a": {"b": [4, 5]}, "c": "world"},
        expected={"a": {"b": [4, 5]}, "c": "world"},
    ),
    RecursiveUpdateTestCase(
        title="Multiple Data Types",
        d1={"int": 1, "float": 1.0, "str": "one", "list": [1, 2, 3], "dict": {"a": 1}},
        d2={"int": 2, "float": 2.0, "str": "two", "list": [4, 5], "dict": {"b": 2}},
        expected={
            "int": 2,
            "float": 2.0,
            "str": "two",
            "list": [4, 5],
            "dict": {"a": 1, "b": 2},
        },
    ),
    RecursiveUpdateTestCase(
        title="Defaultdict Handling",
        d1=defaultdict(dict, {"a": {"b": 1}, "c": 2}),
        d2={"a": {"c": 3}, "d": 4},
        expected={"a": {"b": 1, "c": 3}, "c": 2, "d": 4},
    ),
    # Additional Test Cases
    RecursiveUpdateTestCase(
        title="Overwrite with Nested Non-Mapping",
        d1={"x": {"y": {"z": 1}}},
        d2={"x": {"y": 2}},
        expected={"x": {"y": 2}},
    ),
    RecursiveUpdateTestCase(
        title="Overwrite with Nested Mapping",
        d1={"x": {"y": 2}},
        d2={"x": {"y": {"z": 3}}},
        expected={"x": {"y": {"z": 3}}},
    ),
]


@pytest.mark.parametrize(
    "test_case", recursive_update_test_cases, ids=lambda tc: tc.title
)
def test_recursive_update(test_case: RecursiveUpdateTestCase) -> None:
    """Test the recursive_update function with various input dictionaries."""
    assert _recursive_update(test_case.d1, test_case.d2) == test_case.expected
