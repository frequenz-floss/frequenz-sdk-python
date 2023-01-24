# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Test for Config"""
import pathlib
import re
from typing import Any, Dict, List, Optional, Set

import pytest

# pylint: disable = no-name-in-module
import toml
from pydantic import BaseModel, StrictBool, StrictFloat, StrictInt

from frequenz.sdk.config import Config


class Item(BaseModel):
    """Test item"""

    item_id: int
    name: str


class TestConfig:
    """Test for Config"""

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
    list_non_strict_bool = '["false", "0", "true", "1"]'
    item_data = '[{"item_id": 1, "name": "My Item"}]'
    dict_str_int = '{"a": 1, "b": 2, "c": 3}'
    var_none = 'null'
    my_dict_key1 = 'value1'
    my_dict2_key1 = '[1,2,3,3]'
    my_dict2_key2 = '[3]'
    my_dict3_key1 = '["test"]'
    """

    @pytest.fixture()
    def config_file(self, tmp_path: pathlib.Path) -> pathlib.Path:
        """Create a test config file."""
        file_path = tmp_path / TestConfig.conf_path
        file_path.parent.mkdir()
        file_path.touch()
        file_path.write_text(TestConfig.conf_content)
        return file_path

    def test_get(self, config_file: pathlib.Path) -> None:
        """Test get function"""
        conf_vars = toml.load(config_file)
        config = Config(conf_vars=conf_vars)

        assert config.get("logging_lvl") == "DEBUG"
        assert config.get("var1") == "1"
        assert config.get("var2") is None
        assert config.get("var2", default=0) == 0

    def test_getitem(self, config_file: pathlib.Path) -> None:
        """Test getitem function"""
        conf_vars = toml.load(config_file)
        config = Config(conf_vars=conf_vars)

        assert config["logging_lvl"] == "DEBUG"
        assert config["var1"] == "1"
        with pytest.raises(KeyError, match="Unknown config name var2"):
            assert config["var2"]

    def test_contains(self, config_file: pathlib.Path) -> None:
        """Test contains function"""
        conf_vars = toml.load(config_file)
        config = Config(conf_vars=conf_vars)

        assert "logging_lvl" in config
        assert "var1" in config
        assert "var2" not in config

    @pytest.mark.parametrize(
        "key, expected_type, value",
        [
            ("logging_lvl", str, "DEBUG"),
            ("var_int", int, 5),
            ("var_int", StrictInt, 5),
            ("var_int", float, 5.0),
            ("var_float", float, 3.14),
            ("var_float", StrictFloat, 3.14),
            ("var_float", int, 3),
            ("var_bool", int, 1),
            ("var_bool", float, 1.0),
            ("var_bool", bool, True),
            ("var1", bool, 1),
            ("var_bool", StrictBool, True),
            ("list_int", List[int], [1, 2, 3]),
            ("list_int", List[StrictInt], [1, 2, 3]),
            ("list_float", List[int], [1, 2, 3]),
            ("list_int", List[float], [1.0, 2.0, 3.0]),
            ("list_float", List[float], [1, 2.0, 3.5]),
            ("list_non_strict_bool", List[bool], 2 * [False] + 2 * [True]),
            ("list_non_strict_bool", List[str], ["false", "0", "true", "1"]),
            ("item_data", List[Item], [Item(item_id=1, name="My Item")]),
            ("dict_str_int", Dict[str, int], {"a": 1, "b": 2, "c": 3}),
            ("var_none", Optional[float], None),
        ],
    )
    def test_get_as_success(
        self, key: str, expected_type: Any, value: Any, config_file: pathlib.Path
    ) -> None:
        """Test get_as function with proper arguments"""

        conf_vars = toml.load(config_file)
        config = Config(conf_vars=conf_vars)
        result = config.get_as(key, expected_type)
        assert result == value

    @pytest.mark.parametrize(
        "key, expected_type",
        [
            ("var_float", StrictInt),
            ("var_int", StrictFloat),
            ("var1", StrictBool),
            ("list_float", List[StrictInt]),
            ("list_int", List[StrictFloat]),
            ("list_non_strict_bool", List[int]),
        ],
    )
    def test_get_as_validation_error(
        self, key: str, expected_type: Any, config_file: pathlib.Path
    ) -> None:
        """Test get_as function which raise ValidationError"""
        conf_vars = toml.load(config_file)
        config = Config(conf_vars=conf_vars)

        err_msg = (
            f"Could not convert config variable: {key} = '{config[key]}' "
            f"to type {str(expected_type)}"
        )

        with pytest.raises(ValueError, match=re.escape(err_msg)):
            config.get_as(key, expected_type)

    @pytest.mark.parametrize(
        "key_prefix, expected_values_type, value",
        [
            ("my_dict_", str, {"key1": "value1"}),
            ("my_dict2_", Set[int], {"key1": {1, 2, 3}, "key2": {3}}),
        ],
    )
    def test_get_dict_values_success(
        self,
        key_prefix: str,
        expected_values_type: Any,
        value: Any,
        config_file: pathlib.Path,
    ) -> None:
        """Test get_as function with proper arguments"""

        conf_vars = toml.load(config_file)
        config = Config(conf_vars=conf_vars)
        result = config.get_dict(key_prefix, expected_values_type)
        assert result == value

    @pytest.mark.parametrize(
        "key_prefix, expected_values_type",
        [
            ("my_dict_", int),
            ("my_dict2_", int),
            ("my_dict3_", Set[int]),
        ],
    )
    def test_get_dict_success(
        self, key_prefix: str, expected_values_type: Any, config_file: pathlib.Path
    ) -> None:
        """Test get_as function with proper arguments"""

        conf_vars = toml.load(config_file)
        config = Config(conf_vars=conf_vars)

        err_msg_re = (
            f"Could not convert config variable: {re.escape(key_prefix)}.* = '.*' "
            f"to type {re.escape(str(expected_values_type))}"
        )

        with pytest.raises(ValueError, match=re.compile(err_msg_re)):
            print(config.get_dict(key_prefix, expected_values_type))
