# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Test for ConfigManager"""
import pathlib

import pytest
from frequenz.channels import Broadcast

# pylint: disable = no-name-in-module
from pydantic import BaseModel

from frequenz.sdk.configs import Config
from frequenz.sdk.configs.config_manager import ConfigManager


class Item(BaseModel):
    """Test item"""

    item_id: int
    name: str


def create_content(number: int) -> str:
    """Utility function to create content to be written to a config file."""
    return f"""
    logging_lvl = "ERROR"
    var1 = "0"
    var2 = "{number}"
    """


class TestActorConfigManager:
    """Test for ConfigManager"""

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
    """

    @pytest.fixture()
    def config_file(self, tmp_path: pathlib.Path) -> pathlib.Path:
        """Create a test config file."""
        file_path = tmp_path / TestActorConfigManager.conf_path
        file_path.parent.mkdir()
        file_path.touch()
        file_path.write_text(TestActorConfigManager.conf_content)
        return file_path

    @pytest.fixture()
    def real_config_file(
        self, tmp_path: pathlib.Path = pathlib.Path("/tmp/")
    ) -> pathlib.Path:
        """Create a test config file."""
        file_path = tmp_path / TestActorConfigManager.conf_path
        if not file_path.exists():
            file_path.parent.mkdir()
            file_path.touch()
        file_path.write_text(TestActorConfigManager.conf_content)
        return file_path

    async def test_update(self, config_file: pathlib.Path) -> None:
        """
        Test ConfigManager by checking if:
        - the initial content of the content file is correct
        - the config file modifications are picked up and the new content
            is correct
        """
        config_channel: Broadcast[Config] = Broadcast(
            "Config Channel", resend_latest=True
        )
        _config_manager = ConfigManager(
            conf_file=str(config_file), output=config_channel.new_sender()
        )

        config_receiver = config_channel.new_receiver()

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

        # pylint: disable=protected-access,no-member
        await _config_manager._stop()  # type: ignore
