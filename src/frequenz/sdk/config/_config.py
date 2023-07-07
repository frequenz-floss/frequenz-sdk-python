# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Read and update config variables."""

import logging
from typing import Any, Dict, Optional, TypeVar

# pylint not finding parse_raw_as is a false positive
from pydantic import ValidationError, parse_raw_as  # pylint: disable=no-name-in-module

_logger = logging.getLogger(__name__)

T = TypeVar("T")


class Config:
    """
    Stores config variables.

    Config variables are read from a file.
    Only single file can be read.
    If new file is read, then previous configs will be forgotten.
    """

    def __init__(self, conf_vars: Dict[str, Any]):
        """Instantiate the config store and read config variables from the file.

        Args:
            conf_vars: Dict containing configuration variables
        """
        self._conf_store: Dict[str, Any] = conf_vars

    def get(self, key: str, default: Any = None) -> Any:
        """Get the value for the specified key.

        If the key is not in the configs, then return default.

        Args:
            key: Key to be searched.
            default: Value to be returned if the key is not found.  Defaults to
                None.

        Returns:
            value in str format or default.
        """
        return self._conf_store.get(key, default)

    def get_dict(
        self, key_prefix: str, expected_values_type: Optional[T]
    ) -> Dict[str, Any]:
        """Get a dictionary based on config key prefixes.

        For example, if key_prefix is "my_dict", then the following config store:
            {
                'some_key': 'some_value',
                'my_dict_key1': 'value1',
                'my_dict_key2': 'value2',
            }
        Will return:
            {
                'key1': 'value1',
                'key2': 'value2',
            }

        Args:
            key_prefix: Only get configuration variables starting with this
                prefix.
            expected_values_type: If provided, the value will be validated against
                this type.

        Returns:
            A dictionary containing the keys prefixed with `key_prefix` as keys
                (but with the prefix removed) and the values as values.
        """
        result: Dict[str, Any] = {}
        for key, value in self._conf_store.items():
            if key.startswith(key_prefix):
                new_key = key[len(key_prefix) :]
                if expected_values_type is not None:
                    value = self.get_as(key, expected_values_type)
                result[new_key] = value
        return result

    def get_as(self, key: str, expected_type: Any) -> Any:
        """Get and convert the value to specified type.

        Check if type of the value is as expected.  If type is correct, then
        return converted value.  Otherwise Raise ValueError.

        Type can be:
            * Any typing module type.
            * Any pydantic strict types (e.g. pydantic.StrictInt)

        Args:
            key: Key to be search
            expected_type: type for the value

        Raises:
            ValueError: If can't convert value to the expected type.
            KeyError: If specified key is not in config.

        Returns:
            Value for the specified key, converted to specified type.

        Example:
            For `var1='[1, 2.0, 3.5]'`:
                * `get_as("var1", List[int])` -> `[1,2,3]`
                * `get_as("var1", List[float])` -> `[1.0,2.0,3.5]`
                * `get_as("var1", List[pydantic.StrictInt])` -> [ValueError][]
                * `get_as("var1", List[pydantic.StrictFloat])` -> [ValueError][]

            For `var1='[1,2,3]'`:
                * `get_as("var1", List[pydantic.StrictInt])` -> `[1,2,3]`

        """
        value = self[key]

        if str is expected_type:
            return value

        try:
            parsed_value: Any = parse_raw_as(expected_type, value)
        except (ValidationError, ValueError) as err:
            raise ValueError(
                f"Could not convert config variable: {key} = '{value}' "
                f"to type {str(expected_type)}, err:" + str(err)
            ) from err

        return parsed_value

    def __getitem__(self, key: str) -> Any:
        """Get the value for the specified key.

        If the key is not in the configs, then raise KeyError.

        Args:
            key: key to be searched.

        Raises:
            KeyError: If key is not in found.

        Returns:
            Dictionary if the corresponding value is a subsection in the .toml
                file or a primitive type it is a simple value.
        """
        value = self._conf_store.get(key, None)
        if value is None:
            raise KeyError(f"Unknown config name {key}")

        return value

    def __contains__(self, key: str) -> bool:
        """Return whether the specified key is in the storage.

        Args:
            key: Config variable name.

        Returns:
            True if key is in the storage, otherwise returns False.
        """
        return key in self._conf_store
