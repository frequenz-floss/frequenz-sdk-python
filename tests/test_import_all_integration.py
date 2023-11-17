# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests to verify that all modules can be imported successfully."""

import importlib
import os

import pytest


@pytest.mark.integration
def test_all_modules_importable() -> None:
    """Test that all modules can be imported.

    This test is mainly to ensure that there are no circular imports.
    """
    src_path = "src"

    for dir_path, _, files in os.walk(src_path):
        for file in files:
            if file.endswith(".py") and not file.startswith("__"):
                module_path = os.path.join(dir_path, file)
                module_name = os.path.splitext(module_path.replace(os.sep, "."))[0]

                try:
                    importlib.import_module(module_name)
                except ImportError as error:
                    assert False, f"Failed to import {module_name}: {error}"
