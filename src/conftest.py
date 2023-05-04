# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Pytest plugin to validate and run docstring code examples.

Code examples are often wrapped in triple backticks (```) within our docstrings.
This plugin extracts these code examples and validates them using pylint.
"""

from __future__ import annotations

import ast
import os
import subprocess

from sybil import Sybil
from sybil.evaluators.python import pad
from sybil.parsers.myst import CodeBlockParser


def get_import_statements(code: str) -> list[str]:
    """Get all import statements from a given code string.

    Args:
        code: The code to extract import statements from.

    Returns:
        A list of import statements.
    """
    tree = ast.parse(code)
    import_statements = []

    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            import_statement = ast.get_source_segment(code, node)
            import_statements.append(import_statement)

    return import_statements


def path_to_import_statement(path: str) -> str:
    """Convert a path to a Python file to an import statement.

    Args:
        path: The path to convert.

    Returns:
        The import statement.
    """
    if not path.endswith(".py"):
        raise ValueError("Path must point to a Python file (.py)")

    # Remove 'src/' prefix if present
    if path.startswith("src/"):
        path = path[4:]

    # Remove the '.py' extension and replace '/' with '.'
    module_path = path[:-3].replace("/", ".")

    # Create the import statement
    import_statement = f"from {module_path} import *"
    return import_statement


class CustomPythonCodeBlockParser(CodeBlockParser):
    """Code block parser that validates extracted code examples using pylint.

    This parser is a modified version of the default Python code block parser
    from the Sybil library.
    It uses pylint to validate the extracted code examples.

    All code examples are preceded by the original file's import statements as
    well as an wildcard import of the file itself.
    This allows us to use the code examples as if they were part of the original
    file.

    Additionally, the code example is padded with empty lines to make sure the
    line numbers are correct.

    Pylint warnings which are unimportant for code examples are disabled.
    """

    def __init__(self):
        super().__init__("python", None)

    def evaluate(self, example) -> None | str:
        """Validate the extracted code example using pylint.

        Args:
            example: The extracted code example.

        Returns:
            None if the code example is valid, otherwise the pylint output.
        """
        # Get the import statements for the original file
        import_statements = get_import_statements(example.document.text)
        # Add a wildcard import of the original file
        import_statements.append(
            path_to_import_statement(os.path.relpath(example.path))
        )
        imports_code = "\n".join(import_statements)

        example_with_imports = f"{imports_code}\n\n{example.parsed}"

        # Make sure the line numbers are correct (unfortunately, this is not
        # exactly correct, but good enough to find the line in question)
        source = pad(
            example_with_imports,
            example.line + example.parsed.line_offset - len(import_statements),
        )

        try:
            # pylint disable parameters with descriptions
            pylint_disable_params = [
                "C0114",  # Missing module docstring
                "C0115",  # Missing class docstring
                "C0116",  # Missing function or method docstring
                "W0401",  # Wildcard import
                "W0404",  # Reimport
                "W0611",  # Unused import
                "W0612",  # Unused variable
                "W0614",  # Unused import from wildcard
                "E0611",  # No name in module
                "E1142",  # Await used outside async function
            ]

            pylint_command = [
                "pylint",
                "--disable",
                ",".join(pylint_disable_params),
                "--from-stdin",
                example.path,
            ]

            subprocess.run(
                pylint_command,
                input=source,
                text=True,
                capture_output=True,
                check=True,
            )
        except subprocess.CalledProcessError as exception:
            return (
                f"Pylint validation failed for code example:\n"
                f"{example_with_imports}\nError: {exception}\nOutput: {exception.output}"
            )

        return None


pytest_collect_file = Sybil(
    parsers=[CustomPythonCodeBlockParser()],
    patterns=["*.py"],
).pytest()
