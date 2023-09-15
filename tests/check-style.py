#! /usr/bin/env python3
import os
import ast
import sys


def check_number_literals_in_ast_Constant(node: ast.Constant, file_path: str) -> int:
    """Check if large numeric literals use underscores as separators.

    Args:
        node: The AST node to check.
        file_path: The path to the file containing the AST node.

    Returns:
        1 if the node is a large numeric literal without underscores, 0 otherwise.
    """
    if not isinstance(node.value, (int, float)):
        return 0

    # Get the original source code for the number
    start_line, start_col = node.lineno, node.col_offset
    end_line, end_col = node.end_lineno, node.end_col_offset
    with open(file_path, "r") as f:
        lines = f.readlines()
    original_number = "".join(
        line[start_col:end_col].strip() for line in lines[start_line - 1 : end_line]
    )

    sanitized_number = original_number
    if "." in original_number:
        # Only look at the part before the decimal point
        sanitized_number = original_number[: original_number.index(".")]

    if sanitized_number.startswith("0x"):
        sanitized_number = sanitized_number[2:]

    if "e" in sanitized_number.lower():
        # Skip numbers with scientific notation
        return 0

    if "_" in sanitized_number:
        # Skip numbers with underscores
        return 0

    if len(sanitized_number.replace(".", "").replace("-", "")) <= 4:
        # Skip small numbers
        return 0

    print(
        f"Number {original_number} in file {file_path} at line {start_line} should use underscores for better readability."
    )
    return 1


def check_number_literals_in_file(file_path) -> int:
    """Check if large numeric literals use underscores as separators."""
    failures = 0
    with open(file_path, "r") as f:
        tree = ast.parse(f.read(), filename=file_path)
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant):
                if isinstance(node.value, (int, float)):
                    failures += check_number_literals_in_ast_Constant(node, file_path)
    return failures


def traverse_files(root_dir: str) -> int:
    """Traverse through all .py files in root directory and its subdirectories.

    Args:
        root_dir: The root directory to traverse through.

    Returns:
        The number of failures.
    """
    failures = 0
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith(".py"):
                file_path = os.path.join(dirpath, filename)
                failures += check_number_literals_in_file(file_path)
    return failures

if __name__ == "__main__":
    for arg in sys.argv[1:]:
        if arg in ("-h", "--help"):
            print("Usage: python3 check-style.py [root_dir] [root_dir_2 ...]")
            exit(0)

    for arg in sys.argv[1:]:
        if not os.path.isdir(arg):
            print(f"Error: {arg} is not a valid directory.")
            exit(1)

    failures = 0
    for arg in sys.argv[1:]:
        failures += traverse_files(arg)

    if failures == 0:
        print("No failures. Good job!")
    else:
        print(f"{failures} failures found. Please fix them.")
    exit(1 if failures > 0 else 0)
