# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Automation for code quality checks and unit tests for the Frequenz SDK.

This file specified all the checks that can be run from command line invocations
of `nox`.

The following checks are performed:

1. `formatting` :: checks that the code is formatted with `black` and the imports
   are sorted with `isort`.
2. `mypy`       :: type checks all source files with `mypy --strict`.
3. `pylint`     :: lints all source files with `pylint`.
4. `docstrings` :: checks that all public functions have docstrings with
   1. a one-line imperative description at the top, followed by additional
      description, using `pydocstyle`.
   2. function parameters, return values, and raised exceptions are documented
      following the google style guide for these items:
      https://google.github.io/styleguide/pyguide.html#doc-function-args, using
      `darglint`.
5. `pytest_min` :: run all unittests using `pytest`, with the oldest supported
   versions of all dependencies installed.
6. `pytest_max` :: run all unittests using `pytest`, with the latest supported
   versions of all dependencies installed.

Usage:

1. Run all checks in a *new* venv.

        nox

2. Run all checks in an *exising* venv.  This would be much faster if venv for
   all tests exist already.  If they don't exist, new venvs will be created.

        nox -R

3. Run a subset of available checks:

        nox -e mypy pylint    # create new venvs for specified checks.
        nox -R -e mypy pylint # reuse venvs for specified checks if available.

4. The `pytest_min` and `pytest_max` checks run `pytest` on all available tests,
   including test coverage generation.  But this can be slow for fast local
   test-devlop cycles, and so `pytest` can also be invoked with optional custom
   arguments, in which case, only the specified arguments are passed to
   `pytest`.  This can be done as follows:

        nox -R -e pytest_min [-- <args for pytest>]
        nox -R -e pytest_min -- -s -x tests/timeseries/test_logical_meter.py
"""

from __future__ import annotations

from typing import Any, Iterable

import nox
import toml

DEFAULT_PATH_PACKAGES = {
    "benchmarks": "benchmarks",
    "docs": "docs",
    "examples": "examples",
    "src": "frequenz.sdk",
    "tests": "tests",
    "noxfile.py": "noxfile",
}
"""A list of path to be used by default and its corresponding package name.

The package name is needed for mypy, as it takes packages when full import
checking needs to be done.
"""


def min_dependencies() -> list[str]:
    """Extract the minimum dependencies from pyproject.toml.

    Raises:
        RuntimeError: If minimun dependencies are not properly
            set in pyproject.toml.

    Returns:
        the minimun dependencies defined in pyproject.toml.

    """
    with open("pyproject.toml", "r", encoding="utf-8") as toml_file:
        data = toml.load(toml_file)

    dependencies = data.get("project", {}).get("dependencies", {})
    if not dependencies:
        raise RuntimeError(f"No dependencies found in file: {toml_file.name}")

    min_deps: list[str] = []
    for dep in dependencies:
        min_dep = dep.split(",")[0]
        if any(op in min_dep for op in (">=", "==")):
            min_deps.append(min_dep.replace(">=", "=="))
        else:
            raise RuntimeError(f"Minimum requirement is not set: {dep}")
    return min_deps


def _source_file_paths(session: nox.Session) -> list[str]:
    """Return the file paths to run the checks on.

    If positional arguments are present in the nox session, we use those as the
    file paths, and if not, we use all source files.

    Args:
        session: the nox session.

    Returns:
        the file paths to run the checks on.
    """
    if session.posargs:
        return session.posargs
    return list(DEFAULT_PATH_PACKAGES.keys())


# Run all checks except `ci_checks` by default.  When running locally with just
# `nox` or `nox -R`, these are the checks that will run.
nox.options.sessions = [
    "formatting",
    "mypy",
    "pylint",
    "docstrings",
    "pytest_min",
    "pytest_max",
]


@nox.session
def ci_checks_max(session: nox.Session) -> None:
    """Run all checks with max dependencies in a single session.

    This is faster than running the checks separately, so it is suitable for CI.

    This does NOT run pytest_min, so that needs to be run separately as well.

    Args:
        session: the nox session.
    """
    session.install("-e", ".[dev]")

    formatting(session, False)
    mypy(session, False)
    pylint(session, False)
    docstrings(session, False)
    pytest_max(session, False)


@nox.session
def formatting(session: nox.Session, install_deps: bool = True) -> None:
    """Check code formatting with black and isort.

    Args:
        session: the nox session.
        install_deps: True if dependencies should be installed.
    """
    if install_deps:
        session.install("-e", ".[format]")

    paths = _source_file_paths(session)
    session.run("black", "--check", *paths)
    session.run("isort", "--check", *paths)


@nox.session
def mypy(session: nox.Session, install_deps: bool = True) -> None:
    """Check type hints with mypy.

    Args:
        session: the nox session.
        install_deps: True if dependencies should be installed.
    """
    if install_deps:
        # install the package itself as editable, so that it is possible to do
        # fast local tests with `nox -R -e mypy`.
        session.install("-e", ".[mypy]")

    def _flatten(iterable: Iterable[Iterable[Any]]) -> Iterable[Any]:
        return [item for sublist in iterable for item in sublist]

    args = (
        session.posargs
        if session.posargs
        else _flatten(("-p", p) for p in DEFAULT_PATH_PACKAGES.values())
    )

    session.run(
        "mypy",
        "--install-types",
        "--namespace-packages",
        "--non-interactive",
        "--explicit-package-bases",
        "--strict",
        *args,
    )


@nox.session
def pylint(session: nox.Session, install_deps: bool = True) -> None:
    """Check for code smells with pylint.

    Args:
        session: the nox session.
        install_deps: True if dependencies should be installed.
    """
    if install_deps:
        # install the package itself as editable, so that it is possible to do
        # fast local tests with `nox -R -e pylint`.
        session.install("-e", ".[pylint]")

    paths = _source_file_paths(session)
    session.run(
        "pylint",
        "--extension-pkg-whitelist=pydantic",
        *paths,
    )


@nox.session
def docstrings(session: nox.Session, install_deps: bool = True) -> None:
    """Check docstring tone with pydocstyle and param descriptions with darglint.

    Args:
        session: the nox session.
        install_deps: True if dependencies should be installed.
    """
    if install_deps:
        session.install("-e", ".[docs-lint]")

    paths = _source_file_paths(session)
    session.run("pydocstyle", *paths)

    # Darglint checks that function argument and return values are documented.
    # This is needed only for the `src` dir, so we exclude the other top level
    # dirs that contain code, unless some paths were specified by argument, in
    # which case we use those untouched.
    darglint_paths = session.posargs or filter(
        lambda path: not (path.startswith("tests") or path.startswith("benchmarks")),
        _source_file_paths(session),
    )
    session.run(
        "darglint",
        "-v2",  # for verbose error messages.
        *darglint_paths,
    )


@nox.session
def pytest_max(session: nox.Session, install_deps: bool = True) -> None:
    """Test the code against max dependency versions with pytest.

    Args:
        session: the nox session.
        install_deps: True if dependencies should be installed.
    """
    if install_deps:
        # install the package itself as editable, so that it is possible to do
        # fast local tests with `nox -R -e pytest_max`.
        session.install("-e", ".[pytest]")

    _pytest_impl(session, "max")


@nox.session
def pytest_min(session: nox.Session, install_deps: bool = True) -> None:
    """Test the code against min dependency versions with pytest.

    Args:
        session: the nox session.
        install_deps: True if dependencies should be installed.
    """
    if install_deps:
        # install the package itself as editable, so that it is possible to do
        # fast local tests with `nox -R -e pytest_min`.
        session.install("-e", ".[pytest]", *min_dependencies())

    _pytest_impl(session, "min")


def _pytest_impl(session: nox.Session, max_or_min_deps: str) -> None:
    session.run(
        "pytest",
        "-W=all",
        "-vv",
        "--cov=frequenz.sdk",
        "--cov-report=term",
        f"--cov-report=html:.htmlcov-{max_or_min_deps}",
        *session.posargs,
    )
