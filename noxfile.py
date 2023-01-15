# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Project's noxfile.

For more information please see nox documentation:
https://nox.thea.codes/en/stable/
"""

from typing import List

import nox
import toml


def min_dependencies() -> List[str]:
    """Extract the minimum dependencies from pyproject.toml.

    Raises:
        Exception: If minimun dependencies are not properly
            set in pyproject.toml.

    Returns:
        the minimun dependencies defined in pyproject.toml.

    """
    with open("pyproject.toml", "r", encoding="utf-8") as toml_file:
        data = toml.load(toml_file)

    dependencies = data.get("project", {}).get("dependencies", {})
    if not dependencies:
        raise Exception(f"No dependencies found in file: {toml_file.name}")

    min_deps: List[str] = []
    for dep in dependencies:
        min_dep = dep.split(",")[0]
        if any(op in min_dep for op in (">=", "==")):
            min_deps.append(min_dep.replace(">=", "=="))
        else:
            raise Exception(f"Minimum requirement is not set: {dep}")
    return min_deps


FMT_DEPS = ["black", "isort"]
DOCSTRING_DEPS = ["pydocstyle", "darglint"]
PYTEST_DEPS = [
    "pytest",
    "pytest-cov",
    "pytest-mock",
    "pytest-asyncio",
    "time-machine",
    "async-solipsism",
]
MYPY_DEPS = ["mypy", "pandas-stubs", "grpc-stubs"]
MIN_DEPS = min_dependencies()


def _source_file_paths(session: nox.Session) -> List[str]:
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
    return [
        "benchmarks",
        "docs",
        "examples",
        "src",
        "tests",
        "noxfile.py",
    ]


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
    session.install(
        ".[docs]", "pylint", "nox", *PYTEST_DEPS, *FMT_DEPS, *DOCSTRING_DEPS, *MYPY_DEPS
    )

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
        session.install(*FMT_DEPS)

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
        session.install("-e", ".[docs]", "nox", *MYPY_DEPS, *PYTEST_DEPS)

    common_args = [
        "--install-types",
        "--namespace-packages",
        "--non-interactive",
        "--explicit-package-bases",
        "--strict",
    ]

    # If we have session arguments, we just use those...
    if session.posargs:
        session.run("mypy", *common_args, *session.posargs)
        return

    check_paths = _source_file_paths(session)
    pkg_paths = [
        path
        for path in check_paths
        if not path.startswith("src") and not path.endswith(".py")
    ]
    file_paths = [path for path in check_paths if path.endswith(".py")]

    pkg_args = []
    for pkg in pkg_paths:
        if pkg == "src":
            pkg = "frequenz.channels"
        pkg_args.append("-p")
        pkg_args.append(pkg)

    session.run("mypy", *common_args, *pkg_args)
    session.run("mypy", *common_args, *file_paths)


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
        session.install("-e", ".[docs]", "pylint", "nox", *PYTEST_DEPS)

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
        session.install(*DOCSTRING_DEPS, "toml")

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
        session.install("-e", ".", *PYTEST_DEPS)

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
        session.install("-e", ".", *PYTEST_DEPS, *MIN_DEPS)

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
