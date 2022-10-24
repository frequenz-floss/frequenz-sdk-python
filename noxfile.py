from typing import List
import nox


FMT_DEPS = ["black", "isort"]
LINT_DEPS = ["mypy", "pylint"]
DOCSTRING_DEPS = ["pydocstyle", "darglint"]
PYTEST_DEPS = ["pytest", "pytest-cov", "pytest-mock", "pytest-asyncio"]


def source_file_paths(session: nox.Session) -> List[str]:
    """Return the file paths to run the checks on.

    If positional arguments are present, we use those as the file paths, and if
    not, we use all source files."""
    if session.posargs:
        return session.posargs
    return ["src", "tests", "examples", "benchmarks"]


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
    """
    session.install(".", *PYTEST_DEPS, *FMT_DEPS, *LINT_DEPS, *DOCSTRING_DEPS)

    formatting(session, False)
    mypy(session, False)
    pylint(session, False)
    docstrings(session, False)
    pytest_max(session, False)


@nox.session
def formatting(session: nox.Session, install_deps: bool = True) -> None:
    """Check code formatting with black and isort."""
    if install_deps:
        session.install(*FMT_DEPS)

    session.run("black", "--check", *source_file_paths(session))
    session.run("isort", "--check", *source_file_paths(session))


@nox.session
def mypy(session: nox.Session, install_deps: bool = True) -> None:
    """Check type hints with mypy."""
    if install_deps:
        # install the package itself as editable, so that it is possible to do
        # fast local tests with `nox -R -e mypy`.
        session.install("-e", ".", "mypy", *PYTEST_DEPS)

    # Since we use other packages in the frequenz namespace, we need to run the
    # checks for frequenz.sdk from the installed package instead of the src
    # directory.
    mypy_paths = [
        path for path in source_file_paths(session) if not path.startswith("src")
    ]

    mypy_cmd = [
        "mypy",
        "--ignore-missing-imports",
        "--install-types",
        "--namespace-packages",
        "--non-interactive",
        "--explicit-package-bases",
        "--follow-imports=silent",
        "--strict",
    ]

    # Runs on the installed package
    session.run(*mypy_cmd, "-p", "frequenz.sdk")
    # Runs on the rest of the source folders
    session.run(*mypy_cmd, *mypy_paths)


@nox.session
def pylint(session: nox.Session, install_deps: bool = True) -> None:
    """Check for code smells with pylint."""
    if install_deps:
        # install the package itself as editable, so that it is possible to do
        # fast local tests with `nox -R -e pylint`.
        session.install("-e", ".", "pylint", *PYTEST_DEPS)

    session.run(
        "pylint",
        "--extension-pkg-whitelist=pydantic",
        *source_file_paths(session),
    )


@nox.session
def docstrings(session: nox.Session, install_deps: bool = True) -> None:
    """Check docstring tone with pydocstyle and param descriptions with darglint."""
    if install_deps:
        session.install(*DOCSTRING_DEPS, "toml")

    session.run("pydocstyle", *source_file_paths(session))

    # Darglint checks that function argument and return values are documented.
    # This is needed only for the `src` dir, so we exclude the other top level
    # dirs that contain code.
    darglint_paths = filter(
        lambda path: not (path.startswith("tests") or path.startswith("benchmarks")),
        source_file_paths(session),
    )
    session.run(
        "darglint",
        "-v2",  # for verbose error messages.
        *darglint_paths,
    )


@nox.session
def pytest_max(session: nox.Session, install_deps: bool = True) -> None:
    """Test the code against max dependency versions with pytest."""
    if install_deps:
        # install the package itself as editable, so that it is possible to do
        # fast local tests with `nox -R -e pytest_max`.
        session.install("-e", ".", *PYTEST_DEPS)

    _pytest_impl(session, "max", install_deps)


@nox.session
def pytest_min(session: nox.Session, install_deps: bool = True) -> None:
    """Test the code against min dependency versions with pytest."""
    if install_deps:
        # install the package itself as editable, so that it is possible to do
        # fast local tests with `nox -R -e pytest_min`.
        session.install("-e", ".", *PYTEST_DEPS, "-r", "minimum-requirements-ci.txt")

    _pytest_impl(session, "min", install_deps)


def _pytest_impl(
    session: nox.Session, max_or_min_deps: str, install_deps: bool
) -> None:
    session.run(
        "pytest",
        "-W=all",
        "-vv",
        "--cov=frequenz.sdk",
        "--cov-report=term",
        f"--cov-report=html:.htmlcov-{max_or_min_deps}",
    )
