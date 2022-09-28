# Frequenz Python SDK

A development kit to interact with the Frequenz development platform.

## Testing locally

### Prerequisites

#### Python version

* For x86_64 Python 3.8 - 3.10 are supported (tested).
* For arm64 only Python 3.8 is supported (due to some dependencies that only support 3.8).

#### Development setup

You need to install the following development dependencies to be able to get
and build all dependencies:

```sh
python -m pip install grpcio-tools mypy-protobuf nox wheel
```

### Running the whole test suite

Run the following command to run tests:

```sh
nox
```

You can also use `nox -R` to reuse the current testing environment to speed up
test at the expense of a higher chance to end up with a dirty test environment.

### Running tests individually

For a better development test cycle you can install the runtime and test
dependencies and run `pytest` manually.

```sh
python -m pip install .
python -m pip install pytest pytest-asyncio

# And for example
pytest tests/test_sdk.py
```
