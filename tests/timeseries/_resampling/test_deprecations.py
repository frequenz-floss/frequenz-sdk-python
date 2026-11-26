# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Tests for the deprecated resampler configuration classes."""

import subprocess
import sys
import textwrap
from datetime import timedelta

import pytest
from frequenz.core.warnings import asserting_no_deprecations

from frequenz.sdk.timeseries import (
    ResamplerConfig,
    ResamplerConfig2,
    ResamplingFunction,
)

_PERIOD = timedelta(seconds=1)

_RESAMPLER_CONFIG_MESSAGE = (
    "frequenz.sdk.timeseries.ResamplerConfig is deprecated. "
    "Use frequenz.sdk.timeseries.ResamplerConfig3 instead, but note it uses a "
    "wall clock timer and has different defaults, so resampled values can change."
)

_RESAMPLING_FUNCTION_MESSAGE = (
    "frequenz.sdk.timeseries.ResamplingFunction is deprecated. "
    "Use frequenz.sdk.timeseries.ResamplingFunction3 instead, which receives a "
    "resampling context instead of the configuration."
)


def test_resampler_config_is_deprecated() -> None:
    """Test creating a `ResamplerConfig` warns once."""
    with pytest.deprecated_call() as record:
        ResamplerConfig(_PERIOD)

    assert [str(w.message) for w in record] == [_RESAMPLER_CONFIG_MESSAGE]


def test_resampling_function_is_deprecated() -> None:
    """Test subclassing `ResamplingFunction` warns once."""
    with pytest.deprecated_call() as record:

        class _Function(ResamplingFunction):  # pylint: disable=unused-variable
            pass

    assert [str(w.message) for w in record] == [_RESAMPLING_FUNCTION_MESSAGE]


def test_resampler_config2_does_not_warn_about_resampler_config() -> None:
    """Test creating a `ResamplerConfig2` doesn't warn about its base class."""
    with asserting_no_deprecations():
        ResamplerConfig2(_PERIOD)


def test_import_does_not_warn() -> None:
    """Test importing the SDK doesn't emit the SDK's own deprecation warnings.

    `ResamplerConfig2` subclasses the deprecated `ResamplerConfig`, which warns
    when the subclass is defined. It runs in a new interpreter, as the modules are
    already imported here.
    """
    code = textwrap.dedent("""\
        import warnings

        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            import frequenz.sdk.actor
            import frequenz.sdk.microgrid
            import frequenz.sdk.timeseries

        for warning in record:
            if "frequenz.sdk." in str(warning.message):
                print(warning.message)
        """)

    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )

    assert result.stdout == ""
