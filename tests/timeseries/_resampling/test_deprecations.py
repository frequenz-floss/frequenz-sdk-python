# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Tests for the deprecated resampler configuration classes."""

import subprocess
import sys
import textwrap
from datetime import datetime, timedelta, timezone

import pytest
from frequenz.core.warnings import asserting_no_deprecations

from frequenz.sdk.timeseries import (
    ResamplerConfig,
    ResamplerConfig2,
    ResamplerConfig3,
    ResamplingContext,
    ResamplingFunction,
    ResamplingFunction2,
    SourceProperties,
)

_PERIOD = timedelta(seconds=1)

_RESAMPLER_CONFIG_MESSAGE = (
    "frequenz.sdk.timeseries.ResamplerConfig is deprecated. "
    "Use frequenz.sdk.timeseries.ResamplerConfig3 instead, but note it uses a "
    "wall clock timer and has different defaults, so resampled values can change."
)

_RESAMPLER_CONFIG2_MESSAGE = (
    "frequenz.sdk.timeseries.ResamplerConfig2 is deprecated. "
    "Use frequenz.sdk.timeseries.ResamplerConfig3 instead, but note it has "
    "different defaults and resampling function signature, so resampled "
    "values can change."
)

_RESAMPLING_FUNCTION_MESSAGE = (
    "frequenz.sdk.timeseries.ResamplingFunction is deprecated. "
    "Use frequenz.sdk.timeseries.ResamplingFunction3 instead, which receives a "
    "resampling context instead of the configuration."
)

_RESAMPLING_FUNCTION2_MESSAGE = (
    "frequenz.sdk.timeseries.ResamplingFunction2 is deprecated. "
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


def test_resampler_config2_is_deprecated() -> None:
    """Test creating a `ResamplerConfig2` warns once, and not about its base class."""
    with pytest.deprecated_call() as record:
        ResamplerConfig2(_PERIOD)

    assert [str(w.message) for w in record] == [_RESAMPLER_CONFIG2_MESSAGE]


def test_resampling_function2_is_deprecated() -> None:
    """Test subclassing `ResamplingFunction2` warns once."""
    with pytest.deprecated_call() as record:

        class _Function(ResamplingFunction2):  # pylint: disable=unused-variable
            pass

    assert [str(w.message) for w in record] == [_RESAMPLING_FUNCTION2_MESSAGE]


def test_resampler_config3_does_not_warn() -> None:
    """Test the replacement doesn't warn, nor its default resampling function."""
    with asserting_no_deprecations():
        config = ResamplerConfig3(_PERIOD)
        config.resampling_function(
            [(datetime(2026, 1, 1, tzinfo=timezone.utc), 1.0)],
            ResamplingContext(
                window_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
                config=config,
                source_properties=SourceProperties(),
            ),
        )


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
