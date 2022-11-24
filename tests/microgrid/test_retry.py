# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for retry strategies."""

# pylint: disable=chained-comparison

from frequenz.sdk.microgrid.client import (
    ExponentialBackoff,
    LinearBackoff,
    RetryStrategy,
)


class TestLinearBackoff:
    """Tests for the linear backoff retry strategy."""

    def test_no_limit(self) -> None:
        """Test base case."""
        interval = 3
        jitter = 0
        limit = None
        retry = LinearBackoff(interval=interval, jitter=jitter, limit=limit)

        for _ in range(10):
            assert retry.next_interval() == interval

    def test_iter(self) -> None:
        """Test iterator."""
        assert list(LinearBackoff(1, 0, 3)) == [1, 1, 1]

    def test_with_limit(self) -> None:
        """Test limit works."""
        interval = 3
        jitter = 0
        limit = 5
        retry: RetryStrategy = LinearBackoff(
            interval=interval, jitter=jitter, limit=limit
        )

        for _ in range(limit):
            assert retry.next_interval() == interval
        assert retry.next_interval() is None

        retry.reset()
        for _ in range(limit - 1):
            assert retry.next_interval() == interval
        retry.reset()
        for _ in range(limit):
            assert retry.next_interval() == interval
        assert retry.next_interval() is None

    def test_with_jitter_no_limit(self) -> None:
        """Test with jitter but no limit."""
        interval = 3
        jitter = 1
        limit = None
        retry: RetryStrategy = LinearBackoff(
            interval=interval, jitter=jitter, limit=limit
        )

        prev = 0.0
        for _ in range(5):
            next_val = retry.next_interval()
            assert next_val is not None
            assert next_val > interval and next_val < (interval + jitter)
            assert next_val != prev
            prev = next_val

    def test_with_jitter_with_limit(self) -> None:
        """Test with jitter and limit."""
        interval = 3
        jitter = 1
        limit = 2
        retry: RetryStrategy = LinearBackoff(
            interval=interval, jitter=jitter, limit=limit
        )

        prev = 0.0
        for _ in range(2):
            next_val = retry.next_interval()
            assert next_val is not None
            assert next_val > interval and next_val < (interval + jitter)
            assert next_val != prev
            prev = next_val
        assert retry.next_interval() is None

        retry.reset()
        next_val = retry.next_interval()
        assert next_val is not None
        assert next_val > interval and next_val < (interval + jitter)
        assert next_val != prev

    def test_deep_copy(self) -> None:
        """Test if deep copies are really deep copies."""
        retry = LinearBackoff(1.0, 0.0, 2)

        copy1 = retry.copy()
        assert copy1.next_interval() == 1.0
        assert copy1.next_interval() == 1.0
        assert copy1.next_interval() is None

        copy2 = copy1.copy()
        assert copy1.next_interval() is None
        assert copy2.next_interval() == 1.0
        assert copy2.next_interval() == 1.0
        assert copy2.next_interval() is None


class TestExponentialBackoff:
    """Tests for the exponential backoff retry strategy."""

    def test_no_limit(self) -> None:
        """Test base case."""
        retry = ExponentialBackoff(3, 30, 2, 0.0)

        assert retry.next_interval() == 3.0
        assert retry.next_interval() == 6.0
        assert retry.next_interval() == 12.0
        assert retry.next_interval() == 24.0
        assert retry.next_interval() == 30.0
        assert retry.next_interval() == 30.0

    def test_with_limit(self) -> None:
        """Test limit works."""
        retry = ExponentialBackoff(3, jitter=0.0, limit=3)

        assert retry.next_interval() == 3.0
        assert retry.next_interval() == 6.0
        assert retry.next_interval() == 12.0
        assert retry.next_interval() is None

    def test_deep_copy(self) -> None:
        """Test if deep copies are really deep copies."""
        retry = ExponentialBackoff(3.0, 30.0, 2, 0.0, 2)

        copy1 = retry.copy()
        assert copy1.next_interval() == 3.0
        assert copy1.next_interval() == 6.0
        assert copy1.next_interval() is None

        copy2 = copy1.copy()
        assert copy1.next_interval() is None
        assert copy2.next_interval() == 3.0
        assert copy2.next_interval() == 6.0
        assert copy2.next_interval() is None

    def test_with_jitter_no_limit(self) -> None:
        """Test with jitter but no limit."""
        initial_interval = 3
        max_interval = 100
        jitter = 1
        multiplier = 2
        limit = None
        retry: RetryStrategy = ExponentialBackoff(
            initial_interval=initial_interval,
            max_interval=max_interval,
            multiplier=multiplier,
            jitter=jitter,
            limit=limit,
        )

        prev = 0.0
        for count in range(5):
            next_val = retry.next_interval()
            exp_backoff_interval = initial_interval * multiplier**count
            assert next_val is not None
            assert initial_interval <= next_val <= max_interval
            assert next_val >= min(exp_backoff_interval, max_interval)
            assert next_val <= min(exp_backoff_interval + jitter, max_interval)
            assert next_val != prev
            prev = next_val

    def test_with_jitter_with_limit(self) -> None:
        """Test with jitter and limit."""
        initial_interval = 3
        max_interval = 100
        jitter = 1
        multiplier = 2
        limit = 2
        retry: RetryStrategy = ExponentialBackoff(
            initial_interval=initial_interval,
            max_interval=max_interval,
            multiplier=multiplier,
            jitter=jitter,
            limit=limit,
        )

        prev = 0.0
        for count in range(2):
            next_val = retry.next_interval()
            exp_backoff_interval = initial_interval * multiplier**count
            assert next_val is not None
            assert initial_interval <= next_val <= max_interval
            assert next_val >= min(exp_backoff_interval, max_interval)
            assert next_val <= min(exp_backoff_interval + jitter, max_interval)
            assert next_val != prev
            prev = next_val
        assert retry.next_interval() is None

        retry.reset()
        next_val = retry.next_interval()
        count = 0
        exp_backoff_interval = initial_interval * multiplier**count
        assert next_val is not None
        assert initial_interval <= next_val <= max_interval
        assert next_val >= min(exp_backoff_interval, max_interval)
        assert next_val <= min(exp_backoff_interval + jitter, max_interval)
        assert next_val != prev
