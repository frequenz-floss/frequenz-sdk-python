# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Helper classes for tracking values from time-series data streams."""
from __future__ import annotations

import enum
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Collection, Dict, Generic, Optional, Set, TypeVar

from .formula import Formula

Key = TypeVar("Key")
Value = TypeVar("Value")

logger = logging.Logger(__name__)

SYMBOL_SEGMENT_SEPARATOR = "_"


class SymbolComponentCategory(enum.Enum):
    """Allowed component categories used in symbols for formula calculations."""

    INVERTER = "inverter"
    BATTERY = "battery"
    EV_CHARGER = "ev_charger"
    METER = "meter"


class ComponentField(enum.Enum):
    """Name of the field from streamed component data."""


class BatteryField(ComponentField):
    """Name of the fields from streamed battery data."""

    SOC = "soc"
    CAPACITY = "capacity"
    POWER_UPPER_BOUND = "power_upper_bound"
    POWER_LOWER_BOUND = "power_lower_bound"


class InverterField(ComponentField):
    """Name of the fields from streamed inverter data."""

    ACTIVE_POWER = "active_power"
    ACTIVE_POWER_UPPER_BOUND = "active_power_upper_bound"
    ACTIVE_POWER_LOWER_BOUND = "active_power_lower_bound"


class MeterField(ComponentField):
    """Name of the fields from streamed meter data."""

    ACTIVE_POWER = "active_power"


class EVChargerField(ComponentField):
    """Name of the fields from streamed ev charger data."""

    ACTIVE_POWER = "active_power"


@dataclass(frozen=True)
class SymbolMapping:
    """Mappings between formula symbols and streamed component data."""

    category: SymbolComponentCategory
    component_id: int
    field: ComponentField

    @property
    def symbol(self) -> str:
        """Create a sympy-compatible symbol from a symbol mapping.

        For instance,
        SymbolMapping(SymbolComponentCategory.METER, 5, ComponentField.ACTIVE_POWER)
        becomes "meter_5_active_power".

        Returns:
            sympy-compatible symbol mapping encoded as a string
        """
        return SYMBOL_SEGMENT_SEPARATOR.join(
            [self.category.value, str(self.component_id), self.field.value]
        )


@dataclass
class TimeSeriesEntry(Generic[Value]):
    """Describes a single observed value (of arbitrary type) at a specific timestamp."""

    class Status(enum.Enum):
        """Possible status values of a `TimeSeriesEntry`."""

        VALID = "valid"
        UNKNOWN = "unknown"
        ERROR = "error"

    timestamp: datetime
    value: Optional[Value] = None
    status: Status = Status.VALID
    broken_component_ids: Set[int] = field(default_factory=set)

    @staticmethod
    def create_error(timestamp: datetime) -> TimeSeriesEntry[Value]:
        """Create a `TimeSeriesEntry` that contains an error.

        This can happen when the value would be NaN, e.g.
        TimeSeriesEntry(value=1 / 0)

        Args:
            timestamp: Timestamp

        Returns:
            A single observed value (of arbitrary type) at a specific timestamp.
        """
        return TimeSeriesEntry(
            timestamp=timestamp, value=None, status=TimeSeriesEntry.Status.ERROR
        )

    @staticmethod
    def create_unknown(
        timestamp: datetime, broken_component_ids: Optional[Set[int]] = None
    ) -> TimeSeriesEntry[Value]:
        """Create a `TimeSeriesEntry` that contains an unknown value.

        This can happen when the value cannot be determined, because a component is
            broken.

        Args:
            timestamp: Timestamp
            broken_component_ids: broken component ids

        Returns:
            A single observed value (of arbitrary type) at a specific timestamp.
        """
        return TimeSeriesEntry(
            timestamp=timestamp,
            value=None,
            status=TimeSeriesEntry.Status.UNKNOWN,
            broken_component_ids=broken_component_ids or set(),
        )


@dataclass(frozen=True)
class CacheEntryLookupResult(Generic[Value]):
    """Component ids and meter connections for components of a specific type."""

    class Status(enum.Enum):
        """Possible outcomes of looking up a key in a cache."""

        HIT = "hit"
        MISS = "miss"
        EXPIRED = "expired"

    status: Status
    entry: Optional[TimeSeriesEntry[Value]] = None


class LatestEntryCache(Generic[Key, Value]):
    """Cache of the most recent values observed in one or more time series.

    Each time series is identified in the cache via a unique `Key`, and it is
    expected that every time series will be of the same type of `Value`.
    """

    def __init__(self) -> None:
        """Initialize the class."""
        self._latest_timestamp = datetime.min.replace(tzinfo=timezone.utc)
        self._entries: Dict[Key, TimeSeriesEntry[Value]] = {}

    @property
    def latest_timestamp(self) -> datetime:
        """Get the most recently observed timestamp across all keys in the cache.

        Returns:
            The highest timestamp out of all entries in the cache, or `datetime.min`
                if there are no cache entries.
        """
        return self._latest_timestamp

    def clear(self) -> None:
        """Clear all entries from the cache.

        This will not affect `latest_timestamp`.  If you want to restore the
        cache to its initial state, use the `reset` method instead.
        """
        self._entries.clear()

    def __contains__(self, key: Key) -> bool:
        """Check if a cache entry exists for the specified key.

        Args:
            key: key to search for.

        Returns:
            `True` if the cache contains an entry for `key`, `False` otherwise.
        """
        return key in self._entries

    def get(
        self,
        key: Key,
        timedelta_tolerance: timedelta = timedelta.max,
        default: Optional[TimeSeriesEntry[Value]] = None,
    ) -> CacheEntryLookupResult[Value]:
        """Get the cached entry for the specified key, if any.

        Args:
            key: key for which to look up the cached entry
            timedelta_tolerance: maximum permitted time difference between
                `latest_timestamp` and the timestamp of the cached entry
            default: what to return if the `key` is not found in the cache, or if the
                cached entry is not within the limits of `timedelta_tolerance`

        Returns:
            The cached entry associated with the provided `key`, or `default` if
                either there is no entry for the `key`, or if the difference between the
                cached timestamp and `latest_timestamp` is greater than
                `timedelta_tolerance`

        Raises:
            ValueError: when either timedelta_tolerance is negative or an entry
                retrieved from the cache has a timestamp greater than the latest saved
                timestamp across all cache keys.
        """
        if timedelta_tolerance < timedelta(0):
            raise ValueError(
                f"timedelta_tolerance cannot be less than 0, but "
                f"{timedelta_tolerance} was provided"
            )

        entry = self._entries.get(key, None)

        if entry is None:
            logger.debug("LatestEntryCache: missing data for key %s", key)
            return CacheEntryLookupResult(
                status=CacheEntryLookupResult.Status.MISS, entry=default
            )

        if entry.timestamp > self._latest_timestamp:
            raise ValueError(
                "Timestamp of single entry in the cache cannot be greater"
                " than the latest timestamp across entries for all keys."
            )

        delta = self._latest_timestamp - entry.timestamp

        if delta > timedelta_tolerance:
            logger.debug("LatestEntryCache: data for key %s is outdated", key)
            logger.debug("latest timestamp: %s", self._latest_timestamp)
            logger.debug("entry timestamp: %s", entry.timestamp)
            logger.debug("timedelta: %s", delta)
            logger.debug("tolerance: %s", timedelta_tolerance)
            return CacheEntryLookupResult(
                status=CacheEntryLookupResult.Status.EXPIRED, entry=default
            )

        return CacheEntryLookupResult(
            status=CacheEntryLookupResult.Status.HIT, entry=entry
        )

    def keys(self) -> Collection[Key]:
        """Get all the keys with entries in the cache.

        Returns:
            All the keys with an entry in the cache
        """
        return self._entries.keys()

    def __len__(self) -> int:
        """Get the total number of entries stored in the cache.

        Returns:
            The total number of keys with entries in the cache.
        """
        return len(self._entries)

    def pop(
        self,
        key: Key,
        default: Optional[TimeSeriesEntry[Value]] = None,
    ) -> CacheEntryLookupResult[Value]:
        """Pop the entry for the specified key from the cache, if it exists.

        Note that this does not affect `latest_timestamp`, even if the removed
        entry was the only one in the cache.  Use the `reset_latest_timestamp`
        method to eliminate the impact of timestamps from removed entries.

        Args:
            key: key whose cache entry to remove
            default: what to return if the `key` is not found in the cache

        Returns:
            Popped value if it exists, `None` otherwise
        """
        entry = self._entries.pop(key, None)
        if entry is None:
            return CacheEntryLookupResult(
                status=CacheEntryLookupResult.Status.MISS, entry=default
            )
        return CacheEntryLookupResult(
            status=CacheEntryLookupResult.Status.HIT, entry=entry
        )

    def reset(self) -> None:
        """Reset the cache to its default initial state.

        This will clear out all cache entries and reset the `latest_timestamp`
        property to `datetime.min`.  It is equivalent to separately calling the
        `clear` and `reset_latest_timestamp` methods in succession, but is very
        slightly more efficient.
        """
        self.clear()
        self._latest_timestamp = datetime.min.replace(tzinfo=timezone.utc)

    def reset_latest_timestamp(self) -> bool:
        """Reset the `latest_timestamp` property to the lowest possible value.

        This will be equal to either the highest timestamp out of any entries
        remaining in the cache, or `datetime.min` if no entries remain.

        Note that this is an O(N) operation, so may be expensive.  It is meant
        to be used only in order to correct an error introduced into the cache,
        e.g. after removing one or more entries with invalid timestamps.

        Returns:
            `True` if the latest timestamp was modified, `False` otherwise.

        Raises:
            ValueError: if the new latest timestamp is greater than the previous one
        """
        previous = self._latest_timestamp

        self._latest_timestamp = max(
            map(lambda x: x.timestamp, self._entries.values()),
            default=datetime.min.replace(tzinfo=timezone.utc),
        )

        if self._latest_timestamp > previous:
            raise ValueError(
                "The new latest timestamp after reset cannot be greater "
                "than the latest timestamp before the reset."
            )

        return previous != self._latest_timestamp

    def update(self, key: Key, entry: TimeSeriesEntry[Value]) -> bool:
        """Insert or update an entry for a specific key.

        Args:
            key: key to associate with the entry
            entry: entry to add to the cache (will be accepted if `key` is new,
                or if the timestamp of the existing entry is older than
                `entry.timestamp`)

        Returns:
            `True` if `entry` was added to the cache, `False` otherwise

        Raises:
            AttributeError: if timestamps are not timezone-aware
        """
        # sanity check that entry timestamp is valid
        if (
            entry.timestamp.tzinfo is None
            or entry.timestamp.tzinfo.utcoffset(entry.timestamp) is None
        ):
            raise AttributeError("Entry's timestamp must be timezone-aware.")

        self._latest_timestamp = max(self._latest_timestamp, entry.timestamp)

        last = self._entries.get(key, None)
        if last is None or entry.timestamp > last.timestamp:
            self._entries[key] = entry
            return True

        logger.debug("TimeSeriesEntryCache: entry for key %s is outdated", key)
        logger.debug("previously observed timestamp: %s", last.timestamp)
        logger.debug("entry timestamp: %s", entry.timestamp)

        return False


class TimeSeriesFormula(Formula, Generic[Value]):
    """Combines values from multiple time series via a specified algebraic formula.

    Formulas are handled using the `sympy` library for symbolic mathematics.  It is
    expected that each individual time series will have the same underlying type of
    `Value`, and that this `Value` supports all the operations the formula requires
    (for example, if the formula is `x / 7` and `Value` does not support division,
    then the formula cannot be applied).
    """

    @staticmethod
    def _is_component_broken(
        cache_lookup_result: CacheEntryLookupResult.Status,
    ) -> bool:
        """Check if a component is broken.

        Args:
            cache_lookup_result: an enum saying if data from component with
                `component_id` was found in the cache or not

        Returns:
            `True` if component is broken  and `False` otherwise
        """
        return cache_lookup_result in {
            CacheEntryLookupResult.Status.MISS,
            CacheEntryLookupResult.Status.EXPIRED,
        }

    # pylint:disable=too-many-arguments
    def evaluate(
        self,
        cache: LatestEntryCache[str, Value],
        formula_name: str = "",
        symbol_to_symbol_mapping: Optional[Dict[str, SymbolMapping]] = None,
        timedelta_tolerance: timedelta = timedelta.max,
        default_entry: Optional[TimeSeriesEntry[Value]] = None,
    ) -> Optional[TimeSeriesEntry[Value]]:
        """Evaluate the formula using time-series values from the provided cache.

        The underlying assumption of the evaluation process is that measurements from
        each time series will be offset slightly in time, but may still be combined if
        that offset is small enough.  By default the permitted offset is the maximum
        possible `timedelta`, in which case the mere existence of a value in each time
        series will be enough.

        This is probably not desirable for real-world use case so users should take care
        to always set the `timedelta_tolerance` parameter to a value that matches their
        use-case (for example, if the rate of update of time series is 0.2 sec, a
        `timedelta_tolerance` of 0.2 sec is recommended).

        Args:
            cache: cache of the most recent time series values, where the key should
                match the variable name in the formula (so e.g. if the formula is `x +
                y` then the cache should contain keys `"x"` and `"y"`)
            formula_name: user-defined name for the formula
            symbol_to_symbol_mapping: mapping of symbols in string representation to
                symbol mappings
            timedelta_tolerance: maximum permitted time difference between
                `cache.latest_timestamp` and the timestamp of any cached entry (if this
                is violated, the result will be the same as if an entry for the given
                key does not exist)
            default_entry: what entry to use if the entry for a given symbol is not
                found in the cache, or if the cached entry is not within the limits of
                `timedelta_tolerance`. If None, then formula won't be evaluated and None
                will be returned.

        Returns:
            Result of the formula, with a `timestamp` equal to the highest timestamp
                among all of the combined time series entries, and a `value` that equals
                the formula result, or `None` if any of the required time series
                variables are not present in `cache` or are older than
                `timedelta_tolerance`
        """
        kwargs: Dict[str, Optional[Value]] = {}
        timestamp = datetime.min.replace(tzinfo=timezone.utc)

        symbol_to_symbol_mapping = symbol_to_symbol_mapping or {}
        formula_broken_component_ids: Set[int] = set()
        broken_meter_found = False

        for symbol in self._symbols:
            symbol_mapping = symbol_to_symbol_mapping.get(symbol)

            cache_lookup_result = cache.get(
                symbol, timedelta_tolerance, default=default_entry
            )

            # If the value wasn't found in the cache and no symbol mapping was provided
            # for this symbol (meaning it cannot be determined what component category
            # it refers to), then return `default_entry` if it was provided, otherwise
            # the formula  cannot be evaluated and `None` will be returned
            # if cache_lookup_result.entry is None:
            #     return default_entry if default_entry is not None else None

            # Symbol metadata is not available, e.g. component category
            if symbol_mapping is None:

                # Component data is available and up to date
                if cache_lookup_result.entry is not None:
                    kwargs[symbol] = cache_lookup_result.entry.value
                    timestamp = max(timestamp, cache_lookup_result.entry.timestamp)

                # Otherwise, apply the default entry if it was provided, and if it
                # wasn't return `None` as the formula result as it cannot be evaluated
                else:
                    if default_entry is None:
                        return None
                    kwargs[symbol] = default_entry.value
                    timestamp = max(timestamp, default_entry.timestamp)

            # Symbol metadata is available,  e.g. component category
            else:
                # When a component is broken, keep collecting ids of broken components
                # that this formula relies on, and if there's any meter among these,
                # return `UNKNOWN` as the formula result, otherwise fill in the values
                # of broken components with the `default_entry` and evaluate the formula
                if self._is_component_broken(cache_lookup_result.status):
                    formula_broken_component_ids.add(symbol_mapping.component_id)

                    # If a meter is broken the formula cannot be evaluated
                    if symbol_mapping.category == SymbolComponentCategory.METER:
                        broken_meter_found = True
                        continue

                    # For component categories different than meter, apply the
                    # default entry
                    if default_entry is None:
                        return None
                    kwargs[symbol] = default_entry.value
                    timestamp = max(timestamp, default_entry.timestamp)

                # Component data is available and up to date
                if cache_lookup_result.entry is not None:
                    kwargs[symbol] = cache_lookup_result.entry.value
                    timestamp = max(timestamp, cache_lookup_result.entry.timestamp)

        if broken_meter_found:
            return TimeSeriesEntry.create_unknown(
                timestamp=timestamp,
                broken_component_ids=formula_broken_component_ids,
            )

        try:
            return TimeSeriesEntry(timestamp=timestamp, value=self(**kwargs))
        except Exception:  # pylint:disable=broad-except
            logger.exception(
                'Formula "%s" raised an unexpected Exception', formula_name
            )
            return TimeSeriesEntry.create_error(timestamp=timestamp)
