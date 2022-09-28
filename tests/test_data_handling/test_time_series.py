"""Tests for the frequenz.sdk.data_handling.time_series module.

Copyright
Copyright © 2022 Frequenz Energy-as-a-Service GmbH

License
MIT
"""

import datetime as dt

import pytz

import frequenz.sdk.data_handling.time_series as ts

# pylint: disable=invalid-name,too-many-locals,too-many-statements,missing-function-docstring
from frequenz.sdk.data_handling.power import ComplexPower


def test_LatestEntryCache() -> None:
    cache1 = ts.LatestEntryCache[int, float]()

    assert cache1.latest_timestamp == pytz.utc.localize(dt.datetime.min)
    assert 1 not in cache1
    assert 2 not in cache1
    assert 3 not in cache1
    assert 1001 not in cache1
    assert len(cache1) == 0
    assert len(cache1.keys()) == 0

    assert cache1.get(1).entry is None
    assert cache1.get(2).entry is None
    assert cache1.get(3).entry is None
    assert cache1.get(1001).entry is None

    # popping entries that do not exist returns `None`, or whatever
    # alternative default we specify
    custom_default = ts.TimeSeriesEntry(
        timestamp=dt.datetime.fromisoformat("2019-01-01T00:00:00+01:00"),
        value=123.45,
    )
    assert cache1.pop(1).entry is None
    assert cache1.pop(1, custom_default).entry == custom_default
    assert cache1.pop(2).entry is None
    assert cache1.pop(2, custom_default).entry == custom_default
    assert cache1.pop(3).entry is None
    assert cache1.pop(3, custom_default).entry == custom_default
    assert cache1.pop(1001).entry is None
    assert cache1.pop(1001, custom_default).entry == custom_default

    # if we add a time series entry for a new key, then it does not
    # matter what its timestamp is, the entry will always be added
    # to the cache (`update` returns `True`)
    ts1a = dt.datetime.fromisoformat("2021-03-11T10:49:00+00:00")
    e1a = ts.TimeSeriesEntry(timestamp=ts1a, value=1.0)
    assert cache1.update(1, e1a) is True

    assert cache1.latest_timestamp == ts1a
    assert 1 in cache1
    assert 2 not in cache1
    assert 3 not in cache1
    assert 1001 not in cache1
    assert len(cache1) == 1
    assert list(cache1.keys()) == [1]

    # with only one entry in the cache, the latest timestamp is
    # the same as that entry's timestamp, and so even if we set
    # zero timedelta tolerance, we still get it back
    assert cache1.get(1).entry == e1a
    assert cache1.get(1, dt.timedelta(0)).entry == e1a

    assert cache1.get(2).entry is None
    assert cache1.get(2).entry is None
    assert cache1.get(3).entry is None
    assert cache1.get(1001).entry is None

    # if we add an entry for a different key that is older than
    # the latest timestamp, the latter does not change, so all
    # timedeltas are still measured relative to the most recent
    # of all entry timestamps, but the entry will still be added
    # to the cache
    ts2a = dt.datetime.fromisoformat("2021-03-11T10:48:59+00:00")
    e2a = ts.TimeSeriesEntry(timestamp=ts2a, value=2.0)
    assert cache1.update(2, e2a) is True

    assert cache1.latest_timestamp == ts1a
    assert 1 in cache1
    assert 2 in cache1
    assert 3 not in cache1
    assert 1001 not in cache1
    assert len(cache1) == 2
    assert list(cache1.keys()) == [1, 2]

    assert cache1.get(1).entry == e1a
    assert cache1.get(1, dt.timedelta(0)).entry == e1a  # still most recent timestamp

    assert cache1.get(2).entry == e2a
    assert cache1.get(2, dt.timedelta(seconds=1)).entry == e2a  # within tolerance
    assert cache1.get(2, dt.timedelta(seconds=0.999)).entry is None  # outside tolerance
    assert cache1.get(2, dt.timedelta(0)).entry is None  # outside tolerance

    assert cache1.get(3).entry is None
    assert cache1.get(1001).entry is None

    # if we add an entry for yet another key that has exactly the
    # same timestamp as the latest timestamp, again the latter does
    # not change, and again the entry for the new key is accepted
    # into the cache
    e3a = ts.TimeSeriesEntry(timestamp=ts1a, value=3.0)
    assert cache1.update(3, e3a) is True

    assert cache1.latest_timestamp == ts1a
    assert 1 in cache1
    assert 2 in cache1
    assert 3 in cache1
    assert 1001 not in cache1
    assert len(cache1) == 3
    assert list(cache1.keys()) == [1, 2, 3]

    assert cache1.get(1).entry == e1a
    assert cache1.get(1, dt.timedelta(0)).entry == e1a  # still most recent timestamp

    assert cache1.get(2).entry == e2a
    assert cache1.get(2, dt.timedelta(seconds=1)).entry == e2a  # within tolerance
    assert cache1.get(2, dt.timedelta(seconds=0.999)).entry is None  # outside tolerance
    assert cache1.get(2, dt.timedelta(0)).entry is None  # outside tolerance

    assert cache1.get(3).entry == e3a
    assert cache1.get(3, dt.timedelta(0)).entry == e3a  # same as most recent timestamp

    assert cache1.get(1001).entry is None

    # if we add an entry for an existing key, but whose timestamp is
    # less than that of the cached entry, the existing cache entry is
    # kept and the outdated one discarded (`update` returns `False`)
    ts1old = e1a.timestamp - dt.timedelta(seconds=0.01)
    ts2old = e2a.timestamp - dt.timedelta(seconds=0.01)
    ts3old = e3a.timestamp - dt.timedelta(seconds=0.001)

    assert cache1.update(1, ts.TimeSeriesEntry(ts1old, 0.1)) is False
    assert cache1.update(2, ts.TimeSeriesEntry(ts2old, 0.2)) is False
    assert cache1.update(3, ts.TimeSeriesEntry(ts3old, 0.3)) is False

    assert cache1.latest_timestamp == ts1a

    assert cache1.get(1).entry == e1a
    assert cache1.get(1, dt.timedelta(0)).entry == e1a  # still most recent timestamp

    assert cache1.get(2).entry == e2a
    assert cache1.get(2, dt.timedelta(seconds=1)).entry == e2a  # within tolerance
    assert cache1.get(2, dt.timedelta(seconds=0.999)).entry is None  # outside tolerance
    assert cache1.get(2, dt.timedelta(0)).entry is None  # outside tolerance

    assert cache1.get(3).entry == e3a
    assert cache1.get(3, dt.timedelta(0)).entry == e3a  # same as most recent timestamp

    assert cache1.get(1001).entry is None

    # the same thing happens if the timestamp is exactly equal to the
    # existing cached entry  (no overwrites of a value for the exact
    # same timestamp!)
    assert cache1.update(1, ts.TimeSeriesEntry(e1a.timestamp, 0.1)) is False
    assert cache1.update(2, ts.TimeSeriesEntry(e2a.timestamp, 0.2)) is False
    assert cache1.update(3, ts.TimeSeriesEntry(e3a.timestamp, 0.3)) is False

    assert cache1.latest_timestamp == ts1a

    assert cache1.get(1).entry == e1a
    assert cache1.get(1, dt.timedelta(0)).entry == e1a  # still most recent timestamp

    assert cache1.get(2).entry == e2a
    assert cache1.get(2, dt.timedelta(seconds=1)).entry == e2a  # within tolerance
    assert cache1.get(2, dt.timedelta(seconds=0.999)).entry is None  # outside tolerance
    assert cache1.get(2, dt.timedelta(0)).entry is None  # outside tolerance

    assert cache1.get(3).entry == e3a
    assert cache1.get(3, dt.timedelta(0)).entry == e3a  # same as most recent timestamp

    assert cache1.get(1001).entry is None

    # if entries with newer timestamps are reported, then the update
    # will take place
    ts1b = dt.datetime.fromisoformat("2021-03-11T10:49:00.250+00:00")
    e1b = ts.TimeSeriesEntry(ts1b, 101.0)
    assert cache1.update(1, e1b) is True

    assert cache1.latest_timestamp == ts1b

    assert cache1.get(1).entry == e1b
    assert cache1.get(1, dt.timedelta(0)).entry == e1b  # new most recent timestamp

    assert cache1.get(2).entry == e2a
    assert cache1.get(2, dt.timedelta(seconds=1.25)).entry == e2a  # within tolerance
    assert cache1.get(2, dt.timedelta(seconds=1.249)).entry is None  # outside tolerance
    assert cache1.get(2, dt.timedelta(0)).entry is None  # outside tolerance

    assert cache1.get(3).entry == e3a
    assert cache1.get(3, dt.timedelta(seconds=0.25)).entry == e3a  # within tolerance
    assert cache1.get(3, dt.timedelta(seconds=0.249)).entry is None  # outside tolerance
    assert cache1.get(3, dt.timedelta(0)).entry is None  # outside tolerance

    assert cache1.get(1001).entry is None

    ts2b = dt.datetime.fromisoformat("2021-03-11T10:49:00.500+00:00")
    e2b = ts.TimeSeriesEntry(ts2b, 102.0)
    assert cache1.update(2, e2b) is True

    assert cache1.latest_timestamp == ts2b

    assert cache1.get(1).entry == e1b
    assert cache1.get(1, dt.timedelta(seconds=0.25)).entry == e1b  # within tolerance
    assert cache1.get(1, dt.timedelta(seconds=0.249)).entry is None  # outside tolerance
    assert cache1.get(1, dt.timedelta(0)).entry is None  # outside tolerance

    assert cache1.get(2).entry == e2b
    assert cache1.get(2, dt.timedelta(0)).entry == e2b  # new most recent timestamp

    assert cache1.get(3).entry == e3a
    assert cache1.get(3, dt.timedelta(seconds=0.5)).entry == e3a  # within tolerance
    assert cache1.get(3, dt.timedelta(seconds=0.499)).entry is None  # outside tolerance
    assert cache1.get(3, dt.timedelta(0)).entry is None  # outside tolerance

    assert cache1.get(1001).entry is None

    ts3b = dt.datetime.fromisoformat("2021-03-11T10:49:00.502+00:00")
    e3b = ts.TimeSeriesEntry(ts3b, 103.0)
    assert cache1.update(3, e3b) is True

    assert cache1.latest_timestamp == ts3b

    assert cache1.get(1).entry == e1b
    assert cache1.get(1, dt.timedelta(seconds=0.252)).entry == e1b  # within tolerance
    assert cache1.get(1, dt.timedelta(seconds=0.251)).entry is None  # outside tolerance
    assert cache1.get(1, dt.timedelta(0)).entry is None  # outside tolerance

    assert cache1.get(2).entry == e2b
    assert cache1.get(2, dt.timedelta(seconds=0.002)).entry == e2b  # within tolerance
    assert cache1.get(2, dt.timedelta(seconds=0.001)).entry is None  # outside tolerance
    assert cache1.get(2, dt.timedelta(0)).entry is None  # outside tolerance

    assert cache1.get(3).entry == e3b
    assert cache1.get(3, dt.timedelta(0)).entry == e3b  # new most recent timestamp

    assert cache1.get(1001).entry is None

    # if we supply a custom `default` it will be returned instead of `None`,
    # both when the key does not exist and when the timestamp is outside the
    # timedelta tolerance
    assert cache1.get(1).entry == e1b
    assert cache1.get(1, dt.timedelta(seconds=0.252)).entry == e1b  # within tolerance
    assert (
        cache1.get(1, dt.timedelta(seconds=0.251), default=e1a).entry == e1a
    )  # outside
    assert cache1.get(1, dt.timedelta(0), default=e2a).entry == e2a  # outside

    assert cache1.get(2).entry == e2b
    assert cache1.get(2, dt.timedelta(seconds=0.002)).entry == e2b  # within tolerance
    assert (
        cache1.get(2, dt.timedelta(seconds=0.001), default=e1b).entry == e1b
    )  # outside
    assert cache1.get(2, dt.timedelta(0), default=e2a).entry == e2a  # outside

    assert cache1.get(3).entry == e3b
    assert cache1.get(3, dt.timedelta(0)).entry == e3b  # new most recent timestamp
    assert (
        cache1.get(3, dt.timedelta(0), default=e2b).entry == e3b
    )  # never outside tolerance

    assert cache1.get(1001).entry is None
    assert cache1.get(1001, default=e3b).entry == e3b

    # if we pop an entry it does not automatically reset the latest timestamp ...
    assert cache1.pop(3).entry == e3b

    assert cache1.latest_timestamp == ts3b

    assert 1 in cache1
    assert 2 in cache1
    assert 3 not in cache1
    assert 1001 not in cache1
    assert len(cache1) == 2
    assert list(cache1.keys()) == [1, 2]

    assert cache1.get(1).entry == e1b
    assert cache1.get(1, dt.timedelta(seconds=0.252)).entry == e1b  # within tolerance
    assert cache1.get(1, dt.timedelta(seconds=0.251)).entry is None  # outside tolerance
    assert cache1.get(1, dt.timedelta(0)).entry is None  # outside tolerance

    assert cache1.get(2).entry == e2b
    assert cache1.get(2, dt.timedelta(seconds=0.002)).entry == e2b  # within tolerance
    assert cache1.get(2, dt.timedelta(seconds=0.001)).entry is None  # outside tolerance
    assert cache1.get(2, dt.timedelta(0)).entry is None  # outside tolerance

    assert cache1.get(3).entry is None
    assert cache1.get(1001).entry is None

    # ... but we can address this by calling the `reset_latest_timestamp` method,
    # in which case it is set to the largest timestamp of all remaining entries
    # (returning `True` if this changed its value, `False` otherwise)
    assert cache1.reset_latest_timestamp() is True

    assert cache1.latest_timestamp < ts3b
    assert cache1.latest_timestamp == ts2b

    # attempting to reset the latest timestamp when it is already equal to the
    # correct value will return `False`
    assert cache1.reset_latest_timestamp() is False

    # if we `clear` the cache it removes all the keys but does not reset
    # `latest_timestamp` ...
    cache1.clear()

    assert cache1.latest_timestamp == ts2b

    assert 1 not in cache1
    assert 2 not in cache1
    assert 3 not in cache1
    assert 1001 not in cache1
    assert len(cache1) == 0
    assert len(cache1.keys()) == 0

    assert cache1.get(1).entry is None
    assert cache1.get(2).entry is None
    assert cache1.get(3).entry is None
    assert cache1.get(1001).entry is None

    # ... but if we call the `reset_latest_timestamp` method then with no
    # cached entries it will be reset to the starting value `datetime.min`
    assert cache1.reset_latest_timestamp() is True
    assert cache1.latest_timestamp == pytz.utc.localize(dt.datetime.min)

    # if we `remove` the last entry in the cache it similarly does not
    # reset the `latest_timestamp` value
    assert cache1.update(2, e2b) is True
    assert cache1.latest_timestamp == e2b.timestamp
    assert 2 in cache1
    assert len(cache1) == 1
    assert list(cache1.keys()) == [2]
    assert cache1.get(2).entry == e2b

    assert cache1.reset_latest_timestamp() is False  # reset value changes nothing

    assert cache1.pop(2).entry == e2b
    assert cache1.latest_timestamp == e2b.timestamp  # unchanged
    assert 2 not in cache1
    assert len(cache1) == 0
    assert len(cache1.keys()) == 0
    assert cache1.get(2).entry is None

    # ... but if we manually request a reset, then again, with no entries in
    # the cache, it will be reset to its default start value `datetime.min`
    assert cache1.reset_latest_timestamp() is True
    assert cache1.latest_timestamp == pytz.utc.localize(dt.datetime.min)

    # if we want to both clear out all entries and reset `latest_timestamp` all
    # at the same time then we can do this with the `reset` method
    assert cache1.update(1, e1b) is True
    assert cache1.update(2, e2b) is True
    assert cache1.update(3, e3b) is True

    assert len(cache1) == 3
    assert list(cache1.keys()) == [1, 2, 3]
    assert cache1.latest_timestamp == e3b.timestamp

    cache1.reset()
    assert len(cache1) == 0
    assert len(cache1.keys()) == 0
    assert cache1.latest_timestamp == pytz.utc.localize(dt.datetime.min)


def test_TimeSeriesFormula() -> None:
    f1 = ts.TimeSeriesFormula[int]("x + y")

    cache1 = ts.LatestEntryCache[str, int]()

    # without any entries in the cache, we cannot evaluate the formula
    assert f1.evaluate(cache1) is None

    # if we have an entry only for only one of the required symbols,
    # then we still cannot evaluate the formula
    ts1a = dt.datetime.fromisoformat("2021-03-11T10:49:00+00:00")
    xa = ts.TimeSeriesEntry(ts1a, 9)
    assert cache1.update("x", xa) is True
    assert f1.evaluate(cache1) is None  # missing data

    # a second entry for the same symbol still results in None
    ts1b = dt.datetime.fromisoformat("2021-03-11T10:49:00.200+00:00")
    xb = ts.TimeSeriesEntry(ts1b, 7)
    assert cache1.update("x", xb) is True
    assert f1.evaluate(cache1) is None  # missing data

    # with an entry for the second component, whether or not we can
    # calculate a result depends on whether or not both entries are
    # within the timedelta tolerance (relative to the most recent
    # timestamp among all cache entries): if a result is generated,
    # its timestamp will be equal to the most recent among all the
    # symbols used in the formula
    ts2a = dt.datetime.fromisoformat("2021-03-11T10:49:00+00:00")
    ya = ts.TimeSeriesEntry(ts2a, 5)
    assert cache1.update("y", ya) is True

    expected_ya = ts.TimeSeriesEntry(xb.timestamp, 12)
    assert f1.evaluate(cache1) == expected_ya
    assert (
        f1.evaluate(cache1, timedelta_tolerance=dt.timedelta(seconds=0.200))
        == expected_ya
    )
    assert f1.evaluate(cache1, timedelta_tolerance=dt.timedelta(seconds=0.199)) is None
    assert f1.evaluate(cache1, timedelta_tolerance=dt.timedelta(0)) is None

    # a more recent entry will result in a change to both timestamp and
    # and value of the formula result, and changes the tolerance limits
    ts2b = dt.datetime.fromisoformat("2021-03-11T10:49:00.500+00:00")
    yb = ts.TimeSeriesEntry(ts2b, -3)
    assert cache1.update("y", yb) is True

    expected_yb = ts.TimeSeriesEntry(yb.timestamp, 4)
    assert f1.evaluate(cache1) == expected_yb
    assert (
        f1.evaluate(cache1, timedelta_tolerance=dt.timedelta(seconds=0.300))
        == expected_yb
    )
    assert f1.evaluate(cache1, timedelta_tolerance=dt.timedelta(seconds=0.299)) is None
    assert f1.evaluate(cache1, timedelta_tolerance=dt.timedelta(0)) is None

    # a still more recent observation for a symbol name not in the formula
    # will not change the result of the formula itself but affects tolerance
    ts3a = dt.datetime.fromisoformat("2021-03-11T10:49:00.750+00:00")
    za = ts.TimeSeriesEntry(ts3a, 999)
    assert cache1.update("z", za) is True

    assert f1.evaluate(cache1) == expected_yb
    assert (
        f1.evaluate(cache1, timedelta_tolerance=dt.timedelta(seconds=0.550))
        == expected_yb
    )
    assert f1.evaluate(cache1, timedelta_tolerance=dt.timedelta(seconds=0.549)) is None
    assert f1.evaluate(cache1, timedelta_tolerance=dt.timedelta(0)) is None

    # if the timestamps of all observations are exactly equal then we will
    # get the same result all the way down to timedelta_tolerance == 0
    xc = ts.TimeSeriesEntry(ts3a, 8)
    assert cache1.update("x", xc) is True

    expected_xc = ts.TimeSeriesEntry(xc.timestamp, 5)
    assert f1.evaluate(cache1) == expected_xc
    assert (
        f1.evaluate(cache1, timedelta_tolerance=dt.timedelta(seconds=0.250))
        == expected_xc
    )
    assert f1.evaluate(cache1, timedelta_tolerance=dt.timedelta(seconds=0.249)) is None
    assert f1.evaluate(cache1, timedelta_tolerance=dt.timedelta(0)) is None

    yc = ts.TimeSeriesEntry(ts3a, 6)
    assert cache1.update("y", yc) is True

    expected_yc = ts.TimeSeriesEntry(yc.timestamp, 14)
    assert f1.evaluate(cache1) == expected_yc
    assert f1.evaluate(cache1, timedelta_tolerance=dt.timedelta(0)) == expected_yc

    # some alternative formulas involving more variables and operations
    f2 = ts.TimeSeriesFormula[float]("x4 - (x5 + x6)")

    f3 = ts.TimeSeriesFormula[float]("(x5 + x6) * x7")

    f4 = ts.TimeSeriesFormula[float]("x7 / x8")

    cache2 = ts.LatestEntryCache[str, float]()

    ts4a = dt.datetime.fromisoformat("2021-03-11T12:42:00+00:00")
    x4a = ts.TimeSeriesEntry(ts4a, 7.5)
    assert cache2.update("x4", x4a) is True

    assert f2.evaluate(cache2) is None
    assert f3.evaluate(cache2) is None
    assert f4.evaluate(cache2) is None

    ts5a = dt.datetime.fromisoformat("2021-03-11T12:42:00.100+00:00")
    x5a = ts.TimeSeriesEntry(ts5a, 4.5)
    assert cache2.update("x5", x5a) is True

    assert f2.evaluate(cache2) is None
    assert f3.evaluate(cache2) is None
    assert f4.evaluate(cache2) is None

    ts6a = dt.datetime.fromisoformat("2021-03-11T12:42:00.200+00:00")
    x6a = ts.TimeSeriesEntry(ts6a, 2.25)
    assert cache2.update("x6", x6a) is True

    expected_456 = ts.TimeSeriesEntry(x6a.timestamp, 0.75)
    assert f2.evaluate(cache2) == expected_456
    assert (
        f2.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.200))
        == expected_456
    )
    assert f2.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.199)) is None
    assert f2.evaluate(cache2, timedelta_tolerance=dt.timedelta(0)) is None

    assert f3.evaluate(cache2) is None
    assert f4.evaluate(cache2) is None

    ts7a = dt.datetime.fromisoformat("2021-03-11T12:42:00.300+00:00")
    x7a = ts.TimeSeriesEntry(timestamp=ts7a, value=3.0)
    assert cache2.update("x7", x7a) is True

    assert f2.evaluate(cache2) == expected_456
    assert (
        f2.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.300))
        == expected_456
    )
    assert f2.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.299)) is None
    assert f2.evaluate(cache2, timedelta_tolerance=dt.timedelta(0)) is None

    expected_567 = ts.TimeSeriesEntry(x7a.timestamp, 20.25)
    assert f3.evaluate(cache2) == expected_567
    assert (
        f3.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.200))
        == expected_567
    )
    assert f3.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.199)) is None
    assert f3.evaluate(cache2, timedelta_tolerance=dt.timedelta(0)) is None

    assert f4.evaluate(cache2) is None

    ts8a = dt.datetime.fromisoformat("2021-03-11T12:42:00.400+00:00")
    x8a = ts.TimeSeriesEntry(timestamp=ts8a, value=12.0)
    assert cache2.update("x8", x8a) is True

    assert f2.evaluate(cache2) == expected_456
    assert (
        f2.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.400))
        == expected_456
    )
    assert f2.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.399)) is None
    assert f2.evaluate(cache2, timedelta_tolerance=dt.timedelta(0)) is None

    assert f3.evaluate(cache2) == expected_567
    assert (
        f3.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.300))
        == expected_567
    )
    assert f3.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.299)) is None
    assert f3.evaluate(cache2, timedelta_tolerance=dt.timedelta(0)) is None

    expected_78 = ts.TimeSeriesEntry(x8a.timestamp, 0.25)
    assert f4.evaluate(cache2) == expected_78
    assert (
        f4.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.100))
        == expected_78
    )
    assert f4.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.099)) is None
    assert f4.evaluate(cache2, timedelta_tolerance=dt.timedelta(0)) is None

    # since f4 involves division, let's test a case where the denominator
    # is zero ...
    ts8z = dt.datetime.fromisoformat("2021-03-11T12:42:00.500+00:00")
    x8z = ts.TimeSeriesEntry(timestamp=ts8z, value=0.0)
    assert cache2.update("x8", x8z) is True

    assert f2.evaluate(cache2) == expected_456
    assert (
        f2.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.500))
        == expected_456
    )
    assert f2.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.499)) is None
    assert f2.evaluate(cache2, timedelta_tolerance=dt.timedelta(0)) is None

    assert f3.evaluate(cache2) == expected_567
    assert (
        f3.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.400))
        == expected_567
    )
    assert f3.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.399)) is None
    assert f3.evaluate(cache2, timedelta_tolerance=dt.timedelta(0)) is None

    # cases where the formula would be calculated result in zero-division
    # errors being raised, otherwise we get `None` as is normal when at
    # least one of the variables is outside the timedelta tolerance
    f4_result = f4.evaluate(cache2)
    assert f4_result is not None
    assert f4_result.status == ts.TimeSeriesEntry.Status.ERROR

    f4_result_outdated = f4.evaluate(
        cache2, timedelta_tolerance=dt.timedelta(seconds=0.200)
    )
    assert f4_result_outdated is not None
    assert f4_result_outdated.status == ts.TimeSeriesEntry.Status.ERROR
    assert f4.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.199)) is None
    assert f4.evaluate(cache2, timedelta_tolerance=dt.timedelta(0)) is None


def test_TimeSeriesFormula_ComplexPower() -> None:
    # since the major reason for `ComplexPower` to exist is for us to
    # use it in time series formulas combining different compoments'
    # power measurements, we test this specific use case in detail

    S = ComplexPower

    # define a simple formula and observe how it changes
    # as cached observations change
    f1 = ts.TimeSeriesFormula[ComplexPower]("x + y")

    cache1 = ts.LatestEntryCache[str, ComplexPower]()

    # the formula needs observations for 2 different time series:
    # with data only for one, a `None` result will be generated
    ts1a = dt.datetime.fromisoformat("2021-03-11T10:49:00+00:00")
    xa = ts.TimeSeriesEntry(ts1a, S(-5.0))
    assert cache1.update("x", xa) is True
    assert f1.evaluate(cache1) is None  # missing data

    # a second observation for the same time series still results in None
    ts1b = dt.datetime.fromisoformat("2021-03-11T10:49:00.200+00:00")
    xb = ts.TimeSeriesEntry(ts1b, S(-7.0))
    assert cache1.update("x", xb) is True
    assert f1.evaluate(cache1) is None  # missing data

    # with an observation for the second component, whether or not we can
    # calculate a result depends on whether both values are within the
    # timedelta tolerance (relative to the most recent observation across
    # all component_id keys); if a result is generated, its timestamp will
    # be equal to the most recent of the two required observations
    ts2a = dt.datetime.fromisoformat("2021-03-11T10:49:00+00:00")
    ya = ts.TimeSeriesEntry(ts2a, S(5.0))
    assert cache1.update("y", ya) is True

    expected_ya = ts.TimeSeriesEntry(ts1b, S(-2.0))
    assert f1.evaluate(cache1) == expected_ya
    assert (
        f1.evaluate(cache1, timedelta_tolerance=dt.timedelta(seconds=0.200))
        == expected_ya
    )
    assert f1.evaluate(cache1, timedelta_tolerance=dt.timedelta(seconds=0.199)) is None
    assert f1.evaluate(cache1, timedelta_tolerance=dt.timedelta(0)) is None

    # a more recent observation will result in a change to both timestamp
    # and value of the formula result, and changes the tolerance limits
    ts2b = dt.datetime.fromisoformat("2021-03-11T10:49:00.500+00:00")
    yb = ts.TimeSeriesEntry(ts2b, S(4.5 + 1.0j))
    assert cache1.update("y", yb) is True

    expected_yb = ts.TimeSeriesEntry(ts2b, S(-2.5 + 1.0j))
    assert f1.evaluate(cache1) == expected_yb
    assert (
        f1.evaluate(cache1, timedelta_tolerance=dt.timedelta(seconds=0.300))
        == expected_yb
    )
    assert f1.evaluate(cache1, timedelta_tolerance=dt.timedelta(seconds=0.299)) is None
    assert f1.evaluate(cache1, timedelta_tolerance=dt.timedelta(0)) is None

    # a still more recent observation for a component_id not in the formula
    # will not change the result of the formula itself but affects tolerance
    ts3a = dt.datetime.fromisoformat("2021-03-11T10:49:00.750+00:00")
    za = ts.TimeSeriesEntry(ts3a, S(-99.9))
    assert cache1.update("z", za) is True

    assert f1.evaluate(cache1) == expected_yb
    assert (
        f1.evaluate(cache1, timedelta_tolerance=dt.timedelta(seconds=0.550))
        == expected_yb
    )
    assert f1.evaluate(cache1, timedelta_tolerance=dt.timedelta(seconds=0.549)) is None
    assert f1.evaluate(cache1, timedelta_tolerance=dt.timedelta(0)) is None

    # if the timestamps of all observations are exactly equal then we will
    # get the same result all the way down to timedelta_tolerance == 0
    xc = ts.TimeSeriesEntry(ts3a, S(-8.25 - 0.5j))
    assert cache1.update("x", xc) is True

    expected_xc = ts.TimeSeriesEntry(ts3a, S(-3.75 + 0.5j))
    assert f1.evaluate(cache1) == expected_xc
    assert (
        f1.evaluate(cache1, timedelta_tolerance=dt.timedelta(seconds=0.250))
        == expected_xc
    )
    assert f1.evaluate(cache1, timedelta_tolerance=dt.timedelta(seconds=0.249)) is None
    assert f1.evaluate(cache1, timedelta_tolerance=dt.timedelta(0)) is None

    yc = ts.TimeSeriesEntry(ts3a, S(8.5 + 0.25j))
    assert cache1.update("y", yc) is True

    expected_yc = ts.TimeSeriesEntry(ts3a, S(0.25 - 0.25j))
    assert f1.evaluate(cache1) == expected_yc
    assert f1.evaluate(cache1, timedelta_tolerance=dt.timedelta(0)) == expected_yc

    # some alternative formulas involving all the supported operations
    # (addition, subtraction, multiplication, division): for simplicity
    # we just focus on entirely active (real) power values
    f2 = ts.TimeSeriesFormula[ComplexPower]("x4 - (x5 + x6)")

    f3 = ts.TimeSeriesFormula[ComplexPower]("x5 + x6 - 3 * x7")

    f4 = ts.TimeSeriesFormula[ComplexPower]("-x7 - x8 / 4.0")

    cache2 = ts.LatestEntryCache[str, ComplexPower]()

    ts4a = dt.datetime.fromisoformat("2021-03-11T12:42:00+00:00")
    x4a = ts.TimeSeriesEntry(ts4a, S(-7.5))
    assert cache2.update("x4", x4a) is True

    assert f2.evaluate(cache2) is None
    assert f3.evaluate(cache2) is None
    assert f4.evaluate(cache2) is None

    ts5a = dt.datetime.fromisoformat("2021-03-11T12:42:00.100+00:00")
    x5a = ts.TimeSeriesEntry(ts5a, S(4.5))
    assert cache2.update("x5", x5a) is True

    assert f2.evaluate(cache2) is None
    assert f3.evaluate(cache2) is None
    assert f4.evaluate(cache2) is None

    ts6a = dt.datetime.fromisoformat("2021-03-11T12:42:00.200+00:00")
    x6a = ts.TimeSeriesEntry(ts6a, S(-3.0))
    assert cache2.update("x6", x6a) is True

    expected_f2 = ts.TimeSeriesEntry(ts6a, S(-9.0))
    assert f2.evaluate(cache2) == expected_f2
    assert (
        f2.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.200))
        == expected_f2
    )
    assert f2.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.199)) is None
    assert f2.evaluate(cache2, timedelta_tolerance=dt.timedelta(0)) is None

    assert f3.evaluate(cache2) is None
    assert f4.evaluate(cache2) is None

    ts7a = dt.datetime.fromisoformat("2021-03-11T12:42:00.300+00:00")
    x7a = ts.TimeSeriesEntry(timestamp=ts7a, value=S(-5.25))
    assert cache2.update("x7", x7a) is True

    assert f2.evaluate(cache2) == expected_f2
    assert (
        f2.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.300))
        == expected_f2
    )
    assert f2.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.299)) is None
    assert f2.evaluate(cache2, timedelta_tolerance=dt.timedelta(0)) is None

    expected_f3 = ts.TimeSeriesEntry(ts7a, S(17.25))
    assert f3.evaluate(cache2) == expected_f3
    assert (
        f3.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.200))
        == expected_f3
    )
    assert f3.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.199)) is None
    assert f3.evaluate(cache2, timedelta_tolerance=dt.timedelta(0)) is None

    assert f4.evaluate(cache2) is None

    ts8a = dt.datetime.fromisoformat("2021-03-11T12:42:00.400+00:00")
    x8a = ts.TimeSeriesEntry(timestamp=ts8a, value=S(11.0))
    assert cache2.update("x8", x8a) is True

    assert f2.evaluate(cache2) == expected_f2
    assert (
        f2.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.400))
        == expected_f2
    )
    assert f2.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.399)) is None
    assert f2.evaluate(cache2, timedelta_tolerance=dt.timedelta(0)) is None

    assert f3.evaluate(cache2) == expected_f3
    assert (
        f3.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.300))
        == expected_f3
    )
    assert f3.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.299)) is None
    assert f3.evaluate(cache2, timedelta_tolerance=dt.timedelta(0)) is None

    expected_f4 = ts.TimeSeriesEntry(ts8a, S(2.5))
    assert f4.evaluate(cache2) == expected_f4
    assert (
        f4.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.100))
        == expected_f4
    )
    assert f4.evaluate(cache2, timedelta_tolerance=dt.timedelta(seconds=0.099)) is None
    assert f4.evaluate(cache2, timedelta_tolerance=dt.timedelta(0)) is None
