# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Ringbuffer implementation with serialization support."""

# For use of the class type hint inside the class itself.
from __future__ import annotations

import pickle
from datetime import datetime, timedelta, timezone
from os.path import exists

from ._ringbuffer import FloatArray, OrderedRingBuffer

# Version of the file dumping/loading format
FILE_FORMAT_VERSION: int = 1


class SerializableRingBuffer(OrderedRingBuffer[FloatArray]):
    """Sorted ringbuffer with serialization support."""

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        buffer: FloatArray,
        sampling_period: timedelta,
        path: str,
        time_index_alignment: datetime = datetime(1, 1, 1, tzinfo=timezone.utc),
    ) -> None:
        """Initialize the time aware ringbuffer.

        Args:
            buffer: Instance of a buffer container to use internally.
            sampling_period: Timedelta of the desired resampling period.
            path: Path to where the data should be saved to and loaded from.
            time_index_alignment: Arbitrary point in time used to align
                timestamped data with the index position in the buffer.
                Used to make the data stored in the buffer align with the
                beginning and end of the buffer borders.
                For example, if the `time_index_alignment` is set to
                "0001-01-01 12:00:00", and the `sampling_period` is set to
                1 hour and the length of the buffer is 24, then the data
                stored in the buffer could correspond to the time range from
                "2022-01-01 12:00:00" to "2022-01-02 12:00:00" (date chosen
                arbitrarily here).
        """
        # Overcome a bug in mypy: https://github.com/python/mypy/issues/14774
        super().__init__(buffer, sampling_period, time_index_alignment)  # type: ignore[arg-type]
        self._path = path
        self._file_format_version = FILE_FORMAT_VERSION

    def dump(self) -> None:
        """Dump data to disk.

        Raises:
            I/O related exceptions when the file cannot be written.
        """
        with open(self._path, mode="wb+") as fileobj:
            pickle.dump(self, fileobj)

    @classmethod
    def load(cls, path: str) -> SerializableRingBuffer[FloatArray] | None:
        """Load data from disk.

        Args:
            path: Path to the file where the data is stored.

        Raises:
            I/O related exceptions when file exists but isn't accessable for any
            reason.

        Returns:
            `None` when the file doesn't exist, otherwise an instance of the
            `SerializableRingBuffer` class, loaded from disk.
        """
        if not exists(path):
            return None

        with open(path, mode="rb") as fileobj:
            instance: SerializableRingBuffer[FloatArray] = pickle.load(fileobj)
            instance._path = path  # pylint: disable=protected-access
            # Set latest file format version for next time it dumps.
            # pylint: disable=protected-access
            instance._file_format_version = FILE_FORMAT_VERSION

            return instance
