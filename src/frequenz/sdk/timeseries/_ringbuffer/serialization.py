# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Ringbuffer dumping & loading functions."""

# For use of the class type hint inside the class itself.


import pickle
from os.path import exists

from .buffer import FloatArray, OrderedRingBuffer

FILE_FORMAT_VERSION: int = 1
"""Version of the latest file dumping/loading format."""


def load(path: str) -> OrderedRingBuffer[FloatArray] | None:
    """Load a ringbuffer from disk.

    Args:
        path: Path to the file where the data is stored.

    Raises:
        RuntimeError: when the file format version is unknown.

    Returns:
        `None` when the file doesn't exist, otherwise an instance of the
        `OrderedRingBuffer` class, loaded from disk.
    """
    if not exists(path):
        return None

    with open(path, mode="rb") as fileobj:
        instance: OrderedRingBuffer[FloatArray]
        file_format_version: int

        file_format_version, instance = pickle.load(fileobj)

    if file_format_version != FILE_FORMAT_VERSION:
        raise RuntimeError(
            f"Unknown file format version: {file_format_version}. Can load: {FILE_FORMAT_VERSION}"
        )

    return instance


def dump(  # noqa: DOC502 (OSError is raised indirectly by open and pickle.dump)
    ringbuffer: OrderedRingBuffer[FloatArray],
    path: str,
    file_format_version: int = FILE_FORMAT_VERSION,
) -> None:
    """Dump a ringbuffer to disk.

    Args:
        ringbuffer: Instance of the ringbuffer to dump.
        path: Path to where the data should be saved to.
        file_format_version: Version of the file format, optional.

    Raises:
        OSError: When the file cannot be opened or written.
    """
    with open(path, mode="wb+") as fileobj:
        pickle.dump((file_format_version, ringbuffer), fileobj)
