# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Definition of Singleton metaclass."""

from threading import Lock
from typing import Any


class SingletonMeta(type):
    """This is a thread-safe implementation of Singleton."""

    _instances: dict[Any, type] = {}
    """The dictionary of instances of the singleton classes."""

    _lock: Lock = Lock()
    """The lock to ensure thread safety.

    The lock to acquire when creating a singleton instance, preventing
    multiple threads from creating instances simultaneously.
    """

    def __call__(cls, *args: Any, **kwargs: Any) -> type:
        """Overload function call operator to return the singleton instance.

        Args:
            *args: positional args
            **kwargs: and keyword args for the super class.

        Returns:
            The singleton instance.
        """
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]
