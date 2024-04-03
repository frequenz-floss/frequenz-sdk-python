# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Utility for receiving messages with timeout."""

import asyncio
from typing import TypeVar

from frequenz.channels import Receiver

T = TypeVar("T")


class Timeout:
    """Sentinel for timeout."""


async def receive_timeout(recv: Receiver[T], timeout: float = 0.1) -> T | type[Timeout]:
    """Receive message from receiver with timeout.

    Args:
        recv: Receiver to receive message from.
        timeout: Timeout in seconds.

    Returns:
        Received message or Timeout if timeout is reached.
    """
    try:
        return await asyncio.wait_for(recv.receive(), timeout=timeout)
    except asyncio.TimeoutError:
        return Timeout
