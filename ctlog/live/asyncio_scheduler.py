import asyncio
from typing import Callable


class AsyncioScheduler:
    """Lets a node schedule its timers on asyncio's event loop."""

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def call_later(self, delay_ms: int, callback: Callable[[], None]) -> None:
        self._loop.call_later(delay_ms / 1000, callback)  # asyncio counts in seconds
