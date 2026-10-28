import heapq
from typing import Callable


class Scheduler:
    """Simulated clock and event queue for running the system deterministically."""

    def __init__(self) -> None:
        self._time = 0  # current simulated time in ms
        self._count = 0  # increasing counter to break ties between equal times
        self._tasks = []  # heap of (task_time_ms, count, callback)

    def time_ms(self) -> int:
        return self._time

    def call_later(self, delay_ms: int, callback: Callable[[], None]) -> None:
        """Schedule callback() to run delay_ms after the current time."""
        if delay_ms < 0:
            raise ValueError("Delay (in ms) can't be negative.")

        self._count += 1

        heapq.heappush(self._tasks, (self._time + delay_ms, self._count, callback))

    def run_until(self, max_ms: int) -> bool:
        """Run events in time order until the queue is empty or until max_ms,
        whichever comes first.

        The clock is left at the time of the last event run.

        Returns True if no more events are left, otherwise False.
        """

        while self._tasks and self._tasks[0][0] <= max_ms:
            new_time, _, to_call = heapq.heappop(self._tasks)
            self._time = new_time
            to_call()

        return not self._tasks
