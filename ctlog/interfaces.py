from typing import Callable, Protocol


class Scheduler(Protocol):
    def call_later(self, delay_ms: int, callback: Callable[[], None]) -> None: ...


class Endpoint(Protocol):
    def send(self, to: str, msg: dict) -> None: ...
