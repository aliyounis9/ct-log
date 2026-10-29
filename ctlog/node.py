import random

from ctlog.fifo_broadcast import FIFOBroadcast
from ctlog.log import Log, encode_entry
from ctlog.network import Endpoint
from ctlog.scheduler import Scheduler


class Node:
    """The network connects to it; it connects the fifo
    broadcast to its log. uses the scheduler to send
    status updates regularly."""

    def __init__(
        self,
        node_id: str,
        peers: list[str],
        scheduler: Scheduler,
        seed: int = 0,
        status_interval_ms: int = 100,
    ) -> None:
        self.node_id = node_id
        self._endpoint: Endpoint | None = None
        self._log = Log()
        self._scheduler = scheduler
        self._status_interval_ms = status_interval_ms
        self._random_generator = random.Random(seed)
        self._fifo = FIFOBroadcast(node_id, peers, self._send, self._on_deliver)

    def set_endpoint(self, endpoint: Endpoint) -> None:
        self._endpoint = endpoint

    def start_timers(self) -> None:
        self._schedule_status()

    def submit(self, entry: dict) -> None:
        encode_entry(entry)
        self._fifo.broadcast({"category": "data", "entry": entry})

    def on_message(self, msg: dict) -> None:
        self._fifo.receive(msg)

    def entries(self) -> list[dict]:
        return [self._log.get_entry_at(i) for i in range(len(self._log))]

    def _send(self, to: str, msg: dict) -> None:
        if self._endpoint is None:
            raise RuntimeError(f"Node {self.node_id} isn't connected to a network.")
        self._endpoint.send(to, msg)

    def _on_deliver(self, origin: str, body: dict) -> None:
        if body["category"] != "data":
            raise ValueError(f"Unknown body category: {body['category']}")
        self._log.append_entry(body["entry"])

    def _schedule_status(self) -> None:
        self._scheduler.call_later(self._random_status_delay(), self._on_status_timer)

    def _on_status_timer(self) -> None:
        self._fifo.send_status()
        self._schedule_status()

    def _random_status_delay(self) -> int:
        return self._random_generator.randint(1, self._status_interval_ms)
