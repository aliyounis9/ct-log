import random
from enum import Enum, auto
from typing import Callable

from ctlog.fifo_broadcast import FIFOBroadcast
from ctlog.log import Log, encode_entry
from ctlog.interfaces import Endpoint, Scheduler
from ctlog.total_order_broadcast import TotalOrderBroadcast


class NodeState(Enum):
    RUNNING = auto()
    CRASHED = auto()


class Node:
    """The network connects to it; it connects the fifo broadcast,
    the total order broadcast and its log. uses the scheduler to send
    status updates and heartbeats regularly."""

    def __init__(
        self,
        node_id: str,
        peers: list[str],
        scheduler: Scheduler,
        seed: int = 0,
        tick_interval_ms: int = 100,
        on_new_entry: Callable[[dict], None] | None = None,
    ) -> None:
        self.node_id = node_id
        self._endpoint: Endpoint | None = None
        self._log = Log()
        self._scheduler = scheduler
        self._tick_interval_ms = tick_interval_ms
        self._random_generator = random.Random(seed)
        self._state = NodeState.RUNNING
        self._on_new_entry = on_new_entry
        self._fifo = FIFOBroadcast(
            node_id,
            peers,
            send_down=self._fifo_to_network,
            deliver_up=self._fifo_to_tob,
        )
        self._tob = TotalOrderBroadcast(
            node_id,
            peers,
            send_down=self._tob_to_fifo,
            deliver_up=self._tob_to_node,
        )

    def set_endpoint(self, endpoint: Endpoint) -> None:
        self._endpoint = endpoint

    @property
    def state(self) -> NodeState:
        return self._state

    def start_timers(self) -> None:
        self._schedule_tick()

    def crash(self) -> None:
        """Stop for good: ignore incoming messages, send nothing, stop the timers."""
        self._state = NodeState.CRASHED

    def submit(self, entry: dict) -> None:
        if self._state is NodeState.CRASHED:
            raise RuntimeError(f"Node {self.node_id} has crashed.")
        encode_entry(entry)
        self._tob.broadcast(entry)

    def on_message(self, msg: dict) -> None:
        if self._state is NodeState.CRASHED:
            return
        self._fifo.receive(msg)

    def entries(self) -> list[dict]:
        return [self._log.get_entry_at(i) for i in range(len(self._log))]

    def status(self) -> dict:
        return {
            "state": self._state.name.lower(),
            "log_size": len(self._log),
            "fifo": self._fifo.snapshot(),
            "tob": self._tob.snapshot(),
        }

    def _fifo_to_network(self, to: str, msg: dict) -> None:
        if self._state is NodeState.CRASHED:
            return
        if self._endpoint is None:
            raise RuntimeError(f"Node {self.node_id} isn't connected to a network.")
        self._endpoint.send(to, msg)

    def _tob_to_fifo(self, body: dict) -> None:
        self._fifo.broadcast(body)

    def _fifo_to_tob(self, origin: str, body: dict) -> None:
        self._tob.receive(origin, body)

    def _tob_to_node(self, entry: dict) -> None:
        self._log.append_entry(entry)
        if self._on_new_entry is not None:
            self._on_new_entry(entry)

    def _schedule_tick(self) -> None:
        self._scheduler.call_later(self._random_tick_delay(), self._on_tick)

    def _on_tick(self) -> None:
        if self._state is NodeState.CRASHED:
            return
        self._fifo.send_status()
        self._tob.send_heartbeat()
        self._schedule_tick()

    def _random_tick_delay(self) -> int:
        return self._random_generator.randint(1, self._tick_interval_ms)
