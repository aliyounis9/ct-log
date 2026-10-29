from typing import Callable
import random
from dataclasses import dataclass
import json
from ctlog.scheduler import Scheduler


class Endpoint:
    """A node's connection to the network, through which
    it can only send messages to others."""

    def __init__(self, network: "Network", node_id: str) -> None:
        self._network = network
        self._node_id = node_id

    def send(self, to: str, msg: dict) -> None:
        self._network._send(self._node_id, to, msg)


@dataclass(frozen=True)
class NetworkConditions:
    """Fault settings for the network.

    drop_probability: chance a message is lost.
    latency_ms: (min, max) delay in ms, picked at random for each message.
    duplicate_probability: chance a message that wasn't dropped is
        delivered a second time, 1 to 100 ms after the first copy.
    """

    drop_probability: float = 0.0
    latency_ms: tuple[int, int] = (30, 80)
    duplicate_probability: float = 0.0

    def __post_init__(self) -> None:
        for name in ("drop_probability", "duplicate_probability"):
            value = getattr(self, name)
            if not 0 <= value <= 1:
                raise ValueError(f"{name} must be between 0 and 1; got {value}")

        low, high = self.latency_ms
        if low < 0 or low > high:
            raise ValueError(
                f"latency_ms must be (min, max) with 0 <= min <= max; got {self.latency_ms}"
            )


class Network:
    """A simulated network that delivers messages between nodes
    through the scheduler.

    Messages can be dropped, delayed, duplicated, or reordered. They
    also have to be JSON-serializable.

    The "from" field is set to the sender on every send, replacing any existing value.
    """

    def __init__(
        self,
        scheduler: Scheduler,
        seed: int = 0,
        conditions: NetworkConditions = NetworkConditions(),
    ) -> None:
        self._scheduler = scheduler
        self._random_generator = random.Random(seed)
        self._conditions = conditions
        self._stats: dict[str, int] = {
            "sent": 0,
            "delivered": 0,
            "dropped": 0,
            "duplicated": 0,
        }
        self._handlers: dict[str, Callable[[dict], None]] = {}

    def set_conditions(self, conditions: NetworkConditions) -> None:
        """Replace the fault conditions for messages from now on."""
        self._conditions = conditions

    def get_stats(self) -> dict[str, int]:
        return self._stats.copy()

    def _make_endpoint(self, node_id: str) -> Endpoint:
        return Endpoint(self, node_id)

    def connect(self, node: "Node") -> None:
        """Give the node an endpoint to send with, and register
        its handler to receive messages."""
        if node.node_id in self._handlers:
            raise ValueError(f"Node {node.node_id} already exists on the network.")
        endpoint = self._make_endpoint(node.node_id)
        node.set_endpoint(endpoint)
        self._handlers[node.node_id] = node.on_message

    def _send(self, frm: str, to: str, msg: dict) -> None:
        """Add the sender to the message, serialize the message,
        decide drops and duplicates, and schedule delivery.
        """
        if to not in self._handlers:
            raise ValueError(f"Node {to} doesn't exist on the network.")

        msg_with_sender = {**msg, "from": frm}

        msg_to_send = json.dumps(msg_with_sender)

        self._stats["sent"] += 1

        latencies = [self._random_generator.randint(*self._conditions.latency_ms)]

        if self._random_generator.random() < self._conditions.drop_probability:
            latencies = []
            self._stats["dropped"] += 1

        if (
            latencies
            and self._random_generator.random() < self._conditions.duplicate_probability
        ):
            latencies.append((latencies[0] + self._random_generator.randint(1, 100)))
            self._stats["duplicated"] += 1

        for time_from_now in latencies:
            self._scheduler.call_later(
                time_from_now, lambda: self._deliver(to, msg_to_send)
            )

    def _deliver(self, to: str, msg: str) -> None:
        """Decode the message and pass it to the
        receiver's handler."""
        self._stats["delivered"] += 1
        msg_to_deliver = json.loads(msg)
        handler = self._handlers[to]
        handler(msg_to_deliver)
