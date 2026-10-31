import json
import random
from typing import Callable

from ctlog.faults import NetworkConditions, fault_delays
from ctlog.interfaces import Scheduler


class SimEndpoint:
    """A node's connection to the network, through which
    it can only send messages to others."""

    def __init__(self, network: "Network", node_id: str) -> None:
        self._network = network
        self._node_id = node_id

    def send(self, to: str, msg: dict) -> None:
        self._network._send(self._node_id, to, msg)


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

    def _make_endpoint(self, node_id: str) -> SimEndpoint:
        return SimEndpoint(self, node_id)

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

        latencies = fault_delays(self._conditions, self._random_generator)
        if not latencies:
            self._stats["dropped"] += 1
        elif len(latencies) == 2:
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
