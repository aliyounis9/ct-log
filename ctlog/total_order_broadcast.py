import heapq
from typing import Callable


class TotalOrderBroadcast:
    """Delivers every entry on every node in the same order, using Lamport
    timestamps. Sits on top of FIFOBroadcast."""

    def __init__(
        self,
        node_id: str,
        peers: list[str],
        send_down: Callable[[dict], None],
        deliver_up: Callable[[dict], None],
    ) -> None:
        """send_down(body) hands the body of the message down to FIFOBroadcast.
        deliver_up(entry) is called for each entry, in the same order on every node."""
        self._node_id = node_id
        self._peers = tuple(peer for peer in peers if peer != node_id)
        self._send_down = send_down
        self._deliver_up = deliver_up
        self._lamport_clock = 0
        # heap of (ts, origin, entry)
        # (ts, origin) is unique, so entries will never get compared
        self._pending: list[tuple[int, str, dict]] = []
        members = (node_id, *self._peers)
        self._latest_ts: dict[str, int] = {member: 0 for member in members}
        self._heartbeats_received = 0

    def _build_body(self, category: str, entry: dict | None = None) -> dict:
        msg_body = {
            "category": category,
            "timestamp": self._lamport_clock,
        }
        if entry is not None:
            msg_body["entry"] = entry
        return msg_body

    def broadcast(self, entry: dict) -> None:
        self._lamport_clock += 1
        msg_body = self._build_body("data", entry)
        self._send_down(msg_body)

    def send_heartbeat(self) -> None:
        """Tell every node my clock, so they know nothing older is coming from me.
        The node must call this regularly."""
        self._lamport_clock += 1
        msg_body = self._build_body("heartbeat")
        self._send_down(msg_body)

    def _is_safe_to_deliver(self, timestamp: int, origin: str) -> bool:
        return not self._waiting_for(timestamp, origin)

    def _waiting_for(self, timestamp: int, origin: str) -> list[str]:
        # It's only safe to deliver a message if it has a timestamp
        # that's not bigger than than other nodes' latest timestamps
        # But we can safely exclude the origin since its own messages are guaranteed
        # to be delivered in order (FIFO)
        # And we can ignore THIS node --> a node's own messages don't go through the network,
        # so they're already in the heap, and anything it sends later gets a bigger
        # timestamp since receiving this message already pushed its clock past it

        waiting = []
        for node, latest_ts in self._latest_ts.items():
            if node != self._node_id and node != origin and latest_ts < timestamp:
                waiting.append(node)
        return waiting

    def snapshot(self) -> dict:
        latest = {}
        for node, latest_ts in self._latest_ts.items():
            if node != self._node_id:
                latest[node] = latest_ts

        pending = []
        for timestamp, origin, entry in sorted(self._pending):
            pending.append(
                {
                    "timestamp": timestamp,
                    "origin": origin,
                    "entry": entry,
                    "waiting_for": self._waiting_for(timestamp, origin),
                }
            )
        return {
            "clock": self._lamport_clock,
            "latest": latest,
            "heartbeats_received": self._heartbeats_received,
            "pending": pending,
        }

    def _deliver_safe_entries(self) -> None:
        while self._pending and self._is_safe_to_deliver(
            self._pending[0][0], self._pending[0][1]
        ):
            timestamp, origin, entry = heapq.heappop(self._pending)
            self._deliver_up(entry)

    def receive(self, origin: str, body: dict) -> None:
        """Bodies must arrive exactly once and in the order their
        origins sent them; the delivery rule depends on it."""
        timestamp = body["timestamp"]
        self._lamport_clock = max(self._lamport_clock, timestamp) + 1
        self._latest_ts[origin] = timestamp

        if body["category"] == "data":
            heapq.heappush(self._pending, (timestamp, origin, body["entry"]))
        elif body["category"] == "heartbeat":
            if origin != self._node_id:
                self._heartbeats_received += 1
        else:
            raise ValueError(f"Unknown body category: {body['category']}")

        self._deliver_safe_entries()
