from typing import Callable


class FIFOBroadcast:
    """Gets all message to every node (exactly once)
    in the order its origins sent them."""

    def __init__(
        self,
        node_id: str,
        peers: list[str],
        send_down: Callable[[str, dict], None],
        deliver_up: Callable[[str, dict], None],
    ) -> None:
        """send_down(to, msg) puts a message on the network.
        deliver_up(origin, body) is called for each message that is ready,
        in the order its origin sent it."""
        self._node_id = node_id
        self._peers = tuple(peer for peer in peers if peer != node_id)
        self._send_down = send_down
        self._deliver_up = deliver_up
        self._next_sequence = 1
        members = (node_id, *self._peers)
        self._history: dict[str, dict[int, dict]] = {member: {} for member in members}
        self._next_expected: dict[str, int] = {member: 1 for member in members}
        self._duplicates_ignored = 0
        self._resent = 0
        self._statuses_received = 0

    def broadcast(self, body: dict) -> None:
        msg = {
            "type": "broadcast",
            "origin": self._node_id,
            "seq": self._next_sequence,
            "body": body,
        }
        self._next_sequence += 1
        self._accept(msg)

    def receive(self, msg: dict) -> None:
        if msg["type"] == "broadcast":
            self._accept(msg)
        elif msg["type"] == "status":
            self._on_status(msg)
        else:
            raise ValueError(f"Unknown message type: {msg['type']}")

    def send_status(self) -> None:
        """Tell every peer, for each origin, the last seq I have with no gaps before it."""
        status = {"type": "status", "sender": self._node_id, "have": self._have()}
        for peer in self._peers:
            self._send_down(peer, status)

    def _on_status(self, msg: dict) -> None:
        """Resend to the sender every message it reported it doesn't have yet."""
        self._statuses_received += 1
        for origin, their_last_seq in msg["have"].items():
            for seq in sorted(self._history[origin]):
                if seq > their_last_seq:
                    self._resent += 1
                    self._send_down(msg["sender"], self._history[origin][seq])

    def _accept(self, msg: dict) -> None:
        """Ignore duplicates; store, relay, and (if applicable) deliver messages in order."""
        msg_origin = msg["origin"]
        msg_seq = msg["seq"]
        if msg_seq in self._history[msg_origin]:
            self._duplicates_ignored += 1
            return
        self._history[msg_origin][msg_seq] = msg

        self._relay(msg)
        self._release_in_order(msg_origin)

    def _release_in_order(self, msg_origin: str) -> None:
        while self._next_expected[msg_origin] in self._history[msg_origin]:
            ready_to_deliver_message = self._history[msg_origin][
                self._next_expected[msg_origin]
            ]
            self._next_expected[msg_origin] += 1
            self._deliver_up(msg_origin, ready_to_deliver_message["body"])

    def snapshot(self) -> dict:
        parked = {}
        for origin, messages in self._history.items():
            parked[origin] = []
            for seq in sorted(messages):
                if seq >= self._next_expected[origin]:
                    parked[origin].append(seq)
        return {
            "have": self._have(),
            "parked": parked,
            "duplicates_ignored": self._duplicates_ignored,
            "resent": self._resent,
            "statuses_received": self._statuses_received,
        }

    def _have(self) -> dict[str, int]:
        # the last seq I have from each origin with no gaps before it
        return {origin: seq - 1 for origin, seq in self._next_expected.items()}

    def _relay(self, msg: dict) -> None:
        for peer in self._peers:
            if peer != msg["origin"]:
                self._send_down(peer, msg)
