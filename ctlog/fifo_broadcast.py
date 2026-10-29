from typing import Callable


class FIFOBroadcast:
    """Gets all message to every node (exactly once)
    in the order its origins sent them."""

    def __init__(
        self,
        node_id: str,
        peers: list[str],
        send: Callable[[str, dict], None],
        on_deliver: Callable[[str, dict], None],
    ) -> None:
        """send(to, msg) puts a message on the network.
        on_deliver(origin, body) is called for each message that is ready,
        in the order its origin sent it."""
        self._node_id = node_id
        self._peers = tuple(peer for peer in peers if peer != node_id)
        self._send = send
        self._on_deliver = on_deliver
        self._next_sequence = 1
        members = (node_id, *self._peers)
        self._history: dict[str, dict[int, dict]] = {member: {} for member in members}
        self._next_expected: dict[str, int] = {member: 1 for member in members}

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
        have = {origin: seq - 1 for origin, seq in self._next_expected.items()}
        status = {"type": "status", "sender": self._node_id, "have": have}
        for peer in self._peers:
            self._send(peer, status)

    def _on_status(self, msg: dict) -> None:
        """Resend to the sender every message it reported it doesn't have yet."""
        for origin, their_last_seq in msg["have"].items():
            for seq in sorted(self._history[origin]):
                if seq > their_last_seq:
                    self._send(msg["sender"], self._history[origin][seq])

    def _accept(self, msg: dict) -> None:
        """Ignore duplicates; store, relay, and (if applicable) deliver messages in order."""
        msg_origin = msg["origin"]
        msg_seq = msg["seq"]
        if msg_seq in self._history[msg_origin]:
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
            self._on_deliver(msg_origin, ready_to_deliver_message["body"])

    def _relay(self, msg: dict) -> None:
        for peer in self._peers:
            if peer != msg["origin"]:
                self._send(peer, msg)
