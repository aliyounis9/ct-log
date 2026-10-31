import asyncio
import json
import random
from functools import partial
from typing import Callable

from ctlog.faults import NetworkConditions, fault_delays
from ctlog.interfaces import Scheduler

Address = tuple[str, int]  # (host, port), e.g. ("127.0.0.1", 9001)


class UdpReceiver(asyncio.DatagramProtocol):
    """Passes every UDP message that arrives to the node."""

    def __init__(self, on_message: Callable[[dict], None]) -> None:
        self._on_message = on_message

    def datagram_received(self, data: bytes, addr: Address) -> None:
        self._on_message(json.loads(data.decode()))


class UdpEndpoint:
    """Sends messages over UDP with the same faults as the simulated network."""

    def __init__(
        self,
        node_id: str,
        addresses: dict[str, Address],
        transport: asyncio.DatagramTransport,
        scheduler: Scheduler,
        conditions: NetworkConditions,
        rng: random.Random,
    ) -> None:
        self._node_id = node_id
        self._addresses = addresses
        self._transport = transport
        self._scheduler = scheduler
        self._conditions = conditions
        self._rng = rng

    def send(self, to: str, msg: dict) -> None:
        if to not in self._addresses:
            raise ValueError(f"Node {to} isn't in the address book.")
        data = json.dumps({**msg, "from": self._node_id}).encode()

        for delay_ms in fault_delays(self._conditions, self._rng):
            self._scheduler.call_later(
                delay_ms, partial(self._transport.sendto, data, self._addresses[to])
            )
