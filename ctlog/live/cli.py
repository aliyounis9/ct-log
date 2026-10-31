"""Run the cluster as real processes and manage it from a prompt.

python -m ctlog start         # start every node in cluster_config.json, then open the prompt
python -m ctlog run-node a    # run one node (start does this for each node)
"""

import argparse
import asyncio
import cmd
import json
import random
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

from ctlog.faults import NetworkConditions
from ctlog.live.asyncio_scheduler import AsyncioScheduler
from ctlog.live.udp_endpoint import Address, UdpEndpoint, UdpReceiver
from ctlog.node import Node

HOST = "127.0.0.1"
CONTROL_PORT_OFFSET = 1000  # control port = protocol port + 1000
ROOT = Path(__file__).resolve().parents[2]
CLUSTER_FILE = ROOT / "cluster_config.json"
LOG_DIR = ROOT / "logs"


def load_cluster() -> tuple[dict[str, int], NetworkConditions]:
    """Node ID -> protocol port, and the network conditions, from cluster_config.json."""
    config = json.loads(CLUSTER_FILE.read_text())
    network = config["network"]
    conditions = NetworkConditions(
        drop_probability=network["drop_probability"],
        duplicate_probability=network["duplicate_probability"],
        latency_ms=tuple(network["latency_ms"]),
    )
    return config["nodes"], conditions


class ControlReceiver(asyncio.DatagramProtocol):
    """Answers client commands: submit an entry, or send back the log."""

    def __init__(self, node: Node) -> None:
        self._node = node
        self._transport: asyncio.DatagramTransport | None = None

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self._transport = transport

    def datagram_received(self, data: bytes, addr: Address) -> None:
        reply = self._handle(json.loads(data.decode()))
        self._transport.sendto(json.dumps(reply).encode(), addr)

    def _handle(self, request: dict) -> dict:
        if request.get("cmd") == "submit":
            try:
                self._node.submit(request["entry"])
            except (TypeError, ValueError) as error:
                return {"ok": False, "error": str(error)}
            return {"ok": True}
        if request.get("cmd") == "log":
            return {"ok": True, "entries": self._node.entries()}
        return {"ok": False, "error": f"unknown command: {request.get('cmd')}"}


async def run_node(node_id: str) -> None:
    ports, conditions = load_cluster()
    addresses = {other: (HOST, port) for other, port in ports.items()}

    loop = asyncio.get_running_loop()
    scheduler = AsyncioScheduler(loop)
    node = Node(
        node_id,
        list(ports),
        scheduler,
        seed=random.randrange(2**32),
        on_new_entry=lambda entry: print(f"{node_id}: delivered {entry}", flush=True),
    )

    # protocol socket: messages between nodes
    transport, _ = await loop.create_datagram_endpoint(
        lambda: UdpReceiver(node.on_message), local_addr=addresses[node_id]
    )
    node.set_endpoint(
        UdpEndpoint(
            node_id, addresses, transport, scheduler, conditions, random.Random()
        )
    )
    node.start_timers()

    await loop.create_datagram_endpoint(
        lambda: ControlReceiver(node),
        local_addr=(HOST, ports[node_id] + CONTROL_PORT_OFFSET),
    )

    print(f"{node_id}: running on port {ports[node_id]}", flush=True)
    await asyncio.Event().wait()


def ask(port: int, request: dict, timeout_s: float = 1.0) -> dict | None:
    """Send one request to a node's control port. None if it doesn't answer."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(timeout_s)
        sock.sendto(json.dumps(request).encode(), (HOST, port + CONTROL_PORT_OFFSET))
        try:
            return json.loads(sock.recv(65535).decode())
        except socket.timeout:
            return None


def side_by_side(logs: dict[str, list[dict] | None], width: int = 26) -> str:
    """One column per node, one row per log position. None means the node is down."""
    up = {node: entries for node, entries in logs.items() if entries is not None}
    rows = max((len(entries) for entries in up.values()), default=0)

    first_difference = None
    for i in range(rows):
        cells = {
            str(entries[i]) if i < len(entries) else None for entries in up.values()
        }
        if len(cells) > 1:
            first_difference = i
            break

    def cell(entries: list[dict] | None, i: int) -> str:
        if entries is None:
            return "DOWN" if i == 0 else ""
        if i >= len(entries):
            return ""
        text = f"{entries[i].get('domain')} ({entries[i].get('issuer')})"
        return text[: width - 2]  # keep a gap between columns

    lines = ["  #  " + "".join(node.ljust(width) for node in logs)]
    for i in range(max(rows, 1)):
        row = f"{i:3}  " + "".join(
            cell(entries, i).ljust(width) for entries in logs.values()
        )
        lines.append(row + (" ◄ first difference" if i == first_difference else ""))

    down = [node for node, entries in logs.items() if entries is None]
    if first_difference is None:
        summary = (
            f"{', '.join(up)} identical ({rows} entries)" if up else "no node answered"
        )
    else:
        summary = f"logs differ from row {first_difference}"
    if down:
        summary += f"; down: {', '.join(down)}"
    lines.append(summary)
    return "\n".join(lines)


class Shell(cmd.Cmd):
    """Type a command after `ctlog>`; `help` lists them."""

    prompt = "ctlog> "

    def __init__(
        self, processes: dict[str, subprocess.Popen], ports: dict[str, int]
    ) -> None:
        super().__init__()
        self._processes = processes
        self._ports = ports

    def do_submit(self, arg: str) -> None:
        """submit NODE DOMAIN ISSUER: send a certificate entry to a node."""
        parts = arg.split()
        if len(parts) != 3 or parts[0] not in self._ports:
            print(
                f"usage: submit NODE DOMAIN ISSUER  (nodes: {', '.join(self._ports)})"
            )
            return
        node, domain, issuer = parts
        reply = ask(
            self._ports[node],
            {"cmd": "submit", "entry": {"domain": domain, "issuer": issuer}},
        )
        print(
            f"{node} is down"
            if reply is None
            else ("ok" if reply["ok"] else reply["error"])
        )

    def do_inspect(self, arg: str) -> None:
        """inspect: every node's log, side by side."""
        logs = {}
        for node, port in self._ports.items():
            reply = ask(port, {"cmd": "log"})
            logs[node] = None if reply is None else reply["entries"]
        print(side_by_side(logs))

    def do_crash(self, arg: str) -> None:
        """crash NODE: kill a node's process (like kill -9)."""
        node = self._running_node(arg, "crash")
        if node is not None:
            self._processes[node].kill()
            self._processes[node].wait()
            print(f"{node} crashed")

    def do_pause(self, arg: str) -> None:
        """pause NODE: freeze a node's process; to the others it looks crashed."""
        node = self._running_node(arg, "pause")
        if node is not None:
            self._processes[node].send_signal(signal.SIGSTOP)
            print(f"{node} paused")

    def do_resume(self, arg: str) -> None:
        """resume NODE: let a paused node continue; it catches up on what it missed."""
        node = self._running_node(arg, "resume")
        if node is not None:
            self._processes[node].send_signal(signal.SIGCONT)
            print(f"{node} resumed")

    def _running_node(self, arg: str, command: str) -> str | None:
        """The node named in arg, if it exists and its process is still running."""
        node = arg.strip()
        if node not in self._processes:
            print(f"usage: {command} NODE  (nodes: {', '.join(self._ports)})")
            return None
        if self._processes[node].poll() is not None:
            print(f"{node} is not running")
            return None
        return node

    def do_quit(self, arg: str) -> bool:
        """quit: stop every node and exit."""
        return True

    do_EOF = do_quit

    def emptyline(self) -> None:
        pass  # an empty line does nothing (cmd would repeat the last command)

    def get_names(self) -> list[str]:
        # hide do_EOF from help; Ctrl-D still works since cmd finds it by name
        return [name for name in super().get_names() if name != "do_EOF"]


def start() -> None:
    ports, _ = load_cluster()
    LOG_DIR.mkdir(exist_ok=True)
    processes = {}
    for node in ports:
        log_file = open(LOG_DIR / f"{node}.log", "w")
        processes[node] = subprocess.Popen(
            [sys.executable, "-m", "ctlog", "run-node", node],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            cwd=ROOT,
        )
        print(
            f"{node}: pid {processes[node].pid}, port {ports[node]}, output in logs/{node}.log"
        )
    time.sleep(0.5)
    for node, process in processes.items():
        if process.poll() is not None:
            print(f"{node} exited right away; see logs/{node}.log")

    try:
        Shell(processes, ports).cmdloop()
    except KeyboardInterrupt:
        print()
    finally:
        for process in processes.values():
            process.send_signal(signal.SIGCONT)  # a paused process ignores terminate()
            process.terminate()
        print("stopped all nodes")


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m ctlog")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("start", help="start every node and open the prompt")
    run = commands.add_parser("run-node", help="run one node (start does this for you)")
    run.add_argument("node_id")
    args = parser.parse_args()

    if args.command == "start":
        start()
    else:
        try:
            asyncio.run(run_node(args.node_id))
        except KeyboardInterrupt:
            pass
