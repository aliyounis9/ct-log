"""python -m ctlog start --> starts every node in cluster_config.json and opens a prompt."""

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
COLUMN_WIDTH = 26


def load_cluster() -> tuple[dict[str, int], NetworkConditions]:
    config = json.loads(CLUSTER_FILE.read_text())
    network = config["network"]
    conditions = NetworkConditions(
        drop_probability=network["drop_probability"],
        duplicate_probability=network["duplicate_probability"],
        latency_ms=tuple(network["latency_ms"]),
    )
    return config["nodes"], conditions


class ControlReceiver(asyncio.DatagramProtocol):
    """Handles commands from the prompt."""

    def __init__(self, node: Node) -> None:
        self._node = node
        self._transport: asyncio.DatagramTransport | None = None

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self._transport = transport

    def datagram_received(self, data: bytes, addr: Address) -> None:
        reply = self._handle(json.loads(data.decode()))
        self._transport.sendto(json.dumps(reply).encode(), addr)

    def _handle(self, request: dict) -> dict:
        if request["cmd"] == "submit":
            try:
                self._node.submit(request["entry"])
            except (TypeError, ValueError) as error:
                return {"ok": False, "error": str(error)}
            return {"ok": True}
        if request["cmd"] == "log":
            return {"ok": True, "entries": self._node.entries()}
        if request["cmd"] == "status":
            return {"ok": True, "status": self._node.status()}
        return {"ok": False, "error": "unknown command"}


async def run_node(node_id: str) -> None:
    ports, conditions = load_cluster()
    addresses = {}
    for other, port in ports.items():
        addresses[other] = (HOST, port)

    def print_entry(entry: dict) -> None:
        print(f"{node_id}: delivered {entry}", flush=True)

    loop = asyncio.get_running_loop()
    scheduler = AsyncioScheduler(loop)
    node = Node(
        node_id,
        list(ports),
        scheduler,
        seed=random.randrange(2**32),
        on_new_entry=print_entry,
    )

    # messages between nodes
    transport, _ = await loop.create_datagram_endpoint(
        lambda: UdpReceiver(node.on_message), local_addr=addresses[node_id]
    )
    endpoint = UdpEndpoint(
        node_id, addresses, transport, scheduler, conditions, random.Random()
    )
    node.set_endpoint(endpoint)
    node.start_timers()

    # commands from the prompt
    await loop.create_datagram_endpoint(
        lambda: ControlReceiver(node),
        local_addr=(HOST, ports[node_id] + CONTROL_PORT_OFFSET),
    )

    print(f"{node_id}: running on port {ports[node_id]}", flush=True)
    await asyncio.Event().wait()  # keeps the node running until it's stopped


def ask(port: int, request: dict) -> dict | None:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(1.0)
        sock.sendto(json.dumps(request).encode(), (HOST, port + CONTROL_PORT_OFFSET))
        try:
            return json.loads(sock.recv(65535).decode())
        except socket.timeout:
            return None  # the node didn't answer


def side_by_side(logs: dict[str, list[dict] | None]) -> str:
    rows = 0
    for entries in logs.values():
        if entries is not None:
            rows = max(rows, len(entries))

    first_difference = None
    for i in range(rows):
        cells = []
        for entries in logs.values():
            if entries is not None and i < len(entries):
                cells.append(entries[i])
        if any(cell != cells[0] for cell in cells):
            first_difference = i
            break

    header = "  #  "
    for node in logs:
        header += node.ljust(COLUMN_WIDTH)
    lines = [header]

    for i in range(max(rows, 1)):
        line = f"{i:3}  "
        for entries in logs.values():
            text = ""
            if entries is None and i == 0:
                text = "DOWN"
            elif entries is not None and i < len(entries):
                text = f"{entries[i]['domain']} ({entries[i]['issuer']})"
            line += text[: COLUMN_WIDTH - 2].ljust(COLUMN_WIDTH)
        if i == first_difference:
            line += " <-- first difference"
        lines.append(line)

    behind = []
    for node, entries in logs.items():
        if entries is not None and len(entries) < rows:
            behind.append(node)

    if first_difference is not None:
        summary = f"logs differ from row {first_difference}"
    elif behind:
        summary = f"no differences, behind: {', '.join(behind)}"
    else:
        summary = f"identical, {rows} entries"
    down = [node for node, entries in logs.items() if entries is None]
    if down:
        summary += f", down: {', '.join(down)}"
    lines.append(summary)
    return "\n".join(lines)


def format_status(node: str, status: dict | None) -> str:
    if status is None:
        return f"{node}: no answer"
    fifo = status["fifo"]
    tob = status["tob"]

    lines = [
        f"{node}: {status['state']}, {status['log_size']} entries, clock {tob['clock']}"
    ]
    lines.append("  fifo")
    lines.append(f"    have: {fifo['have']}")
    for origin, seqs in fifo["parked"].items():
        if not seqs:
            continue
        shown = str(seqs[:5])
        if len(seqs) > 5:
            shown += f" +{len(seqs) - 5} more"
        lines.append(
            f"    parked from {origin}: {shown}, missing {fifo['have'][origin] + 1}"
        )
    lines.append(
        f"    duplicates ignored: {fifo['duplicates_ignored']}, resent: {fifo['resent']}"
    )
    lines.append(f"    statuses received: {fifo['statuses_received']}")

    lines.append("  tob")
    lines.append(f"    latest: {tob['latest']}")
    lines.append(f"    heartbeats received: {tob['heartbeats_received']}")
    lines.append(f"    pending: {len(tob['pending'])}")
    for item in tob["pending"][:5]:
        waiting = ", ".join(item["waiting_for"])
        lines.append(
            f"      ({item['timestamp']}, {item['origin']}) "
            f"{item['entry']['domain']} --> waiting for {waiting}"
        )
    return "\n".join(lines)


class Shell(cmd.Cmd):
    """The prompt. Type help to see the commands."""

    prompt = "ctlog> "

    def __init__(
        self, processes: dict[str, subprocess.Popen], ports: dict[str, int]
    ) -> None:
        super().__init__()
        self._processes = processes
        self._ports = ports

    def do_submit(self, arg: str) -> None:
        """submit NODE DOMAIN ISSUER [NODE DOMAIN ISSUER ...]"""
        parts = arg.split()
        submissions = []
        for i in range(0, len(parts), 3):
            submissions.append(parts[i : i + 3])

        # check everything first so we never send half of them
        for submission in submissions:
            if len(submission) != 3 or submission[0] not in self._ports:
                print("usage: submit NODE DOMAIN ISSUER [NODE DOMAIN ISSUER ...]")
                return
        if not submissions:
            print("usage: submit NODE DOMAIN ISSUER [NODE DOMAIN ISSUER ...]")
            return

        for node, domain, issuer in submissions:
            entry = {"domain": domain, "issuer": issuer}
            reply = ask(self._ports[node], {"cmd": "submit", "entry": entry})
            if reply is None:
                print(f"{node}: down")
            elif reply["ok"]:
                print(f"{node}: ok")
            else:
                print(f"{node}: {reply['error']}")

    def do_inspect(self, arg: str) -> None:
        """inspect"""
        logs = {}
        for node, port in self._ports.items():
            reply = ask(port, {"cmd": "log"})
            if reply is None:
                logs[node] = None
            else:
                logs[node] = reply["entries"]
        print(side_by_side(logs))

    def do_status(self, arg: str) -> None:
        """status [NODE]"""
        if arg == "":
            nodes = list(self._ports)
        elif arg in self._ports:
            nodes = [arg]
        else:
            print("usage: status [NODE]")
            return
        for node in nodes:
            reply = ask(self._ports[node], {"cmd": "status"})
            if reply is None:
                print(format_status(node, None))
            else:
                print(format_status(node, reply["status"]))

    def do_crash(self, arg: str) -> None:
        """crash NODE"""
        if self._is_running(arg, "crash"):
            self._processes[arg].kill()
            self._processes[arg].wait()
            print(f"{arg} crashed")

    def do_pause(self, arg: str) -> None:
        """pause NODE"""
        if self._is_running(arg, "pause"):
            self._processes[arg].send_signal(signal.SIGSTOP)
            print(f"{arg} paused")

    def do_resume(self, arg: str) -> None:
        """resume NODE"""
        if self._is_running(arg, "resume"):
            self._processes[arg].send_signal(signal.SIGCONT)
            print(f"{arg} resumed")

    def _is_running(self, node: str, command: str) -> bool:
        if node not in self._processes:
            print(f"usage: {command} NODE")
            return False
        if self._processes[node].poll() is not None:
            print(f"{node} is not running")
            return False
        return True

    def do_quit(self, arg: str) -> bool:
        """quit"""
        return True

    do_EOF = do_quit  # Ctrl-D

    def emptyline(self) -> None:
        pass  # otherwise cmd repeats the last command

    def get_names(self) -> list[str]:
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
        print(f"{node}: pid {processes[node].pid}, logs/{node}.log")

    time.sleep(0.5)
    for node, process in processes.items():
        if process.poll() is not None:
            print(f"{node} failed to start, see logs/{node}.log")

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
    commands.add_parser("start")
    run = commands.add_parser("run-node")
    run.add_argument("node_id")
    args = parser.parse_args()

    if args.command == "start":
        start()
    else:
        try:
            asyncio.run(run_node(args.node_id))
        except KeyboardInterrupt:
            pass
