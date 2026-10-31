# CT Log

My Distributed Systems project: a Certificate Transparency log, replicated across several nodes.

## What's a CT log?

When a certificate authority (CA) issues a certificate for a domain, the certificate goes into a public,
append-only log. Anyone can read the log, so if a CA issues a certificate for `google.com` that Google never
asked for, Google can spot it. That's what happened with DigiNotar in 2011, when a fake `google.com`
certificate was used to spy on users in Iran. The log doesn't stop bad certificates, it just makes them
impossible to hide.

## Iteration 1

Goal: run the log on several nodes and keep every node's copy **identical**, even when messages get lost,
duplicated or delayed, or a node stops.

Entries are simple records for now: `{"domain": "google.com", "issuer": "DigiNotar"}`.

## Design

Each node sends entries down through two layers, and receives them back up through the same two layers:

```
node a                                                                node b
submit --> TOB --> FIFO --> ~~~~~~~~~ network ~~~~~~~~~ --> FIFO --> TOB --> log
```

| Class | What it does |
|---|---|
| `Log` | stores entries. Append only |
| `FIFOBroadcast` (layer 1) | numbers each node's messages, ignores duplicates, holds messages that arrive early until the gap is filled, forwards new messages to everyone, and asks for missing ones |
| `TotalOrderBroadcast` (layer 2) | gives each entry a Lamport timestamp and only adds it to the log once every other node has moved past that timestamp. Same timestamps and same rule on every node --> same order everywhere |
| `Node` | connects the two layers, the log and the timers |

Nodes send three kinds of messages:

| Message | Layer | Meaning |
|---|---|---|
| data | 2 (numbered by 1) | an entry |
| heartbeat | 2 (numbered by 1) | "my clock is at N, nothing older is coming from me". Stops a quiet node from blocking the others |
| status | 1 only | "I have everything up to N from each node". Whoever has more resends it |

### Sim vs live

The node and both layers are the same code in both. Only the network and the clock change.

| | `ctlog/sim` | `ctlog/live` |
|---|---|---|
| nodes | all in one process | one process per node |
| time | simulated, jumps to the next event | real time (asyncio event loop) |
| network | in memory, random faults from a seed | UDP on localhost, same faults added before sending |
| used for | tests | running the cluster & inspecting it |

UDP fits because, like the simulated network, it promises nothing: on a real network messages can get lost,
duplicated or reordered, and my layers handle that. On localhost that's not supposed to happen, so the live
version adds the faults from `cluster_config.json` before sending.

The simulated network and scheduler are inspired from the course's
[dslabs](https://github.com/cscheffler/dslabs) simulator.

## Setup

Tested with Python 3.14.

```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m pytest -q        # about 75 s
```

## Running the cluster

`cluster_config.json` lists the nodes (name --> UDP port) and the network faults:

```json
{
  "nodes": {"a": 9001, "b": 9002, "c": 9003, "d": 9004, "e": 9005},
  "network": {"drop_probability": 0.3, "duplicate_probability": 0.2, "latency_ms": [30, 200]}
}
```

Start every node (each one is its own process) and open the prompt:

```
python -m ctlog start
```

| Command | What it does |
|---|---|
| `submit NODE DOMAIN ISSUER [...]` | sends an entry to a node. Several at once: `submit a google.com DigiNotar b minerva.edu LetsEncrypt` |
| `inspect` | every node's log side by side |
| `status [NODE]` | what each node's layers hold right now |
| `pause NODE` / `resume NODE` | freezes / unfreezes a node's process |
| `crash NODE` | kills a node's process |
| `quit` | stops everything |

To watch entries being delivered live: `tail -f logs/*.log` in another terminal.
You can also pause or kill nodes yourself: `pkill -STOP -f "run-node d"`, `pkill -CONT -f "run-node d"`, `kill -9 <pid>`.

### Example

Three entries submitted to three nodes at the same time. All nodes agree on one order (not the order I typed):

```
ctlog> submit a google.com DigiNotar b minerva.edu LetsEncrypt c bank.com Sectigo
a: ok
b: ok
c: ok
ctlog> inspect
  #  a                         b                         c                         d                         e
  0  minerva.edu (LetsEncrypt  minerva.edu (LetsEncrypt  minerva.edu (LetsEncrypt  minerva.edu (LetsEncrypt  minerva.edu (LetsEncrypt
  1  bank.com (Sectigo)        bank.com (Sectigo)        bank.com (Sectigo)        bank.com (Sectigo)        bank.com (Sectigo)
  2  google.com (DigiNotar)    google.com (DigiNotar)    google.com (DigiNotar)    google.com (DigiNotar)    google.com (DigiNotar)
identical, 3 entries
```

`status` shows what's going on inside a node: messages waiting behind a gap (`parked`), duplicates thrown
away, resends, and entries that are waiting, plus who they're waiting for:

```
ctlog> status a
a: running, 0 entries, clock 62
  fifo
    have: {'a': 29, 'b': 2, 'c': 0, 'd': 0, 'e': 2}
    parked from b: [4, 5, 8, 10, 12] +3 more, missing 3
    duplicates ignored: 35, resent: 336
    statuses received: 15
  tob
    latest: {'b': 3, 'c': 0, 'd': 0, 'e': 3}
    heartbeats received: 4
    pending: 1
      (59, a) google.com --> waiting for b, c, d, e
```

### Failures

| What I do | What happens |
|---|---|
| drop / duplicate / delay messages (config) | missing messages get resent, duplicates are ignored, logs end up identical |
| `pause d`, then submit | entries wait for `d` (`status` shows "waiting for d") |
| `resume d` | `d` catches up on what it missed and the waiting entries get delivered. Logs identical again |
| `crash d` | entries wait for `d` forever (see limitations) |

## Tests

`python -m pytest -q`. One test file per class, using fakes, plus `tests/test_replication.py`, which runs
whole clusters on the simulated network (4 network settings x 3 seeds each) and checks that:

- an entry reaches every node
- all logs end up identical, with each node's entries in the order it sent them
- same thing when entries are sent to random nodes over time, fast (1 ms apart) or spaced out
- lost messages are never repaired without the timers
- one crashed node stops delivery everywhere
- 10 nodes with lost and duplicated messages still end up identical

## Limitations

- **One crashed node blocks everyone.** Every node waits to hear from all the others before adding an entry,
  and nobody can tell a crashed node from a slow one. I plan to fix this in iteration 3 through introducing a leader.
- **The order isn't submission order** for entries submitted at the same time on different nodes. However, all nodes
  agree on one order.
- **Lots of messages.** Every node relays every heartbeat, and a status triggers resending everything after the gap. 

## Next

- Iteration 2 (Nov 1 - Nov 14): Merkle tree with RFC 6962 hashing, inclusion and consistency proofs.
  All nodes compute the same root; the CLI shows each node's tree size and root side by side.
- Iteration 3 (Nov 15 - Nov 28): save the log to disk and catch up after a restart. Replace Lamport
  ordering with a fixed leader that commits once a majority has an entry, so follower crashes no longer
  stall the log.
- Iteration 4 (Dec 1 - Dec 10): a monitor that flags certificates a domain owner didn't request
  (replaying DigiNotar), signed tree heads, and a malicious leader that shows different clients
  different histories. It gets caught when the monitor compares signed tree heads from two clients:
  two different roots for the same tree size, or a consistency proof that fails. 
- Stretch goals: leader election, so the leader can crash too; independent witnesses that gossip
  signed tree heads.
