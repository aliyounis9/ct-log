import random
import string
from functools import partial
import pytest
from ctlog.scheduler import Scheduler
from ctlog.network import Network, NetworkConditions
from ctlog.node import Node

IDS = ["a", "b", "c"]
SEED_RANGE = 1999

PERFECT = {}
DUPLICATES = {"duplicate_probability": 1.0}
MIXED = {"drop_probability": 0.3, "duplicate_probability": 0.3, "latency_ms": (1, 200)}
HEAVY_LOSS = {"drop_probability": 0.9}

NETWORKS = [
    pytest.param(seed, conditions, id=f"{name}-{seed}")
    for name, conditions in [
        ("perfect", PERFECT),
        ("duplicates", DUPLICATES),
        ("mixed", MIXED),
        ("heavy-loss", HEAVY_LOSS),
    ]
    for seed in range(3)
]


def make_cluster(seed=0, start_timers=True, num_nodes=3, **conditions):
    ids = list(string.ascii_lowercase[:num_nodes])
    seeds = random.Random(seed)
    scheduler = Scheduler()
    network = Network(
        scheduler,
        seed=seeds.randrange(SEED_RANGE),
        conditions=NetworkConditions(**conditions),
    )
    nodes = [
        Node(node_id, ids, scheduler, seed=seeds.randrange(SEED_RANGE))
        for node_id in ids
    ]
    for node in nodes:
        network.connect(node)
        if start_timers:
            node.start_timers()
    return scheduler, network, nodes


def submit_from_everyone(nodes, count):
    for k in range(count):
        for node in nodes:
            node.submit({"domain": f"site{k}.com", "issuer": node.node_id})


def submit_over_time(scheduler, nodes, count, interval_ms, seed):
    """Submit `count` entries, `interval_ms` apart, each to a random node.
    Returns the entries in the order they were submitted."""
    rng = random.Random(seed)
    submitted = []
    for k in range(count):
        node = rng.choice(nodes)
        entry = {"domain": f"site{k}.com", "issuer": node.node_id}
        scheduler.call_later(k * interval_ms, partial(node.submit, entry))
        submitted.append(entry)
    return submitted


def domains_from(node, issuer):
    return [entry["domain"] for entry in node.entries() if entry["issuer"] == issuer]


@pytest.mark.parametrize("seed, conditions", NETWORKS)
def test_entry_reaches_every_node(seed, conditions):
    scheduler, network, nodes = make_cluster(seed, **conditions)
    entry = {"domain": "minerva.edu", "issuer": "a"}
    nodes[0].submit(entry)
    assert nodes[0].entries() == []  # waits until the other nodes have moved past it

    scheduler.run_until(50_000)
    for node in nodes:
        assert node.entries() == [entry]


@pytest.mark.parametrize("seed, conditions", NETWORKS)
def test_logs_end_up_identical(seed, conditions):
    scheduler, network, nodes = make_cluster(seed, **conditions)
    submit_from_everyone(nodes, 50)
    scheduler.run_until(50_000)

    for node in nodes:
        for issuer in IDS:
            assert domains_from(node, issuer) == [f"site{k}.com" for k in range(50)]
    a, b, c = (node.entries() for node in nodes)
    assert a == b == c


def test_no_repair_without_timers():
    scheduler, network, nodes = make_cluster(start_timers=False, **MIXED)
    submit_from_everyone(nodes, 10)
    scheduler.run_until(5000)

    assert any(len(node.entries()) < 30 for node in nodes)


@pytest.mark.parametrize("seed, conditions", NETWORKS)
def test_one_crash_stalls_everyone(seed, conditions):
    scheduler, network, nodes = make_cluster(seed, **conditions)
    a, b, c = nodes
    submit_from_everyone(nodes, 5)
    scheduler.run_until(50_000)
    assert all(len(node.entries()) == 15 for node in nodes)

    c.crash()
    scheduler.run_until(55_000)  # let c's last messages arrive
    before_crash = a.entries()

    for k in range(5):
        a.submit({"domain": f"late{k}.com", "issuer": "a"})
        b.submit({"domain": f"late{k}.com", "issuer": "b"})
    scheduler.run_until(120_000)

    # a and b still hear each other, but can't rule out a message from c
    assert a.entries() == before_crash
    assert b.entries() == before_crash


@pytest.mark.parametrize("interval_ms", [1, 300], ids=["fast", "spaced"])
@pytest.mark.parametrize("seed, conditions", NETWORKS)
def test_logs_end_up_identical_over_time(seed, conditions, interval_ms):
    scheduler, network, nodes = make_cluster(seed, **conditions)
    submitted = submit_over_time(scheduler, nodes, 30, interval_ms, seed)
    scheduler.run_until(30 * interval_ms + 50_000)

    a, b, c = (node.entries() for node in nodes)
    assert len(a) == 30
    assert a == b == c
    for node in nodes:  # entries sent to the same node keep their order
        assert [e for e in a if e["issuer"] == node.node_id] == [
            e for e in submitted if e["issuer"] == node.node_id
        ]


def test_ten_nodes_end_up_identical():
    scheduler, network, nodes = make_cluster(num_nodes=10, **MIXED)
    submit_over_time(scheduler, nodes, 30, 1, seed=0)
    scheduler.run_until(50_000)

    logs = [node.entries() for node in nodes]
    assert len(logs[0]) == 30
    assert all(log == logs[0] for log in logs)
