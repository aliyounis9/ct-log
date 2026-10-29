import random
import pytest
from ctlog.scheduler import Scheduler
from ctlog.network import Network, NetworkConditions
from ctlog.node import Node

IDS = ["a", "b", "c"]
SEED_RANGE = 1999

def make_cluster(seed=0, start_timers=True, **conditions):
    seeds = random.Random(seed)
    scheduler = Scheduler()
    network = Network(
        scheduler, seed=seeds.randrange(SEED_RANGE), conditions=NetworkConditions(**conditions)
    )
    nodes = [Node(node_id, IDS, scheduler, seed=seeds.randrange(SEED_RANGE)) for node_id in IDS]
    for node in nodes:
        network.connect(node)
        if start_timers:
            node.start_timers()
    return scheduler, network, nodes


def submit_from_everyone(nodes, count):
    for k in range(count):
        for node in nodes:
            node.submit({"domain": f"site{k}.com", "issuer": node.node_id})


def domains_from(node, issuer):
    return [entry["domain"] for entry in node.entries() if entry["issuer"] == issuer]


def test_entry_reaches_every_node():
    scheduler, network, nodes = make_cluster()
    a = nodes[0]
    a.submit({"domain": "minerva.edu", "issuer": "a"})
    assert a.entries() == [{"domain": "minerva.edu", "issuer": "a"}]

    scheduler.run_until(1000)
    for node in nodes:
        assert node.entries() == [{"domain": "minerva.edu", "issuer": "a"}]


def test_invalid_entry_is_rejected_before_sending():
    scheduler, network, nodes = make_cluster()
    with pytest.raises(TypeError):
        nodes[0].submit("minerva.edu")

    assert network.get_stats()["sent"] == 0
    assert nodes[0].entries() == []


@pytest.mark.parametrize(
    "seed, conditions",
    [
        (0, {"duplicate_probability": 1.0}),
        (
            0,
            {
                "drop_probability": 0.3,
                "duplicate_probability": 0.3,
                "latency_ms": (1, 200),
            },
        ),
        *[(seed, {"drop_probability": 0.9}) for seed in range(30)],
    ],
)
def test_every_node_gets_every_entry_once_in_origin_order(seed, conditions):
    scheduler, network, nodes = make_cluster(seed, **conditions)
    submit_from_everyone(nodes, 50)
    scheduler.run_until(50_000)

    for node in nodes:
        for issuer in IDS:
            assert domains_from(node, issuer) == [f"site{k}.com" for k in range(50)]


def test_without_status_timers_lost_messages_are_never_repaired():
    scheduler, network, nodes = make_cluster(start_timers=False, drop_probability=0.5)
    submit_from_everyone(nodes, 10)
    scheduler.run_until(5000)

    assert any(len(node.entries()) < 30 for node in nodes)
