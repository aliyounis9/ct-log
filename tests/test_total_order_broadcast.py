import random
from ctlog.total_order_broadcast import TotalOrderBroadcast


def make_tob(members=("a", "b", "c")):
    """TotalOrderBroadcast for node "a" that records what it sends down and delivers up."""
    sent = []
    delivered = []
    tob = TotalOrderBroadcast("a", list(members), sent.append, delivered.append)
    return tob, sent, delivered


def data(timestamp, entry):
    return {"category": "data", "timestamp": timestamp, "entry": entry}


def heartbeat(timestamp):
    return {"category": "heartbeat", "timestamp": timestamp}


def test_timestamps_increase():
    tob, sent, delivered = make_tob()
    tob.broadcast("a1")
    tob.send_heartbeat()
    tob.broadcast("a2")

    assert [(body["category"], body["timestamp"]) for body in sent] == [
        ("data", 1),
        ("heartbeat", 2),
        ("data", 3),
    ]


def test_receive_moves_the_clock_past_the_timestamp():
    tob, sent, delivered = make_tob()
    tob.receive("b", heartbeat(50))
    tob.broadcast("a1")

    assert sent[-1]["timestamp"] > 50


def test_entry_waits_for_every_other_node():
    tob, sent, delivered = make_tob()
    tob.receive("b", data(5, "b5"))
    tob.receive("c", heartbeat(3))
    assert delivered == []  # c could still send something with timestamp 4

    tob.receive("c", heartbeat(5))
    assert delivered == ["b5"]


def test_one_heartbeat_releases_several_entries():
    tob, sent, delivered = make_tob()
    tob.receive("b", data(3, "b3"))
    tob.receive("b", data(4, "b4"))
    tob.receive("c", heartbeat(7))

    assert delivered == ["b3", "b4"]


def test_ties_are_broken_by_origin():
    tob, sent, delivered = make_tob()
    tob.receive("c", data(4, "c4"))
    tob.receive("b", data(4, "b4"))
    tob.receive("b", heartbeat(8))
    tob.receive("c", heartbeat(8))

    assert delivered == ["b4", "c4"]


def test_arrival_order_does_not_matter():
    streams = {
        "b": [data(2, "b1"), data(4, "b2"), heartbeat(20)],
        "c": [data(1, "c1"), data(3, "c2"), heartbeat(20)],
        "d": [data(1, "d1"), data(4, "d2"), heartbeat(20)],
    }

    for seed in range(50):
        # mix the streams randomly, but keep each origin's own order (like FIFO does)
        rng = random.Random(seed)
        queues = {origin: list(bodies) for origin, bodies in streams.items()}
        tob, sent, delivered = make_tob(members=("a", "b", "c", "d"))
        while any(queues.values()):
            origin = rng.choice([o for o, bodies in queues.items() if bodies])
            tob.receive(origin, queues[origin].pop(0))

        assert delivered == ["c1", "d1", "b1", "c2", "b2", "d2"]


def test_two_node_cluster_delivers_immediately():
    tob, sent, delivered = make_tob(members=("a", "b"))
    tob.receive("b", data(1, "b1"))

    assert delivered == ["b1"]
