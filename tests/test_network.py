import pytest
from unittest.mock import Mock
from ctlog.scheduler import Scheduler
from ctlog.network import Network, NetworkConditions


def make_network(seed=0, **conditions):
    scheduler = Scheduler()
    network = Network(scheduler, seed=seed, conditions=NetworkConditions(**conditions))
    a = Mock(node_id="a")
    b = Mock(node_id="b")
    network.connect(a)
    network.connect(b)
    a.endpoint = a.set_endpoint.call_args.args[0]
    b.endpoint = b.set_endpoint.call_args.args[0]
    return scheduler, network, a, b


def received(node):
    return [call.args[0] for call in node.on_message.call_args_list]


def test_same_seed_gets_same_history():
    def run(seed):
        scheduler, network, a, b = make_network(
            seed, drop_probability=0.3, duplicate_probability=0.3
        )
        for i in range(100):
            a.endpoint.send("b", {"i": i})
        scheduler.run_until(1000)
        return received(b)

    assert run(seed=7) == run(seed=7)


def test_send_adds_from_and_copies_the_message():
    scheduler, network, a, b = make_network()
    msg = {"entry": {"domain": "minerva.edu"}}

    a.endpoint.send("b", msg)
    msg["entry"]["domain"] = "evil.com"
    scheduler.run_until(1000)

    assert received(b) == [{"entry": {"domain": "minerva.edu"}, "from": "a"}]
    assert "from" not in msg
    a.on_message.assert_not_called()


def test_sender_cannot_fake_from():
    scheduler, network, a, b = make_network()
    a.endpoint.send("b", {"x": 1, "from": "b"})
    scheduler.run_until(500)

    assert received(b) == [{"x": 1, "from": "a"}]


def test_message_arrives_within_latency():
    scheduler, network, a, b = make_network(latency_ms=(10, 20))
    a.endpoint.send("b", {"x": 1})

    scheduler.run_until(9)
    b.on_message.assert_not_called()

    scheduler.run_until(20)
    b.on_message.assert_called_once()


@pytest.mark.parametrize("drop, expected_delivered", [(0.0, 50), (1.0, 0)])
def test_drop_probability(drop, expected_delivered):
    scheduler, network, a, b = make_network(drop_probability=drop)
    for i in range(50):
        a.endpoint.send("b", {"i": i})
    scheduler.run_until(1000)

    assert b.on_message.call_count == expected_delivered
    assert network.get_stats()["dropped"] == 50 - expected_delivered


def test_duplicate_arrives_after_the_original():
    scheduler, network, a, b = make_network(
        latency_ms=(10, 10), duplicate_probability=1.0
    )
    a.endpoint.send("b", {"x": 1})

    scheduler.run_until(10)
    assert b.on_message.call_count == 1

    scheduler.run_until(1000)
    assert received(b) == [{"x": 1, "from": "a"}] * 2
    assert network.get_stats()["duplicated"] == 1

    first, second = received(b)
    assert first is not second


def test_messages_can_arrive_out_of_order():
    def arrival_order(seed):
        scheduler, network, a, b = make_network(seed)
        for i in range(10):
            a.endpoint.send("b", {"i": i})
        scheduler.run_until(1000)
        return [msg["i"] for msg in received(b)]

    assert any(arrival_order(seed) != list(range(10)) for seed in range(20))


def test_bad_sends_and_connects_raise():
    scheduler, network, a, b = make_network()

    with pytest.raises(ValueError):
        a.endpoint.send("c", {"x": 1})
    with pytest.raises(ValueError):
        network.connect(Mock(node_id="a"))
    with pytest.raises(TypeError):  # a set is not JSON-serializable
        a.endpoint.send("b", {"x": {1, 2}})
    assert network.get_stats()["sent"] == 0


@pytest.mark.parametrize(
    "conditions",
    [
        {"drop_probability": -0.1},
        {"duplicate_probability": 1.5},
        {"latency_ms": (-1, 10)},
        {"latency_ms": (50, 10)},
    ],
)
def test_invalid_conditions_raise(conditions):
    with pytest.raises(ValueError):
        NetworkConditions(**conditions)


def test_stats_add_up():
    scheduler, network, a, b = make_network(
        drop_probability=0.3, duplicate_probability=0.3, latency_ms=(1, 200)
    )
    for i in range(300):
        a.endpoint.send("b", {"i": i})
        b.endpoint.send("a", {"i": i})

    assert scheduler.run_until(10**6) is True  # no messages are in flight
    stats = network.get_stats()
    assert stats["delivered"] == stats["sent"] - stats["dropped"] + stats["duplicated"]
    assert stats["delivered"] == a.on_message.call_count + b.on_message.call_count
