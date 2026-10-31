import pytest
from unittest.mock import Mock
from ctlog.network import Endpoint
from ctlog.node import Node, NodeState
from ctlog.scheduler import Scheduler

CERT = {"domain": "minerva.edu", "issuer": "Let's Encrypt"}


def make_node():
    """Node "a" with peers "b" and "c", connected to a fake endpoint instead of a network."""
    scheduler = Scheduler()
    node = Node("a", ["a", "b", "c"], scheduler)
    endpoint = Mock(spec=Endpoint)
    node.set_endpoint(endpoint)
    return scheduler, node, endpoint


def sent(endpoint):
    return [call.args for call in endpoint.send.call_args_list]


def test_invalid_entry_is_not_sent():
    scheduler, node, endpoint = make_node()
    with pytest.raises(TypeError):
        node.submit("minerva.edu")

    endpoint.send.assert_not_called()
    assert node.entries() == []


def test_submit_sends_to_every_peer():
    scheduler, node, endpoint = make_node()
    node.submit(CERT)

    assert [to for to, _ in sent(endpoint)] == ["b", "c"]
    for _, msg in sent(endpoint):
        assert msg["body"]["entry"] == CERT


def test_each_tick_sends_status_and_heartbeat():
    scheduler, node, endpoint = make_node()
    node.start_timers()
    scheduler.run_until(1000)

    statuses = [to for to, msg in sent(endpoint) if msg["type"] == "status"]
    heartbeats = [
        to
        for to, msg in sent(endpoint)
        if msg["type"] == "broadcast" and msg["body"]["category"] == "heartbeat"
    ]
    for peer in ["b", "c"]:
        # each tick waits at most 100 ms so 1000/100 = 10
        assert statuses.count(peer) >= 10
        assert heartbeats.count(peer) == statuses.count(peer)


def test_submit_before_connecting_raises():
    node = Node("a", ["a", "b", "c"], Scheduler())
    with pytest.raises(RuntimeError):
        node.submit(CERT)


def test_crashed_node_rejects_submit():
    scheduler, node, endpoint = make_node()
    assert node.state is NodeState.RUNNING

    node.crash()
    assert node.state is NodeState.CRASHED
    with pytest.raises(RuntimeError):
        node.submit(CERT)
    endpoint.send.assert_not_called()


def test_crashed_node_ignores_messages():
    scheduler, node, endpoint = make_node()
    status_from_b = {"type": "status", "sender": "b", "have": {"a": 0, "b": 0, "c": 0}}
    node.submit(CERT)
    assert [to for to, _ in sent(endpoint)] == ["b", "c"]
    endpoint.reset_mock()

    # b says it has nothing from a --> a resends CERT to b
    node.on_message(status_from_b)
    assert [to for to, _ in sent(endpoint)] == ["b"]
    endpoint.reset_mock()

    node.crash()
    node.on_message(status_from_b)
    endpoint.send.assert_not_called()


def test_crashed_node_stops_ticking():
    scheduler, node, endpoint = make_node()
    node.start_timers()
    scheduler.run_until(500)
    node.crash()
    endpoint.reset_mock()

    scheduler.run_until(10_000)
    endpoint.send.assert_not_called()
