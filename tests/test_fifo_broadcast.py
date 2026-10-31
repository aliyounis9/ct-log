from ctlog.fifo_broadcast import FIFOBroadcast


def make_fifo():
    """FIFOBroadcast for node "a" that records what it sends and delivers."""
    sent = []
    delivered = []
    fifo = FIFOBroadcast(
        "a",
        ["a", "b", "c"],
        lambda to, msg: sent.append((to, msg)),
        lambda origin, body: delivered.append((origin, body)),
    )
    return fifo, sent, delivered


def msg_from(origin, seq):
    return {"type": "broadcast", "origin": origin, "seq": seq, "body": {"n": seq}}


def test_own_broadcasts_are_numbered_and_sent():
    fifo, sent, delivered = make_fifo()
    fifo.broadcast({"n": 1})
    fifo.broadcast({"n": 2})

    assert delivered == [("a", {"n": 1}), ("a", {"n": 2})]
    assert [(to, msg["seq"]) for to, msg in sent] == [("b", 1), ("c", 1), ("b", 2), ("c", 2)]


def test_messages_are_delivered_in_origin_order():
    fifo, sent, delivered = make_fifo()
    fifo.receive(msg_from("b", 3))
    fifo.receive(msg_from("b", 1))
    assert delivered == [("b", {"n": 1})]

    fifo.receive(msg_from("b", 2))
    assert delivered == [("b", {"n": 1}), ("b", {"n": 2}), ("b", {"n": 3})]


def test_duplicates_are_ignored():
    fifo, sent, delivered = make_fifo()
    fifo.receive(msg_from("b", 1))
    fifo.receive(msg_from("b", 1))

    assert delivered == [("b", {"n": 1})]
    assert [to for to, _ in sent] == ["c"]


def test_status_reports_last_seq_without_gaps():
    fifo, sent, delivered = make_fifo()
    fifo.receive(msg_from("b", 1))
    fifo.receive(msg_from("b", 3))
    sent.clear()

    fifo.send_status()
    assert [to for to, _ in sent] == ["b", "c"]
    for _, status in sent:
        assert status["have"] == {"a": 0, "b": 1, "c": 0}


def test_status_triggers_resend():
    fifo, sent, delivered = make_fifo()
    for n in range(1, 4):
        fifo.broadcast({"n": n})
    fifo.receive(msg_from("c", 1))
    sent.clear()

    fifo.receive({"type": "status", "sender": "b", "have": {"a": 1, "b": 0, "c": 0}})

    resent = [(to, msg["origin"], msg["seq"]) for to, msg in sent]
    assert resent == [("b", "a", 2), ("b", "a", 3), ("b", "c", 1)]
