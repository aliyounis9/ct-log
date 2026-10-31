import pytest
from ctlog.sim.scheduler import SimScheduler


def record(scheduler, history, name):
    """Return a callback function that appends (name, time it ran) to the history."""
    return lambda: history.append((name, scheduler.time_ms()))


def test_events_run_in_time_order():
    scheduler = SimScheduler()
    history = []
    scheduler.call_later(30, record(scheduler, history, "a"))
    scheduler.call_later(10, record(scheduler, history, "c"))
    scheduler.call_later(20, record(scheduler, history, "b"))
    scheduler.run_until(100)
    assert history == [("c", 10), ("b", 20), ("a", 30)]


def test_equal_time_events_run_in_scheduling_order():
    scheduler = SimScheduler()
    history = []
    for name in ["a", "b", "c"]:
        scheduler.call_later(10, record(scheduler, history, name))
    scheduler.run_until(100)
    assert [name for name, _ in history] == ["a", "b", "c"]


def test_callback_can_schedule_another_event():
    scheduler = SimScheduler()
    history = []

    def parent():
        history.append(("parent", scheduler.time_ms()))
        scheduler.call_later(5, record(scheduler, history, "child"))

    scheduler.call_later(10, parent)
    went_idle = scheduler.run_until(100)
    assert history == [("parent", 10), ("child", 15)]
    assert went_idle


def test_run_until_stops_at_max():
    scheduler = SimScheduler()
    history = []
    scheduler.call_later(10, record(scheduler, history, "early"))
    scheduler.call_later(500, record(scheduler, history, "late"))

    assert scheduler.run_until(100) is False
    assert history == [("early", 10)]

    assert scheduler.run_until(1000) is True
    assert history == [("early", 10), ("late", 500)]


def test_negative_delay_raises_value_error():
    scheduler = SimScheduler()
    with pytest.raises(ValueError):
        scheduler.call_later(-1, lambda: None)
