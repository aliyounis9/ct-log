import pytest
from ctlog.faults import NetworkConditions


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
