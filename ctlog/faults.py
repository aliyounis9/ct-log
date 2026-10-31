import random
from dataclasses import dataclass


@dataclass(frozen=True)
class NetworkConditions:
    """Fault settings for the network.

    drop_probability: chance a message is lost.
    latency_ms: (min, max) delay in ms, picked at random for each message.
    duplicate_probability: chance a message that wasn't dropped is
        delivered a second time, 1 to 100 ms after the first copy.
    """

    drop_probability: float = 0.0
    latency_ms: tuple[int, int] = (30, 80)
    duplicate_probability: float = 0.0

    def __post_init__(self) -> None:
        for name in ("drop_probability", "duplicate_probability"):
            value = getattr(self, name)
            if not 0 <= value <= 1:
                raise ValueError(f"{name} must be between 0 and 1; got {value}")

        low, high = self.latency_ms
        if low < 0 or low > high:
            raise ValueError(
                f"latency_ms must be (min, max) with 0 <= min <= max; got {self.latency_ms}"
            )


def fault_delays(conditions: NetworkConditions, rng: random.Random) -> list[int]:
    """Decide what happens to one message: [] if it's dropped, one delay in ms
    normally, or two delays if it's duplicated (the copy arrives 1-100 ms later)."""
    delays = [rng.randint(*conditions.latency_ms)]
    if rng.random() < conditions.drop_probability:
        return []
    if rng.random() < conditions.duplicate_probability:
        delays.append(delays[0] + rng.randint(1, 100))
    return delays
