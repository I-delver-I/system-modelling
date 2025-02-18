"""
Distribution-related utility functions: Erlang, empirical distributions, etc.
"""

import random
import math
import bisect
from dataclasses import dataclass
from typing import TypeVar, Generic, Sequence, Sized, Callable, overload

INF_TIME = float("inf")
TIME_EPS = 1e-6

T = TypeVar("T")
V = TypeVar("V")


def erlang(lambd: float, k: int) -> float:
    """
    Draw a random sample from an Erlang(k, rate=lambd) distribution.
    """
    product = 1.0
    for _ in range(k):
        product *= random.random()
    return -1 / lambd * math.log(product)


class _KeyWrapper(Generic[T, V], Sequence[V], Sized):
    """
    Wraps a sequence with a key function to facilitate searching/sorting by a computed key.
    """

    def __init__(self, sequence: Sequence[T], key: Callable[[T], V]) -> None:
        self.key = key
        self.sequence = sequence

    def __len__(self) -> int:
        return len(self.sequence)

    @overload
    def __getitem__(self, idx: int) -> V:
        ...

    @overload
    def __getitem__(self, idx: slice) -> Sequence[V]:
        ...

    def __getitem__(self, idx):
        if isinstance(idx, slice):
            return list(map(self.key, self.sequence[idx]))
        return self.key(self.sequence[idx])


@dataclass(eq=False)
class EmpiricalPoint:
    """
    Represents a single point in an empirical distribution: a (value, cumulative probability) pair.
    """
    value: float
    cum_proba: float


def empirical(points: list[EmpiricalPoint]) -> float:
    """
    Sample from an empirical distribution given a sorted list of EmpiricalPoints.

    The list must start at cum_proba=0 and end at cum_proba=1.
    """
    num_points = len(points)
    assert num_points >= 2, "Must have at least two empirical points."
    assert points[0].cum_proba == 0, "First point must have cum_proba=0."
    assert points[-1].cum_proba == 1, "Last point must have cum_proba=1."

    proba = random.uniform(0, 1)
    start_idx = bisect.bisect_right(_KeyWrapper(points, key=lambda p: p.cum_proba), proba) - 1
    end_idx = min(start_idx + 1, num_points - 1)

    start, end = points[start_idx], points[end_idx]

    # Linear interpolation between start.value and end.value
    numerator = (end.value - start.value) * (proba - start.cum_proba)
    denominator = end.cum_proba - start.cum_proba
    return start.value + numerator / denominator
