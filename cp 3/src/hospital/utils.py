"""
Utility functions for the hospital package.
"""

from numbers import Number


class MeanMeter:
    """
    A running mean tracker with count and accumulated sum.
    """

    def __init__(self) -> None:
        self.sum = 0.0
        self.count = 0

    @property
    def mean(self) -> float:
        """
        Returns the average of all values fed into this meter.
        """
        return self.sum / max(self.count, 1)

    def update(self, value: Number) -> None:
        """
        Add a new measurement to the meter.
        """
        self.sum += value
        self.count += 1
