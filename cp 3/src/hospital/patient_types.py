"""
Defines the data types used to represent hospital patients.
"""

from enum import Enum
from dataclasses import dataclass, field

from qnet.common import Item


class SickType(int, Enum):
    """
    Denotes the type of sickness (or patient category):
    1) FIRST
    2) SECOND
    3) THIRD
    """
    FIRST = 1
    SECOND = 2
    THIRD = 3

    def __repr__(self) -> str:
        return str(self.value)


@dataclass(eq=False)
class HospitalItem(Item):
    """
    A specialized Item that adds 'sick_type' and 'as_first_sick' flags for the hospital model.
    """
    sick_type: SickType = SickType.FIRST
    as_first_sick: bool = field(repr=False, default=False)
