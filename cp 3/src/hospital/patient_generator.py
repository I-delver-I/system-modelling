"""
Node that creates new HospitalItem objects with randomized sick types.
"""

import random
from typing import Any

from qnet.simulation_node import NM
from qnet.item_generator import BaseFactoryNode

from .patient_types import HospitalItem, SickType


class HospitalFactoryNode(BaseFactoryNode[HospitalItem, NM]):
    """
    Factory node responsible for generating new patients (HospitalItem) with probabilities
    for each of the SickType categories.
    """

    def __init__(self, probas: dict[SickType, float], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.sick_types, self.sick_probas = zip(*probas.items())

    def _get_next_item(self) -> HospitalItem:
        """
        Create a HospitalItem with a random SickType, according to the user-specified probabilities.
        """
        sick_type = self._get_next_type()
        return HospitalItem(
            id=self.next_id,
            created_time=self.current_time,
            sick_type=sick_type
        )

    def _get_next_type(self) -> SickType:
        """
        Choose a random SickType from the provided dictionary of {SickType: probability}.
        """
        return random.choices(self.sick_types, self.sick_probas, k=1)[0]
