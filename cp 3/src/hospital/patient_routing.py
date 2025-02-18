"""
Defines the routing nodes for the hospital simulation:
 - TestingTransitionNode: a probability-based transition node.
 - EmergencyTransitionNode: chooses if the patient goes straight to a chamber or to reception.
"""

import itertools
from typing import Iterable, Optional, Any

from qnet.node import NM, Node, NodeMetrics
from qnet.queueing import QueueingMetrics, QueueingNode
from qnet.transition import BaseTransitionNode, ProbaTransitionNode

from .patient_types import HospitalItem, SickType

HospitalQueueingNode = QueueingNode[HospitalItem, QueueingMetrics]


class TestingTransitionNode(ProbaTransitionNode[HospitalItem, NM]):
    """
    A node that decides where the patient goes after testing,
    with a user-defined probability distribution.
    """

    def _process_item(self, item: HospitalItem) -> None:
        """
        Mark the patient as 'as_first_sick' if it transitions back to the emergency queue.
        """
        if self.next_node is not None:
            item.as_first_sick = True


class EmergencyTransitionNode(BaseTransitionNode[HospitalItem, NM]):
    """
    Routes incoming patients:
    - Type 1 (or patients flagged as 'as_first_sick') go directly to the chamber queue.
    - Other patients go to the reception queue.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.chumber: HospitalQueueingNode = None
        self.reception: HospitalQueueingNode = None

    @property
    def connected_nodes(self) -> Iterable["Node[HospitalItem, NodeMetrics]"]:
        return itertools.chain((self.chumber, self.reception), super().connected_nodes)

    def set_next_nodes(self, chumber: HospitalQueueingNode, reception: HospitalQueueingNode) -> None:
        """
        Assign the two possible queueing routes: chamber or reception.
        """
        self.chumber = chumber
        self.reception = reception

    def _get_next_node(self, item: HospitalItem) -> Optional[Node[HospitalItem, NodeMetrics]]:
        """
        Routes patients to a chamber if they are SickType.FIRST or flagged as_first_sick,
        else to the reception node.
        """
        assert self.chumber is not None and self.reception is not None
        if item.sick_type == SickType.FIRST or item.as_first_sick:
            return self.chumber
        return self.reception
