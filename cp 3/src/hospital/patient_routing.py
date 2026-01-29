"""
Defines the routing nodes for the hospital simulation:
 - TestingTransitionNode: a probability-based transition node.
 - EmergencyTransitionNode: chooses if the patient goes straight to a chamber or to reception.
"""

import itertools
from typing import Iterable, Optional, Any

from qnet.simulation_node import NM, Node, NodeMetrics
from qnet.service_node import QueueingMetrics, QueueingNode
from qnet.routing_node import BaseTransitionNode, ProbaTransitionNode

from .patient_types import HospitalItem, SickType

HospitalQueueingNode = QueueingNode[HospitalItem, QueueingMetrics]


class TestingTransitionNode(BaseTransitionNode[HospitalItem, NM]):
    """
    A custom transition node that decides where the patient goes 
    based strictly on their SickType (Deterministic logic), 
    rather than random probability.
    """
    
    def __init__(self, emergency_node: Node, **kwargs):
        super().__init__(**kwargs)
        self.emergency_node = emergency_node
        
    def _get_next_node(self, item: HospitalItem) -> Optional[Node[HospitalItem, NodeMetrics]]:
        """
        Determines the destination:
        - Type 2 -> Return to Emergency
        - Type 3 -> Leave System (return None)
        """
        if item.sick_type == SickType.SECOND:
            return self.emergency_node
        
        # Type 3 (and theoretically Type 1) leave the system
        return None

    def _process_item(self, item: HospitalItem) -> None:
        """
        Hook called before the item is moved. We use this to update status/stats.
        """
        if item.sick_type == SickType.SECOND:
            # Mark as priority for the return trip
            item.as_first_sick = True
        
        # Note: If _get_next_node returns None (for Type 3), 
        # the base Node class automatically handles 'item.processed = True'
        # and calls _item_out_hook.


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
        # Safety check
        if self.chumber is None or self.reception is None:
            raise ValueError("Next nodes (chamber/reception) are not set!")
        
        # Logic: Type 1 OR Priority Flag -> Chamber
        if item.sick_type == SickType.FIRST or item.as_first_sick:
            return self.chumber
        
        # Logic: All others -> Reception
        return self.reception
