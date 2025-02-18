"""
Implements the bank's transition node logic.
"""

import itertools
from typing import Iterable, Optional, Any

from qnet.common import I
from qnet.node import NM, Node, NodeMetrics
from qnet.transition import BaseTransitionNode

from .service_queue import BankQueueingNode


class BankTransitionNode(BaseTransitionNode[I, NM]):
    """
    A transition node that routes items (cars) to one of two queueing nodes (BankQueueingNode).
    The selection criterion is to choose the queue with the smaller length.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.first: BankQueueingNode[I] = None
        self.second: BankQueueingNode[I] = None

    @property
    def connected_nodes(self) -> Iterable["Node[I, NodeMetrics]"]:
        return itertools.chain((self.first, self.second), super().connected_nodes)

    def set_next_nodes(self, first: BankQueueingNode[I], second: BankQueueingNode[I]) -> None:
        """
        Assign the two possible queueing nodes (e.g., two checkouts).
        """
        self.first = first
        self.second = second

    def to_dict(self) -> dict[str, Any]:
        """
        Returns a dictionary representation of this transition node's important fields.
        """
        return {
            "next_node": self.next_node.name if self.next_node else None,
            "first_queue_size": self.first.queuelen,
            "second_queue_size": self.second.queuelen
        }

    def _get_next_node(self, _: I) -> Optional[Node[I, NodeMetrics]]:
        """
        Decide which queue node to send the item to based on queue lengths.
        """
        return self.first if self.first.queuelen <= self.second.queuelen else self.second
