"""
Implements the BankQueueingNode and its custom metrics for the bank simulation.
"""

from dataclasses import dataclass, field
from typing import Any

from qnet.core_models import I
from qnet.service_node import QueueingNode, QueueingMetrics


@dataclass(eq=False)
class BankQueueingMetrics(QueueingMetrics):
    """
    Extension of the QueueingMetrics with an additional counter of items
    taken from the neighboring queue.
    """
    num_from_neighbor: int = field(init=False, default=0)

    def to_dict(self) -> dict[str, Any]:
        metrics_dict = super().to_dict()
        metrics_dict["num_from_neighbor"] = self.num_from_neighbor
        return metrics_dict


class BankQueueingNode(QueueingNode[I, BankQueueingMetrics]):
    """
    A specialized queueing node that can pull items from a neighbor queue
    if the difference in queue lengths is large enough.
    """

    def __init__(self, min_queuelen_diff: int, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.min_queuelen_diff = min_queuelen_diff
        self.neighbor: BankQueueingNode[I] = None

    def set_neighbor(self, node: "BankQueueingNode[I]") -> None:
        """
        Sets a neighboring queue. Both queue references point to each other.
        """
        self.neighbor = node
        node.neighbor = self

    def end_action(self) -> None:
        """
        After finishing an item, check whether to pull items from the neighbor queue
        if the difference in queue lengths is at least `min_queuelen_diff`.
        """
        item = super().end_action()
        while (self.neighbor.queuelen - self.queuelen) >= self.min_queuelen_diff:
            last_item = self.neighbor.queue.pop()
            self.neighbor._item_in_hook(last_item)
            self.queue.push(last_item)
            self.metrics.num_from_neighbor += 1
        return item
