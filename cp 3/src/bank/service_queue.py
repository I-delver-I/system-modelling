"""
Implements the BankQueueingNode and its custom metrics for the bank simulation.
"""

from dataclasses import dataclass, field
import random
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
    A specialized queueing node that:
    1. Pulls items from neighbor if queue diff is large.
    2. Dynamically changes service time based on neighbor's status.
    """

    def __init__(self, min_queuelen_diff: int, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.min_queuelen_diff = min_queuelen_diff
        self.neighbor: BankQueueingNode[I] = None
        
    def _predict_item_time(self, **kwargs: Any) -> float:
        """
        Calculates the finish time for the current item.
        Overrides the base method to implement dynamic logic.
        """
        # 1. Determine the duration (service time)
        duration = 0.0
        
        # Check if neighbor exists and is busy (no free channels)
        neighbor_is_busy = (
            self.neighbor is not None and 
            self.neighbor.channel_pool.is_occupied
        )

        if neighbor_is_busy:
            # Condition: Both tellers are busy -> Normal Distribution
            # Mean=1.0, Sigma=0.3
            duration = max(0.0, random.normalvariate(mu=1.0, sigma=0.3))
        else:
            # Default: Neighbor is free -> Exponential Distribution
            # Mean=0.3 -> lambda = 1.0 / 0.3
            duration = random.expovariate(lambd=1.0 / 0.3)

        # 2. Return Absolute Finish Time (Current Time + Duration)
        return self.current_time + duration

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
        
        if self.neighbor is not None:
            while (self.neighbor.queuelen - self.queuelen) >= self.min_queuelen_diff:
                # Steal the last item from neighbor's queue
                last_item = self.neighbor.queue.pop()
                
                # Register the 'steal' in metrics/hooks
                self.neighbor._item_in_hook(last_item)
                self.queue.push(last_item)
                self.metrics.num_from_neighbor += 1
                
        return item
