"""
Transition nodes: route items to another node with zero processing time, optionally by probability.
"""

import random
import itertools
from abc import abstractmethod
from typing import Iterable, Optional, Any, cast

from .core_models import INF_TIME, I
from .simulation_node import NM, Node, NodeMetrics, DelayFn
from .helpers import filter_none
from collections import defaultdict


class BaseTransitionNode(Node[I, NM]):
    """
    A node that immediately transfers an item to another node (no queue, no channels).
    """

    def __init__(self, delay_fn: DelayFn = lambda: 0, **kwargs: Any) -> None:
        super().__init__(delay_fn=delay_fn, **kwargs)
        self.item: Optional[I] = None
        self.next_time = INF_TIME

    @property
    def current_items(self) -> Iterable[I]:
        return filter_none((self.item,))

    def start_action(self, item: I) -> None:
        super().start_action(item)
        self.item = item
        self.next_time = self._predict_next_time()

    def end_action(self) -> I:
        item = cast(I, self.item)
        self.set_next_node(self._get_next_node(item))
        self._process_item(item)
        self.next_time = INF_TIME
        self.item = None
        return self._end_action(item)

    def reset(self) -> None:
        super().reset()
        self.item = None
        self.next_time = INF_TIME

    def to_dict(self) -> dict[str, Any]:
        return {
            "item": self.item,
            "next_node": self.next_node.name if self.next_node else None
        }

    def _before_time_update_hook(self, time: float) -> None:
        """
        Ensure that at each time step, we do not have a next_node set in advance.
        """
        self.next_node = None
        super()._before_time_update_hook(time)

    @abstractmethod
    def _get_next_node(self, item: I) -> Optional[Node[I, NodeMetrics]]:
        raise NotImplementedError

    def _process_item(self, _: I) -> None:
        """
        A hook for any custom logic applied before calling self._end_action().
        """
        pass

class PriorityGroupTransitionNode(BaseTransitionNode[I, NM]):
    """
    Implements Grouped Priority Routing.
    
    Logic:
    1. Check Priority 1 group. Identify ALL available nodes.
    2. If candidates exist -> Pick RANDOM one (load balancing).
    3. If none available -> Check Priority 2 group.
    4. ...
    5. If ALL groups are full -> Return a random node from Priority 1 
       (to trigger blocking/waiting for the best resource).
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        # Dictionary mapping priority level to a list of nodes.
        # Structure: { 1: [NodeA, NodeB], 2: [NodeC] }
        self.priority_groups: dict[int, list[Node[I, NodeMetrics]]] = defaultdict(list)

    @property
    def connected_nodes(self) -> Iterable["Node[I, NodeMetrics]"]:
        """
        Required for graph visualization/traversal.
        Returns all potential destination nodes across all priority groups.
        """
        all_destinations = itertools.chain.from_iterable(self.priority_groups.values())
        return itertools.chain(all_destinations, super().connected_nodes)

    def add_next_node(self, node: Node[I, NodeMetrics], priority: int = 1) -> None:
        """
        Add a node to a specific priority group.
        
        Args:
            node: The destination node.
            priority: Integer representing priority level. 
                      Lower number = Higher priority (e.g., 1 is higher than 2).
        """
        self.priority_groups[priority].append(node)

    def _get_next_node(self, _: I) -> Optional[Node[I, NodeMetrics]]:
        """
        Determines the destination based on availability and priority.
        """
        if not self.priority_groups:
            return None

        # Sort keys to ensure we process Priority 1, then 2, then 3...
        sorted_priorities = sorted(self.priority_groups.keys())

        # --- Step 1: Search for an available node ---
        for prio in sorted_priorities:
            nodes_in_group = self.priority_groups[prio]
            
            # Find all nodes in this priority group that have capacity
            available_nodes = [node for node in nodes_in_group if node.can_accept_item()]
            
            if available_nodes:
                # If we found space, pick one randomly to balance the load
                return random.choice(available_nodes)
        
        # --- Step 2: Handle Blocking (All nodes are full) ---
        # If we reached this point, every node in every priority group is busy.
        # According to the blocking logic ("wait for the intended resource"),
        # we should block on the HIGHEST priority group.
        highest_prio = sorted_priorities[0]
        
        # We pick a random node from the highest priority group.
        # The BaseTransitionNode will attempt to push to it, fail (because it's full),
        # and enter the BLOCKED state, waiting for this specific node to free up.
        return random.choice(self.priority_groups[highest_prio])

class ProbaTransitionNode(BaseTransitionNode[I, NM]):
    """
    A node that routes an item to one of multiple next_nodes with given probabilities.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.proba_sum: float = 0
        self.next_nodes: list[Optional[Node[I, NodeMetrics]]] = []
        self.next_probas: list[float] = []

    @property
    def rest_proba(self) -> float:
        """
        The remaining probability after adding any explicitly assigned next nodes.
        """
        return 1 - self.proba_sum

    @property
    def num_next_nodes(self) -> int:
        return len(self.next_nodes)

    @property
    def connected_nodes(self) -> Iterable["Node[I, NodeMetrics]"]:
        return itertools.chain(filter_none(self.next_nodes), super().connected_nodes)

    def add_next_node(self, node: Optional[Node[I, NodeMetrics]], proba: float = 1.0) -> None:
        """
        Add another node to the transition's list, with a probability for selection.
        """
        new_proba_sum = self.proba_sum + proba
        assert new_proba_sum <= 1, f"Total probability cannot exceed 1. Attempted: {new_proba_sum}"
        self.proba_sum = new_proba_sum
        self.next_nodes.append(node)
        self.next_probas.append(proba)

    def _get_next_node(self, _: I) -> Optional[Node[I, NodeMetrics]]:
        """
        Randomly choose among next_nodes with assigned probabilities. 
        Must sum up to 1 in total.
        """
        assert self.proba_sum == 1, "Total probability must be exactly 1."
        return random.choices(self.next_nodes, self.next_probas, k=1)[0]
