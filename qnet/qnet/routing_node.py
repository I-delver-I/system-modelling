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
