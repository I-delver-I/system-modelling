"""
Base Node classes: all nodes must inherit from Node, implementing start_action() and end_action().
"""

from abc import ABC, abstractmethod
import inspect
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Generic, Iterable, Optional, TypeVar, Any, cast

from .core_models import I, SupportsDict, Metrics, ActionRecord, ActionType
from .helpers import filter_none


class NodeState(Enum):
    """
    Three-state model for nodes in a queueing system:
    
    IDLE: Node is not processing anything; either no items, or all items have departed.
    BUSY: Node is actively processing an item; has a scheduled event (next_time is finite).
    BLOCKED: Node finished processing but cannot send item to next_node (next_node is full).
             Waiting for next_node to free up space. No new service events can start until unblocked.
    """
    IDLE = "idle"
    BUSY = "busy"
    BLOCKED = "blocked"

NM = TypeVar("NM", bound="NodeMetrics")
DelayFn = Callable[..., float]


@dataclass(eq=False)
class NodeMetrics(Metrics):
    """
    Basic metrics for a node: number of arrivals/departures, time tracking for last events, etc.
    """
    node_name: str = field(init=False, default="")
    num_in: int = field(init=False, default=0)
    num_out: int = field(init=False, default=0)
    start_action_time: float = field(init=False, default=-1)
    end_action_time: float = field(init=False, default=-1)

    def to_dict(self) -> dict[str, Any]:
        metrics_dict = super().to_dict()
        metrics_dict.update({
            "num_in": self.num_in,
            "num_out": self.num_out
        })
        return metrics_dict


class Node(ABC, SupportsDict, Generic[I, NM]):
    """
    Abstract Node in a queueing network.
    
    Three-state model:
    - IDLE: Not processing, no blocked items
    - BUSY: Processing an item, has scheduled event (next_time is finite)
    - BLOCKED: Finished processing, but cannot send to next_node (next_node is full)
    """

    num_nodes: int = 0

    def __init__(
        self,
        delay_fn: DelayFn,
        metrics: NM,
        name: Optional[str] = None,
        next_node: Optional["Node[I, NodeMetrics]"] = None,
    ) -> None:
        self.num_nodes += 1
        self.delay_fn = delay_fn
        self.delay_params = inspect.signature(self.delay_fn).parameters
        self.metrics = metrics
        self.name = self._get_auto_name() if name is None else name
        self.metrics.node_name = self.name
        self.next_node = next_node
        self.prev_node: Optional[Node[I, NodeMetrics]] = None
        self.current_time: float = 0.0
        self.next_time: float = 0.0
        self.state: NodeState = NodeState.IDLE
        
        # Nodes that are blocked trying to send items to this node
        # Used for pull-based unblocking: when this node frees space, it notifies blocked predecessors
        self.blocked_predecessors: set["Node[I, NodeMetrics]"] = set()

    @property
    def connected_nodes(self) -> Iterable["Node[I, NodeMetrics]"]:
        """
        All nodes that are connected to this node by next_node or prev_node.
        """
        return filter_none((self.prev_node, self.next_node))

    @property
    def current_items(self) -> Iterable[I]:
        """
        Items that are currently being processed or waiting in this node.
        """
        return []

    def can_accept_item(self) -> bool:
        """
        Check if this node can accept a new item (has capacity).
        Default: always accept. Override in subclasses to enforce capacity limits.
        Used for route blocking: a node should not release an item to next_node if next_node is full.
        """
        return True

    def start_action(self, item: I) -> None:
        """
        Called when an item arrives at this node.
        """
        self._item_in_hook(item)
        self.metrics.start_action_time = self.current_time
        item.history.append(ActionRecord(self, ActionType.IN, self.current_time))

    @abstractmethod
    def end_action(self) -> I:
        """
        Called when this node finishes processing its current item (if any).
        Must return the item that was finished.
        """
        raise NotImplementedError

    def update_time(self, time: float) -> None:
        """
        Update the node's internal clock to `time`.
        """
        self._before_time_update_hook(time)
        self.current_time = time
        for itm in self.current_items:
            itm.current_time = time

    def set_next_node(self, node: Optional["Node[I, NodeMetrics]"]) -> None:
        """
        Link this node to another as 'next_node'.
        """
        self.next_node = node
        if node is not None:
            node.prev_node = cast(Node[I, NodeMetrics], self)

    def reset_metrics(self) -> None:
        self.metrics.reset()

    def reset(self) -> None:
        """
        Completely reset the node to its initial state, including metrics and scheduling times.
        """
        self.current_time = 0
        self.next_time = 0
        self.state = NodeState.IDLE
        self.blocked_predecessors.clear()
        self.reset_metrics()

    def to_dict(self) -> dict[str, Any]:
        return {"next_time": self.next_time}

    def _get_auto_name(self) -> str:
        return f"{self.__class__.__name__}{self.num_nodes}"

    def _get_delay(self, **kwargs: Any) -> float:
        return self.delay_fn(**{
            name: value for name, value in kwargs.items()
            if name in self.delay_params
        })

    def _predict_next_time(self, **kwargs: Any) -> float:
        return self.current_time + self._get_delay(**kwargs)

    def _end_action(self, item: I) -> I:
        """
        Helper to finalize the departure of an item from this node, and optionally pass it on.
        """
        self._item_out_hook(item)
        self.metrics.end_action_time = self.current_time
        item.history.append(ActionRecord(self, ActionType.OUT, self.current_time))
        self._start_next_action(item)
        return item

    def _start_next_action(self, item: I) -> None:
        """
        If there is a next_node, pass the item to it; otherwise mark it as fully processed.
        """
        if self.next_node is None:
            item.processed = True
        else:
            self.next_node.start_action(item)

    def _item_in_hook(self, _: I) -> None:
        self.metrics.num_in += 1

    def _item_out_hook(self, _: I) -> None:
        self.metrics.num_out += 1

    def _before_time_update_hook(self, time: float) -> None:
        self.metrics.passed_time += time - self.current_time
