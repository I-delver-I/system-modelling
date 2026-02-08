"""
Model orchestration: runs the simulation by moving time forward to each event.
"""

import statistics
from enum import Flag
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable, Generic, Iterable, Optional, TypeVar, Any, cast

import dill

from .core_models import INF_TIME, TIME_EPS, T, I, Metrics
from .simulation_node import Node, NodeMetrics
from .item_generator import BaseFactoryNode
from .service_node import QueueingNode

if TYPE_CHECKING:
    from .results_logger import BaseLogger

MM = TypeVar("MM", bound="ModelMetrics")


class Nodes(dict[str, Node[I, NodeMetrics]]):
    """
    A dictionary-like structure for all nodes in the simulation.
    """

    @staticmethod
    def from_node_tree_root(node_tree_root: Node[I, NodeMetrics]) -> "Nodes[I]":
        """
        Collect all nodes reachable from the root node (via connected_nodes).
        Raises ValueError if any node has a duplicate name.
        """
        nodes = Nodes[I]()

        def traverse(node: Node[I, NodeMetrics]) -> None:
            if node.name in nodes:
                if nodes[node.name] == node:
                    return
                raise ValueError("Nodes must have different names.")
            nodes[node.name] = node
            for cnode in node.connected_nodes:
                traverse(cnode)

        traverse(node_tree_root)
        return nodes


@dataclass(eq=False)
class EvaluationReport(Generic[T]):
    """
    The result of evaluating a user-defined function on the model (e.g., total failures).
    """
    name: str
    result: T


@dataclass(eq=False)
class Evaluation(Generic[T]):
    """
    An evaluatable metric or function on the model, e.g. to compute after the simulation ends.
    """
    name: str
    evaluate: Callable[["Model"], T]

    def __call__(self, model: "Model") -> EvaluationReport[T]:
        return EvaluationReport[T](name=self.name, result=self.evaluate(model))


class Verbosity(Flag):
    """
    Simulation logging levels.
    """
    NONE = 0b00
    STATE = 0b01
    METRICS = 0b10


@dataclass(eq=False)
class ModelMetrics(Metrics, Generic[I]):
    """
    Basic model-wide metrics, storing the set of all items that have entered the system.
    """
    num_events: int = field(init=False, default=0)
    items: set[I] = field(init=False, default_factory=set)
    num_unblock_cycles: int = field(init=False, default=0)  # NEW: track unblocking attempts

    @property
    def mean_event_intensity(self) -> float:
        """
        Average event rate across the entire simulation time.
        """
        return self.num_events / max(self.passed_time, TIME_EPS)

    @property
    def processed_items(self) -> Iterable[I]:
        """
        All items that have been fully processed.
        """
        return (itm for itm in self.items if itm.processed)

    @property
    def time_per_item(self) -> dict[I, float]:
        """
        Mapping of item -> total time in the system.
        """
        return {itm: itm.time_in_system for itm in self.processed_items}

    @property
    def mean_time_in_system(self) -> float:
        """
        Average time that items spend in the system.
        """
        times = self.time_per_item.values()
        return statistics.mean(times) if times else 0

    def to_dict(self) -> dict[str, Any]:
        metrics_dict = super().to_dict()
        for field_name in ("processed_items", "time_per_item"):
            metrics_dict.pop(field_name, None)  # remove these from dict representation
        metrics_dict["num_events"] = self.num_events
        metrics_dict["num_unblock_cycles"] = self.num_unblock_cycles
        return metrics_dict


class Model(Generic[I, MM]):
    """
    The core class that runs the discrete-event simulation by repeatedly jumping
    from one event time to the next.
    """

    def __init__(
        self,
        nodes: Nodes[I],
        logger: "BaseLogger[I]",
        metrics: MM,
        evaluations: Optional[list[Evaluation]] = None,
        enable_unblock_safety_net: bool = True  # NEW: configurable safety net
    ) -> None:
        self.nodes = nodes
        self.logger = logger
        self.metrics = metrics
        self.evaluations = [] if evaluations is None else evaluations
        self.current_time = 0.0
        self.enable_unblock_safety_net = enable_unblock_safety_net
        # Do not collect items here to avoid mutating node internal structures
        # (some node.current_items implementations may expose internal lists).
        # Collection will occur naturally during simulation steps.

    @property
    def next_time(self) -> float:
        """
        The simulation time of the next event.
        """
        return min(INF_TIME, *(nd.next_time for nd in self.nodes.values()))

    @property
    def model_metrics(self) -> MM:
        return self.metrics

    @property
    def nodes_metrics(self) -> list[NodeMetrics]:
        return [nd.metrics for nd in self.nodes.values()]

    @property
    def evaluation_reports(self) -> list[EvaluationReport]:
        return [ev(self) for ev in self.evaluations]

    def reset_metrics(self) -> None:
        """
        Reset metrics on all nodes and the model itself.
        """
        for node in self.nodes.values():
            node.reset_metrics()
        self.metrics.reset()

    def reset(self) -> None:
        """
        Fully reset the model's state, including node states and metrics.
        """
        self.current_time = 0
        for node in self.nodes.values():
            node.reset()
        self.metrics.reset()

    def simulate(self, end_time: float, verbosity: Verbosity = Verbosity.METRICS) -> None:
        """
        Run the simulation until `end_time`, optionally logging states and metrics.
        """
        while self.step(end_time):
            if Verbosity.STATE in verbosity:
                self.logger.nodes_states(self.current_time, list(self.nodes.values()))

        if Verbosity.METRICS in verbosity:
            self.logger.model_metrics(self.model_metrics)
            self.logger.nodes_metrics(self.nodes_metrics)
            self.logger.evaluation_reports(self.evaluation_reports)

    def step(self, end_time: float = INF_TIME) -> bool:
        """
        Advance the simulation one event at a time, if the next event is before end_time.
        """
        nxt_time = self.next_time
        self._goto(nxt_time, end_time=end_time)
        return nxt_time <= end_time

    def _goto(self, time: float, end_time: float = INF_TIME) -> None:
        """
        Move to a particular simulation time, process all node(s) whose event time is exactly that.
        Includes unblocking safety net to handle cascading unblocks.
        """
        new_time = min(time, end_time)
        self._before_time_update_hook(new_time)
        self.current_time = new_time
        for nd in self.nodes.values():
            nd.update_time(self.current_time)

        # Identify the nodes that have events at the current time
        end_action_nodes = [
            nd for nd in self.nodes.values()
            if abs(self.current_time - nd.next_time) <= TIME_EPS
        ]
        for nd in end_action_nodes:
            nd.end_action()
            self._after_node_end_action_hook(nd)

        # SAFETY NET: Try to unblock any remaining blocked nodes
        # This handles edge cases where unblocking notifications might be missed
        if self.enable_unblock_safety_net:
            self._unblock_safety_net()

        self._collect_items()

    def _unblock_safety_net(self) -> None:
        """
        Safety net to ensure all possible unblocking happens.
        
        Iteratively attempts to unblock all nodes until no more progress is made.
        This handles cascading unblocks in multi-stage networks.
        
        Example: A→B→C where C becomes free should unblock B, then B should unblock A.
        """
        max_iterations = len(self.nodes) * 2  # Prevent infinite loops
        iteration = 0
        
        while iteration < max_iterations:
            iteration += 1
            progress_made = False
            
            # Try to unblock all nodes that have blocked tasks
            for nd in self.nodes.values():
                if hasattr(nd, 'try_unblock') and hasattr(nd, 'blocked_tasks'):
                    initial_blocked = len(nd.blocked_tasks)
                    nd.try_unblock()
                    if len(nd.blocked_tasks) < initial_blocked:
                        progress_made = True
            
            # Try to notify all nodes that might have blocked predecessors
            for nd in self.nodes.values():
                if hasattr(nd, '_notify_blocked_predecessors') and nd.can_accept_item():
                    nd._notify_blocked_predecessors()
            
            # If no node made progress, we're done
            if not progress_made:
                break
        
        if iteration > 1:
            self.metrics.num_unblock_cycles += 1

    def _collect_items(self) -> None:
        """
        Gather newly introduced items from each node's current_items set.
        """
        for nd in self.nodes.values():
            for it in nd.current_items:
                self.metrics.items.add(it)

    def _before_time_update_hook(self, time: float) -> None:
        """
        Called before we finalize the jump to `time`, allowing accumulation of metrics.
        """
        self.metrics.passed_time += time - self.current_time

    def _after_node_end_action_hook(self, node: Node[I, NodeMetrics]) -> None:
        """
        Called after a node completes an event, possibly updating the overall event count.
        """
        # Factory or queueing nodes create "events" (arrivals or completions).
        if isinstance(node, (BaseFactoryNode, QueueingNode)):
            self.metrics.num_events += 1

    def dumps(self) -> bytes:
        """
        Serialize the entire Model object to bytes using dill.
        """
        return dill.dumps(self)

    @staticmethod
    def loads(model_bytes: bytes) -> "Model[I, MM]":
        """
        Deserialize a Model object from dill-serialized bytes.
        """
        return cast(Model[I, MM], dill.loads(model_bytes))