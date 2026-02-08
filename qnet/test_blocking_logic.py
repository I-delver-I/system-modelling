import unittest
from dataclasses import dataclass, field
from typing import List, Any

from qnet.core_models import Queue
from qnet.service_node import (
    QueueingNode,
    ChannelPool,
    QueueingMetrics,
    Task,
    Channel,
    blocking_on_queue_length,
    blocking_on_capacity,
)
from qnet.simulation_engine import Model, Nodes, ModelMetrics
from qnet.results_logger import BaseLogger
from qnet.simulation_node import NodeState


@dataclass
class TestItem:
    name: str
    id: int = 0
    time_in_system: float = field(default=0.0, compare=False)
    processed: bool = field(default=False, compare=False)
    current_time: float = field(default=0.0, compare=False)
    history: List[Any] = field(default_factory=list, compare=False)

    def __hash__(self):
        return hash((self.name, self.id))

    def __repr__(self):
        return f"{self.name}:{self.id}"


class SilentLogger(BaseLogger):
    def log(self, *args, **kwargs):
        pass

    def nodes_states(self, time, nodes):
        pass

    def model_metrics(self, metrics):
        pass

    def nodes_metrics(self, metrics):
        pass

    def evaluation_reports(self, reports):
        pass


class TestBlockingLogic(unittest.TestCase):

    def setUp(self):
        self.logger = SilentLogger()

    def create_node(self, name, channels=1, queue_size=10, delay=1.0):
        return QueueingNode(
            name=name,
            queue=Queue(queue_size),
            channel_pool=ChannelPool(channels),
            metrics=QueueingMetrics(),
            delay_fn=lambda: delay,
        )

    def test_default_blocking_when_downstream_full(self):
        # B has no queue capacity, one busy channel -> cannot accept
        node_b = self.create_node("B", channels=1, queue_size=0, delay=0.0)
        node_b.start_action(TestItem("B_item", id=1))  # occupy B's channel

        node_a = self.create_node("A", channels=1, queue_size=1, delay=0.0)
        node_a.set_next_node(node_b)

        node_a.start_action(TestItem("A_item", id=2))
        # Completion should block because B cannot accept (channel busy & queue full)
        node_a.end_action()

        self.assertEqual(node_a.state, NodeState.BLOCKED)
        self.assertEqual(len(node_a.blocked_tasks), 1)
        self.assertIn(node_a, node_b.blocked_predecessors)
        self.assertEqual(node_a.metrics.num_blocks, 1)

    def test_unblock_and_blocked_time_metrics(self):
        # Setup A->B where B is initially occupied
        node_b = self.create_node("B", channels=1, queue_size=0, delay=0.0)
        node_b.start_action(TestItem("B_item", id=1))

        node_a = self.create_node("A", channels=1, queue_size=1, delay=0.0)
        node_a.set_next_node(node_b)

        node_a.start_action(TestItem("A_item", id=2))
        node_a.end_action()

        # advance time to simulate blocking duration
        node_a.update_time(5.0)
        node_b.update_time(5.0)

        # Free B's channel by finishing its task
        node_b.end_action()

        # Now try to unblock A
        node_a.try_unblock()

        self.assertEqual(len(node_a.blocked_tasks), 0)
        # blocked_time should reflect ~5.0 time units
        self.assertGreaterEqual(node_a.metrics.blocked_time, 5.0)

    def test_multiple_blocked_tasks_and_peak_metric(self):
        # B has capacity 1 and no queue; A will generate multiple completions
        node_b = self.create_node("B", channels=1, queue_size=0, delay=0.0)
        node_b.start_action(TestItem("B_item", id=1))

        node_a = self.create_node("A", channels=2, queue_size=0, delay=0.0)
        node_a.set_next_node(node_b)

        # Start two A tasks with distinct predicted finish times and complete them;
        # both should become blocked sequentially. Use add_task to control times.
        task1 = Task(TestItem("A1", id=1), next_time=1.0)
        task2 = Task(TestItem("A2", id=2), next_time=2.0)
        node_a.add_task(task1)
        node_a.add_task(task2)

        node_a.end_action()  # blocks 1
        node_a.end_action()  # blocks 2

        self.assertEqual(len(node_a.blocked_tasks), 2)
        self.assertEqual(node_a.metrics.max_blocked_tasks, 2)

    def test_custom_blocking_predicate_overrides_default(self):
        # A's custom predicate always blocks regardless of B's capacity
        node_b = self.create_node("B", channels=1, queue_size=5, delay=0.0)

        node_a = self.create_node("A", channels=1, queue_size=5, delay=0.0)
        node_a.set_next_node(node_b)
        node_a.blocking_predicate = lambda: True

        node_a.start_action(TestItem("A_item", id=3))
        node_a.end_action()

        self.assertEqual(node_a.state, NodeState.BLOCKED)
        self.assertEqual(node_a.metrics.num_blocks, 1)

    def test_cascade_unblocking_via_model(self):
        # A -> B -> C cascade: freeing C should unblock B then A
        node_c = self.create_node("C", channels=1, queue_size=0, delay=0.0)
        node_c.start_action(TestItem("C_item", id=1))

        node_b = self.create_node("B", channels=1, queue_size=0, delay=0.0)
        node_b.start_action(TestItem("B_item", id=2))
        node_b.set_next_node(node_c)

        node_a = self.create_node("A", channels=1, queue_size=0, delay=0.0)
        node_a.set_next_node(node_b)

        # Make A finish and become blocked (B full)
        node_a.start_action(TestItem("A_item", id=3))
        node_a.end_action()

        # Also fill B so it blocks A (already filled above)
        # Now free C via model stepping so B can move its item onward
        nodes = Nodes()
        nodes["A"] = node_a
        nodes["B"] = node_b
        nodes["C"] = node_c

        model = Model(nodes, SilentLogger(), ModelMetrics())
        # Move to next events; C will finish first (delay 0), then B will, then A
        model.step()
        model.step()
        model.step()

        # After cascade, no blocked tasks should remain
        self.assertEqual(len(node_a.blocked_tasks), 0)
        self.assertEqual(len(node_b.blocked_tasks), 0)

    def test_blocking_on_capacity_predicate(self):
        # blocking_on_capacity should block when next node cannot accept
        node_b = self.create_node("B", channels=1, queue_size=0, delay=0.0)
        node_b.start_action(TestItem("B_item", id=1))

        node_a = self.create_node("A", channels=1, queue_size=1, delay=0.0)
        node_a.set_next_node(node_b)
        node_a.blocking_predicate = blocking_on_capacity(node_a)

        node_a.start_action(TestItem("A_item", id=4))
        node_a.end_action()

        self.assertEqual(node_a.state, NodeState.BLOCKED)


if __name__ == '__main__':
    unittest.main()
