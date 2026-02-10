import unittest
from dataclasses import dataclass, field
from typing import List, Any
import heapq

# Correct Imports
from qnet.core_models import Queue
# Import Task from service_node to ensure class compatibility
from qnet.service_node import QueueingNode, ChannelPool, QueueingMetrics, blocking_on_queue_length, Channel, Task
from qnet.simulation_node import NodeMetrics, NodeState
from qnet.simulation_engine import Model, Nodes, ModelMetrics
from qnet.results_logger import BaseLogger

# --- Helper Classes ---

@dataclass
class TestItem:
    name: str
    id: int = 0
    # Mutable fields must not affect identity
    time_in_system: float = field(default=0.0, compare=False)
    processed: bool = field(default=False, compare=False)
    current_time: float = field(default=0.0, compare=False)
    history: List[Any] = field(default_factory=list, compare=False) 
    
    def __repr__(self): 
        return self.name
        
    # Explicit hash to prevent "unhashable type" error in Model sets
    def __hash__(self):
        return hash((self.name, self.id))
        
    def __eq__(self, other):
        if not isinstance(other, TestItem):
            return NotImplemented
        return self.name == other.name and self.id == other.id

class SilentLogger(BaseLogger):
    def log(self, *args, **kwargs): pass
    def nodes_states(self, time, nodes): pass
    def model_metrics(self, metrics): pass
    def nodes_metrics(self, metrics): pass
    def evaluation_reports(self, reports): pass

class TestSimulationMechanics(unittest.TestCase):

    def setUp(self):
        self.logger = SilentLogger()
        
    def create_node(self, name, channels=1, delay=1.0):
        return QueueingNode(
            name=name,
            queue=Queue(10),
            channel_pool=ChannelPool(channels),
            metrics=QueueingMetrics(),
            delay_fn=lambda: delay
        )

    # =========================================================================
    # TEST 1: Service Discipline (MinHeap Priority)
    # =========================================================================
    def test_channel_heap_priority(self):
        node = self.create_node("FastSlow", channels=2)
        
        # 1. Add SLOW task (t=10.0)
        item_slow = TestItem("Slow", id=1)
        task_slow = Task(item_slow, next_time=10.0)
        node.add_task(task_slow)
        
        # 2. Add FAST task (t=2.0)
        item_fast = TestItem("Fast", id=2)
        task_fast = Task(item_fast, next_time=2.0)
        node.add_task(task_fast)
        
        # Assertion
        self.assertEqual(node.channel_pool.next_finish_time, 2.0)
        finished_task = node.channel_pool.pop_finished_task()
        self.assertEqual(finished_task.item.name, "Fast")
        print("\n✓ Mechanics: ChannelPool correctly prioritizes earliest finishing tasks.")

    # =========================================================================
    # TEST 2: Custom Blocking Predicates
    # =========================================================================
    def test_custom_blocking_predicate(self):
        node_b = self.create_node("B", channels=1, delay=1.0)
        # Fill B so Queue has 1 item
        node_b.start_action(TestItem("Item_In_Channel", id=1)) 
        node_b.start_action(TestItem("Item_In_Queue", id=2)) 
        
        node_a = self.create_node("A", channels=1, delay=0.1)
        node_a.set_next_node(node_b)
        node_a.blocking_predicate = blocking_on_queue_length(node_a, 1)
        
        node_a.start_action(TestItem("Item_A", id=3))
        node_a.end_action()
        
        self.assertEqual(node_a.state, NodeState.BLOCKED)
        print("\n✓ Mechanics: Custom blocking predicates override standard logic.")

    # =========================================================================
    # TEST 3: Metrics Integration
    # =========================================================================
    def test_metrics_time_integration(self):
        node = self.create_node("MetricsTest", channels=1)
        node.update_time(2.0)
        
        # Add to queue manually
        fake_channel = Channel(id=99)
        node.channel_pool.occupied_channels.add(fake_channel) 
        node.queue.push(TestItem("Q_Item", id=1))
        
        node.update_time(6.0) # Queue=1 for 4.0s
        node.queue.pop()
        node.update_time(10.0) # Idle for 4.0s
        
        expected_mean = 4.0 / 10.0
        self.assertAlmostEqual(node.metrics.mean_queuelen, expected_mean, delta=0.001)
        print(f"\n✓ Mechanics: Metrics integration (L = {node.metrics.mean_queuelen}) is correct.")

    # =========================================================================
    # TEST 4: Engine Event Interleaving
    # =========================================================================
    def test_engine_event_ordering(self):
        node_a = self.create_node("A", delay=5.0)
        node_b = self.create_node("B", delay=3.0)
        
        node_a.start_action(TestItem("A", id=1)) 
        node_b.start_action(TestItem("B", id=2)) 
        
        # Verify state BEFORE Model init
        # (Assuming ChannelPool.tasks is accessible via .heap for inspection)
        if hasattr(node_a.channel_pool.tasks, 'heap'):
            self.assertEqual(len(node_a.channel_pool.tasks.heap), 1, "Heap check A")
            self.assertEqual(len(node_b.channel_pool.tasks.heap), 1, "Heap check B")

        nodes = Nodes()
        nodes["A"] = node_a
        nodes["B"] = node_b
        
        class SpyLogger(SilentLogger): pass
        
        # --- DANGER ZONE: This call was consuming the heap! ---
        model = Model(nodes, SpyLogger(), ModelMetrics())
        # ----------------------------------------------------

        # Verify state AFTER Model init
        if hasattr(node_a.channel_pool.tasks, 'heap'):
             self.assertEqual(len(node_a.channel_pool.tasks.heap), 1, 
                              "CRITICAL: Model init destroyed the heap! Fix QueueingNode.current_items.")

        # Step 1 -> t=3.0
        model.step()
        self.assertEqual(model.current_time, 3.0)
        self.assertEqual(node_b.metrics.num_out, 1)
        self.assertEqual(node_a.metrics.num_out, 0)
        
        # Step 2 -> t=5.0
        model.step()
        self.assertEqual(model.current_time, 5.0)
        self.assertEqual(node_a.metrics.num_out, 1)
        
        print("\n✓ Mechanics: Discrete Event Engine strictly respects time ordering.")

if __name__ == '__main__':
    unittest.main()