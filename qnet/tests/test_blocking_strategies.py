import unittest
from dataclasses import dataclass, field
from typing import List, Any, Optional, Protocol, runtime_checkable
from enum import Enum

# Adjust imports to match your project structure
from qnet.core_models import Queue
from qnet.service_node import QueueingNode, ChannelPool, QueueingMetrics, NodeState, Task
from qnet.simulation_node import NodeMetrics
from qnet.simulation_engine import Model, Nodes, ModelMetrics
from qnet.results_logger import BaseLogger

# --- MOCKS & HELPERS ---

@dataclass(unsafe_hash=True)
class TestItem:
    name: str
    id: int = 0
    time_in_system: float = field(default=0.0, compare=False, hash=False)
    processed: bool = field(default=False, compare=False, hash=False)
    current_time: float = field(default=0.0, compare=False, hash=False)
    history: List[Any] = field(default_factory=list, compare=False, hash=False) 
    def __repr__(self): return self.name

class SilentLogger(BaseLogger):
    def log(self, *args, **kwargs): pass
    def nodes_states(self, time, nodes): pass
    def model_metrics(self, metrics): pass
    def nodes_metrics(self, metrics): pass
    def evaluation_reports(self, reports): pass

# --- STRATEGY DEFINITIONS (Copy these to your library or import them) ---

@runtime_checkable
class BlockingStrategy(Protocol):
    def handle_blocked_item(self, node: 'QueueingNode', item: Any) -> bool:
        ...

class BlockStrategy(BlockingStrategy):
    """Default: Hold item, occupy server."""
    def handle_blocked_item(self, node: 'QueueingNode', item: Any) -> bool:
        task = Task(item=item, next_time=node.current_time, blocked_start_time=node.current_time)
        node.blocked_tasks.append(task)
        node.metrics.num_blocks += 1
        if len(node.blocked_tasks) > node.metrics.max_blocked_tasks:
            node.metrics.max_blocked_tasks = len(node.blocked_tasks)
        node.state = NodeState.BLOCKED
        if node.next_node:
            node.next_node.blocked_predecessors.add(node)
        return False

class DropStrategy(BlockingStrategy):
    """Loss System: Drop item immediately."""
    def handle_blocked_item(self, node: 'QueueingNode', item: Any) -> bool:
        node.metrics.num_drops += 1
        node._item_out_hook(item)
        return True

class ReprocessStrategy(BlockingStrategy):
    """Re-queue: Send back to queue."""
    def handle_blocked_item(self, node: 'QueueingNode', item: Any) -> bool:
        node.metrics.num_blocks += 1
        if not node.queue.is_full:
            node.queue.push(item)
            return True
        else:
            node.metrics.num_drops += 1
            node._item_out_hook(item)
            return True

class RerouteStrategy(BlockingStrategy):
    """Custom: Send to a specific fallback node."""
    def __init__(self, target_node: 'QueueingNode'):
        self.target_node = target_node

    def handle_blocked_item(self, node: 'QueueingNode', item: Any) -> bool:
        if self.target_node.can_accept_item():
            # Log exit from current node
            node._item_out_hook(item)
            # Enter new node
            self.target_node.start_action(item)
            return True
        else:
            # Fallback to Drop if target is full
            node.metrics.num_drops += 1
            node._item_out_hook(item)
            return True

# --- THE TEST SUITE ---

class TestBlockingStrategies(unittest.TestCase):

    def setUp(self):
        self.logger = SilentLogger()

    def create_node(self, name, channels=1, strategy=None, queue_size=10):
        strategy = strategy or BlockStrategy()
        return QueueingNode(
            name=name,
            queue=Queue(queue_size),
            channel_pool=ChannelPool(channels),
            metrics=QueueingMetrics(),
            delay_fn=lambda: 1.0,
            blocking_strategy=strategy
        )

    # 1. TEST BLOCK STRATEGY (Standard Behavior)
    def test_block_strategy(self):
        # Setup: Node -> Wall (Full)
        node = self.create_node("Node", strategy=BlockStrategy())
        wall = self.create_node("Wall", queue_size=0, channels=1)
        wall.start_action(TestItem("Blocker")) # Wall is full
        node.set_next_node(wall)

        # Action: Process item
        item = TestItem("Item1")
        node.start_action(item)
        node.end_action() # This should BLOCK

        # Assertions
        self.assertEqual(node.state, NodeState.BLOCKED)
        self.assertEqual(len(node.blocked_tasks), 1)
        self.assertEqual(node.metrics.num_blocks, 1)
        self.assertFalse(item.processed, "Item should not be processed/gone")
        print("\n✓ Strategy: BLOCK strategy correctly holds item and sets state.")

    # 2. TEST DROP STRATEGY (Loss System)
    def test_drop_strategy(self):
        node = self.create_node("Node", strategy=DropStrategy())
        wall = self.create_node("Wall", queue_size=0, channels=1)
        wall.start_action(TestItem("Blocker"))
        node.set_next_node(wall)

        item = TestItem("Item1")
        node.start_action(item)
        node.end_action() # This should DROP

        # Assertions
        self.assertNotEqual(node.state, NodeState.BLOCKED)
        self.assertEqual(len(node.blocked_tasks), 0)
        self.assertEqual(node.metrics.num_drops, 1)
        self.assertEqual(node.metrics.num_out, 1, "Item should be counted as 'out'")
        print("\n✓ Strategy: DROP strategy correctly discards item and frees channel.")

    # 3. TEST REPROCESS STRATEGY (Re-queue)
    def test_reprocess_strategy(self):
        # Node has queue capacity
        node = self.create_node("Node", strategy=ReprocessStrategy(), queue_size=5)
        wall = self.create_node("Wall", queue_size=0, channels=1)
        wall.start_action(TestItem("Blocker"))
        node.set_next_node(wall)

        item = TestItem("Item1")
        node.start_action(item)
        
        # Verify queue is empty before processing finishes
        self.assertEqual(node.queuelen, 0)
        
        node.end_action() # This should REPROCESS

        # Assertions
        # FIX: The item might have been immediately picked up again by the free channel
        is_in_queue = (node.queuelen == 1 and node.queue.data[0] == item)
        is_processing = (node.channel_pool.num_active_tasks == 1) 
        
        self.assertTrue(is_in_queue or is_processing, 
                        "Item should be re-queued (or immediately re-processed)")
        
        self.assertEqual(node.metrics.num_blocks, 1)
        print("\n✓ Strategy: REPROCESS strategy moves item back to queue.")

    # 4. TEST REROUTE STRATEGY (Custom Logic)
    def test_reroute_strategy(self):
        # Scenario: Primary -> Wall (Full). Fallback -> BackupNode (Empty).
        backup = self.create_node("Backup")
        reroute_strat = RerouteStrategy(target_node=backup)
        
        primary = self.create_node("Primary", strategy=reroute_strat)
        wall = self.create_node("Wall", queue_size=0, channels=1)
        wall.start_action(TestItem("Blocker"))
        primary.set_next_node(wall)

        item = TestItem("Item1")
        primary.start_action(item)
        primary.end_action() # This should REROUTE

        # Assertions
        self.assertEqual(len(primary.blocked_tasks), 0)
        self.assertEqual(primary.metrics.num_out, 1)
        
        # Verify item is now in Backup node
        self.assertEqual(backup.metrics.num_in, 1)
        self.assertEqual(next(iter(backup.current_items)), item)
        print("\n✓ Strategy: CUSTOM REROUTE strategy successfully moved item to backup node.")

    # 5. TEST REFILL LOGIC (The "One Go" Fix)
    def test_refill_on_drop(self):
        """
        Verify that if an item is DROPPED, the node immediately refills 
        the channel from the queue in the same tick.
        """
        node = self.create_node("Node", channels=1, strategy=DropStrategy())
        wall = self.create_node("Wall", queue_size=0, channels=1)
        wall.start_action(TestItem("Blocker"))
        node.set_next_node(wall)

        # Fill Channel AND Queue
        node.start_action(TestItem("Item_Channel"))
        node.start_action(TestItem("Item_Queue"))
        
        self.assertEqual(node.queuelen, 1)

        # Finish item -> It gets DROPPED
        node.end_action()

        # Assertions
        self.assertEqual(node.metrics.num_drops, 1)
        self.assertEqual(node.queuelen, 0, "Queue should be empty (moved to channel)")
        self.assertEqual(node.channel_pool.num_occupied_channels, 1, "Channel should be busy again")
        self.assertEqual(node.state, NodeState.BUSY)
        
        print("\n✓ Logic: Node correctly refills from queue immediately after dropping blocked item.")

if __name__ == '__main__':
    unittest.main()