"""
Critical Blocking Tests - Runnable Implementation
These tests validate the most important aspects of blocking logic
"""

import unittest
from dataclasses import dataclass, field
from typing import List, Any

# Assuming these imports work with your structure
# Adjust import paths as needed
try:
    from qnet.core_models import Queue, Item
    from qnet.service_node import QueueingNode, ChannelPool, QueueingMetrics
    from qnet.simulation_node import NodeState
    IMPORTS_AVAILABLE = True
except ImportError:
    IMPORTS_AVAILABLE = False
    print("Warning: QNet imports not available. Tests will be skipped.")


@dataclass
class TestItem:
    """Minimal test item."""
    name: str
    id: int = field(default=0)
    created_time: float = field(default=0.0)
    time_in_system: float = field(default=0.0)
    processed: bool = field(default=False)
    current_time: float = field(default=0.0)
    history: List[Any] = field(default_factory=list)
    
    def __repr__(self):
        return f"Item({self.name}, t={self.current_time:.1f})"


@unittest.skipIf(not IMPORTS_AVAILABLE, "QNet not available")
class TestCriticalBlocking(unittest.TestCase):
    """Critical tests that MUST pass for blocking to work correctly."""
    
    def setUp(self):
        """Create test fixtures."""
        self.item_counter = 0
    
    def create_item(self, name=None):
        """Create a unique test item."""
        if name is None:
            name = f"Item{self.item_counter}"
        self.item_counter += 1
        return TestItem(name=name, id=self.item_counter)
    
    def create_node(self, name, channels=1, queue_size=0, delay=0.0):
        """Create a queueing node with specified capacity."""
        return QueueingNode(
            name=name,
            queue=Queue(maxlen=queue_size if queue_size >= 0 else None),
            channel_pool=ChannelPool(max_channels=channels),
            metrics=QueueingMetrics(),
            delay_fn=lambda: delay
        )
    
    def test_01_basic_blocking_occurs(self):
        """
        CRITICAL: Verify blocking actually happens when downstream is full.
        
        Setup: A → B (B is full)
        Action: A finishes item
        Result: A enters BLOCKED state
        """
        print("\n" + "="*60)
        print("TEST 1: Basic Blocking")
        print("="*60)
        
        # Create nodes
        node_b = self.create_node("B", channels=1, queue_size=0)
        node_a = self.create_node("A", channels=1, queue_size=0)
        node_a.set_next_node(node_b)
        
        # Fill B
        item_blocker = self.create_item("Blocker")
        node_b.start_action(item_blocker)
        print(f"B state after filling: {node_b.state}")
        self.assertFalse(node_b.can_accept_item(), "B should be full")
        
        # Process item in A
        item_a = self.create_item("ItemA")
        node_a.start_action(item_a)
        print(f"A state after start: {node_a.state}")
        
        # A finishes - should block
        finished = node_a.end_action()
        print(f"A state after end: {node_a.state}")
        print(f"A blocked_tasks: {len(node_a.blocked_tasks)}")
        print(f"A is in B.blocked_predecessors: {node_a in node_b.blocked_predecessors}")
        
        # CRITICAL ASSERTIONS
        self.assertEqual(node_a.state, NodeState.BLOCKED, "A must be BLOCKED")
        self.assertEqual(len(node_a.blocked_tasks), 1, "A must have 1 blocked task")
        self.assertIn(node_a, node_b.blocked_predecessors, "A must be registered with B")
        self.assertEqual(node_a.metrics.num_blocks, 1, "num_blocks must be 1")
        
        print("✓ Basic blocking works correctly")
    
    def test_02_unblocking_restores_flow(self):
        """
        CRITICAL: Verify unblocking actually works.
        
        Setup: A → B (B full, A blocked)
        Action: B finishes and frees space
        Result: A unblocks and sends item
        """
        print("\n" + "="*60)
        print("TEST 2: Unblocking")
        print("="*60)
        
        # Setup from test_01
        node_b = self.create_node("B", channels=1, queue_size=0)
        node_a = self.create_node("A", channels=1, queue_size=0)
        node_a.set_next_node(node_b)
        
        # Fill B
        item_blocker = self.create_item("Blocker")
        node_b.start_action(item_blocker)
        
        # Block A
        item_a = self.create_item("ItemA")
        node_a.start_action(item_a)
        node_a.end_action()
        
        print(f"Initial: A.state={node_a.state}, A.blocked_tasks={len(node_a.blocked_tasks)}")
        
        # Advance time
        node_a.current_time = 10.0
        node_b.current_time = 10.0
        
        # B finishes - this should trigger unblocking
        node_b.end_action()
        
        print(f"After B finishes: B.can_accept={node_b.can_accept_item()}")
        
        # A should try to unblock
        node_a.try_unblock()
        
        print(f"After unblock: A.state={node_a.state}, A.blocked_tasks={len(node_a.blocked_tasks)}")
        print(f"B now has {node_b.num_tasks} tasks")
        
        # CRITICAL ASSERTIONS
        self.assertEqual(len(node_a.blocked_tasks), 0, "A should have no blocked tasks")
        self.assertIn(node_a.state, [NodeState.IDLE, NodeState.BUSY], "A should be IDLE or BUSY")
        self.assertEqual(node_b.num_tasks, 1, "B should have received the item")
        self.assertNotIn(node_a, node_b.blocked_predecessors, "A should be removed from blocked_predecessors")
        
        print("✓ Unblocking works correctly")
    
    def test_03_per_task_blocking_duration(self):
        """
        CRITICAL: Verify blocking duration is tracked per-task, not globally.
        
        This was the main bug in the original implementation.
        """
        print("\n" + "="*60)
        print("TEST 3: Per-Task Blocking Duration")
        print("="*60)
        
        node_b = self.create_node("B", channels=1, queue_size=0)
        node_a = self.create_node("A", channels=2, queue_size=0)  # 2 channels
        node_a.set_next_node(node_b)
        
        # Fill B
        item_blocker = self.create_item("Blocker")
        node_b.start_action(item_blocker)
        
        # Block first item at t=10
        node_a.current_time = 10.0
        item1 = self.create_item("Item1")
        item1.created_time = 10.0
        node_a.start_action(item1)
        node_a.end_action()
        print(f"t=10: Blocked Item1")
        
        # Block second item at t=15
        node_a.current_time = 15.0
        node_b.current_time = 15.0
        item2 = self.create_item("Item2")
        item2.created_time = 15.0
        node_a.start_action(item2)
        node_a.end_action()
        print(f"t=15: Blocked Item2")
        
        self.assertEqual(len(node_a.blocked_tasks), 2, "Should have 2 blocked tasks")
        
        # Unblock first at t=20 (duration = 10)
        node_a.current_time = 20.0
        node_b.current_time = 20.0
        node_b.end_action()  # Free space
        node_a.try_unblock()
        
        print(f"t=20: Unblocked Item1, blocked_time={node_a.metrics.blocked_time}")
        self.assertEqual(node_a.metrics.blocked_time, 10.0, "First task blocked for 10 time units")
        self.assertEqual(len(node_a.blocked_tasks), 1, "Should have 1 blocked task remaining")
        
        # Unblock second at t=30 (duration = 15)
        node_a.current_time = 30.0
        node_b.current_time = 30.0
        node_b.end_action()  # Free space again
        node_a.try_unblock()
        
        print(f"t=30: Unblocked Item2, blocked_time={node_a.metrics.blocked_time}")
        
        # CRITICAL: Total blocked time should be 10 + 15 = 25
        # NOT (30 - 10) = 20 (global tracking bug)
        self.assertEqual(node_a.metrics.blocked_time, 25.0, 
                        "Total blocked time should be 10 + 15 = 25 (per-task tracking)")
        self.assertEqual(node_a.metrics.num_blocks, 2, "Should have 2 blocking events")
        self.assertEqual(node_a.metrics.mean_blocked_time, 12.5, 
                        "Mean blocked time should be 25/2 = 12.5")
        
        print("✓ Per-task blocking duration works correctly")
    
    def test_04_capacity_with_blocked_tasks(self):
        """
        CRITICAL: Blocked tasks count toward capacity.
        
        Setup: Node with 2 channels, 1 active, 1 blocked
        Result: Node should report as full (can't accept without queue space)
        """
        print("\n" + "="*60)
        print("TEST 4: Capacity Accounting")
        print("="*60)
        
        node_b = self.create_node("B", channels=1, queue_size=0)
        node_a = self.create_node("A", channels=2, queue_size=0)  # 2 channels, NO queue
        node_a.set_next_node(node_b)
        
        # Fill B
        item_blocker = self.create_item("Blocker")
        node_b.start_action(item_blocker)
        
        # Start one item in A (channel 1)
        item1 = self.create_item("Item1")
        node_a.start_action(item1)
        print(f"After starting Item1: can_accept={node_a.can_accept_item()}")
        self.assertTrue(node_a.can_accept_item(), "Should still accept (1/2 channels used)")
        
        # Finish and block (channel 1 now has blocked task)
        node_a.end_action()
        print(f"After blocking Item1: can_accept={node_a.can_accept_item()}")
        print(f"  Active channels: {node_a.channel_pool.num_occupied_channels}")
        print(f"  Blocked tasks: {len(node_a.blocked_tasks)}")
        
        # Start another item (channel 2)
        item2 = self.create_item("Item2")
        node_a.start_action(item2)
        print(f"After starting Item2: can_accept={node_a.can_accept_item()}")
        
        # CRITICAL: Now we have 1 active + 1 blocked = 2/2 channels occupied
        # With no queue space, should NOT accept more items
        self.assertFalse(node_a.can_accept_item(), 
                        "Should be full: 1 active + 1 blocked = 2/2 channels")
        
        # Trying to add another should fail
        item3 = self.create_item("Item3")
        initial_failures = node_a.metrics.num_failures
        node_a.start_action(item3)
        
        self.assertEqual(node_a.metrics.num_failures, initial_failures + 1,
                        "Should reject item when full")
        
        print("✓ Capacity accounting works correctly")
    
    def test_05_cascading_unblock_chain(self):
        """
        CRITICAL: Unblocking propagates through chain.
        
        Setup: A → B → C (all blocked)
        Action: C frees space
        Result: B unblocks, then A unblocks
        """
        print("\n" + "="*60)
        print("TEST 5: Cascading Unblock")
        print("="*60)
        
        # Create chain
        node_c = self.create_node("C", channels=1, queue_size=0)
        node_b = self.create_node("B", channels=1, queue_size=0)
        node_a = self.create_node("A", channels=1, queue_size=0)
        
        node_a.set_next_node(node_b)
        node_b.set_next_node(node_c)
        
        # Fill C
        item_c = self.create_item("ItemC")
        node_c.start_action(item_c)
        
        # Block B
        item_b = self.create_item("ItemB")
        node_b.start_action(item_b)
        node_b.end_action()
        print(f"B blocked: {node_b.state}")
        
        # Block A
        item_a = self.create_item("ItemA")
        node_a.start_action(item_a)
        node_a.end_action()
        print(f"A blocked: {node_a.state}")
        
        # Verify initial state
        self.assertEqual(node_b.state, NodeState.BLOCKED)
        self.assertEqual(node_a.state, NodeState.BLOCKED)
        
        # C finishes - triggers cascade
        node_c.current_time = 10.0
        node_b.current_time = 10.0
        node_a.current_time = 10.0
        
        node_c.end_action()
        print(f"After C finishes: C.can_accept={node_c.can_accept_item()}")
        
        # B should unblock
        node_b.try_unblock()
        print(f"After B.try_unblock: B.state={node_b.state}, B.blocked_tasks={len(node_b.blocked_tasks)}")
        
        # A should unblock
        node_a.try_unblock()
        print(f"After A.try_unblock: A.state={node_a.state}, A.blocked_tasks={len(node_a.blocked_tasks)}")
        
        # CRITICAL ASSERTIONS
        self.assertEqual(len(node_b.blocked_tasks), 0, "B should be unblocked")
        self.assertEqual(len(node_a.blocked_tasks), 0, "A should be unblocked")
        self.assertEqual(node_c.num_tasks, 1, "C should have ItemB")
        
        print("✓ Cascading unblock works correctly")
    
    def test_06_fifo_unblock_order(self):
        """
        CRITICAL: Blocked items unblock in FIFO order.
        """
        print("\n" + "="*60)
        print("TEST 6: FIFO Unblock Order")
        print("="*60)
        
        node_b = self.create_node("B", channels=1, queue_size=0)
        node_a = self.create_node("A", channels=3, queue_size=0)  # 3 channels
        node_a.set_next_node(node_b)
        
        # Fill B
        item_blocker = self.create_item("Blocker")
        node_b.start_action(item_blocker)
        
        # Block 3 items in order
        items = []
        for i in range(3):
            node_a.current_time = float(i)
            item = self.create_item(f"Item{i}")
            items.append(item)
            node_a.start_action(item)
            node_a.end_action()
        
        print(f"Blocked items: {[t.item.name for t in node_a.blocked_tasks]}")
        self.assertEqual(len(node_a.blocked_tasks), 3)
        
        # Verify order
        self.assertEqual(node_a.blocked_tasks[0].item.name, "Item0")
        self.assertEqual(node_a.blocked_tasks[1].item.name, "Item1")
        self.assertEqual(node_a.blocked_tasks[2].item.name, "Item2")
        
        # Unblock one
        node_a.current_time = 10.0
        node_b.current_time = 10.0
        node_b.end_action()
        node_a.try_unblock()
        
        print(f"After unblock 1: {[t.item.name for t in node_a.blocked_tasks]}")
        
        # CRITICAL: Item0 should have unblocked (FIFO)
        self.assertEqual(len(node_a.blocked_tasks), 2)
        self.assertEqual(node_a.blocked_tasks[0].item.name, "Item1", "Item1 should be next")
        
        # Unblock another
        node_b.end_action()
        node_a.try_unblock()
        
        print(f"After unblock 2: {[t.item.name for t in node_a.blocked_tasks]}")
        
        # Item1 should have unblocked
        self.assertEqual(len(node_a.blocked_tasks), 1)
        self.assertEqual(node_a.blocked_tasks[0].item.name, "Item2", "Item2 should be last")
        
        print("✓ FIFO unblock order maintained")
    
    def test_07_max_blocked_tasks_metric(self):
        """
        Test max_blocked_tasks metric.
        """
        print("\n" + "="*60)
        print("TEST 7: Max Blocked Tasks Metric")
        print("="*60)
        
        node_b = self.create_node("B", channels=1, queue_size=0)
        node_a = self.create_node("A", channels=10, queue_size=0)
        node_a.set_next_node(node_b)
        
        # Fill B
        node_b.start_action(self.create_item("Blocker"))
        
        # Block 5 items
        for i in range(5):
            item = self.create_item(f"Item{i}")
            node_a.start_action(item)
            node_a.end_action()
        
        print(f"After 5 blocks: max_blocked_tasks={node_a.metrics.max_blocked_tasks}")
        self.assertEqual(node_a.metrics.max_blocked_tasks, 5)
        self.assertEqual(len(node_a.blocked_tasks), 5)
        
        # Unblock 2
        node_a.current_time = 10.0
        node_b.current_time = 10.0
        
        # Free space in B and unblock from A
        node_b.end_action()
        node_a.try_unblock()
        
        node_b.end_action()
        node_a.try_unblock()
        
        print(f"After 2 unblocks: blocked_tasks={len(node_a.blocked_tasks)}, max={node_a.metrics.max_blocked_tasks}")
        self.assertEqual(len(node_a.blocked_tasks), 3)
        self.assertEqual(node_a.metrics.max_blocked_tasks, 5, "Max should still be 5")
        
        # Now node_a has 0 active tasks, 3 blocked tasks
        # Total effective capacity: 3/5 occupied by blocked
        # So we have room for 2 more items
        
        # Block 2 more items (not 3, because capacity)
        successful_blocks = 0
        for i in range(2):
            item = self.create_item(f"NewItem{i}")
            initial_failures = node_a.metrics.num_failures
            
            node_a.start_action(item)
            
            # Check if item was accepted (not failed)
            if node_a.metrics.num_failures == initial_failures:
                # Item was accepted, now end_action to block it
                node_a.end_action()
                successful_blocks += 1
            else:
                print(f"  NewItem{i} was rejected (node full)")
        
        print(f"Successfully blocked {successful_blocks} new items")
        print(f"Current blocked_tasks: {len(node_a.blocked_tasks)}")
        print(f"New max: {node_a.metrics.max_blocked_tasks}")
        
        # Now we should have 3 + 2 = 5 blocked (unless some were rejected)
        # Max should update if we exceeded 5
        expected_blocked = 3 + successful_blocks
        self.assertEqual(len(node_a.blocked_tasks), expected_blocked)
        
        # If we successfully added 2 more, max becomes 5 (not 6, because we only added 2)
        # To get to 6, we'd need to add 3 more
        if successful_blocks == 2:
            # Peak is still 5 (we had 5, dropped to 3, added 2 → back to 5)
            self.assertEqual(node_a.metrics.max_blocked_tasks, 5)
        
        print("✓ Max blocked tasks metric works correctly")


if __name__ == '__main__':
    # Run with verbose output
    suite = unittest.TestLoader().loadTestsFromTestCase(TestCriticalBlocking)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success: {result.wasSuccessful()}")
    
    if result.wasSuccessful():
        print("\n✅ ALL CRITICAL TESTS PASSED")
    else:
        print("\n❌ SOME TESTS FAILED")