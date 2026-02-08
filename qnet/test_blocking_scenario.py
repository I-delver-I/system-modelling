import unittest
from dataclasses import dataclass, field
from typing import List, Any

# Adjust these imports to match your project structure
from qnet.core_models import Queue, Item
from qnet.service_node import QueueingNode, ChannelPool, QueueingMetrics, Task
from qnet.simulation_node import NodeMetrics, NodeState
from qnet.simulation_engine import Model, Nodes, ModelMetrics
from qnet.results_logger import BaseLogger

# --- Helper Classes ---

@dataclass
class TestItem:
    name: str
    id: int = 0
    time_in_system: float = 0.0
    processed: bool = False
    current_time: float = 0.0
    history: List[Any] = field(default_factory=list)
    def __repr__(self): return self.name

class SilentLogger(BaseLogger):
    def log(self, *args, **kwargs): pass
    def nodes_states(self, time, nodes): pass
    def model_metrics(self, metrics): pass
    def nodes_metrics(self, metrics): pass
    def evaluation_reports(self, reports): pass

class TestComprehensiveBlocking(unittest.TestCase):

    def setUp(self):
        self.logger = SilentLogger()

    def create_node(self, name, queue_size=0, channels=1, delay=0.0):
        """Factory for creating nodes cleanly."""
        return QueueingNode(
            name=name,
            queue=Queue(queue_size),
            channel_pool=ChannelPool(channels),
            metrics=QueueingMetrics(),
            delay_fn=lambda: delay
        )

    # =========================================================================
    # SCENARIO 1: The "Virtual Hole" Refill (CRITICAL FIX VERIFICATION)
    # =========================================================================
    def test_refill_from_queue_on_unblock(self):
        """
        Verify that when a blocked item leaves, the node IMMEDIATELY 
        pulls a waiting item from its queue to fill the empty server slot.
        """
        # Node: 1 Channel, 1 Queue Slot.
        node = self.create_node("Server", queue_size=1, channels=1, delay=0.1)
        # Wall: Blocks everything
        wall = self.create_node("Wall", queue_size=0, channels=1, delay=999.0)
        wall.start_action(TestItem("Blocker"))
        node.set_next_node(wall)

        # 1. Fill Node (1 Active)
        node.start_action(TestItem("Item_Active"))
        
        # 2. Fill Queue (1 Queued)
        node.start_action(TestItem("Item_Queued"))

        # 3. Finish Active Item -> It becomes BLOCKED.
        # Node state: 1 Blocked, 1 Queued. 
        node.end_action()
        
        self.assertEqual(len(node.blocked_tasks), 1)
        self.assertEqual(node.queuelen, 1)
        self.assertEqual(node.channel_pool.num_occupied_channels, 0) # Physically empty

        # ACTION: Unblock the Wall
        wall.end_action() 

        # EXPECTATION:
        # 1. "Item_Active" moves to Wall.
        # 2. "Item_Queued" moves from Queue to ChannelPool (Refill).
        
        self.assertEqual(len(node.blocked_tasks), 0, "Node should be unblocked")
        self.assertEqual(node.queuelen, 0, "Queue should be empty (moved to process)")
        self.assertEqual(node.channel_pool.num_occupied_channels, 1, "Server should be busy with new item")
        self.assertEqual(node.state, NodeState.BUSY)
        
        print("\n✓ Scenario 1 (Refill): Queue correctly refills server upon unblocking.")

    # =========================================================================
    # SCENARIO 2: Multi-Channel Saturation
    # =========================================================================
    def test_multi_channel_saturation_logic(self):
        """
        Verify specific behavior of a 2-channel node:
        1. Partial Block (1 blocked, 1 free) -> Accepts new items.
        2. Full Block (2 blocked) -> Rejects new items.
        """
        node = self.create_node("Dual", queue_size=0, channels=2, delay=0.1)
        wall = self.create_node("Wall", queue_size=0, channels=1, delay=999.0)
        wall.start_action(TestItem("Blocker"))
        node.set_next_node(wall)

        # 1. Block first channel
        node.start_action(TestItem("Item1"))
        node.end_action() 
        self.assertEqual(node.state, NodeState.BLOCKED)
        
        # 2. Use second channel (Should work despite Blocked state)
        self.assertTrue(node.can_accept_item())
        node.start_action(TestItem("Item2"))
        self.assertEqual(node.channel_pool.num_occupied_channels, 1)

        # 3. Block second channel
        node.end_action()
        self.assertFalse(node.can_accept_item())
        
        # 4. Try third item -> REJECT
        node.start_action(TestItem("Item3"))
        self.assertEqual(node.metrics.num_failures, 1)

        print("\n✓ Scenario 2 (Multi-Channel): Partial and Full blocking handled correctly.")

    # =========================================================================
    # SCENARIO 3: The "Zipper" Merge (Race Condition)
    # =========================================================================
    def test_zipper_merge_contention(self):
        """
        Two nodes (A, B) feeding one Sink (C).
        C opens 1 spot. Only ONE of A or B should unblock.
        """
        c = self.create_node("C", queue_size=0, channels=1, delay=0.1)
        c.start_action(TestItem("Blocker")) # C is full

        a = self.create_node("A", queue_size=0, channels=1, delay=0.0)
        b = self.create_node("B", queue_size=0, channels=1, delay=0.0)
        a.set_next_node(c)
        b.set_next_node(c)

        # Block A and B
        a.start_action(TestItem("ItemA"))
        a.end_action()
        b.start_action(TestItem("ItemB"))
        b.end_action()

        # Action: C frees up
        c.end_action()

        # Result: C has 1 item. A+B have 1 item total.
        total_blocked = len(a.blocked_tasks) + len(b.blocked_tasks)
        self.assertEqual(total_blocked, 1, "Exactly one node should remain blocked")
        self.assertEqual(c.channel_pool.num_occupied_channels, 1, "C should be busy")

        print("\n✓ Scenario 3 (Zipper): Race condition resolved atomically.")

    # =========================================================================
    # SCENARIO 4: Zero-Buffer Pipeline (Strict Blocking)
    # =========================================================================
    def test_zero_buffer_chain_reaction(self):
        """
        A -> B -> C -> D. All Queue(0).
        D unblocks -> C unblocks -> B unblocks -> A unblocks.
        Instant propagation.
        """
        nodes = [self.create_node(n, 0, 1, 0.1) for n in ["A", "B", "C", "D"]]
        for i in range(3):
            nodes[i].set_next_node(nodes[i+1])

        items = [TestItem(f"Item_{n}") for n in ["A", "B", "C", "D"]]
        
        # FIX: Fill from Back to Front (D -> A)
        # We must ensure the downstream node is full BEFORE the upstream node finishes.
        
        # 1. Fill D (The End of the Line)
        nodes[3].start_action(items[3])
        
        # 2. Fill C. Finish C. C tries to push to D (Full) -> C Blocks.
        nodes[2].start_action(items[2])
        nodes[2].end_action()
        
        # 3. Fill B. Finish B. B tries to push to C (Blocked/Full) -> B Blocks.
        nodes[1].start_action(items[1])
        nodes[1].end_action()
        
        # 4. Fill A. Finish A. A tries to push to B (Blocked/Full) -> A Blocks.
        nodes[0].start_action(items[0])
        nodes[0].end_action()

        # Verify Chain Block
        self.assertEqual(len(nodes[0].blocked_tasks), 1, "A blocked")
        self.assertEqual(len(nodes[1].blocked_tasks), 1, "B blocked")
        self.assertEqual(len(nodes[2].blocked_tasks), 1, "C blocked")
        self.assertEqual(len(nodes[3].blocked_tasks), 0, "D active")

        # ACTION: D finishes
        nodes[3].end_action() 

        # RESULT: D is free (IDLE). C item moves to D. B item moves to C. A item moves to B. A is IDLE.
        self.assertEqual(len(nodes[0].blocked_tasks), 0, "A should unblock")
        self.assertEqual(nodes[0].state, NodeState.IDLE, "A should be empty")
        
        # Check propagation content
        # B should have Item_A
        self.assertEqual(next(iter(nodes[1].current_items)).name, "Item_A")
        # D should have Item_C
        self.assertEqual(next(iter(nodes[3].current_items)).name, "Item_C")

        print("\n✓ Scenario 4 (Zero-Buffer): Instant chain unblocking verified.")

    # =========================================================================
    # SCENARIO 5: Deadlock Stability (Circular)
    # =========================================================================
    def test_circular_deadlock_stability(self):
        """
        A -> B -> A. Both full.
        System should settle into BLOCKED state without crashing (recursion depth).
        """
        a = self.create_node("A", 0, 1, 1.0)
        b = self.create_node("B", 0, 1, 1.0)
        a.set_next_node(b)
        b.set_next_node(a)

        a.start_action(TestItem("ItemA"))
        b.start_action(TestItem("ItemB"))
        
        # Force block
        a.end_action() # A blocked on B
        b.end_action() # B blocked on A

        self.assertEqual(a.state, NodeState.BLOCKED)
        self.assertEqual(b.state, NodeState.BLOCKED)

        # Manually inject a "God Event": Clear B's blockage
        # This triggers the notification loop: B -> A -> B ...
        b.blocked_tasks.clear()
        b._notify_blocked_predecessors() 

        # A should have pushed to B
        self.assertEqual(len(a.blocked_tasks), 0)
        self.assertEqual(b.num_tasks, 1) # B has ItemA
        
        print("\n✓ Scenario 5 (Deadlock): Circular notification handled safely.")

    # =========================================================================
    # SCENARIO 6: Ordering (FIFO)
    # =========================================================================
    def test_fifo_ordering_blocked_items(self):
        """
        Node A (2 channels) blocks 2 items (Item1, Item2).
        Next node opens 1 spot.
        Item1 MUST go first.
        """
        a = self.create_node("A", 0, 2, 0.1)
        wall = self.create_node("Wall", 0, 1, 999)
        wall.start_action(TestItem("Blocker"))
        a.set_next_node(wall)

        # 1. Block Item 1
        a.start_action(TestItem("Item1"))
        a.end_action()
        
        # 2. Block Item 2
        a.start_action(TestItem("Item2"))
        a.end_action()

        # Action: Wall opens
        wall.end_action()

        # Expect: Wall has Item1. A still has Item2 blocked.
        wall_item = next(iter(wall.current_items))
        self.assertEqual(wall_item.name, "Item1")
        self.assertEqual(a.blocked_tasks[0].item.name, "Item2")

        print("\n✓ Scenario 6 (FIFO): Blocked items unblocked in arrival order.")

if __name__ == '__main__':
    unittest.main()