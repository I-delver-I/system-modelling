import unittest
from collections import Counter
from qnet.routing_node import PriorityGroupTransitionNode
from qnet.simulation_node import NodeMetrics

# --- MOCKS ---
class MockNode:
    def __init__(self, name: str, is_full: bool = False):
        self.name = name
        self._is_full = is_full
    
    def can_accept_item(self) -> bool:
        return not self._is_full
    
    def __repr__(self):
        return f"MockNode({self.name})"

# --- ADVANCED TEST SUITE ---
class TestDeepPriorityRouting(unittest.TestCase):
    
    def setUp(self):
        self.router = PriorityGroupTransitionNode(
            name="DeepRouter", 
            metrics=NodeMetrics()
        )

    def test_priority_gaps(self):
        """
        TRICKY: Priorities are not sequential (1, 10, 100).
        Ensures the router correctly jumps over missing priority levels.
        """
        n1 = MockNode("Prio_1", is_full=True)
        n10 = MockNode("Prio_10", is_full=True)
        n100 = MockNode("Prio_100", is_full=False) # Free!

        self.router.add_next_node(n1, priority=1)
        self.router.add_next_node(n10, priority=10)
        self.router.add_next_node(n100, priority=100)

        selected = self.router._get_next_node(None)
        
        self.assertEqual(selected, n100, "Should skip gaps and find the first free node at Prio 100.")

    def test_load_balancing_distribution(self):
        """
        STATISTICAL: If Prio 1 has 3 free nodes, are they picked roughly equally?
        This catches bugs where we might always pick the first index [0].
        """
        nodes = [MockNode(f"N{i}") for i in range(3)]
        for n in nodes:
            self.router.add_next_node(n, priority=1)

        # Run 300 trials
        counts = Counter()
        for _ in range(300):
            selected = self.router._get_next_node(None)
            counts[selected.name] += 1

        # Check distribution
        for n in nodes:
            count = counts[n.name]
            self.assertGreater(count, 50, f"Node {n.name} was picked too rarely ({count}/300). Distribution is broken.")
        
        print(f"\n[OK] Distribution: {dict(counts)}")

    def test_strict_blocking_hierarchy(self):
        """
        CRITICAL: If ALL nodes are full, we MUST return a node from Priority 1.
        We must NEVER return a node from Priority 2, even if Priority 1 has only 1 node 
        and Priority 2 has 100 nodes.
        
        Why? Because we must queue for the *best* resource, not the *most abundant* backup.
        """
        prio1_node = MockNode("Gold_Server", is_full=True)
        
        self.router.add_next_node(prio1_node, priority=1)
        
        # Add 50 full backup nodes
        for i in range(50):
            self.router.add_next_node(MockNode(f"Silver_{i}", is_full=True), priority=2)

        # Run multiple times to ensure we never accidentally slip to Prio 2
        for _ in range(20):
            selected = self.router._get_next_node(None)
            self.assertEqual(selected, prio1_node, "Must block on Highest Priority, never on backups.")

    def test_dynamic_state_switching(self):
        """
        DYNAMIC: Simulate a simulation timeline where nodes fill up and empty out.
        """
        primary = MockNode("Primary")
        backup = MockNode("Backup")
        
        self.router.add_next_node(primary, priority=1)
        self.router.add_next_node(backup, priority=2)

        # T=0: Both Free -> Expect Primary
        primary._is_full = False
        backup._is_full = False
        self.assertEqual(self.router._get_next_node(None), primary)

        # T=1: Primary Full -> Expect Backup
        primary._is_full = True
        self.assertEqual(self.router._get_next_node(None), backup)

        # T=2: Both Full -> Expect Primary (Blocking)
        backup._is_full = True
        self.assertEqual(self.router._get_next_node(None), primary)

        # T=3: Primary Frees up -> Expect Primary
        primary._is_full = False
        self.assertEqual(self.router._get_next_node(None), primary)
        
        print("\n[OK] Dynamic: Correctly adapts to changing node states.")

    def test_empty_router(self):
        """
        EDGE CASE: Router with no destinations.
        """
        selected = self.router._get_next_node(None)
        self.assertIsNone(selected, "Empty router should return None.")

if __name__ == '__main__':
    unittest.main()