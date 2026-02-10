from qnet.core_models import Queue
from qnet.service_node import QueueingNode, QueueingMetrics, ChannelPool
from qnet.item_generator import FactoryNode
from qnet.simulation_node import NodeMetrics
from qnet.simulation_engine import Model, Nodes, ModelMetrics, Verbosity
from qnet.results_logger import CLILogger

def main():
    print(">>> SETTING UP CASCADING BLOCKAGE SIMULATION <<<")
    
    # 1. Setup a chain: Factory -> A -> B -> C -> Exit
    # We make C very slow initially to cause a backup, then fast to clear it.
    
    # Delays
    fast_delay = lambda: 0.1
    slow_delay = lambda: 10.0
    
    # Nodes with Capacity 1 (Queue 0, Channel 1) to force immediate blocking
    node_a = QueueingNode(Queue(0), ChannelPool(1), delay_fn=fast_delay, metrics=QueueingMetrics(), name="A")
    node_b = QueueingNode(Queue(0), ChannelPool(1), delay_fn=fast_delay, metrics=QueueingMetrics(), name="B")
    
    # Node C is the bottleneck
    node_c = QueueingNode(Queue(0), ChannelPool(1), delay_fn=slow_delay, metrics=QueueingMetrics(), name="C")
    
    # Factory pumps items fast
    factory = FactoryNode(delay_fn=fast_delay, metrics=NodeMetrics(), name="Source")
    
    factory.set_next_node(node_a)
    node_a.set_next_node(node_b)
    node_b.set_next_node(node_c)
    
    nodes = Nodes.from_node_tree_root(factory)
    model = Model(nodes, CLILogger(), ModelMetrics(), enable_unblock_safety_net=True)
    
    print("\n--- PHASE 1: FILLING THE PIPELINE (0.0 to 2.0s) ---")
    # This should fill C, block B, then block A.
    model.simulate(end_time=2.0, verbosity=Verbosity.NONE)
    
    print(f"State at 2.0s:")
    print(f"Node C (Bottleneck): {node_c.state.name} | Items: {node_c.num_tasks}")
    print(f"Node B: {node_b.state.name} | Blocked Tasks: {len(node_b.blocked_tasks)}")
    print(f"Node A: {node_a.state.name} | Blocked Tasks: {len(node_a.blocked_tasks)}")
    
    # Verify blockage
    if len(node_b.blocked_tasks) > 0 and len(node_a.blocked_tasks) > 0:
        print("✓ Pipeline is successfully backed up.")
    else:
        print("X Pipeline failed to back up.")

    print("\n--- PHASE 2: RELEASING THE BOTTLENECK (2.0 to 12.5s) ---")
    # Node C was set to 10.0s delay. It started around 0.1s. It should finish around 10.1s.
    # When C finishes, it should pull from B, which pulls from A.
    
    # We step carefully to catch the transition
    model.simulate(end_time=12.5, verbosity=Verbosity.NONE)
    
    print(f"State at 12.5s (After C finishes):")
    print(f"Node C: {node_c.state.name} | Processed: {node_c.metrics.num_out}")
    print(f"Node B: {node_b.state.name} | Blocked Tasks: {len(node_b.blocked_tasks)}")
    print(f"Node A: {node_a.state.name} | Blocked Tasks: {len(node_a.blocked_tasks)}")
    
    # Logic check
    # C should have finished 1 item.
    # B should have unblocked (blocked_tasks 0) and is now processing A's item.
    # A should have unblocked (blocked_tasks 0) and is now processing a new item from Source.
    
    if len(node_b.blocked_tasks) == 0 and len(node_a.blocked_tasks) == 0:
        print("✓ UNBLOCKING CHAIN SUCCESSFUL: A and B are no longer blocked.")
    else:
        print("X UNBLOCKING FAILURE: Nodes are still blocked.")

    print("\n--- METRICS CHECK ---")
    print(f"Total Unblock Cycles (Safety Net triggers): {model.metrics.num_unblock_cycles}")
    print(f"Node B Max Blocked Tasks: {node_b.metrics.max_blocked_tasks}")
    print(f"Node B Total Blocked Time: {node_b.metrics.blocked_time:.4f}s")

if __name__ == "__main__":
    main()