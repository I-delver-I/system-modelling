"""
Executes a bank simulation with two checkouts, using QNet library components.
"""

import random
from functools import partial

from src.bank import BankQueueingNode, BankQueueingMetrics, BankTransitionNode
from qnet.core_models import Item, Queue
from qnet.service_node import QueueingMetrics, Task, ChannelPool
from qnet.item_generator import FactoryNode
from qnet.results_logger import CLILogger
from qnet.simulation_engine import Model, ModelMetrics, Nodes, Evaluation, Verbosity


def run_simulation() -> None:
    """
    Main entry point to run the bank queue simulation.
    """
    # Factory node: incoming cars
    incoming_cars = FactoryNode(
        name="1_incoming_cars",
        metrics=QueueingMetrics(),
        delay_fn=partial(random.expovariate, lambd=1.0 / 0.5),
    )

    # Transition node that decides which checkout the car goes to
    transition = BankTransitionNode[Item, QueueingMetrics](
        name="2_first_vs_second",
        metrics=QueueingMetrics()
    )

    # Two single-channel checkouts with queue capacity of 3 each
    checkout1 = BankQueueingNode[Item](
        name="3_first_checkout",
        min_queuelen_diff=2,
        queue=Queue(maxlen=3),
        metrics=BankQueueingMetrics(),
        channel_pool=ChannelPool[Item](max_channels=1),
        delay_fn=partial(random.expovariate, lambd=1.0 / 0.3),
    )

    checkout2 = BankQueueingNode[Item](
        name="4_second_checkout",
        min_queuelen_diff=2,
        queue=Queue(maxlen=3),
        metrics=BankQueueingMetrics(),
        channel_pool=ChannelPool[Item](max_channels=1),
        delay_fn=partial(random.expovariate, lambd=1.0 / 0.3),
    )

    # Connect the nodes
    incoming_cars.set_next_node(transition)
    transition.set_next_nodes(first=checkout1, second=checkout2)
    checkout1.set_neighbor(checkout2)

    # Initial conditions:
    # - Both checkouts are initially busy with a normal-distributed finishing time
    checkout1.add_task(
        Task[Item](
            item=Item(id=incoming_cars.next_id, created_time=0.0),
            next_time=random.normalvariate(mu=1.0, sigma=0.3),
        )
    )
    checkout2.add_task(
        Task[Item](
            item=Item(id=incoming_cars.next_id, created_time=0.0),
            next_time=random.normalvariate(mu=1.0, sigma=0.3),
        )
    )

    # - Each queue initially has 2 waiting items
    for _ in range(2):
        checkout1.queue.push(Item(id=incoming_cars.next_id, created_time=0.0))
    for _ in range(2):
        checkout2.queue.push(Item(id=incoming_cars.next_id, created_time=0.0))

    # - Next car arrival is scheduled at t = 0.1
    incoming_cars.next_time = 0.1

    # Custom evaluation methods
    def total_failure_proba(_: Model[Item, ModelMetrics]) -> float:
        m1 = checkout1.metrics
        m2 = checkout2.metrics
        total_in = m1.num_in + m2.num_in
        total_fail = m1.num_failures + m2.num_failures
        return total_fail / max(total_in, 1)

    def num_switched_checkout(_: Model[Item, ModelMetrics]) -> int:
        return checkout1.metrics.num_from_neighbor + checkout2.metrics.num_from_neighbor

    def mean_cars_in_bank(_: Model[Item, ModelMetrics]) -> float:
        m1 = checkout1.metrics
        m2 = checkout2.metrics
        return (
            m1.mean_channels_load + m1.mean_queuelen +
            m2.mean_channels_load + m2.mean_queuelen
        )

    # Build the model
    model = Model(
        nodes=Nodes[Item].from_node_tree_root(incoming_cars),
        evaluations=[
            Evaluation[float](name="total_failure_proba", evaluate=total_failure_proba),
            Evaluation[float](name="mean_cars_in_bank", evaluate=mean_cars_in_bank),
            Evaluation[int](name="num_switched_checkout", evaluate=num_switched_checkout),
        ],
        metrics=ModelMetrics[Item](),
        logger=CLILogger[Item]()
    )

    # Run the simulation until time=10000
    model.simulate(end_time=10000, verbosity=Verbosity.METRICS)


if __name__ == "__main__":
    run_simulation()
