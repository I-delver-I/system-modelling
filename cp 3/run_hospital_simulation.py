"""
Executes a hospital simulation with 3 patient types and multiple steps:
 registration, potential lab testing, and final chamber assignment.
"""

import random
from functools import partial

from src.hospital import (
    HospitalItem,
    SickType,
    HospitalFactoryNode,
    HospitalModelMetrics,
    TestingTransitionNode,
    EmergencyTransitionNode
)


from qnet.core_models import PriorityQueue, Queue
from qnet.simulation_node import NodeMetrics
from qnet.probability_distributions import erlang
from qnet.service_node import QueueingNode, QueueingMetrics, ChannelPool
from qnet.results_logger import CLILogger
from qnet.simulation_engine import Model, Nodes
from qnet.simulation_engine import Model, ModelMetrics, Nodes, Evaluation, Verbosity


def _priority_fn(item: HospitalItem) -> int:
    """
    Priority function that ensures patients of type 1 (or flagged as_first_sick) have higher priority
    by returning 0 for them, and 1 for the others. Actually, we invert the logic here: 
    'return 1' for type != FIRST, 0 otherwise, so the queue is effectively a priority queue that
    picks smaller numbers first.
    """
    return int(item.sick_type != SickType.FIRST and not item.as_first_sick)

def average_emergency_queue(model: Model[HospitalItem, HospitalModelMetrics]) -> float:
        return model.nodes["2_at_emergency"].metrics.mean_queuelen

def run_simulation() -> None:
    """
    Main entry point to run the hospital patient-flow simulation.
    """

    # Patient type probabilities
    sick_type_probas = {
        SickType.FIRST: 0.5,
        SickType.SECOND: 0.1,
        SickType.THIRD: 0.4
    }

    # Factory node: incoming sick people
    incoming_sick_people = HospitalFactoryNode[NodeMetrics](
        name="1_sick_people",
        probas=sick_type_probas,
        metrics=NodeMetrics(),
        delay_fn=partial(random.expovariate, lambd=1.0 / 15)
    )

    # Average time of registration depending on SickType
    at_emergency_mean = {
        SickType.FIRST: 15,
        SickType.SECOND: 40,
        SickType.THIRD: 30
    }

    # At emergency: 2 channels, priority queue (those who completed prior exam first)
    at_emergency = QueueingNode[HospitalItem, QueueingMetrics](
        name="2_at_emergency",
        queue=PriorityQueue[HospitalItem](priority_fn=_priority_fn, fifo=True),
        metrics=QueueingMetrics(),
        channel_pool=ChannelPool(max_channels=2),
        delay_fn=lambda item: random.expovariate(
            lambd=1.0 / at_emergency_mean[item.sick_type]
        ),
    )

    # Decide if patient goes to chamber or reception
    emergency_transition = EmergencyTransitionNode[NodeMetrics](
        name="3_chamber_vs_reception", 
        metrics=NodeMetrics()
    )

    # Path to chamber requires 3 possible "guides"
    to_chumber = QueueingNode[HospitalItem, QueueingMetrics](
        name="4_to_chumber",
        queue=Queue[HospitalItem](),
        metrics=QueueingMetrics(),
        channel_pool=ChannelPool(max_channels=3),
        delay_fn=partial(random.uniform, a=3, b=8)
    )

    # Path to reception, then testing
    to_reception = QueueingNode[HospitalItem, QueueingMetrics](
        name="5_to_reception",
        queue=Queue[HospitalItem](),
        metrics=QueueingMetrics(),
        channel_pool=ChannelPool(),  # unlimited channels
        delay_fn=partial(random.uniform, a=2, b=5)
    )

    at_reception = QueueingNode[HospitalItem, QueueingMetrics](
        name="6_at_reception",
        queue=Queue[HospitalItem](),
        metrics=QueueingMetrics(),
        channel_pool=ChannelPool(),  # unlimited
        delay_fn=partial(erlang, lambd=3 / 4.5, k=3)
    )

    on_testing = QueueingNode[HospitalItem, QueueingMetrics](
        name="7_on_testing",
        queue=Queue[HospitalItem](),
        metrics=QueueingMetrics(),
        channel_pool=ChannelPool(max_channels=2),
        delay_fn=partial(erlang, lambd=2 / 4, k=2)
    )

    testing_transition = TestingTransitionNode[NodeMetrics](
        name="8_after_testing",
        metrics=NodeMetrics()
    )

    # Linking
    incoming_sick_people.set_next_node(at_emergency)
    at_emergency.set_next_node(emergency_transition)
    emergency_transition.set_next_nodes(chumber=to_chumber, reception=to_reception)
    to_reception.set_next_node(at_reception)
    at_reception.set_next_node(on_testing)
    on_testing.set_next_node(testing_transition)

    # 20% chance that patients go back to emergency, 80% they leave the system
    testing_transition.add_next_node(at_emergency, proba=0.2)
    testing_transition.add_next_node(None, proba=testing_transition.rest_proba)

    # Build and run the model
    model = Model(
        nodes=Nodes[HospitalItem].from_node_tree_root(incoming_sick_people),
        evaluations=[
        Evaluation[float](name="Average Emergency Queue", evaluate=average_emergency_queue)
        ],
        logger=CLILogger[HospitalItem](),
        metrics=HospitalModelMetrics()
    )

    model.simulate(end_time=100000)


if __name__ == "__main__":
    run_simulation()
