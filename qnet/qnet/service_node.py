"""
QueueingNode: a node with a waiting queue and a pool of service channels (ChannelPool).
"""

import itertools
from dataclasses import dataclass, field
from logging import DEBUG
from typing import Iterator, Iterable, Optional, Generic, ClassVar, TypeVar, Any, Callable

from .core_models import INF_TIME, TIME_EPS, I, T, SupportsDict, BoundedCollection, MinHeap, ActionRecord, ActionType
from .simulation_node import Node, NodeMetrics, NodeState
from .helpers import filter_none

QM = TypeVar("QM", bound="QueueingMetrics")

# BlockingPredicate: B(S,t) function that returns True if blocking should occur
# S = state (can access 'node' and 'next_node' via closure)
# t = current_time
BlockingPredicate = Callable[[], bool]


@dataclass(eq=False)
class QueueingMetrics(NodeMetrics):
    """
    Standard queueing metrics with proper blocking tracking.
    """
    total_wait_time: float = field(init=False, default=0)
    load_time_per_channel: dict[int, float] = field(init=False, default_factory=dict)
    in_time: float = field(init=False, default=0)
    out_time: float = field(init=False, default=0)
    in_intervals_sum: float = field(init=False, default=0)
    out_intervals_sum: float = field(init=False, default=0)
    num_failures: int = field(init=False, default=0)
    blocked_time: float = field(init=False, default=0)
    num_blocks: int = field(init=False, default=0)
    max_blocked_tasks: int = field(init=False, default=0)  # NEW: peak blocked queue

    @property
    def mean_in_interval(self) -> float:
        return self.in_intervals_sum / max(self.num_in - 1, 1)

    @property
    def mean_out_interval(self) -> float:
        return self.out_intervals_sum / max(self.num_out - 1, 1)

    @property
    def mean_queuelen(self) -> float:
        return self.total_wait_time / max(self.passed_time, TIME_EPS)

    @property
    def mean_load_per_channel(self) -> dict[int, float]:
        return {
            ch: load / max(self.passed_time, TIME_EPS)
            for ch, load in self.load_time_per_channel.items()
        }

    @property
    def mean_channels_load(self) -> float:
        return sum(self.mean_load_per_channel.values())

    @property
    def failure_proba(self) -> float:
        return self.num_failures / max(self.num_in, 1)

    @property
    def mean_wait_time(self) -> float:
        return self.total_wait_time / max(self.num_out, 1)

    @property
    def mean_load_time_per_channel(self) -> dict[int, float]:
        return {
            ch: load / max(self.num_out, 1)
            for ch, load in self.load_time_per_channel.items()
        }

    @property
    def mean_blocked_time(self) -> float:
        """Average time blocked per blocking event."""
        return self.blocked_time / max(self.num_blocks, 1)

    @property
    def blocking_proba(self) -> float:
        """Probability that a service completion is blocked by downstream."""
        return self.num_blocks / max(self.num_out, 1)

    @property
    def mean_load_time(self) -> float:
        return sum(self.mean_load_time_per_channel.values())


# Helper functions for common blocking predicates
def blocking_on_capacity(node: "QueueingNode") -> BlockingPredicate:
    """
    B(S,t) = (downstream queue + channels at max capacity)
    Block if downstream cannot accept more items.
    """
    return lambda: not node.next_node.can_accept_item() if node.next_node else False


def blocking_on_load_threshold(node: "QueueingNode", threshold: float) -> BlockingPredicate:
    """
    B(S,t) = (downstream load > threshold)
    Block if downstream server utilization exceeds threshold (0.0 to 1.0).
    """
    def predicate() -> bool:
        if node.next_node is None or not hasattr(node.next_node, 'metrics'):
            return False
        metrics = node.next_node.metrics
        if hasattr(metrics, 'mean_channels_load'):
            return metrics.mean_channels_load > threshold
        return False
    return predicate


def blocking_on_queue_length(node: "QueueingNode", max_queue: int) -> BlockingPredicate:
    """
    B(S,t) = (downstream queue length >= max_queue)
    Block if downstream queue reaches or exceeds specified length.
    """
    def predicate() -> bool:
        if node.next_node is None or not hasattr(node.next_node, 'queuelen'):
            return False
        return node.next_node.queuelen >= max_queue
    return predicate


def blocking_on_time_window(node: "QueueingNode", start_time: float, end_time: float) -> BlockingPredicate:
    """
    B(S,t) = (current_time in [start_time, end_time])
    Block during a specific time window (e.g., maintenance period).
    """
    def predicate() -> bool:
        return start_time <= node.current_time <= end_time
    return predicate


@dataclass(order=True, unsafe_hash=True)
class Task(SupportsDict, Generic[T]):
    """
    A single service task assigned to some channel, with a predicted finish time next_time.
    """
    id_gen: ClassVar[Iterator[int]] = itertools.count()

    id: int = field(init=False, repr=False, compare=False)
    item: T = field(compare=False)
    next_time: float
    blocked_start_time: Optional[float] = field(default=None, compare=False)

    def __post_init__(self) -> None:
        self.id = next(self.id_gen)

    def to_dict(self) -> dict[str, Any]:
        return {
            "item": self.item,
            "next_time": self.next_time,
            "blocked_start_time": self.blocked_start_time
        }


@dataclass(eq=False)
class Channel(SupportsDict, Generic[T]):
    """
    Represents a single service channel (like a server).
    """
    id: int

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id}


class ChannelPool(SupportsDict, Generic[T]):
    """
    Maintains a pool of channels, possibly limited by max_channels.
    Tasks are stored in a MinHeap, keyed by their next_time (finish time).
    """

    def __init__(self, max_channels: Optional[int] = None) -> None:
        self.max_channels = max_channels
        self.tasks = MinHeap[Task[T]](maxlen=max_channels)
        self.task_to_channel: dict[Task[T], Channel[T]] = {}
        self.current_id: int = 0
        self.free_channels = {Channel[T](self.current_id)}
        self.occupied_channels: set[Channel[T]] = set()

    @property
    def num_active_tasks(self) -> int:
        return len(self.tasks)

    @property
    def num_occupied_channels(self) -> int:
        return len(self.occupied_channels)

    @property
    def is_occupied(self) -> bool:
        """
        True if the pool is at full capacity (no free channels).
        """
        return self.max_channels is not None and self.num_occupied_channels == self.max_channels

    @property
    def is_empty(self) -> bool:
        return self.num_occupied_channels == 0

    @property
    def next_finish_time(self) -> float:
        """
        Return the earliest finishing time among active tasks.
        If there are no tasks, return INF_TIME.
        """
        nxt_task = self.tasks.min
        return INF_TIME if nxt_task is None else nxt_task.next_time

    def clear(self) -> None:
        self.tasks.clear()
        self.task_to_channel.clear()
        self.current_id = 0
        self.free_channels = {Channel[T](self.current_id)}
        self.occupied_channels.clear()

    def add_task(self, task: Task[T]) -> None:
        """
        Occupy one channel (if available) to handle 'task'.
        """
        ch = self._occupy_channel()
        self.tasks.push(task)
        self.task_to_channel[task] = ch

    def pop_finished_task(self) -> Task[T]:
        """
        Removes and returns the task in the channel pool with the smallest next_time.
        (Earliest finishing task).
        """
        task = self.tasks.pop()
        channel = self.task_to_channel.pop(task)
        self._free_channel(channel)
        return task

    def to_dict(self) -> dict[str, Any]:
        return {
            "max_channels": self.max_channels,
            "tasks": self.tasks,
            "current_id": self.current_id,
            "free_channels": self.free_channels,
            "occupied_channels": self.occupied_channels,
        }

    def _occupy_channel(self) -> Channel[T]:
        ch = self.free_channels.pop()
        # If free_channels is now empty, attempt to add another channel ID if possible
        if not self.free_channels and (self.max_channels is None or self.current_id + 1 < self.max_channels):
            self.current_id += 1
            self.free_channels.add(Channel[T](self.current_id))
        self.occupied_channels.add(ch)
        return ch

    def _free_channel(self, channel: Channel[T]) -> None:
        self.free_channels.add(channel)
        self.occupied_channels.remove(channel)


class QueueingNode(Node[I, QM]):
    """
    Node with a waiting queue (queue) and a ChannelPool (channel_pool).
    
    - Per-task blocking duration tracking
    - Correct state transitions
    - Integrated blocking predicates
    - Active unblocking notifications
    """

    def __init__(
        self,
        queue: BoundedCollection[I],
        channel_pool: ChannelPool[I],
        blocking_predicate: Optional[BlockingPredicate] = None,
        **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self.queue = queue
        self.channel_pool = channel_pool
        self.next_time = INF_TIME
        self.blocked_tasks: list[Task[I]] = []
        
        # Blocking predicate B(S,t): returns True if blocking should occur
        # If None (default), blocking uses default logic (next_node.can_accept_item())
        # If specified, blocking is determined by the custom predicate
        self.blocking_predicate = blocking_predicate

    @property
    def current_items(self) -> Iterable[I]:
        """
        Combines items in the waiting queue + items currently served in the channel pool.
        """
        return itertools.chain(self.queue.data, (tsk.item for tsk in self.channel_pool.tasks.data))

    @property
    def num_tasks(self) -> int:
        return self.channel_pool.num_active_tasks

    @property
    def queuelen(self) -> int:
        return len(self.queue)

    def can_accept_item(self) -> bool:
        """
        Check if this node has capacity to accept a new item.
        Returns False if both channels are occupied AND queue is full.
        """
        # Count blocked tasks as occupying server capacity
        effective_busy_channels = self.channel_pool.num_occupied_channels + len(self.blocked_tasks)
        channels_full = False
        
        if self.channel_pool.max_channels is not None:
            if effective_busy_channels >= self.channel_pool.max_channels:
                channels_full = True
        
        if channels_full:
            return not self.queue.is_full
        else:
            return True

    def start_action(self, item: I) -> None:
        super().start_action(item)
        
        # Calculate effective occupancy (Active Processing + Blocked Items)
        effective_busy_channels = self.channel_pool.num_occupied_channels + len(self.blocked_tasks)
        
        channels_full = False
        if self.channel_pool.max_channels is not None:
            if effective_busy_channels >= self.channel_pool.max_channels:
                channels_full = True

        # If channels are full (physically or blocked), attempt to queue
        if channels_full:
            if self.queue.is_full:
                self._failure_hook()
            else:
                self.queue.push(item)
                # Item is queued. 
                # IMPORTANT: If we are blocked, we stay BLOCKED. 
                # If we were BUSY, we stay BUSY.
        else:
            # Directly occupy a channel
            task = Task[I](
                item=item,
                next_time=self._predict_item_time(item=item)
            )
            self.add_task(task)  # add_task sets state to BUSY

    def end_action(self) -> I:
        """
        Proper blocking logic with per-task tracking.
        
        1. Pop finished task from channel pool
        2. Determine if blocking should occur (BEFORE state changes)
        3. Process next item from queue if available
        4. Either send item or enter BLOCKED state
        5. Notify blocked predecessors if we freed space
        """
        if DEBUG:
            self._validate_blocking_invariants()
        
        # Step 1: Pop the finished item from the channel pool
        finished_item = self.channel_pool.pop_finished_task().item
        
        # Step 2: Determine if blocking should occur (BEFORE changing state)
        will_be_blocked = self._should_block()
        
        # Step 3: Handle the finished item (Place it or Send it)
        if will_be_blocked:
            # === BLOCKING PATH ===
            # The item stays in the node, conceptually occupying a channel
            task = Task[I](
                item=finished_item, 
                next_time=self.current_time,
                blocked_start_time=self.current_time
            )
            self.blocked_tasks.append(task)
            self.metrics.num_blocks += 1
            
            if len(self.blocked_tasks) > self.metrics.max_blocked_tasks:
                self.metrics.max_blocked_tasks = len(self.blocked_tasks)
            
            self.state = NodeState.BLOCKED
            self.next_node.blocked_predecessors.add(self)
                
            # Note: We do NOT return immediately. We might still have a 2nd free channel!
        else:
            # === NORMAL PATH ===
            # The item leaves. Capacity is truly freed.
            # We temporarily set IDLE if empty; add_task will set back to BUSY if we refill.
            if self.channel_pool.num_active_tasks == 0 and not self.blocked_tasks:
                self.state = NodeState.IDLE
                
            self._end_action(finished_item)
            
            # Since we successfully moved an item, we might have unblocked ourselves
            # or created space. Notify predecessors!
            self.try_unblock() # (This handles its own notifications)
        
        # Step 4: Refill from Queue
        # We only pull from queue if we have REAL capacity.
        # Capacity = Occupied Channels + Blocked Tasks
        effective_occupancy = self.channel_pool.num_occupied_channels + len(self.blocked_tasks)
        
        can_refill = True
        if self.channel_pool.max_channels is not None:
            if effective_occupancy >= self.channel_pool.max_channels:
                can_refill = False
        
        if can_refill and not self.queue.is_empty:
            nxt_item = self.queue.pop()
            new_task = Task[I](
                item=nxt_item,
                next_time=self._predict_item_time(item=nxt_item)
            )
            self.add_task(new_task) # Sets state to BUSY (preserves BLOCKED if mixed)

        # Ensure our next_time reflects current channel pool state
        self.next_time = self._predict_next_time()

        return finished_item

    def reset(self) -> None:
        super().reset()
        self.next_time = INF_TIME
        self.queue.clear()
        self.channel_pool.clear()
        self.blocked_tasks.clear()
        self.state = NodeState.IDLE

    def try_unblock(self) -> None:
        """
        Pull-based unblocking with per-task duration tracking.
        
        Attempts to send blocked tasks to next_node if space becomes available.
        Properly tracks blocking duration for each task individually.
        """
        if not self.blocked_tasks:
            self._notify_blocked_predecessors()
            return
        
        if self.next_node is None:
            print(f"WARNING: {self.name} has {len(self.blocked_tasks)} blocked tasks but is terminal. Clearing.")
            self.blocked_tasks.clear()
            self._notify_blocked_predecessors()
            return
        
        # Try to unblock as many tasks as possible
        did_unblock = False
        
        while self.blocked_tasks and self.next_node.can_accept_item():
            task = self.blocked_tasks.pop(0)
            item = task.item
            
            # Metric tracking
            if task.blocked_start_time is not None:
                block_duration = self.current_time - task.blocked_start_time
                self.metrics.blocked_time += block_duration
            
            # Send the item
            self._end_action(item)
            did_unblock = True
            
            # We must immediately try to fill it from our OWN queue if possible.
            if not self.queue.is_empty:
                # Move item from Queue -> ChannelPool
                next_item = self.queue.pop()
                # We need to wrap it in a task and schedule it
                new_task = Task[I](
                    item=next_item,
                    next_time=self._predict_item_time(item=next_item)
                )
                self.add_task(new_task) # This sets BUSY state if not BLOCKED
            
            # Update state after each unblock
            if did_unblock:
                if not self.blocked_tasks:
                    self.next_node.blocked_predecessors.discard(self)
                    # If we cleared all blocked tasks, we revert to BUSY (if working) or IDLE
                    if self.channel_pool.num_active_tasks > 0:
                        self.state = NodeState.BUSY
                    elif self.queue.is_empty:
                        self.state = NodeState.IDLE
                else:
                    # We still have blocked tasks, remain BLOCKED
                    self.state = NodeState.BLOCKED
                    
        self._notify_blocked_predecessors()

    def _notify_blocked_predecessors(self) -> None:
        """
        Actively notify blocked predecessors that space is available.
        This ensures cascading unblocks in multi-stage networks.
        """
        if not self.can_accept_item():
            return  # No space available
        
        # Notify all blocked predecessors
        for blocked_pred in list(self.blocked_predecessors):
            if hasattr(blocked_pred, 'try_unblock'):
                blocked_pred.try_unblock()
                # If we're full again after one unblock, stop notifying
                if not self.can_accept_item():
                    break
                
    # Call in end_action() when DEBUG enabled:
    # if DEBUG:
    #     self._validate_blocking_invariants()
    def _validate_blocking_invariants(self) -> None:
        """
        Validate blocking state invariants (call in DEBUG mode).
        
        Invariants:
        1. BLOCKED state ↔ blocked_tasks not empty
        2. blocked_tasks not empty → next_node must exist
        3. IDLE state → no active tasks, no blocked tasks
        4. blocked_predecessors must all be in BLOCKED state
        """
        # Invariant 1: BLOCKED ↔ has blocked tasks
        if self.state == NodeState.BLOCKED:
            assert len(self.blocked_tasks) > 0, \
                f"{self.name}: BLOCKED state but no blocked_tasks"
        
        if len(self.blocked_tasks) > 0:
            assert self.state == NodeState.BLOCKED, \
                f"{self.name}: Has {len(self.blocked_tasks)} blocked_tasks but state is {self.state}"
        
        # Invariant 2: blocked tasks require next_node
        if len(self.blocked_tasks) > 0:
            assert self.next_node is not None, \
                f"{self.name}: Has blocked_tasks but no next_node"
        
        # Invariant 3: IDLE means nothing happening
        if self.state == NodeState.IDLE:
            assert self.channel_pool.num_active_tasks == 0, \
                f"{self.name}: IDLE but has {self.channel_pool.num_active_tasks} active tasks"
            assert len(self.blocked_tasks) == 0, \
                f"{self.name}: IDLE but has {len(self.blocked_tasks)} blocked tasks"
        
        # Invariant 4: All blocked predecessors should have blocked tasks
        for pred in self.blocked_predecessors:
            if hasattr(pred, 'blocked_tasks'):
                assert len(pred.blocked_tasks) > 0, \
                    f"{pred.name}: In {self.name}.blocked_predecessors but has no blocked_tasks"

    def _should_block(self) -> bool:
        """
        Centralized blocking decision logic.
        
        Returns True if the finished item should be blocked (not sent immediately).
        Uses custom blocking_predicate if provided, otherwise uses default logic.
        """
        if self.next_node is None:
            return False  # No next node = no blocking
        
        if self.blocking_predicate is not None:
            # Use custom predicate
            return self.blocking_predicate()
        else:
            # Default: block if next_node cannot accept
            return not self.next_node.can_accept_item()

    def add_task(self, task: Task[I]) -> None:
        """
        Add a newly created task to the channel pool and recalculate next_time.
        Sets node state to BUSY since we now have active tasks.
        """
        self._before_add_task_hook(task)
        self.channel_pool.add_task(task)
        self.next_time = self._predict_next_time()
        
        if self.state != NodeState.BLOCKED:
            self.state = NodeState.BUSY

    def to_dict(self) -> dict[str, Any]:
        node_dict = super().to_dict()
        node_dict.update({
            "channel_pool": self.channel_pool,
            "queue": self.queue,
            "num_failures": self.metrics.num_failures,
            "blocked_tasks": len(self.blocked_tasks),  # NEW
            "state": self.state.value  # NEW
        })
        return node_dict

    def _predict_item_time(self, **kwargs: Any) -> float:
        return self.current_time + self._get_delay(**kwargs)

    def _predict_next_time(self, **_: Any) -> float:
        return self.channel_pool.next_finish_time

    def _before_time_update_hook(self, time: float) -> None:
        super()._before_time_update_hook(time)
        dtime = time - self.current_time
        # Add load time to each occupied channel
        for ch in self.channel_pool.occupied_channels:
            self.metrics.load_time_per_channel[ch.id] = (
                self.metrics.load_time_per_channel.get(ch.id, 0) + dtime
            )
        # Accumulate total waiting time
        self.metrics.total_wait_time += self.queuelen * dtime

    def _item_out_hook(self, item: I) -> None:
        super()._item_out_hook(item)
        # For out interval
        if self.metrics.num_out > 1:
            self.metrics.out_intervals_sum += self.current_time - self.metrics.out_time
        self.metrics.out_time = self.current_time

    def _item_in_hook(self, item: I) -> None:
        super()._item_in_hook(item)
        # For in interval
        if self.metrics.num_in > 1:
            self.metrics.in_intervals_sum += self.current_time - self.metrics.in_time
        self.metrics.in_time = self.current_time

    def _before_add_task_hook(self, _: Task[I]) -> None:
        """
        Override to add custom logic before pushing the task to the channel pool.
        """
        pass

    def _failure_hook(self) -> None:
        """
        Called when an arriving item cannot be queued because both the channel pool
        and the queue are full (i.e., no capacity).
        """
        self.metrics.num_failures += 1