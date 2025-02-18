"""
QueueingNode: a node with a waiting queue and a pool of service channels (ChannelPool).
"""

import itertools
from dataclasses import dataclass, field
from typing import Iterator, Iterable, Optional, Generic, ClassVar, TypeVar, Any

from .core_models import INF_TIME, TIME_EPS, I, T, SupportsDict, BoundedCollection, MinHeap
from .simulation_node import Node, NodeMetrics
from .helpers import filter_none

QM = TypeVar("QM", bound="QueueingMetrics")


@dataclass(eq=False)
class QueueingMetrics(NodeMetrics):
    """
    Standard queueing metrics:
     - total_wait_time across all waiting items
     - per-channel load_time
     - intervals between arrivals/departures
     - number of times items were rejected (num_failures)
    """
    total_wait_time: float = field(init=False, default=0)
    load_time_per_channel: dict[int, float] = field(init=False, default_factory=dict)
    in_time: float = field(init=False, default=0)
    out_time: float = field(init=False, default=0)
    in_intervals_sum: float = field(init=False, default=0)
    out_intervals_sum: float = field(init=False, default=0)
    num_failures: int = field(init=False, default=0)

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
    def mean_load_time(self) -> float:
        return sum(self.mean_load_time_per_channel.values())


@dataclass(order=True, unsafe_hash=True)
class Task(SupportsDict, Generic[T]):
    """
    A single service task assigned to some channel, with a predicted finish time next_time.
    The 'order=True' ensures tasks are sorted by next_time for the min-heap usage.
    """
    id_gen: ClassVar[Iterator[int]] = itertools.count()

    id: int = field(init=False, repr=False, compare=False)
    item: T = field(compare=False)
    next_time: float

    def __post_init__(self) -> None:
        self.id = next(self.id_gen)

    def to_dict(self) -> dict[str, Any]:
        return {
            "item": self.item,
            "next_time": self.next_time
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
    If the pool is busy, items are queued if capacity allows. Otherwise, item is lost (failure).
    """

    def __init__(self, queue: BoundedCollection[I], channel_pool: ChannelPool[I], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.queue = queue
        self.channel_pool = channel_pool
        self.next_time = INF_TIME

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

    def start_action(self, item: I) -> None:
        super().start_action(item)
        # If channel pool is full, we attempt to queue
        if self.channel_pool.is_occupied:
            if self.queue.is_full:
                self._failure_hook()
            else:
                self.queue.push(item)
        else:
            # Directly occupy a channel
            task = Task[I](
                item=item,
                next_time=self._predict_item_time(item=item)
            )
            self.add_task(task)

    def end_action(self) -> I:
        # Pop the earliest finishing task from the channel pool
        item = self.channel_pool.pop_finished_task().item
        # If there's someone in the queue, serve them next
        if not self.queue.is_empty:
            nxt_item = self.queue.pop()
            new_task = Task[I](
                item=nxt_item,
                next_time=self._predict_item_time(item=nxt_item)
            )
            self.add_task(new_task)
        else:
            # Otherwise, no upcoming event from this node
            self.next_time = self._predict_next_time()
        return self._end_action(item)

    def reset(self) -> None:
        super().reset()
        self.next_time = INF_TIME
        self.queue.clear()
        self.channel_pool.clear()

    def add_task(self, task: Task[I]) -> None:
        """
        Add a newly created task to the channel pool and recalculate next_time.
        """
        self._before_add_task_hook(task)
        self.channel_pool.add_task(task)
        self.next_time = self._predict_next_time()

    def to_dict(self) -> dict[str, Any]:
        node_dict = super().to_dict()
        node_dict.update({
            "channel_pool": self.channel_pool,
            "queue": self.queue,
            "num_failures": self.metrics.num_failures
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
