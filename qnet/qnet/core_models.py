"""
Common classes and interfaces used throughout the QNet library.
"""

import inspect
import itertools
import heapq
from collections import deque
from enum import Enum
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, fields, _MISSING_TYPE
from typing import (
    TypeVar, Generic, Optional, Iterable, Callable,
    Protocol, Union, Any, cast, runtime_checkable
)

INF_TIME = float("inf")
TIME_EPS = 1e-6

I = TypeVar("I", bound="Item")
M = TypeVar("M", bound="Metrics")
T = TypeVar("T")


@runtime_checkable
class SupportsDict(Protocol):
    """
    Protocol specifying that the implementing class can convert itself into a dictionary.
    """

    def to_dict(self) -> dict[str, Any]:
        ...


class ActionType(str, Enum):
    """
    Possible actions a node can perform on an item: item arrival (IN) or departure (OUT).
    """
    IN = "in"
    OUT = "out"


@dataclass(eq=False)
class ActionRecord(Generic[T]):
    """
    Records a single action (IN or OUT) on a node, along with the current simulation time.
    """
    node: T
    action_type: ActionType
    time: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "node": self.node,
            "action_type": self.action_type,
            "time": self.time
        }


@dataclass(eq=False)
class Item(SupportsDict):
    """
    Basic item class for queueing systems, storing creation time, current time, and a history of actions.
    """
    id: str
    created_time: float = field(repr=False)
    current_time: float = field(init=False, repr=False)
    processed: bool = field(init=False, repr=False, default=False)
    history: list[ActionRecord] = field(init=False, repr=False, default_factory=list)

    def __post_init__(self) -> None:
        self.current_time = self.created_time

    @property
    def released_time(self) -> Optional[float]:
        """
        Returns the time when this item is considered 'processed' (OUT from the last node).
        """
        return self.current_time if self.processed else None

    @property
    def time_in_system(self) -> float:
        """
        Total time the item has been in the system so far.
        """
        return self.current_time - self.created_time

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id}


@dataclass(eq=False)
class Metrics(Protocol):
    """
    A base protocol for any simulation metrics.
    Must define a `to_dict()` method and a `reset()` method.
    """
    passed_time: float = field(init=False, default=0)

    def to_dict(self) -> dict[str, Any]:
        metrics_dict = {
            name: getattr(self, name)
            for name, _ in inspect.getmembers(
                type(self),
                lambda val: isinstance(val, property) and val.fget is not None
            )
        }
        return metrics_dict

    def reset(self) -> None:
        """
        Resets all fields of a dataclass-based metrics object to default.
        """
        for param in fields(self):
            if not isinstance(param.default, _MISSING_TYPE):
                default_val = param.default
            elif not isinstance(param.default_factory, _MISSING_TYPE):
                default_val = param.default_factory()
            else:
                continue
            setattr(self, param.name, default_val)


class BoundedCollection(ABC, SupportsDict, Generic[T]):
    """
    Abstract base for a bounded or unbounded collection (queue, stack, priority queue).
    """

    @abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError

    @property
    @abstractmethod
    def bounded(self) -> bool:
        raise NotImplementedError

    @property
    @abstractmethod
    def maxlen(self) -> Optional[int]:
        raise NotImplementedError

    @property
    @abstractmethod
    def data(self) -> Iterable[T]:
        raise NotImplementedError

    @property
    def is_empty(self) -> bool:
        return len(self) == 0

    @property
    def is_full(self) -> bool:
        return self.bounded and len(self) == self.maxlen

    @abstractmethod
    def clear(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def push(self, item: T) -> Optional[T]:
        raise NotImplementedError

    @abstractmethod
    def pop(self) -> T:
        raise NotImplementedError

    def to_dict(self) -> dict[str, Any]:
        return {
            "items": list(self.data),
            "max_size": self.maxlen
        }


class Queue(BoundedCollection[T]):
    """
    A FIFO queue with an optional capacity limit.
    """

    def __init__(self, maxlen: Optional[int] = None) -> None:
        self.queue: deque[T] = deque(maxlen=maxlen)

    def __len__(self) -> int:
        return len(self.queue)

    @property
    def bounded(self) -> bool:
        return self.maxlen is not None

    @property
    def maxlen(self) -> Optional[int]:
        return self.queue.maxlen

    @property
    def data(self) -> Iterable[T]:
        return self.queue

    def clear(self) -> None:
        self.queue.clear()

    def push(self, item: T) -> Optional[T]:
        self.queue.append(item)
        return None

    def pop(self) -> T:
        return self.queue.popleft()
    
    def revoke(self) -> T:
        return self.queue.pop()


class LIFOQueue(Queue[T]):
    """
    A LIFO (stack-like) queue.
    """

    def pop(self) -> T:
        return self.queue.pop()


class MinHeap(BoundedCollection[T]):
    """
    A minimum-heap structure with an optional capacity (maxlen).
    If maxlen is reached, the new item can replace the largest element.
    """

    def __init__(self, maxlen: Optional[int] = None) -> None:
        self._maxlen = maxlen
        self.heap: list[T] = []
        heapq.heapify(self.heap)

    def __len__(self) -> int:
        return len(self.heap)

    @property
    def bounded(self) -> bool:
        return self.maxlen is not None

    @property
    def maxlen(self) -> Optional[int]:
        return self._maxlen

    @property
    def data(self) -> Iterable[T]:
        return self.heap

    @property
    def min(self) -> Optional[T]:
        return None if self.is_empty else self.heap[0]

    def clear(self) -> None:
        self.heap.clear()

    def push(self, item: T) -> Optional[T]:
        if self.is_full:
            return heapq.heapreplace(self.heap, item)
        heapq.heappush(self.heap, item)
        return None

    def pop(self) -> T:
        return heapq.heappop(self.heap)


PriorityTuple = Union[tuple[float, T], tuple[float, int, T]]


class PriorityQueue(MinHeap[T]):
    """
    A priority queue. 
    If `fifo` is set, ties in priority are decided by arrival order (FIFO vs LIFO).
    """

    def __init__(
        self,
        priority_fn: Callable[[T], float],
        fifo: Optional[bool] = None,
        **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self.fifo = fifo
        self.priority_fn = priority_fn
        if self.fifo is not None:
            self.counter = itertools.count()

    @property
    def data(self) -> Iterable[T]:
        return (item[-1] for item in self.heap)

    def push(self, item: T) -> Optional[T]:
        priority = self.priority_fn(item)
        if self.fifo is None:
            element: PriorityTuple[T] = (priority, item)
        else:
            # Ensure stable ordering for ties if fifo=True
            count = next(self.counter)
            # If fifo=True => store count as +count, if fifo=False => -count
            order_val = count if self.fifo else -count
            element = (priority, order_val, item)
        return super().push(element)

    def pop(self) -> T:
        # Return the actual T, ignoring the (priority, count) prefix
        return super().pop()[-1]
