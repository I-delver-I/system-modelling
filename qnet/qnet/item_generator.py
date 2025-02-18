"""
Factory node: continuously creates items for a queueing network.
"""

import itertools
from abc import abstractmethod
from typing import Iterable, Optional, Any

from .core_models import I, Item
from .simulation_node import NM, Node
from .helpers import filter_none


class BaseFactoryNode(Node[I, NM]):
    """
    Abstract Node that generates new items at some specified arrival process (delay_fn).
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.item: Optional[I] = None
        self.next_time = self._predict_next_time()
        self.counter = itertools.count()

    @property
    def current_items(self) -> Iterable[I]:
        return filter_none((self.item,))

    @property
    def next_id(self) -> str:
        return f"{self.num_nodes}_{next(self.counter)}"

    def start_action(self, item: I) -> None:
        super().start_action(item)
        raise RuntimeError("start_action() must not be called on a factory node!")

    def end_action(self) -> I:
        self.item = self._get_next_item()
        self.next_time = self._predict_next_time()
        return self._end_action(self.item)

    def reset(self) -> None:
        super().reset()
        self.item = None
        self.next_time = self._predict_next_time()

    def to_dict(self) -> dict[str, Any]:
        node_dict = super().to_dict()
        node_dict.update({
            "last_item": self.item,
            "last_created_time": self.item.created_time if self.item else None
        })
        return node_dict

    @abstractmethod
    def _get_next_item(self) -> I:
        raise NotImplementedError


class FactoryNode(BaseFactoryNode[Item, NM]):
    """
    Simple factory node that produces generic Items.
    """

    def _get_next_item(self) -> Item:
        """
        Produce a new Item with the current simulation time as created_time.
        """
        return Item(
            id=self.next_id,
            created_time=self.current_time
        )
