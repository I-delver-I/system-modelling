"""
Logging utilities: console logger for node states, node metrics, model metrics, etc.
"""

from __future__ import annotations

import prettytable as pt
from abc import ABC, abstractmethod
from typing import Callable, Generic, TypeVar, Any, Mapping, Iterable

from .core_models import T, I, SupportsDict, Metrics
from .simulation_node import Node, NodeMetrics
from .simulation_engine import EvaluationReport, ModelMetrics

M_contra = TypeVar("M_contra", bound=Metrics, contravariant=True)
N_contra = TypeVar("N_contra", bound=Node, contravariant=True)

LoggerDispatcher = Callable[[T], dict[str, Any]]
NodeLoggerDispatcher = LoggerDispatcher[N_contra]
MetricsLoggerDispatcher = LoggerDispatcher[M_contra]


class BaseLogger(ABC, Generic[I]):
    """
    Abstract logger that prints or records simulation progress, node states, etc.
    """

    @abstractmethod
    def nodes_states(self, time: float, nodes: list[Node[I, NodeMetrics]]) -> None:
        raise NotImplementedError

    @abstractmethod
    def model_metrics(self, model_metrics: ModelMetrics) -> None:
        raise NotImplementedError

    @abstractmethod
    def nodes_metrics(self, nodes_metrics: list[NodeMetrics]) -> None:
        raise NotImplementedError

    @abstractmethod
    def evaluation_reports(self, evaluation_reports: list[EvaluationReport]) -> None:
        raise NotImplementedError


class CLILogger(BaseLogger[I]):
    """
    A command-line logger that uses prettytable for tabular displays of states and metrics.
    """

    def __init__(self, precision: int = 3, max_column_width: int = 60, max_table_width: int = 100) -> None:
        self.precision = precision
        self.table_kwargs = dict(align="l", max_width=max_column_width, max_table_width=max_table_width)

    def _format_float(self, float_: float) -> str:
        return f"{float_:.{self.precision}f}"

    def _format_dict(
        self,
        dict_: Mapping,
        join_chars: str = ", ",
        split_chars: str = "=",
        start_chars: str = "",
        sort_by_key: bool = False
    ) -> str:
        items = dict_.items()
        if sort_by_key:
            items = sorted(items, key=lambda x: x[0])
        dict_str = join_chars.join(f"{k}{split_chars}{self._format(v)}" for k, v in items)
        return f"{start_chars}{dict_str}"

    def _format_iter(self, iter_: Iterable, join_chars: str = ", ", with_braces: bool = True) -> str:
        iter_str = join_chars.join(map(self._format, iter_))
        if with_braces:
            return f"[{iter_str}]"
        return iter_str

    def _format_class(self, value: Any, info: Any) -> str:
        return f"{type(value).__name__}({self._format(info)})"

    def _to_dict(self, value: SupportsDict) -> dict[str, Any]:
        return value.to_dict()

    def _format(self, value: Any) -> str:
        """
        Generic converter of python objects to a string, used in tabular logging.
        """
        if isinstance(value, SupportsDict):
            return self._format_class(value, self._to_dict(value))
        if isinstance(value, Mapping):
            return self._format_dict(value)
        if isinstance(value, Iterable) and not isinstance(value, str):
            return self._format_iter(value)
        if isinstance(value, float):
            return self._format_float(value)
        return str(value)

    def _format_metrics_dict(self, metrics_dict: dict[str, Any]) -> str:
        """
        Formats a dictionary of metrics into multiple lines for pretty printing.
        """
        out_metrics_dict: dict[str, Any] = {}
        for name, val in metrics_dict.items():
            if isinstance(val, dict):
                out_metrics_dict[name] = self._format_dict(
                    val, join_chars="\n", split_chars=": ", start_chars="\n", sort_by_key=True
                )
            else:
                out_metrics_dict[name] = val
        return self._format_dict(out_metrics_dict, join_chars="\n", split_chars=": ", sort_by_key=True)

    def nodes_states(self, time: float, nodes: list[Node[I, NodeMetrics]]) -> None:
        table = pt.PrettyTable(field_names=["Node", "State", "Action"], **self.table_kwargs)
        for node in nodes:
            state_dict = self._to_dict(node)
            action = (
                "end" if abs(node.metrics.end_action_time - time) < 1e-12 else
                ("start" if abs(node.metrics.start_action_time - time) < 1e-12 else "--")
            )
            table.add_row([node.name, self._format(state_dict), action])
        print(f"Time: {self._format_float(time)}")
        print(table.get_string(title="Nodes States", hrules=pt.ALL, sortby="Node"))

    def model_metrics(self, model_metrics: ModelMetrics[I]) -> None:
        table = pt.PrettyTable(field_names=["Metrics"], **self.table_kwargs)
        metrics_dict = self._to_dict(model_metrics)
        table.add_row([self._format_metrics_dict(metrics_dict)])
        print(table.get_string(title="Model Metrics"))

    def nodes_metrics(self, nodes_metrics: list[NodeMetrics]) -> None:
        table = pt.PrettyTable(field_names=["Node", "Metrics"], **self.table_kwargs)
        for metrics in nodes_metrics:
            metrics_dict = self._to_dict(metrics)
            table.add_row([metrics.node_name, self._format_metrics_dict(metrics_dict)])
        print(table.get_string(title="Nodes Metrics", hrules=pt.ALL, sortby="Node"))

    def evaluation_reports(self, evaluation_reports: list[EvaluationReport]) -> None:
        table = pt.PrettyTable(field_names=["Report", "Result"], **self.table_kwargs)
        for report in evaluation_reports:
            table.add_row([report.name, self._format(report.result)])
        print(table.get_string(title="Evaluation Reports", hrules=pt.ALL, sortby="Report"))
