import heapq
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from base_element import Element, Stats
from utils import INF_TIME, TIME_EPS, TIME_PR, TIME_FORMATTER, format_params


@dataclass(eq=False)
class ProcessStats(Stats):
    """
    Статистика для процесора (ProcessElement).
    Зберігає кількість оброблених подій, загальний час очікування,
    час "зайнятості" хендлерів, кількість відмов тощо.
    """
    element: 'ProcessElement'
    num_in_events: int = field(init=False, default=1)
    wait_time: float = field(init=False, default=0.0)
    busy_time: float = field(init=False, default=0.0)
    num_failures: int = field(init=False, default=0)

    def __repr__(self) -> str:
        return (
            f'Num processed: {self.num_events}. '
            f'Mean queue size: {self.mean_queue_size:.{TIME_PR}f}. '
            f'Mean busy handlers: {self.mean_busy_handlers:.{TIME_PR}f}. '
            f'Mean wait time: {self.mean_wait_time:.{TIME_PR}f}. '
            f'Failure probability: {self.failure_proba:.{TIME_PR}f}'
        )

    @property
    def mean_queue_size(self) -> float:
        """Середній розмір черги."""
        return self.wait_time / max(self.element.current_time, TIME_EPS)

    @property
    def mean_busy_handlers(self) -> float:
        """Середня кількість зайнятих хендлерів."""
        return self.busy_time / max(self.element.current_time, TIME_EPS)

    @property
    def failure_proba(self) -> float:
        """Імовірність відмови (відношення відмов до вхідних подій)."""
        return self.num_failures / max(self.num_in_events, 1)

    @property
    def mean_wait_time(self) -> float:
        """Середній час очікування у черзі на одне успішне обслуговування."""
        return self.wait_time / max(self.num_events, 1)


@dataclass(order=True)
class Handler:
    """
    Описує процес "обробника" (хендлера).
    Зберігає ідентифікатор події, що обробляється,
    та час, коли цей обробник звільниться.
    """
    next_time: float
    in_event: int = field(compare=False)

    def __repr__(self) -> str:
        return f'Handler({format_params(self, ["in_event", ("next_time", TIME_FORMATTER)])})'


class ProcessElement(Element):
    """
    Елемент, що моделює обслуговування заявок чергою та одним чи кількома хендлерами.
    """

    def __init__(self, max_queue_size: int, num_handlers: int = 1, **kwargs: Any) -> None:
        """
        :param max_queue_size: максимальний розмір черги;
        :param num_handlers: кількість паралельних хендлерів.
        """
        super().__init__(**kwargs)

        self.max_queue_size = max_queue_size
        self.num_handlers = num_handlers

        # Черга зберігає ідентифікатори/номери подій, що очікують обробки.
        self.queue: deque[int] = deque()

        # Порожнє значення next_time, доки не буде жодної події.
        self.next_time = INF_TIME

        # Статистика
        self.stats = ProcessStats(self)

        # Активні обробники з пріорітетом за часом звільнення (next_time).
        self.handlers: list[Handler] = []
        heapq.heapify(self.handlers)

    def _get_str_state(self) -> str:
        return format_params(
            self,
            [
                'stats.num_events',
                ('next_time', TIME_FORMATTER),
                'num_handlers',
                'handlers',
                'queue',
                'stats.num_failures'
            ]
        )

    def start_action(self) -> None:
        """
        Обробляє вхід події. Якщо є вільний хендлер, викликається негайно;
        якщо ні – заявка потрапляє в чергу (якщо є місце), інакше реєструється відмова.
        """
        # Якщо всі хендлери зайняті:
        if len(self.handlers) == self.num_handlers:
            # Перевірити, чи є місце в черзі
            if len(self.queue) < self.max_queue_size:
                self.queue.append(self.stats.num_in_events)
            else:
                self.stats.num_failures += 1
        else:
            # Додаємо новий обробник у heap
            handler = Handler(next_time=self._predict_next_time(), in_event=self.stats.num_in_events)
            heapq.heappush(self.handlers, handler)
            self.next_time = self.handlers[0].next_time

        self.stats.num_in_events += 1

    def end_action(self) -> None:
        """
        Викликається, коли завершив роботу перший (найшвидший) обробник з купи.
        Якщо у черзі є події, беремо одну на обробку тією ж миттю.
        """
        # Витягуємо найраніший обробник
        handler = heapq.heappop(self.handlers)

        # Якщо у черзі є заявки, одразу ставимо наступну в обробку
        if self.queue:
            handler.in_event = self.queue.popleft()
            handler.next_time = self._predict_next_time()
            heapq.heappush(self.handlers, handler)

        # Оновити глобальний next_time (час наступного вивільнення)
        self.next_time = self.handlers[0].next_time if self.handlers else INF_TIME

        super().end_action()

    def set_current_time(self, next_time: float) -> None:
        """
        При оновленні глобального часу збільшуємо сумарний час очікування (для всіх в черзі) 
        та сумарний час зайнятості (для всіх хендлерів).
        """
        dtime = next_time - self.current_time
        # Для всіх подій у черзі додається dtime очікування
        self.stats.wait_time += len(self.queue) * dtime
        # Для всіх зайнятих хендлерів збільшуємо час зайнятості
        self.stats.busy_time += len(self.handlers) * dtime

        super().set_current_time(next_time)
