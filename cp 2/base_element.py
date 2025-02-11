import random
from dataclasses import dataclass, field
from typing import Callable, Optional

from utils import TIME_FORMATTER, format_params, instance_counter

GetDelayFn = Callable[[], float]


@dataclass(eq=False)
class Stats:
    """
    Базова статистика для елемента.
    Зберігає кількість подій (events), що сталися з цим елементом.
    """
    element: 'Element'
    num_events: int = field(init=False, default=0)

    def __repr__(self) -> str:
        return f'Num events: {self.num_events}'


@instance_counter
class Element:
    """
    Базовий елемент моделі, який може мати наступний(-і) елемент(-и) 
    та виконує певну "дію" (start_action/end_action).
    """

    def __init__(self, get_delay: GetDelayFn, name: Optional[str] = None) -> None:
        """
        :param get_delay: функція/виклик, що повертає затримку (час обслуговування/очікування).
        :param name: ім’я елемента, якщо None – генерується автоматично.
        """
        self._name = self._generate_name() if name is None else name
        self.get_delay = get_delay

        self.current_time: float = 0
        self.next_time: float = 0

        # Наступні елементи та їхні імовірності переходу
        self.next_elements: list[Element] = []
        self.next_probas: list[float] = []

        # Статистика
        self.stats = Stats(self)

    @property
    def name(self) -> str:
        """Повертає ім’я елемента."""
        return self._name

    def _generate_name(self) -> str:
        return f'{self.__class__.__name__}{self._next_id()}'  # _next_id успадкований із @instance_counter

    def _get_str_state(self) -> str:
        """
        Формує короткий рядок зі станом для виведення/відлагодження.
        """
        return format_params(self, ['stats.num_events', ('next_time', TIME_FORMATTER)])

    def __repr__(self) -> str:
        return f'{self.name} ({self._get_str_state()})'

    def start_action(self) -> None:
        """
        Метод, що зветрається при надходженні події до даного елемента.
        За замовчуванням не робить нічого, конкретні дії перевизначаються у підкласах.
        """
        pass

    def end_action(self) -> None:
        """
        Метод, що викликається при завершенні події, 
        а також ініціює передання події на наступний елемент (якщо він існує).
        """
        self.stats.num_events += 1
        next_element = self._get_next_element()
        if next_element is not None:
            next_element.start_action()

    def set_current_time(self, next_time: float) -> None:
        """
        Оновлює внутрішній лічильник часу цього елемента.
        """
        self.current_time = next_time

    def add_next_element(self, element: 'Element', proba: float = 1.0) -> None:
        """
        Додає в список наступних елементів об’єкт `element` 
        з імовірністю переходу `proba`.
        """
        self.next_probas.append(proba)
        self.next_elements.append(element)

    def _get_next_element(self) -> Optional['Element']:
        """
        Вибір наступного елемента за заданими імовірностями.
        Якщо сума < 1, решта ймовірності припадає на "None" (пропуск).
        """
        proba_sum = sum(self.next_probas)
        if proba_sum > 1:
            raise RuntimeError(
                f'Next elements\' probas must sum to 1 or less (got {proba_sum}).'
            )
        probas = self.next_probas
        elements = self.next_elements

        # Якщо сума не дотягує до 1, додаємо можливий перехід "у нікуди" (None).
        if proba_sum < 1:
            probas = probas + [1 - proba_sum]
            elements = elements + [None]

        return random.choices(elements, weights=probas, k=1)[0]

    def _predict_next_time(self) -> float:
        """
        Обчислює момент часу для наступної події з урахуванням поточного часу 
        та випадково згенерованої затримки.
        """
        return self.current_time + self.get_delay()
