from dataclasses import dataclass
from typing import Any

from base_element import Element, Stats


@dataclass(eq=False)
class CreateStats(Stats):
    """
    Спеціалізована статистика для елемента-джерела подій (CreateElement).
    Зберігає загальну кількість створених подій (num_events).
    """

    def __repr__(self) -> str:
        return f'Num created: {self.num_events}.'


class CreateElement(Element):
    """
    Елемент, який генерує нові "заявки"/події з певним розподілом інтервалів.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        # Вираховуємо час до першої події.
        self.next_time = self._predict_next_time()
        self.stats = CreateStats(self)

    def end_action(self) -> None:
        """
        Кожен виклик створює нову подію, потім обчислює час наступної.
        """
        self.next_time = self._predict_next_time()
        super().end_action()
