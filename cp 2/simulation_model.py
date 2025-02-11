from typing import Dict

from base_element import Element, Stats
from utils import INF_TIME
from process import TIME_EPS


class Model:
    """
    Клас, що відповідає за "прогон" (simulation run) усієї мережі елементів: 
    пошук наступного моменту події, оновлення часу у всіх елементах, 
    виклики end_action там, де подія закінчується.
    """

    def __init__(self, parent: Element) -> None:
        self.elements = self._collect_elements(parent)

    def _collect_elements(self, parent: Element) -> list[Element]:
        """
        Збирає усі елементи, до яких можна дійти від `parent`.
        Використовується пошук у глибину (DFS).
        """
        visited = set()

        def dfs(elem: Element) -> None:
            if elem not in visited:
                visited.add(elem)
                for child in elem.next_elements:
                    if child is not None:
                        dfs(child)

        dfs(parent)
        return list(visited)

    def simulate(self, end_time: float, verbose: bool = False) -> Dict[str, Stats]:
        """
        Запускає імітацію до моменту часу `end_time`.
        :param end_time: час завершення симуляції
        :param verbose: якщо True, виводить детальні стани на кожному кроці.
        :return: словник з іменами елементів та зібраною статистикою.
        """
        current_time = 0.0

        while current_time < end_time:
            # Шукаємо найближчий час "події" серед усіх елементів
            next_time = min(elem.next_time for elem in self.elements)
            if next_time == INF_TIME:
                # Немає подій, що очікують обробки
                break

            current_time = next_time
            # Оновити час для всіх елементів
            for elem in self.elements:
                elem.set_current_time(current_time)

            # Викликаємо end_action для тих, хто має подію на час next_time
            updated_names = []
            for elem in self.elements:
                if abs(elem.next_time - next_time) < TIME_EPS:
                    elem.end_action()
                    updated_names.append(elem.name)

            if verbose:
                self._print_states(current_time, updated_names)

        return {elem.name: elem.stats for elem in self.elements}

    def _print_states(self, current_time: float, updated_names: list[str]) -> None:
        """
        Допоміжний метод для виведення станів у verbose-режимі.
        """
        states_repr = ' | '.join(str(elem) for elem in self.elements)
        print(f'{current_time:.5f}: [Updated: {updated_names}]. States: {states_repr}\n')
