from dataclasses import dataclass
from typing import Any

from base_element import Element, Stats

@dataclass(eq=False)
class DisposeStats(Stats):
    """
    Спеціалізована статистика для елемента Dispose (вихід).
    Зберігає загальну кількість оброблених (вийшлих) заявок.
    """

    def __repr__(self) -> str:
        return f'Num disposed: {self.num_events}.'
      
class DisposeElement(Element):
    """
    Елемент, що імітує вихід заявок із системи.
    Не генерує нових подій, а лише фіксує надходження.
    """

    def __init__(self, **kwargs: Any) -> None:
        # Якщо затримка не передана, встановлюємо 0 (миттєвий вихід)
        if 'get_delay' not in kwargs:
            kwargs['get_delay'] = lambda: 0.0
            
        super().__init__(**kwargs)
        
        # Dispose не планує подій сам, він пасивний
        self.next_time = float('inf') 
        self.stats = DisposeStats(self)
        
    def start_action(self) -> None:
        """
        CRITICAL FIX: Count the item immediately when it enters this block.
        """
        # 1. Count the item
        self.stats.num_events += 1
        
        # 2. Call parent logic (optional for Dispose, but good practice)
        super().start_action()
    
    def end_action(self) -> None:
        """
        Для Dispose подія завершення обслуговування не потрібна,
        оскільки заявка просто зникає.
        """
        pass
