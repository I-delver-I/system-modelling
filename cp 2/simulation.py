import random
from functools import partial

from creator import CreateElement
from process import ProcessElement
from simulation_model import Model
from dispose import DisposeElement

def run_simulation() -> None:
    """
    Приклад запуску моделі (Завдання 3):
      - Модель згідно з рис. 2.1: Create -> P1 -> P2 -> P3 -> Dispose.
    """
    create1 = CreateElement(get_delay=partial(random.expovariate, lambd=1.0 / 0.2), 
                            name='CreateElement1')
    
    # Параметри процесів
    process1 = ProcessElement(
        name='ProcessElement1',
        max_queue_size=10,
        get_delay=partial(random.expovariate, lambd=1.0 / 1.2)
    )
    process2 = ProcessElement(
        name='ProcessElement2',
        max_queue_size=8,
        get_delay=partial(random.expovariate, lambd=1.0 / 2.0)
    )
    process3 = ProcessElement(
        name='ProcessElement3',
        max_queue_size=1,
        get_delay=partial(random.expovariate, lambd=1.0 / 1.0)
    )
    
    # Елемент виходу (DESPOSE з рис. 2.1)
    dispose = DisposeElement(name='DisposeElement1')

    # З’єднання блоків
    create1.add_next_element(process1)
    process1.add_next_element(process2)
    process2.add_next_element(process3)
    process3.add_next_element(dispose)

    # Створюємо модель та запускаємо
    model = Model(parent=create1)
    stats = model.simulate(end_time=1000, verbose=False)

    print('Final statistics:')
    for name, element_stats in stats.items():
        print(f'{name}:\n{element_stats}\n')

def run_modified_simulation() -> None:
    """
    Приклад запуску моделі:
      - Один елемент CreateElement (джерело подій),
      - Три послідовні ProcessElement із різними параметрами.
      - 10% заявок із третього процесу повертаються до першого.
      - Вихід: 90% заявок із P3 йдуть у Dispose.
    """
    # Параметри створення (середній інтервал 0.2)
    create1 = CreateElement(get_delay=partial(random.expovariate, lambd=1.0 / 0.2), 
                            name='CreateElement1')

    # Параметри процесів
    process1 = ProcessElement(
        name='ProcessElement1',
        max_queue_size=10,
        num_handlers=5,
        get_delay=partial(random.expovariate, lambd=1.0 / 1.2)
    )
    process2 = ProcessElement(
        name='ProcessElement2',
        max_queue_size=8,
        num_handlers=7,
        get_delay=partial(random.expovariate, lambd=1.0 / 2.0)
    )
    process3 = ProcessElement(
        name='ProcessElement3',
        max_queue_size=1,
        num_handlers=2,
        get_delay=partial(random.expovariate, lambd=1.0 / 1.0)
    )
    
    # Елемент виходу
    dispose = DisposeElement(name='DisposeElement1')

    # З’єднання блоків
    create1.add_next_element(process1)
    process1.add_next_element(process2)
    process2.add_next_element(process3)
    
    # Розгалуження (Завдання 6):
    # 10% заявок повертаються із третього процесу назад у process1
    process3.add_next_element(process1, proba=0.1)
    # 90% заявок виходять із системи (успішно оброблені)
    process3.add_next_element(dispose, proba=0.9)

    # Створюємо модель та запускаємо
    model = Model(parent=create1)
    stats = model.simulate(end_time=1000, verbose=False)

    print('Final statistics:')
    for name, element_stats in stats.items():
        print(f'{name}:\n{element_stats}\n')

if __name__ == '__main__':
    print('|Simulation (Task 3)|')
    run_simulation()
    print('|Modified simulation (Task 5 & 6)|')
    run_modified_simulation()
    
    