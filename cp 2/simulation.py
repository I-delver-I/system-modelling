import random
from functools import partial

from creator import CreateElement
from process import ProcessElement
from simulation_model import Model


def run_modified_simulation() -> None:
    """
    Приклад запуску моделі:
      - Один елемент CreateElement (джерело подій),
      - Три послідовні ProcessElement із різними параметрами.
      - 10% заявок із третього процесу повертаються до першого.
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

    # З’єднання блоків
    create1.add_next_element(process1)
    process1.add_next_element(process2)
    process2.add_next_element(process3)
    # 10% заявок повертаються із третього процесу назад у process1
    process3.add_next_element(process1, proba=0.1)

    # Створюємо модель та запускаємо
    model = Model(parent=create1)
    stats = model.simulate(end_time=1000, verbose=False)

    print('Final statistics:')
    for name, element_stats in stats.items():
        print(f'{name}:\n{element_stats}\n')
        
def run_simulation() -> None:
    """
    Приклад запуску моделі:
      - Один елемент CreateElement (джерело подій),
      - Три послідовні ProcessElement із різними параметрами.
    """
    create1 = CreateElement(get_delay=partial(random.expovariate, lambd=1.0 / 0.2))
    
    # Параметри процесів
    process1 = ProcessElement(
        max_queue_size=10,
        get_delay=partial(random.expovariate, lambd=1.0 / 1.2)
    )
    process2 = ProcessElement(
        max_queue_size=8,
        get_delay=partial(random.expovariate, lambd=1.0 / 2.0)
    )
    process3 = ProcessElement(
        max_queue_size=1,
        get_delay=partial(random.expovariate, lambd=1.0 / 1.0)
    )

    # З’єднання блоків
    create1.add_next_element(process1)
    process1.add_next_element(process2)
    process2.add_next_element(process3)

    # Створюємо модель та запускаємо
    model = Model(parent=create1)
    stats = model.simulate(end_time=1000, verbose=False)

    print('Final statistics:')
    for name, element_stats in stats.items():
        print(f'{name}:\n{element_stats}\n')


if __name__ == '__main__':
    print('|Simulation|')
    run_simulation()
    print('|Modified simulation|')
    run_modified_simulation()
    
    