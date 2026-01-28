import random
from functools import partial

from creator import CreateElement
from process import ProcessElement
from simulation_model import Model
from dispose import DisposeElement

def run_task_1_and_2_simulation() -> None:
    """
    ЗАВДАННЯ 1 та 2:
    1. Реалізувати просту модель: Create -> Process.
    2. Статистика: Обчислити середнє завантаження пристрою (реалізовано в статистиці ProcessElement).
    """    
    # Create: Експоненційний розподіл, середній інтервал = 2.0
    creator = CreateElement(
        get_delay=partial(random.expovariate, lambd=1.0 / 2.0),
        name='CREATE'
    )
    
    # Process: Експоненційний розподіл, середній час обслуговування = 1.0, 1 канал
    processor = ProcessElement(
        name='PROCESSOR',
        max_queue_size=5,
        num_handlers=1,
        get_delay=partial(random.expovariate, lambd=1.0 / 1.0)
    )
    
    # З'єднання: Create -> Process
    creator.add_next_element(processor)
    
    # Запуск симуляції
    model = Model(parent=creator)
    stats = model.simulate(end_time=1000, verbose=False)
    
    # Виведення результатів
    for name, element_stats in stats.items():
        print(f'{name}:\n{element_stats}')

def run_task_3_simulation() -> None:
    """
    Приклад запуску моделі (Завдання 3):
      - Модель згідно з рис. 2.1: Create -> P1 -> P2 -> P3 -> Dispose.
    """
    create = CreateElement(get_delay=partial(random.expovariate, lambd=1.0 / 0.2), 
                            name='CREATE')
    
    # Параметри процесів
    process1 = ProcessElement(
        name='PROCESS_1',
        max_queue_size=10,
        get_delay=partial(random.expovariate, lambd=1.0 / 1.2)
    )
    process2 = ProcessElement(
        name='PROCESS_2',
        max_queue_size=8,
        get_delay=partial(random.expovariate, lambd=1.0 / 2.0)
    )
    process3 = ProcessElement(
        name='PROCESS_3',
        max_queue_size=1,
        get_delay=partial(random.expovariate, lambd=1.0 / 1.0)
    )
    
    # Елемент виходу (DESPOSE з рис. 2.1)
    dispose = DisposeElement(name='DISPOSE')

    # З’єднання блоків
    create.add_next_element(process1)
    process1.add_next_element(process2)
    process2.add_next_element(process3)
    process3.add_next_element(dispose)

    # Створюємо модель та запускаємо
    model = Model(parent=create)
    stats = model.simulate(end_time=1000, verbose=False)

    print('Final statistics:')
    for name, element_stats in stats.items():
        print(f'{name}:\n{element_stats}\n')
        
def run_task_4_simulation() -> None:
    """
    ЗАВДАННЯ 4:
    Верифікація моделі із Завдання 3.
    Ми змінюємо інтенсивність вхідного потоку (Delay Create) і дивимося, 
    як це впливає на черги всіх процесів та кількість відмов.
    """    
    # Заголовок таблиці
    print(f"{'Delay':<8} | {'P1 Fail %':<10} | {'P1 Queue':<10} | {'P2 Queue':<10} | {'P3 Queue':<10} | {'Disposed':<10}")
    print("-" * 75)
    
    delays_to_test = [0.5, 0.8, 0.9, 1.0, 1.1, 1.5, 2.0, 5.0]
    
    for delay in delays_to_test:
        # 1. Створення повної моделі (як у Завданні 3)
        create = CreateElement(get_delay=partial(random.expovariate, lambd=1.0 / delay), name="CREATE")
        
        process1 = ProcessElement(
            max_queue_size=5, 
            get_delay=partial(random.expovariate, lambd=1.0 / 1.0), 
            name="PROCESS_1"
            )
        process2 = ProcessElement(
            max_queue_size=5, 
            get_delay=partial(random.expovariate, lambd=1.0 / 1.0), 
            name="PROCESS_2"
            )
        process3 = ProcessElement(
            max_queue_size=5, 
            get_delay=partial(random.expovariate, lambd=1.0 / 1.0), 
            name="PROCESS_3"
            )
        
        dispose = DisposeElement(name="DISPOSE")
        
        # 2. З'єднання
        create.add_next_element(process1)
        process1.add_next_element(process2)
        process2.add_next_element(process3)
        process3.add_next_element(dispose)
        
        # 3. Запуск
        model = Model(parent=create)
        model.simulate(end_time=1000, verbose=False)
        
        # 4. Збір статистики
        process1_fail = process1.stats.failure_proba * 100
        process1_mean_queue_size = process1.stats.mean_queue_size
        process2_mean_queue_size = process2.stats.mean_queue_size
        process3_mean_queue_size = process3.stats.mean_queue_size
        disposed = dispose.stats.num_events
        
        # 5. Вивід рядка таблиці
        print(f"{delay:<8.1f} | {process1_fail:<10.2f} | {process1_mean_queue_size:<10.4f} "
              f"| {process2_mean_queue_size:<10.4f} | {process3_mean_queue_size:<10.4f} | {disposed:<10}")

def run_task_5_and_6_simulation() -> None:
    """
    ЗАВДАННЯ 5 та 6:
    Багатоканальність та розгалуження (зворотний зв'язок).
    """
    # Параметри створення (середній інтервал 0.2)
    create = CreateElement(get_delay=partial(random.expovariate, lambd=1.0 / 0.2), 
                            name='CREATE')

    # Параметри процесів
    process1 = ProcessElement(
        name='PROCESS_1',
        max_queue_size=10,
        num_handlers=5,
        get_delay=partial(random.expovariate, lambd=1.0 / 1.2)
    )
    process2 = ProcessElement(
        name='PROCESS_2',
        max_queue_size=8,
        num_handlers=7,
        get_delay=partial(random.expovariate, lambd=1.0 / 2.0)
    )
    process3 = ProcessElement(
        name='PROCESS_3',
        max_queue_size=1,
        num_handlers=2,
        get_delay=partial(random.expovariate, lambd=1.0 / 1.0)
    )
    
    # Елемент виходу
    dispose = DisposeElement(name='DISPOSE')

    # З’єднання блоків
    create.add_next_element(process1)
    process1.add_next_element(process2)
    process2.add_next_element(process3)
    
    # Розгалуження (Завдання 6):
    # 10% назад у P1, 90% у Dispose
    process3.add_next_element(process1, proba=0.1)
    process3.add_next_element(dispose, proba=0.9)

    model = Model(parent=create)
    stats = model.simulate(end_time=1000, verbose=False)

    print('Final statistics:')
    for name, element_stats in stats.items():
        print(f'{name}:\n{element_stats}\n')

if __name__ == '__main__':
    print(f"\n{'='*20} Завдання 1 та 2: Проста модель {'='*20}")
    run_task_1_and_2_simulation()
    print(f"\n{'='*20} Завдання 3: Лінійна модель (Рис. 2.1) {'='*20}")
    run_task_3_simulation()
    print(f"\n{'='*20} Завдання 4: Верифікація моделі (Завдання 3) {'='*20}")
    run_task_4_simulation()
    print(f"\n{'='*20} Завдання 5 та 6: Складна модель {'='*20}")
    run_task_5_and_6_simulation()
    
    