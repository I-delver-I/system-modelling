from typing import Union, TypeVar, Type, Any

INF_TIME = float('inf')
TIME_EPS = 1e-6
TIME_PR = 5  # Кількість знаків після коми для відображення часу
TIME_FORMATTER = f'{{value:.{TIME_PR}f}}'

T = TypeVar('T')
PathFormat = Union[str, tuple[str, str]]


def _get_param_by_path(obj: Any, path: str) -> tuple[str, Any]:
    """
    Розбиває шлях на поля, рекурсивно отримує значення з об’єкта.
    Наприклад, path='stats.num_events' поверне (num_events, <значення>).
    """
    value = obj
    name = None
    for name in path.split('.'):
        value = getattr(value, name)
    return name, value


def _format_param(obj: Any, path: str, formatter: str = '{name}={value}') -> str:
    """
    Повертає відформатований рядок формату `formatter`,
    підставляючи {name} і {value} з `_get_param_by_path`.
    """
    name, value = _get_param_by_path(obj, path)
    return formatter.format(name=name, value=value)


def format_params(obj: Any, param_args: list[PathFormat]) -> str:
    """
    Функція для створення короткого рядка зі станом об’єкта.
    param_args – список значень або (значення, формат), де формат – це шаблон.
    """
    result_parts: list[str] = []
    for arg in param_args:
        if isinstance(arg, tuple):
            path, local_format = arg
            result_parts.append(_format_param(obj, path, f'{{name}}={local_format}'))
        else:
            result_parts.append(_format_param(obj, arg))
    return ', '.join(result_parts)


def instance_counter(cls: Type[T]) -> Type[T]:
    """
    Декоратор-клас: додає класу лічильник створених екземплярів
    та метод _next_id() для автоматичного іменування.
    """
    cls.count = 0

    def _next_id(sub_cls: Type[T]) -> int:
        sub_cls.count += 1
        return sub_cls.count

    cls._next_id = classmethod(_next_id)
    return cls
