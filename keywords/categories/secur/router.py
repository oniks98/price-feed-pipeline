"""
Роутер категорійних обробників для Secur.

Визначає, який обробник використовувати для кожної категорії товарів Secur.
"""

from typing import Optional, Callable, List, Set

from keywords.core.helpers import SpecAccessor


# Тип для функцій-обробників категорій
CategoryHandler = Callable[[SpecAccessor, str, str, Set[str]], List[str]]


# Мапінг категорій на обробники
# Поки що порожній - будемо додавати категорії по мірі необхідності
_CATEGORY_HANDLERS: dict[str, CategoryHandler] = {
    # "302306": handle_signalization,  # Сигналізація
    # "302302": handle_sensors,        # Датчики
    # "300402": handle_sirens,         # Сирени
    # "302308": handle_buttons,        # Кнопки
}


def get_category_handler(category_id: str) -> Optional[CategoryHandler]:
    """
    Отримати обробник для категорії.

    Args:
        category_id: Ідентифікатор категорії

    Returns:
        Функція-обробник або None якщо для категорії немає спеціального обробника
    """
    return _CATEGORY_HANDLERS.get(category_id)
