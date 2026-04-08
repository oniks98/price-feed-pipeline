"""
Генератор ключових слів для SD-карт.
Категорія: 63705
"""

from typing import List, Set

from keywords.core.helpers import SpecAccessor
from keywords.utils.spec_helpers import (
    extract_capacity,
    extract_speed,
    is_spec_allowed
)


def generate(
    accessor: SpecAccessor,
    lang: str,
    base: str,
    allowed: Set[str]
) -> List[str]:
    """
    Генерація ключових слів для SD-карт.
    
    Повертає ТІЛЬКИ Блок 2 (характеристики).
    
    Allowed specs з CSV (після мапінгу):
    - Виробник
    - Форм-фактор
    - Тип карти пам'яті
    - Об'єм пам'яті
    - Швидкість зчитування
    - Швидкість запису

    Args:
        accessor: Accessor для характеристик
        lang: Мова (ru/ua)
        base: Базове ключове слово (sd карта)
        allowed: Множина дозволених характеристик (строгий white list)

    Returns:
        Список ключових слів (тільки характеристики)
    """
    keywords = []

    # 1. Виробник (додається в generic.py, пропускаємо тут)

    # 2. Об'єм пам'яті
    if is_spec_allowed("Об'єм пам'яті", allowed):
        capacity_info = extract_capacity(accessor, "Об'єм пам'яті")
        if capacity_info:
            capacity = capacity_info["formatted"]

            if lang == "ru":
                keywords.extend([
                    f"сд карта {capacity}",
                    f"micro sd {capacity}",
                    f"sd карта {capacity}",
                    f"карта памяти {capacity}",
                    f"{capacity} sd карта"
                ])
            else:
                keywords.extend([
                    f"сд карта {capacity}",
                    f"micro sd {capacity}",
                    f"sd карта {capacity}",
                    f"карта пам'яті {capacity}",
                    f"{capacity} sd карта"
                ])

    # 3. Тип карти пам'яті (microSD / SD)
    if is_spec_allowed("Тип карти пам'яті", allowed):
        card_type = accessor.value("Тип карти пам'яті")
        if card_type:
            card_type_lower = card_type.lower()
            
            # Отримуємо об'єм для комбінації
            capacity = None
            if is_spec_allowed("Об'єм пам'яті", allowed):
                capacity_info = extract_capacity(accessor, "Об'єм пам'яті")
                if capacity_info:
                    capacity = capacity_info["formatted"]
            
            if "microsd" in card_type_lower or "micro sd" in card_type_lower:
                if capacity:
                    keywords.append(f"microsd {capacity}")
            elif "sd" in card_type_lower:
                if capacity:
                    keywords.append(f"sd {capacity}")

    # 4. Швидкість зчитування (якщо висока швидкість >= 90 МБ/с)
    if is_spec_allowed("Швидкість зчитування", allowed):
        read_speed = extract_speed(accessor, "Швидкість зчитування")
        if read_speed and int(read_speed) >= 90:
            if lang == "ru":
                keywords.append("быстрая sd карта")
            else:
                keywords.append("швидка sd карта")

    return keywords
