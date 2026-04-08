"""
Генератор ключових слів для жорстких дисків (HDD/SSD).
Категорія: 70704
"""

import re
from typing import List, Set

from keywords.core.helpers import SpecAccessor
from keywords.utils.spec_helpers import (
    extract_capacity,
    extract_interface,
    extract_rpm,
    is_spec_allowed
)


def generate(
    accessor: SpecAccessor,
    lang: str,
    base: str,
    allowed: Set[str]
) -> List[str]:
    """
    Генерація ключових слів для жорстких дисків.
    
    Повертає ТІЛЬКИ Блок 2 (характеристики).
    
    Allowed specs з CSV (після мапінгу):
    - Виробник
    - Форм-фактор
    - Об'єм накопичувача
    - Інтерфейс
    - Швидкість обертання

    Args:
        accessor: Accessor для характеристик
        lang: Мова (ru/ua)
        base: Базове ключове слово (жесткий диск / жорсткий диск)
        allowed: Множина дозволених характеристик (строгий white list)

    Returns:
        Список ключових слів (тільки характеристики)
    """
    keywords = []

    # 1. Виробник (додається в generic.py, пропускаємо тут)

    # 2. Об'єм накопичувача
    if is_spec_allowed("Об'єм накопичувача", allowed):
        capacity_info = extract_capacity(accessor, "Об'єм накопичувача")
        if capacity_info:
            capacity = capacity_info["formatted"]
            keywords.extend([
                f"{base} {capacity}",
                f"{capacity} {base}"
            ])

    # 3. Інтерфейс
    if is_spec_allowed("Інтерфейс", allowed):
        interface = extract_interface(accessor, "Інтерфейс")
        if interface:
            keywords.append(f"{base} {interface}")

    # 4. Форм-фактор
    if is_spec_allowed("Форм-фактор", allowed):
        form_factor = accessor.value("Форм-фактор")
        if form_factor:
            # Витягуємо розмір (напр. "3.5" або "2.5")
            match = re.search(r"(\d\.\d)[\"\']?", form_factor)
            if match:
                size = match.group(1)
                keywords.append(f"{base} {size}\"")

    # 5. Швидкість обертання (якщо є - HDD, якщо немає - можливо SSD)
    if is_spec_allowed("Швидкість обертання", allowed):
        rpm = extract_rpm(accessor, "Швидкість обертання")
        if rpm:
            if lang == "ru":
                keywords.append(f"{base} {rpm} об/мин")
            else:
                keywords.append(f"{base} {rpm} об/хв")
        else:
            # Якщо немає швидкості обертання, можливо це SSD
            keywords.append(f"ssd {base}")

    return keywords
