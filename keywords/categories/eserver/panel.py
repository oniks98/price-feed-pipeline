"""
Генератор ключових слів для патч-панелей.
Категорія: 5092902
"""

import re
from typing import List, Set

from keywords.core.helpers import SpecAccessor
from keywords.utils.spec_helpers import is_spec_allowed


def generate(
    accessor: SpecAccessor,
    lang: str,
    base: str,
    allowed: Set[str]
) -> List[str]:
    """
    Генерація ключових слів для патч-панелей.
    
    Повертає ТІЛЬКИ Блок 2 (характеристики).
    Блок 1 (модель/бренд) і Блок 3 (universal phrases) додаються GenericProcessor.

    Args:
        accessor: Accessor для характеристик
        lang: Мова (ru/ua)
        base: Базове ключове слово
        allowed: Множина дозволених характеристик

    Returns:
        Список ключових слів (тільки характеристики)
    """
    keywords = []

    # 1. Тип екранування
    if is_spec_allowed("Тип", allowed):
        screen_type = accessor.value("Тип")
        if screen_type:
            screen_type_lower = screen_type.lower()
            keywords.extend([
                f"{base} {screen_type_lower}",
                f"{screen_type_lower} {base}"
            ])

    # 2. Кількість портів
    if is_spec_allowed("Кількість портів", allowed):
        port_count = accessor.value("Кількість портів")
        if port_count:
            match = re.search(r"(\d+)", port_count)
            if match:
                count = match.group(1)
                if lang == "ru":
                    keywords.extend([
                        f"{base} {count} портов",
                        f"{base} на {count} портов"
                    ])
                else:
                    keywords.extend([
                        f"{base} {count} портів",
                        f"{base} на {count} портів"
                    ])

    # 3. Форм-фактор
    if is_spec_allowed("Форм-фактор", allowed):
        form_factor = accessor.value("Форм-фактор")
        if form_factor:
            match = re.search(r"(\d+)[\"\']?", form_factor)
            if match:
                size = match.group(1)
                keywords.extend([
                    f"{base} {size}\"",
                    f"{base} {size} дюймов" if lang == "ru" else f"{base} {size} дюймів"
                ])

    # 4. Категорія
    if is_spec_allowed("Категорія", allowed):
        category = accessor.value("Категорія")
        if category:
            category_clean = category.lower().replace("кат.", "").replace("cat.", "").strip()
            if category_clean:
                keywords.extend([
                    f"{base} cat {category_clean}",
                    f"{base} категория {category_clean}" if lang == "ru" else f"{base} категорія {category_clean}"
                ])

    # 5. Тип портів
    if is_spec_allowed("Тип портів", allowed):
        port_type = accessor.value("Тип портів")
        if port_type:
            port_type_clean = port_type.lower().replace("|", " ").strip()
            if "rj-45" in port_type_clean or "rj45" in port_type_clean:
                keywords.extend([
                    f"{base} rj45",
                    f"рж-45 {base}"
                ])
            if "rj-12" in port_type_clean or "rj12" in port_type_clean:
                keywords.extend([
                    f"{base} rj12",
                    f"рж-12 {base}"
                ])

    return keywords
