"""
Генератор ключових слів для монтажних коробок.
Категорія: 5092913

СТРОГО використовує тільки allowed_specs:
- Виробник
- Матеріал корпусу
- Максимально допустиме навантаження
"""

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
    Генерація ключових слів для монтажних коробок.

    Args:
        accessor: Accessor для характеристик
        lang: Мова (ru/ua)
        base: Базове ключове слово
        allowed: Множина дозволених характеристик

    Returns:
        Список ключових слів
    """
    keywords = []

    # Базові фрази (завжди додаємо)
    if lang == "ru":
        keywords.extend([
            f"монтажная {base}",
            f"{base} для камеры"
        ])
    else:
        keywords.extend([
            f"монтажна {base}",
            f"{base} для камери"
        ])

    # 1. Матеріал корпусу (СТРОГО перевіряємо allowed_specs)
    if is_spec_allowed("Матеріал корпусу", allowed):
        material = accessor.value("Матеріал корпусу")
        if material:
            material_lower = material.lower()
            
            if "метал" in material_lower or "алюм" in material_lower:
                if lang == "ru":
                    keywords.append(f"{base} металлическая")
                else:
                    keywords.append(f"{base} металева")
            
            elif "пластик" in material_lower:
                if lang == "ru":
                    keywords.append(f"{base} пластиковая")
                else:
                    keywords.append(f"{base} пластикова")

    # 2. Максимально допустиме навантаження (СТРОГО перевіряємо allowed_specs)
    if is_spec_allowed("Максимально допустиме навантаження", allowed):
        max_load = accessor.value("Максимально допустиме навантаження")
        if max_load and any(char.isdigit() for char in max_load):
            if lang == "ru":
                keywords.append(f"{base} усиленная")
            else:
                keywords.append(f"{base} посилена")

    return keywords
