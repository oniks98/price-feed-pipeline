"""
Генератор ключових слів для кронштейнів та кожухів для камер.
Категорія: 301112

СТРОГО використовує тільки allowed_specs:
- Виробник
- Матеріал
- Тип кріплення кронштейна
- Маx нагрузка на кронштейн
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
    Генерація ключових слів для кронштейнів та кожухів.

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
            f"{base} для камеры",
            f"крепление для видеокамеры"
        ])
    else:
        keywords.extend([
            f"{base} для камери",
            f"кріплення для відеокамери"
        ])

    # 1. Матеріал (СТРОГО перевіряємо allowed_specs)
    if is_spec_allowed("Матеріал", allowed):
        material = accessor.value("Матеріал")
        if material:
            material_lower = material.lower()
            
            if "метал" in material_lower or "алюм" in material_lower:
                if lang == "ru":
                    keywords.append(f"{base} металлический")
                else:
                    keywords.append(f"{base} металевий")
            
            elif "пластик" in material_lower:
                if lang == "ru":
                    keywords.append(f"{base} пластиковый")
                else:
                    keywords.append(f"{base} пластиковий")

    # 2. Тип кріплення кронштейна (СТРОГО перевіряємо allowed_specs)
    if is_spec_allowed("Тип кріплення кронштейна", allowed):
        mount_type = accessor.value("Тип кріплення кронштейна")
        if mount_type:
            mount_lower = mount_type.lower()
            
            # Настінне кріплення
            if "настінн" in mount_lower or "настенн" in mount_lower or "стіна" in mount_lower or "стена" in mount_lower:
                if lang == "ru":
                    keywords.append(f"{base} настенный")
                    keywords.append(f"настенный {base}")
                else:
                    keywords.append(f"{base} настінний")
                    keywords.append(f"настінний {base}")
            
            # Стельове кріплення
            elif "стельов" in mount_lower or "потолоч" in mount_lower or "стеля" in mount_lower or "потолок" in mount_lower:
                if lang == "ru":
                    keywords.append(f"{base} потолочный")
                    keywords.append(f"потолочный {base}")
                else:
                    keywords.append(f"{base} стельовий")
                    keywords.append(f"стельовий {base}")
            
            # Кріплення для монтажу на стовп/балку
            elif "стовп" in mount_lower or "балк" in mount_lower or "столб" in mount_lower:
                if lang == "ru":
                    keywords.append(f"{base} для столба")
                    keywords.append(f"{base} на столб")
                else:
                    keywords.append(f"{base} для стовпа")
                    keywords.append(f"{base} на стовп")
            
            # Кріплення для монтажу камери відеоспостереження на кут
            elif "кут" in mount_lower or "угол" in mount_lower or "кутов" in mount_lower or "углов" in mount_lower:
                if lang == "ru":
                    keywords.append(f"{base} угловой")
                    keywords.append(f"угловой {base}")
                else:
                    keywords.append(f"{base} кутовий")
                    keywords.append(f"кутовий {base}")
            
            # Стельове кріплення для купольних камер
            elif "купол" in mount_lower:
                if lang == "ru":
                    keywords.append(f"{base} для купольной камеры")
                    keywords.append(f"{base} купольный")
                else:
                    keywords.append(f"{base} для купольної камери")
                    keywords.append(f"{base} купольний")

    # 3. Максимальне навантаження (СТРОГО перевіряємо allowed_specs)
    if is_spec_allowed("Маx нагрузка на кронштейн", allowed):
        max_load = accessor.value("Маx нагрузка на кронштейн")
        if max_load and any(char.isdigit() for char in max_load):
            if lang == "ru":
                keywords.append(f"{base} усиленный")
            else:
                keywords.append(f"{base} посилений")

    return keywords
