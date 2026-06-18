"""
Генератор ключових слів для акумуляторів.
Категорія: 5280501

СТРОГО використовує тільки allowed_specs:
- Виробник
- Тип акумулятора
- Ємність акумулятору
- Напруга

ВАЖЛИВО: Обробник повертає ТІЛЬКИ Блок 2 (характеристики)!
Блок 1 (модель/бренд) та Блок 3 (універсальні фрази) обробляються GenericProcessor.
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
    Генерація ключових слів для акумуляторів.
    
    Повертає ТІЛЬКИ Блок 2 (характеристики).

    Args:
        accessor: Accessor для характеристик
        lang: Мова (ru/ua)
        base: Базове ключове слово
        allowed: Множина дозволених характеристик

    Returns:
        Список ключових слів (тільки характеристики)
    """
    keywords = []

    # Тип акумулятора
    if is_spec_allowed("Тип акумулятора", allowed):
        battery_type = accessor.value("Тип акумулятора")
        if battery_type:
            battery_type_upper = battery_type.upper()
            
            # AGM
            if "AGM" in battery_type_upper:
                if lang == "ru":
                    keywords.extend([
                        f"{base} ажм",
                        f"agm {base}"
                    ])
                else:
                    keywords.extend([
                        f"{base} ажм",
                        f"agm {base}"
                    ])
            
            # GEL (гелевий)
            elif "GEL" in battery_type_upper:
                if lang == "ru":
                    keywords.extend([
                        f"гелевый {base}",
                        f"{base} гелевый",
                        f"{base} gel"
                    ])
                else:
                    keywords.extend([
                        f"гелевий {base}",
                        f"{base} гелевий",
                        f"{base} gel"
                    ])
            
            # LiFePO4 (літій-залізо-фосфатний)
            elif "LIFEPO4" in battery_type_upper or "LFP" in battery_type_upper:
                if lang == "ru":
                    keywords.extend([
                        f"{base} lifepo4",
                        f"литиевый {base}",
                        f"{base} литий железо фосфатный"
                    ])
                else:
                    keywords.extend([
                        f"{base} lifepo4",
                        f"літієвий {base}",
                        f"{base} літій залізо фосфатний"
                    ])

    # Ємність акумулятору
    if is_spec_allowed("Ємність акумулятору", allowed):
        capacity_value = accessor.value("Ємність акумулятору")
        capacity_unit = accessor.unit("Ємність акумулятору")
        
        if capacity_value:
            # Витягуємо число з значення
            capacity_match = re.search(r'(\d+)', str(capacity_value))
            if capacity_match:
                capacity_num = capacity_match.group(1)
                
                # Визначаємо одиницю виміру
                unit_lower = ""
                if capacity_unit:
                    # Обробляємо варіанти: "А. г", "А•г", "Ач", "Ah", "А год"
                    unit_clean = capacity_unit.replace(" ", "").replace("•", "").replace(".", "").lower()
                    if "аг" in unit_clean or "ач" in unit_clean or "ah" in unit_clean.lower():
                        unit_lower = "ач" if lang == "ru" else "аг"
                
                # Якщо одиниця не визначена, використовуємо значення за замовчуванням
                if not unit_lower:
                    unit_lower = "ач" if lang == "ru" else "аг"
                
                # Генеруємо ключові слова
                keywords.extend([
                    f"{base} {capacity_num}{unit_lower}",
                    f"{capacity_num} {unit_lower} {base} "
                ])

    # Напруга
    if is_spec_allowed("Напруга", allowed):
        voltage_value = accessor.value("Напруга")
        voltage_unit = accessor.unit("Напруга")
        
        if voltage_value:
            # Витягуємо число з значення
            voltage_match = re.search(r'(\d+)', str(voltage_value))
            if voltage_match:
                voltage_num = voltage_match.group(1)
                
                # Визначаємо одиницю виміру (зазвичай "В" або "V")
                unit_lower = "в"
                if voltage_unit:
                    if voltage_unit.lower() in ["в", "v", "вольт", "volt"]:
                        unit_lower = "в"
                
                # Генеруємо ключові слова
                keywords.extend([
                    f"{base} {voltage_num}{unit_lower}",
                    f"{voltage_num} {unit_lower} {base}"
                ])

    return keywords
