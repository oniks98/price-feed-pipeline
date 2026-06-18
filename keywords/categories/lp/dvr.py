"""
Генератор ключових слів для відеореєстраторів (DVR/NVR).
Категорія: 301102
"""

import re
from typing import List, Set, Optional

from keywords.core.helpers import SpecAccessor
from keywords.utils.spec_helpers import is_spec_allowed


def generate(
    accessor: SpecAccessor,
    lang: str,
    base: str,
    allowed: Set[str],
    name: str = ""
) -> List[str]:
    """
    Генерація ключових слів для відеореєстраторів.
    
    Повертає ТІЛЬКИ Блок 2 (характеристики).
    Блок 1 (модель/бренд) і Блок 3 (universal phrases) додаються GenericProcessor.

    Args:
        accessor: Accessor для характеристик
        lang: Мова (ru/ua)
        base: Базове ключове слово
        allowed: Множина дозволених характеристик
        name: Назва товару (для AI технологій)

    Returns:
        Список ключових слів (тільки характеристики)
    """
    keywords = []

    # 1. Кількість каналів (формат: "N-канальний відеореєстратор")
    if is_spec_allowed("Кількість каналів", allowed):
        channels = _get_channels(accessor)
        if channels:
            if lang == "ru":
                keywords.append(f"{channels}-канальный {base}")
            else:
                keywords.append(f"{channels}-канальний {base}")

    # 2. Тип відеореєстратора (IP/NVR, Аналоговий/Гібридний/XVR)
    if is_spec_allowed("Тип відеореєстратора", allowed):
        dvr_type_keywords = _get_dvr_type_keywords(accessor, lang, base)
        keywords.extend(dvr_type_keywords)

    # 3. Підтримка PoE
    if is_spec_allowed("Підтримка PoE", allowed):
        poe_keywords = _get_poe_keywords(accessor, lang, base)
        keywords.extend(poe_keywords)

    # 4. AI технології (WizSense/AcuSense)
    if name:
        ai_keywords = _get_ai_keywords(name, lang, base)
        keywords.extend(ai_keywords)

    return keywords


def _get_channels(accessor: SpecAccessor) -> Optional[str]:
    """Витягування кількості каналів"""
    value = accessor.value("Кількість каналів")
    if not value:
        return None

    # Витягуємо число зі значення
    match = re.search(r"\d+", value)
    if match:
        return match.group(0)

    return None


def _get_dvr_type_keywords(
    accessor: SpecAccessor,
    lang: str,
    base: str
) -> List[str]:
    """
    Визначення типу відеореєстратора.
    
    Типи:
    - IP/NVR: мережевий відеореєстратор
    - HDVR/XVR: аналоговий/гібридний/мультиформатний
    """
    value = accessor.value("Тип відеореєстратора")
    if not value:
        return []

    value_lower = value.lower()
    keywords = []

    # 1. IP відеореєстратор (NVR)
    if "ip" in value_lower or "nvr" in value_lower:
        if lang == "ru":
            keywords.extend([
                f"ip {base}",
                f"айпи {base}",
                f"сетевой {base}"
            ])
        else:
            keywords.extend([
                f"ip {base}",
                f"айпі {base}",
                f"мережевий {base}"
            ])

    # 2. HDVR/XVR (аналоговий/гібридний/мультиформатний)
    elif "hdvr" in value_lower or "xvr" in value_lower:
        if lang == "ru":
            keywords.extend([
                f"аналоговый {base}",
                f"гибридный {base}",
                f"мультиформатный {base}"
            ])
        else:
            keywords.extend([
                f"аналоговий {base}",
                f"гібридний {base}",
                f"мультиформатний {base}"
            ])

    return keywords


def _get_poe_keywords(
    accessor: SpecAccessor,
    lang: str,
    base: str
) -> List[str]:
    """
    Перевірка підтримки PoE.
    
    Якщо підтримується - повертає варіанти фраз з PoE.
    """
    value = accessor.value("Підтримка PoE")
    if not value or value.strip().lower() != "так":
        return []

    # Повертаємо всі варіанти ключових фраз
    if lang == "ru":
        return [
            f"пое {base}",
            f"{base} с пое",
            "nvr poe",
            "регистратор poe"
        ]
    else:
        return [
            f"пое {base}",
            f"{base} з пое",
            "nvr poe",
            "реєстратор poe"
        ]


def _get_ai_keywords(
    name: str,
    lang: str,
    base: str
) -> List[str]:
    """
    Витягування ключових слів для AI технологій.
    
    Технології:
    - WizSense (Dahua): інтелектуальна аналітика
    - AcuSense (Hikvision): розпізнавання людей/транспорту
    """
    name_lower = name.lower()
    keywords = []

    # Перевірка на WizSense (Dahua)
    if "wizsense" in name_lower:
        if lang == "ru":
            keywords.extend([
                f"{base} с ai",
                f"умный {base}",
                f"{base} с искусственным интеллектом",
                f"wizsense {base}"
            ])
        else:
            keywords.extend([
                f"{base} з ai",
                f"розумний {base}",
                f"{base} зі штучним інтелектом",
                f"wizsense {base}"
            ])

    # Перевірка на AcuSense (Hikvision)
    elif "acusense" in name_lower:
        if lang == "ru":
            keywords.extend([
                f"{base} с ai",
                f"умный {base}",
                f"{base} с искусственным интеллектом",
                f"acusense {base}"
            ])
        else:
            keywords.extend([
                f"{base} з ai",
                f"розумний {base}",
                f"{base} зі штучним інтелектом",
                f"acusense {base}"
            ])

    return keywords
