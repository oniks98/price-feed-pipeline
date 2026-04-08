"""
Генератор ключових слів для шаф, стійок, кронштейнів та рам.
Категорія: 70306

СТРОГО використовує тільки allowed_specs:
- Виробник
- Тип корпусу
- Варіант установки
- Тип монтажу
- Максимально допустима статичне навантаження
- Робоча висота (U)
- Колір шафи

Типи пристроїв визначаються за ЗНАЧЕННЯМ характеристики "Тип корпусу":
1. Шафа серверна
2. Шафа телекомунікаційна
3. Бокс електромонтажний
4. Антивандальний ящик
5. Стійка монтажна
6. Стійка серверна
7. Кронштейн
8. Поворотна рама

ВАЖЛИВО: Обробник повертає ТІЛЬКИ Блок 2 (характеристики)!
Блок 1 (модель/бренд) та Блок 3 (універсальні фрази) обробляються GenericProcessor.
Бренд + base додається в GenericProcessor._generate_spec_keywords(), НЕ тут!
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
    Генерація ключових слів для шаф, стійок, кронштейнів та рам.
    
    Повертає ТІЛЬКИ Блок 2 (характеристики).
    НЕ додає бренд - це робить GenericProcessor!

    Args:
        accessor: Accessor для характеристик
        lang: Мова (ru/ua)
        base: Базове ключове слово
        allowed: Множина дозволених характеристик

    Returns:
        Список ключових слів (тільки характеристики)
    """
    keywords = []

    # Визначаємо тип пристрою за ЗНАЧЕННЯМ характеристики "Тип корпусу"
    device_type = None
    device_type_value = accessor.value("Тип корпусу")
    
    if device_type_value:
        device_type_lower = device_type_value.lower()
        
        # Шафа серверна
        if "шафа серверна" in device_type_lower or "шкаф серверный" in device_type_lower or "серверн" in device_type_lower:
            device_type = "server_cabinet"
        # Шафа телекомунікаційна
        elif "телекомунікаційн" in device_type_lower or "телекоммуникацион" in device_type_lower:
            device_type = "telecom_cabinet"
        # Бокс електромонтажний
        elif "бокс" in device_type_lower and ("електромонтаж" in device_type_lower or "электромонтаж" in device_type_lower):
            device_type = "electrical_box"
        # Антивандальний ящик
        elif "антивандал" in device_type_lower or ("ящик" in device_type_lower and "антивандал" in base.lower()):
            device_type = "vandal_box"
        # Кронштейн
        elif "кронштейн" in device_type_lower:
            device_type = "bracket"
        # Поворотна рама
        elif "поворот" in device_type_lower and "рам" in device_type_lower:
            device_type = "rotating_frame"
        # Стійка серверна
        elif "стійка серверна" in device_type_lower or "стойка серверная" in device_type_lower or ("стійка" in device_type_lower and "серверн" in device_type_lower) or ("стойка" in device_type_lower and "серверн" in device_type_lower):
            device_type = "server_rack"
        # Стійка монтажна
        elif "стійка" in device_type_lower or "стойка" in device_type_lower:
            device_type = "mounting_rack"

    if not device_type:
        return keywords

    # Обробка різних типів шаф/стійок
    if device_type == "server_cabinet":
        keywords.extend(_generate_server_cabinet_keywords(accessor, lang, base, allowed))
    elif device_type == "telecom_cabinet":
        keywords.extend(_generate_telecom_cabinet_keywords(accessor, lang, base, allowed))
    elif device_type == "electrical_box":
        keywords.extend(_generate_electrical_box_keywords(accessor, lang, base, allowed))
    elif device_type == "vandal_box":
        keywords.extend(_generate_vandal_box_keywords(accessor, lang, base, allowed))
    elif device_type == "mounting_rack":
        keywords.extend(_generate_mounting_rack_keywords(accessor, lang, base, allowed))
    elif device_type == "server_rack":
        keywords.extend(_generate_server_rack_keywords(accessor, lang, base, allowed))
    elif device_type == "bracket":
        keywords.extend(_generate_bracket_keywords(accessor, lang, base, allowed))
    elif device_type == "rotating_frame":
        keywords.extend(_generate_rotating_frame_keywords(accessor, lang, base, allowed))

    return keywords


def _generate_server_cabinet_keywords(
    accessor: SpecAccessor,
    lang: str,
    base: str,
    allowed: Set[str]
) -> List[str]:
    """Генерація ключових слів для серверних шаф (тільки характеристики)"""
    keywords = []

    # 1. Варіант установки (Настінний, Підлоговий)
    if is_spec_allowed("Варіант установки", allowed):
        installation = accessor.value("Варіант установки")
        if installation:
            installation_lower = installation.lower()
            
            if "настен" in installation_lower or "настін" in installation_lower:
                if lang == "ru":
                    keywords.extend([
                        f"{base} настенный",
                        f"настенный {base}"
                    ])
                else:
                    keywords.extend([
                        f"{base} настінний",
                        f"настінний {base}"
                    ])
            
            if "підлог" in installation_lower or "напол" in installation_lower:
                if lang == "ru":
                    keywords.extend([
                        f"{base} напольный",
                        f"напольный {base}"
                    ])
                else:
                    keywords.extend([
                        f"{base} підлоговий",
                        f"підлоговий {base}"
                    ])

    # 2. Тип монтажу (Навісний, Вбудований)
    if is_spec_allowed("Тип монтажу", allowed):
        mounting = accessor.value("Тип монтажу")
        if mounting:
            mounting_lower = mounting.lower()
            
            if "навіс" in mounting_lower or "навес" in mounting_lower:
                if lang == "ru":
                    keywords.extend([
                        f"{base} навесной",
                        f"навесной {base}"
                    ])
                else:
                    keywords.extend([
                        f"{base} навісний",
                        f"навісний {base}"
                    ])
            
            if "вбудов" in mounting_lower or "встра" in mounting_lower:
                if lang == "ru":
                    keywords.extend([
                        f"{base} встраиваемый",
                        f"встраиваемый {base}"
                    ])
                else:
                    keywords.extend([
                        f"{base} вбудований",
                        f"вбудований {base}"
                    ])

    # 3. Максимально допустима статичне навантаження (до X кг)
    if is_spec_allowed("Максимально допустима статичне навантаження", allowed):
        load = accessor.value("Максимально допустима статичне навантаження")
        if load:
            match = re.search(r"(\d+)", load)
            if match:
                load_value = match.group(1)
                if lang == "ru":
                    keywords.append(f"{base} с нагрузкой до {load_value} кг")
                else:
                    keywords.append(f"{base} з навантаженням до {load_value} кг")

    # 4. Робоча висота (U)
    if is_spec_allowed("Робоча висота (U)", allowed):
        height_u = accessor.value("Робоча висота (U)")
        if height_u:
            # Видаляємо "U" якщо воно є
            height_clean = height_u.replace("U", "").replace("u", "").strip()
            match = re.search(r"(\d+)", height_clean)
            if match:
                u_value = match.group(1)
                keywords.append(f"{base} {u_value}U")

    # 5. Колір шафи (Чорний, Сірий)
    if is_spec_allowed("Колір шафи", allowed):
        color = accessor.value("Колір шафи")
        if color:
            color_lower = color.lower()
            
            if "чорн" in color_lower or "черн" in color_lower:
                if lang == "ru":
                    keywords.extend([
                        f"{base} черный",
                        f"черный {base}"
                    ])
                else:
                    keywords.extend([
                        f"{base} чорний",
                        f"чорний {base}"
                    ])
            
            if "сір" in color_lower or "сер" in color_lower:
                if lang == "ru":
                    keywords.extend([
                        f"{base} серый",
                        f"серый {base}"
                    ])
                else:
                    keywords.extend([
                        f"{base} сірий",
                        f"сірий {base}"
                    ])

    return keywords


def _generate_telecom_cabinet_keywords(
    accessor: SpecAccessor,
    lang: str,
    base: str,
    allowed: Set[str]
) -> List[str]:
    """Генерація ключових слів для телекомунікаційних шаф (тільки характеристики)"""
    # Використовуємо ту саму логіку, що й для серверних шаф
    return _generate_server_cabinet_keywords(accessor, lang, base, allowed)


def _generate_electrical_box_keywords(
    accessor: SpecAccessor,
    lang: str,
    base: str,
    allowed: Set[str]
) -> List[str]:
    """Генерація ключових слів для електромонтажних боксів (тільки характеристики)"""
    keywords = []

    # 1. Варіант установки / Тип монтажу (Настінний, Вбудований, Зовнішній)
    installation = None
    if is_spec_allowed("Варіант установки", allowed):
        installation = accessor.value("Варіант установки")
    
    if not installation and is_spec_allowed("Тип монтажу", allowed):
        installation = accessor.value("Тип монтажу")
    
    if installation:
        installation_lower = installation.lower()
        
        if "настен" in installation_lower or "настін" in installation_lower:
            if lang == "ru":
                keywords.extend([
                    f"{base} настенный",
                    f"настенный {base}"
                ])
            else:
                keywords.extend([
                    f"{base} настінний",
                    f"настінний {base}"
                ])
        
        if "вбудов" in installation_lower or "встра" in installation_lower:
            if lang == "ru":
                keywords.extend([
                    f"{base} встраиваемый",
                    f"встраиваемый {base}"
                ])
            else:
                keywords.extend([
                    f"{base} вбудований",
                    f"вбудований {base}"
                ])
        
        if "зовніш" in installation_lower or "наруж" in installation_lower or "назовн" in installation_lower:
            if lang == "ru":
                keywords.extend([
                    f"{base} наружный",
                    f"наружный {base}"
                ])
            else:
                keywords.extend([
                    f"{base} зовнішній",
                    f"зовнішній {base}"
                ])
        
        if "навіс" in installation_lower or "навес" in installation_lower:
            if lang == "ru":
                keywords.extend([
                    f"{base} навесной",
                    f"навесной {base}"
                ])
            else:
                keywords.extend([
                    f"{base} навісний",
                    f"навісний {base}"
                ])

    # 2. Робоча висота (U) - якщо є
    if is_spec_allowed("Робоча висота (U)", allowed):
        height_u = accessor.value("Робоча висота (U)")
        if height_u:
            height_clean = height_u.replace("U", "").replace("u", "").strip()
            match = re.search(r"(\d+)", height_clean)
            if match:
                u_value = match.group(1)
                keywords.append(f"{base} {u_value}U")

    return keywords


def _generate_vandal_box_keywords(
    accessor: SpecAccessor,
    lang: str,
    base: str,
    allowed: Set[str]
) -> List[str]:
    """Генерація ключових слів для антивандальних ящиків (тільки характеристики)"""
    # Використовуємо ту саму логіку, що й для боксів
    return _generate_electrical_box_keywords(accessor, lang, base, allowed)


def _generate_mounting_rack_keywords(
    accessor: SpecAccessor,
    lang: str,
    base: str,
    allowed: Set[str]
) -> List[str]:
    """Генерація ключових слів для монтажних стійок (тільки характеристики)"""
    keywords = []

    # 1. Робоча висота (U)
    if is_spec_allowed("Робоча висота (U)", allowed):
        height_u = accessor.value("Робоча висота (U)")
        if height_u:
            height_clean = height_u.replace("U", "").replace("u", "").strip()
            match = re.search(r"(\d+)", height_clean)
            if match:
                u_value = match.group(1)
                keywords.append(f"{base} {u_value}U")

    # 2. Максимально допустима статичне навантаження
    if is_spec_allowed("Максимально допустима статичне навантаження", allowed):
        load = accessor.value("Максимально допустима статичне навантаження")
        if load:
            match = re.search(r"(\d+)", load)
            if match:
                load_value = match.group(1)
                if lang == "ru":
                    keywords.append(f"{base} с нагрузкой до {load_value} кг")
                else:
                    keywords.append(f"{base} з навантаженням до {load_value} кг")

    return keywords


def _generate_server_rack_keywords(
    accessor: SpecAccessor,
    lang: str,
    base: str,
    allowed: Set[str]
) -> List[str]:
    """Генерація ключових слів для серверних стійок (тільки характеристики)"""
    # Використовуємо ту саму логіку, що й для монтажних стійок
    return _generate_mounting_rack_keywords(accessor, lang, base, allowed)


def _generate_bracket_keywords(
    accessor: SpecAccessor,
    lang: str,
    base: str,
    allowed: Set[str]
) -> List[str]:
    """Генерація ключових слів для кронштейнів (тільки характеристики)"""
    keywords = []

    # 1. Робоча висота (U)
    if is_spec_allowed("Робоча висота (U)", allowed):
        height_u = accessor.value("Робоча висота (U)")
        if height_u:
            height_clean = height_u.replace("U", "").replace("u", "").strip()
            match = re.search(r"(\d+)", height_clean)
            if match:
                u_value = match.group(1)
                keywords.append(f"{base} {u_value}U")

    # 2. Максимально допустима статичне навантаження
    if is_spec_allowed("Максимально допустима статичне навантаження", allowed):
        load = accessor.value("Максимально допустима статичне навантаження")
        if load:
            match = re.search(r"(\d+)", load)
            if match:
                load_value = match.group(1)
                if lang == "ru":
                    keywords.append(f"{base} с нагрузкой до {load_value} кг")
                else:
                    keywords.append(f"{base} з навантаженням до {load_value} кг")

    return keywords


def _generate_rotating_frame_keywords(
    accessor: SpecAccessor,
    lang: str,
    base: str,
    allowed: Set[str]
) -> List[str]:
    """Генерація ключових слів для поворотних рам (тільки характеристики)"""
    # Використовуємо ту саму логіку, що й для кронштейнів
    return _generate_bracket_keywords(accessor, lang, base, allowed)
