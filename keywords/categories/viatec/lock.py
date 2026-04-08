"""
Генератор ключових слів для замків.
Категорія: 301010

СТРОГО використовує тільки allowed_specs:
- Виробник
- Тип замка
- Тип встановлення замку
- Протокол зв'язку
- Відкриття пристрою
- Управління пристроєм

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
    Генерація ключових слів для замків.
    
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

    # 1. Тип замка
    if is_spec_allowed("Тип замка", allowed):
        lock_type = accessor.value("Тип замка")
        if lock_type:
            lock_type_lower = lock_type.lower()
            
            # Електромагнітний
            if "електромагніт" in lock_type_lower or "электромагнит" in lock_type_lower:
                if lang == "ru":
                    keywords.extend([
                        f"электромагнитный {base}",
                        f"{base} электромагнитный"
                    ])
                else:
                    keywords.extend([
                        f"електромагнітний {base}",
                        f"{base} електромагнітний"
                    ])
            
            # Електромеханічний (включає електрозащелки і умні замки)
            elif "електромеханіч" in lock_type_lower or "электромеханич" in lock_type_lower:
                if lang == "ru":
                    keywords.extend([
                        f"электромеханический {base}",
                        f"{base} электромеханический",
                        f"электрозащелка"
                    ])
                else:
                    keywords.extend([
                        f"електромеханічний {base}",
                        f"{base} електромеханічний",
                        f"електрозахисна засувка"
                    ])
            
            # Ригельна система
            elif "ригельн" in lock_type_lower:
                if lang == "ru":
                    keywords.extend([
                        f"ригельный {base}",
                        f"{base} ригельный"
                    ])
                else:
                    keywords.extend([
                        f"ригельний {base}",
                        f"{base} ригельний"
                    ])
            
            # Біометричний
            elif "біометрич" in lock_type_lower or "биометрич" in lock_type_lower:
                if lang == "ru":
                    keywords.extend([
                        f"биометрический {base}",
                        f"{base} биометрический",
                        f"{base} с отпечатком пальца"
                    ])
                else:
                    keywords.extend([
                        f"біометричний {base}",
                        f"{base} біометричний",
                        f"{base} з відбитком пальця"
                    ])

    # 2. Тип встановлення замку
    if is_spec_allowed("Тип встановлення замку", allowed):
        installation = accessor.value("Тип встановлення замку")
        if installation:
            installation_lower = installation.lower()
            
            # Накладний
            if "накладн" in installation_lower:
                if lang == "ru":
                    keywords.extend([
                        f"накладной {base}",
                        f"{base} накладной"
                    ])
                else:
                    keywords.extend([
                        f"накладний {base}",
                        f"{base} накладний"
                    ])
            
            # Врізний
            elif "врізн" in installation_lower or "врезн" in installation_lower:
                if lang == "ru":
                    keywords.extend([
                        f"врезной {base}",
                        f"{base} врезной"
                    ])
                else:
                    keywords.extend([
                        f"врізний {base}",
                        f"{base} врізний"
                    ])

    # 3. Протокол зв'язку
    if is_spec_allowed("Протокол зв'язку", allowed):
        protocol = accessor.value("Протокол зв'язку")
        if protocol:
            protocol_lower = protocol.lower()
            
            # Wi-Fi
            if "wi-fi" in protocol_lower or "wifi" in protocol_lower:
                if lang == "ru":
                    keywords.extend([
                        f"{base} с wifi",
                        f"wifi {base}",
                        f"{base} с вай фай"
                    ])
                else:
                    keywords.extend([
                        f"{base} з wifi",
                        f"wifi {base}",
                        f"{base} з вай фай"
                    ])
            
            # Bluetooth
            if "bluetooth" in protocol_lower or "блютуз" in protocol_lower:
                if lang == "ru":
                    keywords.extend([
                        f"{base} с bluetooth",
                        f"bluetooth {base}",
                        f"{base} с блютуз"
                    ])
                else:
                    keywords.extend([
                        f"{base} з bluetooth",
                        f"bluetooth {base}",
                        f"{base} з блютуз"
                    ])

    # 4. Відкриття пристрою
    if is_spec_allowed("Відкриття пристрою", allowed):
        opening = accessor.value("Відкриття пристрою")
        if opening:
            opening_lower = opening.lower()
            
            # Ідентифікатором (картка)
            if "ідентифікатор" in opening_lower or "идентификатор" in opening_lower:
                if lang == "ru":
                    keywords.extend([
                        f"{base} с картой",
                        f"{base} по карте"
                    ])
                else:
                    keywords.extend([
                        f"{base} з карткою",
                        f"{base} по картці"
                    ])
            
            # Кодонабірна клавіатура
            if "кодонабор" in opening_lower or "код" in opening_lower:
                if lang == "ru":
                    keywords.extend([
                        f"{base} с кодом",
                        f"{base} с клавиатурой",
                        f"кодовый {base}"
                    ])
                else:
                    keywords.extend([
                        f"{base} з кодом",
                        f"{base} з клавіатурою",
                        f"кодовий {base}"
                    ])
            
            # Відбиток пальця
            if "відбиток" in opening_lower or "отпечаток" in opening_lower or "паль" in opening_lower:
                if lang == "ru":
                    keywords.extend([
                        f"{base} с отпечатком пальца",
                        f"{base} по отпечатку пальца"
                    ])
                else:
                    keywords.extend([
                        f"{base} з відбитком пальця",
                        f"{base} по відбитку пальця"
                    ])
            
            # Мобільний телефон
            if "мобільн" in opening_lower or "мобильн" in opening_lower or "телефон" in opening_lower:
                if lang == "ru":
                    keywords.extend([
                        f"{base} с телефона",
                        f"{base} через телефон"
                    ])
                else:
                    keywords.extend([
                        f"{base} з телефону",
                        f"{base} через телефон"
                    ])

    # 5. Управління пристроєм
    if is_spec_allowed("Управління пристроєм", allowed):
        control = accessor.value("Управління пристроєм")
        if control:
            control_lower = control.lower()
            
            # Мобільний телефон
            if "мобільн" in control_lower or "мобильн" in control_lower or "телефон" in control_lower:
                if lang == "ru":
                    keywords.extend([
                        f"{base} с управлением через телефон",
                        f"умный {base}",
                        f"смарт {base}"
                    ])
                else:
                    keywords.extend([
                        f"{base} з керуванням через телефон",
                        f"розумний {base}",
                        f"смарт {base}"
                    ])

    return keywords
