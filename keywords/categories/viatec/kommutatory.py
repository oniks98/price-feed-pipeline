"""
Генератор ключових слів для комутаторів.
Категорія: 71903
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
    Генерація ключових слів для комутаторів.
    
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

    # 1. Тип комутатора
    if is_spec_allowed("Тип комутатора", allowed):
        switch_type = accessor.value("Тип комутатора")
        if switch_type:
            switch_type_lower = switch_type.lower()
            
            # Некерований
            if "некерований" in switch_type_lower or "неуправляемый" in switch_type_lower:
                if lang == "ru":
                    keywords.extend([
                        f"{base} неуправляемый",
                        f"неуправляемый {base}"
                    ])
                else:
                    keywords.extend([
                        f"{base} некерований",
                        f"некерований {base}"
                    ])
            
            # Smart
            if "smart" in switch_type_lower:
                keywords.extend([
                    f"{base} smart",
                    f"smart {base}"
                ])
            
            # Керований рівня 2
            if "керований" in switch_type_lower or "управляемый" in switch_type_lower:
                if "рівня 2" in switch_type_lower or "уровня 2" in switch_type_lower or switch_type_lower.endswith("2"):
                    if lang == "ru":
                        keywords.extend([
                            f"{base} управляемый",
                            f"управляемый {base}",
                            f"{base} l2",
                            f"l2 {base}"
                        ])
                    else:
                        keywords.extend([
                            f"{base} керований",
                            f"керований {base}",
                            f"{base} l2",
                            f"l2 {base}"
                        ])
                
                # Керований рівня 3
                if "рівня 3" in switch_type_lower or "уровня 3" in switch_type_lower or "вище" in switch_type_lower or "выше" in switch_type_lower:
                    keywords.extend([
                        f"{base} l3",
                        f"l3 {base}"
                    ])

    # 2. Підтримка PoE
    if is_spec_allowed("Підтримка PoE", allowed):
        poe = accessor.value("Підтримка PoE")
        if poe and poe.lower() == "так":
            keywords.extend([
                f"{base} poe",
                f"poe {base}"
            ])
            if lang == "ru":
                keywords.append(f"{base} с поддержкой poe")
            else:
                keywords.append(f"{base} з підтримкою poe")

    # 3. Форм-фактор
    if is_spec_allowed("Форм-фактор", allowed):
        form_factor = accessor.value("Форм-фактор")
        if form_factor:
            form_factor_lower = form_factor.lower()
            
            # Настільний
            if "настільн" in form_factor_lower or "настольн" in form_factor_lower:
                if lang == "ru":
                    keywords.extend([
                        f"{base} настольный",
                        f"настольный {base}"
                    ])
                else:
                    keywords.extend([
                        f"{base} настільний",
                        f"настільний {base}"
                    ])
            
            # Зовнішній
            if "зовнішн" in form_factor_lower or "внешн" in form_factor_lower:
                if lang == "ru":
                    keywords.append(f"внешний {base}")
                else:
                    keywords.append(f"зовнішній {base}")
            
            # Навісний
            if "навісн" in form_factor_lower or "навесн" in form_factor_lower:
                if lang == "ru":
                    keywords.append(f"навесной {base}")
                else:
                    keywords.append(f"навісний {base}")

    # 4. Тип портів
    if is_spec_allowed("Тип портів", allowed):
        port_type = accessor.value("Тип портів")
        if port_type:
            port_type_lower = port_type.lower()
            
            # Fast Ethernet
            if "fast ethernet" in port_type_lower or "fast" in port_type_lower:
                if lang == "ru":
                    keywords.extend([
                        f"fast {base}",
                        f"{base} fast ethernet",
                        f"фаст {base}"
                    ])
                else:
                    keywords.extend([
                        f"fast {base}",
                        f"{base} fast ethernet",
                        f"фаст {base}"
                    ])
            
            # Gigabit Ethernet
            if "gigabit" in port_type_lower or "1000" in port_type_lower:
                if lang == "ru":
                    keywords.extend([
                        f"gigabit {base}",
                        f"{base} gigabit ethernet",
                        f"гигабитный {base}"
                    ])
                else:
                    keywords.extend([
                        f"gigabit {base}",
                        f"{base} gigabit ethernet",
                        f"гігабітний {base}"
                    ])
            
            # 10 Gigabit Ethernet
            if "10gigabit" in port_type_lower or "10g" in port_type_lower:
                keywords.extend([
                    f"10g {base}",
                    f"10 gigabit {base}",
                    f"{base} 10gbe"
                ])
            
            # SFP
            if "sfp" in port_type_lower:
                keywords.extend([
                    f"{base} sfp",
                    f"sfp {base}"
                ])

    # 5. Загальна кількість портів
    if is_spec_allowed("Загальна кількість портів", allowed):
        port_count = accessor.value("Загальна кількість портів")
        if port_count:
            match = re.search(r"(\d+)", port_count)
            if match:
                count = match.group(1)
                if lang == "ru":
                    keywords.extend([
                        f"{base} {count} портов",
                        f"{base} на {count} портов",
                        f"{count} портовый {base}",
                        f"{count}p {base}"
                    ])
                else:
                    keywords.extend([
                        f"{base} {count} портів",
                        f"{base} на {count} портів",
                        f"{count} портовий {base}",
                        f"{count}p {base}"
                    ])

    # 6. Можливість монтажу в стійку
    if is_spec_allowed("Можливість монтажу в стійку", allowed):
        rack_mount = accessor.value("Можливість монтажу в стійку")
        if rack_mount and rack_mount.lower() == "так":
            if lang == "ru":
                keywords.extend([
                    f"{base} rack",
                    f"rack {base}",
                    f"{base} в стойку",
                    f"{base} 19\""
                ])
            else:
                keywords.extend([
                    f"{base} rack",
                    f"rack {base}",
                    f"{base} в стійку",
                    f"{base} 19\""
                ])

    return keywords
