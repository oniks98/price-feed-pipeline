"""
Генератор ключових слів для домофонів та відеодомофонів.
Категорія: 3029

СТРОГО використовує тільки allowed_specs:

Для відеодомофонів (монітори):
- Виробник
- Діагональ екрану (дюйм)
- Порт для SD-карти
- Інтерфейс
- Протокол зв'язку

Для викличних панелей:
- Виробник
- Кількість абонентів
- Кут огляду камери по горизонталі
- Роздільна здатність камери (ТВЛ)
- Роздільна здатність камери (Мп)
- Протокол зв'язку

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
    Генерація ключових слів для домофонів та відеодомофонів.
    
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

    # Визначаємо тип пристрою за ЗНАЧЕННЯМ характеристики "Тип устройства"
    device_type = None
    device_type_value = accessor.value("Тип устройства")
    
    if device_type_value:
        device_type_lower = device_type_value.lower()
        
        # Відеодомофон (монітор)
        if "відеодомофон" in device_type_lower or "видеодомофон" in device_type_lower:
            device_type = "monitor"
        # Відеопанель (виклична панель)
        elif "відеопанель" in device_type_lower or "видеопанель" in device_type_lower or "виклична" in device_type_lower:
            device_type = "panel"
        # Аудіодомофон (тільки голос, без характеристик)
        elif "аудіодомофон" in device_type_lower or "аудиодомофон" in device_type_lower:
            device_type = "audio"

    if not device_type:
        return keywords

    # Аудіодомофони не мають специфічних характеристик
    if device_type == "audio":
        return keywords

    # Обробка монітора відеодомофону
    if device_type == "monitor":
        keywords.extend(_generate_monitor_keywords(accessor, lang, base, allowed))

    # Обробка викличної панелі
    elif device_type == "panel":
        keywords.extend(_generate_panel_keywords(accessor, lang, base, allowed))

    return keywords


def _generate_monitor_keywords(
    accessor: SpecAccessor,
    lang: str,
    base: str,
    allowed: Set[str]
) -> List[str]:
    """Генерація ключових слів для моніторів відеодомофонів (тільки характеристики)"""
    keywords = []

    # 1. Діагональ екрану - ЧИСЛО ДЮЙМІВ
    if is_spec_allowed("Діагональ екрану (дюйм)", allowed):
        diagonal = accessor.value("Діагональ екрану (дюйм)")
        if diagonal:
            match = re.search(r"(\d+(?:\.\d+)?)", diagonal)
            if match:
                size = match.group(1)
                try:
                    size_num = float(size)
                    if size_num == int(size_num):
                        size = str(int(size_num))
                except ValueError:
                    pass
                
                if lang == "ru":
                    keywords.extend([
                        f"{base} {size} дюймов"
                    ])
                else:
                    keywords.extend([
                        f"{base} {size} дюймів"
                    ])

    # 2. Порт для SD-карти - З ЗАПИСОМ / З СД КАРТОЙ
    if is_spec_allowed("Порт для SD-карти", allowed):
        sd_port = accessor.value("Порт для SD-карти")
        if sd_port and ("так" in sd_port.lower() or "yes" in sd_port.lower() or "є" in sd_port.lower()):
            if lang == "ru":
                keywords.extend([
                    f"{base} с записью",
                    f"{base} с сд картой"
                ])
            else:
                keywords.extend([
                    f"{base} з записом",
                    f"{base} з сд карткою"
                ])

    # 3. Інтерфейс - БЕЗДРОТОВИЙ = ВАЙ ФАЙ / WI FI
    if is_spec_allowed("Інтерфейс", allowed):
        interface = accessor.value("Інтерфейс")
        if interface:
            interface_lower = interface.lower()
            
            # Wi-Fi / Бездротовий
            if "wi-fi" in interface_lower or "wifi" in interface_lower or "бездротов" in interface_lower or "беспровод" in interface_lower:
                if lang == "ru":
                    keywords.extend([
                        f"{base} вай фай",
                        f"wi fi {base}"
                    ])
                else:
                    keywords.extend([
                        f"{base} вай фай",
                        f"wi fi {base}"
                    ])

    # 4. Протокол зв'язку - RJ-45 = IP + АЙ ПИ
    if is_spec_allowed("Протокол зв'язку", allowed):
        protocol = accessor.value("Протокол зв'язку")
        if protocol:
            protocol_lower = protocol.lower()
            
            # RJ-45 = IP протокол
            if "rj-45" in protocol_lower or "rj45" in protocol_lower or "ethernet" in protocol_lower:
                if lang == "ru":
                    keywords.extend([
                        f"ip {base}",
                        f"{base} ай пи"
                    ])
                else:
                    keywords.extend([
                        f"ip {base}",
                        f"{base} ай пі"
                    ])

    return keywords


def _generate_panel_keywords(
    accessor: SpecAccessor,
    lang: str,
    base: str,
    allowed: Set[str]
) -> List[str]:
    """Генерація ключових слів для викличних панелей (тільки характеристики)"""
    keywords = []

    # 1. Кількість абонентів - БОЛЬШЕ 2 = НА ЧИСЛО АБОНЕНТА
    if is_spec_allowed("Кількість абонентів", allowed):
        subscribers = accessor.value("Кількість абонентів")
        if subscribers:
            match = re.search(r"(\d+)", subscribers)
            if match:
                count = int(match.group(1))
                
                # Тільки якщо більше 2 абонентів
                if count > 2:
                    if lang == "ru":
                        keywords.append(f"{base} на {count} абонента")
                    else:
                        keywords.append(f"{base} на {count} абонентів")

    # 2. Кут огляду камери - БОЛЬШЕ 90 = ШИРОКОУГОЛЬНАЯ
    # Перевіряємо обидва варіанти: загальний та з уточненням
    view_angle = None
    if is_spec_allowed("Кут огляду камери", allowed):
        view_angle = accessor.value("Кут огляду камери")
    
    if not view_angle and is_spec_allowed("Кут огляду камери по горизонталі", allowed):
        view_angle = accessor.value("Кут огляду камери по горизонталі")
    
    if view_angle:
        match = re.search(r"(\d+)", view_angle)
        if match:
            angle = int(match.group(1))
            
            # Широкий кут огляду (понад 90 градусів)
            if angle > 90:
                if lang == "ru":
                    keywords.extend([
                        f"{base} с широкоугольной камерой",
                        f"широкоугольная {base}"
                    ])
                else:
                    keywords.extend([
                        f"{base} з ширококутною камерою",
                        f"ширококутна {base}"
                    ])

    # 3. Роздільна здатність камери (ТВЛ) - ЧИСЛО = АНАЛОГОВАЯ
    if is_spec_allowed("Роздільна здатність камери (ТВЛ)", allowed):
        resolution_tvl = accessor.value("Роздільна здатність камери (ТВЛ)")
        if resolution_tvl:
            match = re.search(r"(\d+)", resolution_tvl)
            if match:
                if lang == "ru":
                    keywords.append(f"аналоговая {base}")
                else:
                    keywords.append(f"аналогова {base}")

    # 4. Роздільна здатність камери (Мп) - ЧИСЛО = ЦИФРОВАЯ
    if is_spec_allowed("Роздільна здатність камери (Мп)", allowed):
        resolution_mp = accessor.value("Роздільна здатність камери (Мп)")
        if resolution_mp:
            match = re.search(r"(\d+(?:\.\d+)?)", resolution_mp)
            if match:
                mp = match.group(1)
                if lang == "ru":
                    keywords.extend([
                        f"{base} {mp} мп",
                        f"цифровая {base}"
                    ])
                else:
                    keywords.extend([
                        f"{base} {mp} мп",
                        f"цифрова {base}"
                    ])

    # 5. Протокол зв'язку - RJ-45 = IP + АЙ ПИ
    if is_spec_allowed("Протокол зв'язку", allowed):
        protocol = accessor.value("Протокол зв'язку")
        if protocol:
            protocol_lower = protocol.lower()
            
            # RJ-45 = IP протокол
            if "rj-45" in protocol_lower or "rj45" in protocol_lower or "ethernet" in protocol_lower:
                if lang == "ru":
                    keywords.extend([
                        f"ip {base}",
                        f"{base} ай пи"
                    ])
                else:
                    keywords.extend([
                        f"ip {base}",
                        f"{base} ай пі"
                    ])

    return keywords
