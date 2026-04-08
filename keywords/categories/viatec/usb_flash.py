"""
Генератор ключових слів для USB-флешок.
Категорія: 70501
"""

from typing import List, Set

from keywords.core.helpers import SpecAccessor
from keywords.utils.spec_helpers import (
    extract_capacity,
    extract_interface,
    is_spec_allowed
)


def generate(
    accessor: SpecAccessor,
    lang: str,
    base: str,
    allowed: Set[str]
) -> List[str]:
    """
    Генерація ключових слів для USB-флешок.

    Args:
        accessor: Accessor для характеристик
        lang: Мова (ru/ua)
        base: Базове ключове слово
        allowed: Множина дозволених характеристик

    Returns:
        Список ключових слів
    """
    keywords = []

    if not is_spec_allowed("Об'єм пам'яті", allowed):
        return keywords

    # 1. Об'єм пам'яті
    capacity_info = extract_capacity(accessor, "Об'єм пам'яті")
    if not capacity_info:
        return keywords

    capacity = capacity_info["formatted"]

    if lang == "ru":
        keywords.extend([
            f"флешка {capacity}",
            f"usb флешка {capacity}",
            f"{capacity} флешка",
            f"флеш накопитель {capacity}",
            "usb флешка для ноутбука",
            "флешка для компьютера"
        ])
    else:
        keywords.extend([
            f"флешка {capacity}",
            f"usb флешка {capacity}",
            f"{capacity} флешка",
            f"флеш накопичувач {capacity}",
            "usb флешка для ноутбука",
            "флешка для комп'ютера"
        ])

    # 2. Інтерфейс (USB Type-C, USB 3.0, USB 2.0)
    if is_spec_allowed("Інтерфейс", allowed):
        interface = extract_interface(accessor, "Інтерфейс")
        if interface:
            interface_lower = interface.lower()
            if "type-c" in interface_lower or "type c" in interface_lower:
                keywords.append("usb type-c флешка")
            elif "3." in interface_lower or "usb 3" in interface_lower:
                keywords.append("usb 3.0 флешка")
            elif "2." in interface_lower or "usb 2" in interface_lower:
                keywords.append("usb 2.0 флешка")

    # 3. Форм-фактор
    if is_spec_allowed("Форм-фактор", allowed):
        form_factor = accessor.value("Форм-фактор")
        if form_factor and "моноблок" in form_factor.lower():
            if lang == "ru":
                keywords.append("компактная флешка")
            else:
                keywords.append("компактна флешка")

    return keywords
