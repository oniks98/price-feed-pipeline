"""
Утиліти для роботи з характеристиками товарів.
"""

import re
from typing import Optional, Dict, Set

from keywords.core.helpers import SpecAccessor


def extract_capacity(specs: SpecAccessor, name: str) -> Optional[Dict[str, any]]:
    """
    Універсальне витягування об'єму (для HDD/SD/USB).

    Args:
        specs: Accessor для характеристик
        name: Назва характеристики (наприклад, "Об'єм накопичувача")

    Returns:
        {"formatted": "128gb", "size_gb": 128} або None
    """
    raw = specs.value(name)
    if not raw:
        return None

    # Витягуємо число
    match = re.search(r"(\d+)", raw)
    if not match:
        return None

    size = int(match.group(1))
    unit = (specs.unit(name) or "").upper()

    if unit not in ["GB", "TB", "MB"]:
        return None

    # Конвертуємо в GB
    if unit == "GB":
        size_gb = size
        formatted = f"{size // 1000}tb" if size >= 1000 else f"{size}gb"
    elif unit == "TB":
        size_gb = size * 1000
        formatted = f"{size}tb"
    elif unit == "MB":
        size_gb = size / 1000
        formatted = f"{size}mb"
    else:
        return None

    return {
        "formatted": formatted,
        "size_gb": size_gb
    }


def extract_speed(specs: SpecAccessor, name: str) -> Optional[str]:
    """
    Витягування швидкості (для SD-карт).

    Args:
        specs: Accessor для характеристик
        name: Назва характеристики (наприклад, "Швидкість зчитування")

    Returns:
        Швидкість у вигляді числа (наприклад, "90") або None
    """
    raw = specs.value(name)
    if not raw:
        return None

    # Шукаємо число
    match = re.search(r"(\d+)", raw)
    return match.group(1) if match else None


def extract_interface(specs: SpecAccessor, name: str) -> Optional[str]:
    """
    Витягування інтерфейсу (для HDD/USB).

    Args:
        specs: Accessor для характеристик
        name: Назва характеристики (наприклад, "Інтерфейс")

    Returns:
        Інтерфейс в нижньому регістрі (наприклад, "sata", "usb type-c") або None
    """
    raw = specs.value(name)
    if not raw:
        return None

    value_lower = raw.lower()

    # Перевіряємо популярні інтерфейси
    if "sata" in value_lower:
        return "sata"
    elif "m.2" in value_lower or "m2" in value_lower:
        return "m.2"
    elif "nvme" in value_lower:
        return "nvme"
    elif "sas" in value_lower:
        return "sas"
    elif "ide" in value_lower:
        return "ide"
    elif "type-c" in value_lower or "type c" in value_lower:
        return "usb type-c"
    elif "usb 3" in value_lower or "3." in value_lower:
        return "usb 3.0"
    elif "usb 2" in value_lower or "2." in value_lower:
        return "usb 2.0"

    return raw  # Повертаємо як є, якщо не розпізнали


def extract_rpm(specs: SpecAccessor, name: str) -> Optional[str]:
    """
    Витягування швидкості обертання (для HDD).

    Args:
        specs: Accessor для характеристик
        name: Назва характеристики (наприклад, "Швидкість обертання")

    Returns:
        Швидкість обертання (наприклад, "7200") або None
    """
    raw = specs.value(name)
    if not raw:
        return None

    # Шукаємо число (5400, 7200, 10000)
    match = re.search(r"(\d{4,5})", raw)
    return match.group(1) if match else None


def is_spec_allowed(spec_name: str, allowed: Set[str]) -> bool:
    """
    Строга перевірка дозволених характеристик.
    
    Використовує точне співпадіння (exact match), а не fuzzy match.
    Це запобігає випадковим співпадінням типу "тип" → "тип корпусу".

    Args:
        spec_name: Назва характеристики
        allowed: Множина дозволених характеристик (в lower case)

    Returns:
        True, якщо характеристика дозволена (exact match)
    """
    if not allowed:
        return True

    return spec_name.lower() in allowed
