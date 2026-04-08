"""
Моделі даних для генератора ключових слів.
"""

from typing import TypedDict, Optional

# Імпорт констант з окремого модуля (для уникнення циклічних залежностей)
from keywords.constants import (
    CAMERA_TECHNOLOGIES,
    MAX_MODEL_KEYWORDS,
    MAX_SPEC_KEYWORDS,
    MAX_UNIVERSAL_KEYWORDS,
    MAX_TOTAL_KEYWORDS,
)


class Spec(TypedDict, total=False):
    """
    Структура характеристики товару.
    
    Fields:
        name: Назва характеристики (обов'язково)
        value: Значення характеристики (обов'язково)
        unit: Одиниця виміру (опціонально)
    """
    name: str
    value: str
    unit: Optional[str]


# Реекспорт констант для зворотної сумісності
# Використовується в:
# - keywords/processors/viatec/generic.py
# - keywords/processors/eserver/generic.py
# - keywords/processors/secur/generic.py
__all__ = [
    "Spec",
    "CAMERA_TECHNOLOGIES",
    "MAX_MODEL_KEYWORDS",
    "MAX_SPEC_KEYWORDS",
    "MAX_UNIVERSAL_KEYWORDS",
    "MAX_TOTAL_KEYWORDS",
]
