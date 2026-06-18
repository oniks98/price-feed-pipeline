"""
Роутер категорій для постачальника LP.
"""

from typing import Optional, Callable, List, Set

from keywords.core.helpers import SpecAccessor
from keywords.categories.lp import (
    battery,
    boxes,
    camera,
    dvr,
    hdd,
    intercom,
    kommutatory,
    lock,
)


# Реєстр обробників категорій
CATEGORY_HANDLERS = {
    "301105": camera.generate,      # Камери відеоспостереження
    "301102": dvr.generate,         # Відеореєстратори (DVR/NVR)
    "70704": hdd.generate,          # Жорсткі диски
    "5092913": boxes.generate,      # Монтажні коробки
    "3029": intercom.generate,      # Домофони та відеодомофони
    "301010": lock.generate,        # Замки
    "5280501": battery.generate,    # Акумулятори
    "71903": kommutatory.generate,  # Комутатори
}


def get_category_handler(
    category_id: str
) -> Optional[Callable[[SpecAccessor, str, str, Set[str]], List[str]]]:
    """
    Отримати обробник для категорії.

    Args:
        category_id: ID категорії

    Returns:
        Функція-обробник або None
    """
    return CATEGORY_HANDLERS.get(category_id)
