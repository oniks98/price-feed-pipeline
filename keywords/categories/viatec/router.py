"""
Роутер категорій для постачальника Viatec.
"""

from typing import Optional, Callable, List, Set

from keywords.core.helpers import SpecAccessor
from keywords.categories.viatec import (
    hdd, 
    sd_card, 
    usb_flash, 
    mounts, 
    boxes, 
    intercom, 
    lock, 
    battery,
    camera,
    dvr,
    kommutatory
)


# Реєстр обробників категорій
CATEGORY_HANDLERS = {
    "301105": camera.generate,   # Камери відеоспостереження
    "301101": dvr.generate,      # Відеореєстратори (DVR/NVR)
    "70704": hdd.generate,       # Жорсткі диски
    "63705": sd_card.generate,   # SD-карти
    "70501": usb_flash.generate, # USB-флешки
    "301112": mounts.generate,   # Кронштейни та кожухи
    "5092913": boxes.generate,   # Монтажні коробки
    "3029": intercom.generate,   # Домофони та відеодомофони
    "301010": lock.generate,     # Замки
    "5280501": battery.generate, # Акумулятори
    "71903": kommutatory.generate, # Комутатори
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
