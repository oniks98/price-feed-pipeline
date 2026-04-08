"""
Роутер категорій для eserver.
Маппінг category_id -> функція-обробник.
"""

from typing import Callable, Optional

from keywords.categories.eserver import cabinet, panel

CATEGORY_HANDLERS = {
    "70306": cabinet.generate,
    "5092902": panel.generate,
}


def get_category_handler(category_id: str) -> Optional[Callable]:
    """Отримати обробник категорії за ID"""
    return CATEGORY_HANDLERS.get(category_id)
