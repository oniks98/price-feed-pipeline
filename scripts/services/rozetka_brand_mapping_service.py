"""
services/rozetka_brand_mapping_service.py
-------------------------------------------
Канонізує бренди у Rozetka-фіді за вимогами модерації Розетки:
    - написання бренду не відповідає офіційному (TelStream → Anker)
    - бренд відсутній ("Без бренда"/"Без бренду") — Розетка не приймає
      офери без бренду, тому підставляємо Anker.

Правила зберігаються прямо у коді (_BRAND_CORRECTIONS) — список короткий
і змінюється рідко. Щоб додати нове правило — додай рядок у словник нижче
(ключ у нижньому регістрі, як його поверне _normalize()).

Використання в generate_rozetka_feed.py (ПІСЛЯ fill_missing_vendor,
ДО filter_stop_brand_offers — щоб стоп-бренди фільтрувались вже по
канонічній, виправленій назві):

    from services.rozetka_brand_mapping_service import remap_rozetka_vendors
    updated_xml = remap_rozetka_vendors(updated_xml)
"""

from __future__ import annotations

import re
from collections import Counter
from typing import Final

# {нормалізований оригінальний бренд: (новий vendor, нова country_of_origin)}
# Ключі порівнюються через _normalize() (casefold + схлопування пробілів) —
# регістр і зайві пробіли у вихідному фіді значення не мають.
_BRAND_CORRECTIONS: Final[dict[str, tuple[str, str]]] = {
    "nvc": ("Anker", "Китай"),
    "oem": ("Anker", "Китай"),
    "telstream": ("Anker", "Китай"),
    "faraday electronics": ("Faraday", "Китай"),
    "mustang energy": ("Mustang", "Китай"),
    "без бренда": ("Anker", "Китай"),
    "без бренду": ("Anker", "Китай"),
}

_OFFER_RE: Final[re.Pattern[str]] = re.compile(
    r'<offer\s+id="([^"]+)"([^>]*)>(.*?)</offer>', re.DOTALL
)
_VENDOR_RE: Final[re.Pattern[str]] = re.compile(r"<vendor>(.*?)</vendor>", re.DOTALL)
_COUNTRY_RE: Final[re.Pattern[str]] = re.compile(
    r"<country_of_origin>(.*?)</country_of_origin>", re.DOTALL
)


def _normalize(value: str) -> str:
    """Casefold + схлопування пробілів — ключ для порівняння з _BRAND_CORRECTIONS."""
    return re.sub(r"\s+", " ", value).strip().casefold()


def remap_rozetka_vendors(
    xml: str,
    corrections: dict[str, tuple[str, str]] | None = None,
) -> str:
    """
    Замінює <vendor> (і синхронно <country_of_origin>, якщо тег присутній)
    за таблицею _BRAND_CORRECTIONS. Точний матч по нормалізованому значенню
    <vendor> — регістр і пробіли ігноруються.

    Один "поганий" офер (відсутній <vendor>) не валить обробку решти —
    regex просто не знаходить збігу і офер лишається без змін.

    Args:
        xml:         Повний XML-фід.
        corrections: Override таблиці для тестів; продакшн використовує
                     _BRAND_CORRECTIONS.

    Returns:
        Оновлений XML.
    """
    rules = _BRAND_CORRECTIONS if corrections is None else corrections
    applied: Counter[str] = Counter()

    def _on_offer(m: re.Match[str]) -> str:
        offer_id, tail_attrs, body = m.group(1), m.group(2), m.group(3)

        vendor_match = _VENDOR_RE.search(body)
        if not vendor_match:
            return m.group(0)

        original = vendor_match.group(1).strip()
        rule = rules.get(_normalize(original))
        if rule is None:
            return m.group(0)

        new_vendor, new_country = rule
        body = body.replace(vendor_match.group(0), f"<vendor>{new_vendor}</vendor>", 1)

        country_match = _COUNTRY_RE.search(body)
        if country_match:
            body = body.replace(
                country_match.group(0),
                f"<country_of_origin>{new_country}</country_of_origin>",
                1,
            )

        applied[f"{original} → {new_vendor}"] += 1
        return f'<offer id="{offer_id}"{tail_attrs}>{body}</offer>'

    result = _OFFER_RE.sub(_on_offer, xml)

    total = sum(applied.values())
    if total:
        summary = ", ".join(f"{k} ({v})" for k, v in applied.most_common())
        print(f"🏷️  Rozetka заміна брендів: замінено {total} товарів | {summary}")
    else:
        print("🏷️  Rozetka заміна брендів: збігів у фіді немає")

    return result
