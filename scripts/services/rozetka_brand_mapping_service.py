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

Окрім <vendor>/<country_of_origin>, Rozetka дублює виробника й країну
у <param name="Компанія-виробник">/<param name="Країна-виробник ..."> (вони
показуються окремо у картці товару й на них теж лається модерація — саме
цей param лишався незмінним до фіксу, хоча <vendor> вже виправлявся).
Обидва місця виправляються НЕЗАЛЕЖНО одне від одного — кожне звіряється зі
своїм поточним значенням проти _BRAND_CORRECTIONS. Так реальний виробник,
який іноді трапляється лише в param (напр. "KPL", "CINLINELE" — коли у
офера немає <vendor>, але param заповнений вручну/з джерела), ніколи не
затирається значенням "Anker": його значення просто відсутнє у таблиці
правил і лишається без змін.

Param країни зустрічається під двома назвами залежно від того, ДО чи ПІСЛЯ
rename_country_param() (generate_rozetka_feed.py) викликається ця функція:
Prom віддає "Країна-виробник", Rozetka очікує "Країна-виробник товару".
_COUNTRY_PARAM_RE матчить обидві — функція коректна незалежно від порядку.

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
    "kpl": ("Anker", "Китай"),
    "psu": ("Kraft", "Китай"),
    "cinlinele": ("Anker", "Китай"),
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

# Назви <param>, якими Rozetka дублює виробника й країну-виробника всередині
# офера. Групи захоплення: (1) відкриваючий тег цілком, (2) значення,
# (3) закриваючий тег — так атрибути param (напр. unit="") лишаються
# недоторканими, міняється лише вміст.
_VENDOR_PARAM_RE: Final[re.Pattern[str]] = re.compile(
    r'(<param\b[^>]*\bname="Компанія-виробник"[^>]*>)(.*?)(</param>)', re.DOTALL
)
# Матчить ОБИДВІ назви країни-виробника:
#   - "Країна-виробник"        — сира назва Prom; саме вона ще стоїть у XML
#     на момент виклику remap_rozetka_vendors() у generate_rozetka_feed.py,
#     бо rename_country_param() перейменовує її на "...товару" ПІЗНІШЕ.
#   - "Країна-виробник товару" — назва вже після перейменування (напр. якщо
#     цю функцію запустили повторно/окремо на готовому rozetka_feed.xml).
# Альтернатива у групі + обов'язкова закриваюча лапка одразу після неї
# однозначно розрізняють обидва варіанти без ризику часткового збігу.
_COUNTRY_PARAM_RE: Final[re.Pattern[str]] = re.compile(
    r'(<param\b[^>]*\bname="(?:Країна-виробник товару|Країна-виробник)"[^>]*>)(.*?)(</param>)',
    re.DOTALL,
)


def _normalize(value: str) -> str:
    """Casefold + схлопування пробілів — ключ для порівняння з _BRAND_CORRECTIONS."""
    return re.sub(r"\s+", " ", value).strip().casefold()


def _correct_vendor_param(
    body: str,
    rules: dict[str, tuple[str, str]],
) -> tuple[str, str | None]:
    """
    Виправляє <param name="Компанія-виробник"> і парний <param
    name="Країна-виробник товару"> (якщо присутній) за таблицею rules.

    Звіряється з ПОТОЧНИМ значенням самого param, незалежно від <vendor> —
    один "поганий" param (без відповідного <vendor> чи з відмінним від
    нього значенням) не блокує перевірку, а справжній виробник, якого
    немає у rules, ніколи не перезаписується.

    Args:
        body:  Тіло одного <offer> (все, що між <offer ...> і </offer>).
        rules: Таблиця відповідностей — _BRAND_CORRECTIONS або override для тестів.

    Returns:
        (можливо оновлений body, "original → new_vendor" якщо була заміна,
        інакше (body, None) без змін).
    """
    param_match = _VENDOR_PARAM_RE.search(body)
    if param_match is None:
        return body, None

    original = param_match.group(2).strip()
    rule = rules.get(_normalize(original))
    if rule is None:
        return body, None

    new_vendor, new_country = rule
    body = body.replace(
        param_match.group(0),
        f"{param_match.group(1)}{new_vendor}{param_match.group(3)}",
        1,
    )

    country_match = _COUNTRY_PARAM_RE.search(body)
    if country_match is not None:
        body = body.replace(
            country_match.group(0),
            f"{country_match.group(1)}{new_country}{country_match.group(3)}",
            1,
        )

    return body, f"{original} → {new_vendor}"


def remap_rozetka_vendors(
    xml: str,
    corrections: dict[str, tuple[str, str]] | None = None,
) -> str:
    """
    Замінює <vendor> (і синхронно <country_of_origin>, якщо тег присутній),
    а також <param name="Компанія-виробник"> (і синхронно <param
    name="Країна-виробник товару">) за таблицею _BRAND_CORRECTIONS.
    Точний матч по нормалізованому значенню — регістр і пробіли ігноруються.

    <vendor> і param виправляються незалежно один від одного (кожен звіряється
    зі своїм власним значенням), тому один товар рахується у підсумку лише
    один раз, навіть якщо виправлено і тег, і param.

    Один "поганий" офер (відсутній <vendor> й/або param) не валить обробку
    решти — regex просто не знаходить збігу і офер лишається без змін.

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
        label: str | None = None

        vendor_match = _VENDOR_RE.search(body)
        if vendor_match is not None:
            original = vendor_match.group(1).strip()
            rule = rules.get(_normalize(original))
            if rule is not None:
                new_vendor, new_country = rule
                body = body.replace(
                    vendor_match.group(0), f"<vendor>{new_vendor}</vendor>", 1
                )

                country_match = _COUNTRY_RE.search(body)
                if country_match is not None:
                    body = body.replace(
                        country_match.group(0),
                        f"<country_of_origin>{new_country}</country_of_origin>",
                        1,
                    )

                label = f"{original} → {new_vendor}"

        body, param_label = _correct_vendor_param(body, rules)
        label = label or param_label

        if label is None:
            return m.group(0)

        applied[label] += 1
        return f'<offer id="{offer_id}"{tail_attrs}>{body}</offer>'

    result = _OFFER_RE.sub(_on_offer, xml)

    total = sum(applied.values())
    if total:
        summary = ", ".join(f"{k} ({v})" for k, v in applied.most_common())
        print(f"🏷️  Rozetka заміна брендів: замінено {total} товарів | {summary}")
    else:
        print("🏷️  Rozetka заміна брендів: збігів у фіді немає")

    return result
