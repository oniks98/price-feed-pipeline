"""
services/rozetka_params_to_description_service.py
----------------------------------------------------
Будує HTML-блок «Особливості» / «Особенности» з PROM-параметрів (<param>)
і вставляє його в кінець CDATA-вмісту тегів <description_ua> та <description>
кожного офера Rozetka-фіду.

Призначення: покупець на Rozetka бачить характеристики товару прямо в тексті
опису — обома мовами, — навіть якщо конкретний Prom-параметр не потрапив
у параметричну картку Rozetka (rozetka_mappings.xlsx мапить лише категорії,
а не самі параметри товару).

Специфіка генератора Rozetka (generate_rozetka_feed.py) враховано так:

  1. Дві мовні версії опису, CDATA одразу з джерела.
     Rozetka й Kasta читають той самий FEED_URL_PROM (languages=uk,ru) —
     фід вже містить ОБИДВІ версії напряму з джерела: <name>/<description>
     (RU) і <name_ua>/<description_ua> (UA), а самі description-теги вже
     обгорнуті в CDATA одразу з Prom. Логіка вставки — той самий CDATA-aware
     підхід, що і в kasta_params_to_description_service.py: блок вставляється
     ВСЕРЕДИНІ CDATA-секції, перед закриваючим "]]>", а не перед "</description>".

  2. Без пропуску габаритів/ваги.
     Як і в Kasta-генераторі (і на відміну від Epicenter), у Rozetka немає
     окремого шару, що перемаповує вагу/габарити в спеціальні атрибути —
     сирі Prom <param> теги йдуть у фід без перемаппінгу (replace_category_ids
     чіпає лише <categoryId>, rename_country_param — лише один конкретний
     param). Дублювання не виникає, тому ця версія сервісу НЕ пропускає
     габарити/вагу — пропускаються лише параметри виробника/країни (п.3).

  3. Пропуск виробника/країни — включно з перейменованою Rozetka-формою.
     fill_missing_vendor (generate_utils_feed.py, викликається і для Rozetka,
     і для Kasta) гарантує, що кожен офер вже має структуровані <vendor> і
     <country_of_origin>. Параметри-дублікати з <param> («Компанія-виробник»,
     «Країна-виробник» — UA/RU форми) тому пропускаються, так само як
     у Kasta. Додатково пропускається «Країна-виробник товару»: Rozetka
     перейменовує «Країна-виробник» саме в цю форму (rename_country_param
     в generate_rozetka_feed.py) — пропуск обох форм робить сервіс
     безпечним незалежно від того, викликаний він до чи після перейменування.

  4. Рівень інтеграції — повний XML, а не тіло одного офера.
     generate_rozetka_feed.py, як і generate_kasta_feed.py, складається
     з послідовності простих `xml = func(xml)` викликів (apply_market_prices,
     fill_missing_vendor, sanitize_rozetka_text, ...) без окремого per-offer
     диспетчера. Тому головна публічна функція тут працює одразу на рівні
     повного XML і підключається одним рядком (після sanitize_rozetka_text —
     щоб у блок «Особливості» не потрапляв уже очищений від емодзі та
     «причини уцінки» опис, а не сирий):

         from services.rozetka_params_to_description_service import (
             inject_params_into_descriptions,
         )
         ...
         updated_xml = sanitize_rozetka_text(updated_xml)
         updated_xml = inject_params_into_descriptions(updated_xml)
"""

from __future__ import annotations

import logging
import re
from typing import Final

_logger = logging.getLogger(__name__)

# Параметри, які вже присутні в офері як окремі структуровані теги
# (<vendor>, <country_of_origin> — гарантовані fill_missing_vendor) або як
# перейменований Rozetka-специфічний param («Країна-виробник товару» —
# rename_country_param) — в блоці особливостей дублювати не треба.
# UA + RU форми (Prom-фід може віддавати назву параметра будь-якою мовою).
_SKIP_PARAMS: Final[frozenset[str]] = frozenset({
    "Компанія-виробник", "Країна-виробник", "Країна-виробник товару",
    "Компания-производитель", "Страна-производитель",
})

# Заголовки блоку особливостей — окремо для кожної мовної версії опису.
_BLOCK_HEADER_UA: Final[str] = "Особливості"
_BLOCK_HEADER_RU: Final[str] = "Особенности"

# Rozetka не документує ліміт символів на <description>/<description_ua>.
# Використовуємо той самий консервативний ліміт, що і Kasta/Epicenter (12 100),
# як безпечний дефолт — за потреби уточни фактичний ліміт Rozetka і зміни тут.
_DESCRIPTION_MAX_CHARS: Final[int] = 12_100

# ---------------------------------------------------------------------------
# Regex — одноразова компіляція
# ---------------------------------------------------------------------------

_CDATA_RE: Final[re.Pattern[str]] = re.compile(r'<!\[CDATA\[(.*?)\]\]>', re.DOTALL)

_PROM_PARAM_RE: Final[re.Pattern[str]] = re.compile(
    r'<param\b[^>]*\bname="([^"]+)"[^>]*>(.*?)</param>',
    re.DOTALL,
)

_OFFER_RE: Final[re.Pattern[str]] = re.compile(
    r'<offer\s+id="(\d+)"([^>]*)>(.*?)</offer>',
    re.DOTALL,
)


# ---------------------------------------------------------------------------
# Parsing PROM <param> тегів з тіла офера
# ---------------------------------------------------------------------------

def _strip_cdata(value: str) -> str:
    """Витягує текст з CDATA-обгортки; якщо її немає — повертає рядок як є."""
    m = _CDATA_RE.match(value.strip())
    return m.group(1).strip() if m else value.strip()


def extract_prom_params(body: str) -> dict[str, str]:
    """
    Парсить усі <param name="...">value</param> з тіла офера.

    Prom може віддавати кілька <param> з однаковим name (multiselect) —
    дублікати об'єднуються через ", " (як у epicenter/kasta сервісах).
    """
    prom_params: dict[str, str] = {}
    for m in _PROM_PARAM_RE.finditer(body):
        name = m.group(1).strip()
        value = _strip_cdata(m.group(2))
        if not value:
            continue
        if name in prom_params:
            prom_params[name] = f"{prom_params[name]}, {value}"
        else:
            prom_params[name] = value
    return prom_params


# ---------------------------------------------------------------------------
# Building the «Особливості» / «Особенности» HTML block
# ---------------------------------------------------------------------------

def build_params_block(prom_params: dict[str, str], header: str) -> str:
    """
    Будує HTML-блок особливостей з PROM-параметрів під заданий заголовок.

    Args:
        prom_params: {param_name: param_value} — вже розпарсені з офера.
        header:      текст заголовка блоку («Особливості» / «Особенности»).

    Returns:
        HTML-рядок для вставки в опис. Порожній рядок якщо немає параметрів.
    """
    items = [
        f"<li>{name}: {value}</li>"
        for name, value in prom_params.items()
        if name not in _SKIP_PARAMS and value.strip()
    ]
    if not items:
        return ""

    rows = "\n        ".join(items)
    return (
        "\n<div>"
        f"\n  <p><strong>{header}:</strong></p>"
        "\n  <ul>"
        f"\n        {rows}"
        "\n  </ul>"
        "\n</div>"
    )


def _trim_block_to_limit(existing: str, block: str, limit: int, header: str) -> str:
    """
    Повертає block, урізаний так щоб len(existing) + len(block) <= limit.

    Урізає по цілих <li>...</li> рядках щоб не ламати HTML.
    Якщо навіть заголовок не вміщується — повертає порожній рядок.
    """
    budget = limit - len(existing)
    if budget <= 0:
        return ""

    if len(block) <= budget:
        return block

    header_html = (
        "\n<div>"
        f"\n  <p><strong>{header}:</strong></p>"
        "\n  <ul>\n        "
    )
    footer_html = "\n  </ul>\n</div>"
    overhead = len(header_html) + len(footer_html)

    if overhead >= budget:
        _logger.debug("params_block trim | budget too small for header (%d chars)", budget)
        return ""

    li_budget = budget - overhead
    li_items: list[str] = re.findall(r'<li>.*?</li>', block, flags=re.DOTALL)
    kept: list[str] = []
    used = 0
    for item in li_items:
        # +len("\n        ") — роздільник між елементами
        cost = len(item) + (len("\n        ") if kept else 0)
        if used + cost > li_budget:
            break
        kept.append(item)
        used += cost

    if not kept:
        return ""

    dropped = len(li_items) - len(kept)
    if dropped:
        _logger.debug("params_block trim | dropped %d li items to fit %d-char limit", dropped, limit)

    rows = "\n        ".join(kept)
    return header_html + rows + footer_html


# ---------------------------------------------------------------------------
# Injection into a single <description>/<description_ua> tag (CDATA-aware)
# ---------------------------------------------------------------------------

def _description_tag_re(tag: str) -> re.Pattern[str]:
    """
    Regex для одного description-тегу з підтримкою CDATA і без.

    group(1) — вміст CDATA (якщо тег обгорнутий у CDATA)
    group(2) — сирий вміст (якщо CDATA відсутня)

    (?:\\s[^>]*)? матчить теги як з атрибутами, так і без.
    Точна назва тегу гарантує що "description" НЕ матчить "description_ua"
    і навпаки: після літерала тега одразу йде або ">", або пробіл —
    "_ua" не задовольняє жодному з варіантів.
    """
    return re.compile(
        rf'<{tag}(?:\s[^>]*)?>(?:<!\[CDATA\[(.*?)\]\]>|(.*?))</{tag}>',
        re.DOTALL,
    )


def inject_params_into_tag(
    body: str,
    tag: str,
    prom_params: dict[str, str],
    header: str,
) -> tuple[str, bool]:
    """
    Вставляє блок особливостей у кінець вмісту заданого description-тегу:
    перед "]]>" якщо вміст обгорнутий у CDATA, перед "</tag>" якщо ні.

    Якщо тег відсутній, параметрів немає, або блок не влазить у ліміт
    навіть після урізання — body повертається без змін (safe fallback,
    жоден офер через це не падає).

    Returns:
        (оновлений body, чи відбулась вставка)
    """
    block = build_params_block(prom_params, header)
    if not block:
        return body, False

    match = _description_tag_re(tag).search(body)
    if not match:
        _logger.debug("rozetka params_to_description | <%s> не знайдено — пропуск", tag)
        return body, False

    is_cdata = match.group(1) is not None
    existing_content = match.group(1) if is_cdata else (match.group(2) or "")

    block = _trim_block_to_limit(existing_content, block, _DESCRIPTION_MAX_CHARS, header)
    if not block:
        return body, False

    insertion_point = match.end(1) if is_cdata else match.end(2)
    return body[:insertion_point] + block + body[insertion_point:], True


# ---------------------------------------------------------------------------
# Public API — рівень повного XML (відповідає конвенції generate_rozetka_feed.py)
# ---------------------------------------------------------------------------

def inject_params_into_descriptions(xml: str) -> str:
    """
    Для кожного офера Rozetka-фіду вставляє блок «Особливості» в
    <description_ua> (UA) і блок «Особенности» в <description> (RU).

    Params для обох блоків беруться з тих самих <param> тегів офера —
    Prom-фід віддає їх мовно змішаними (напр. "Вес" поряд з
    "Компанія-виробник"), тому обидві мовні версії опису отримують
    однаковий (неперекладений) набір значень, лише під своїм заголовком.

    Не змінює офери без <param> тегів або без відповідного description-тегу
    (safe fallback — одиничний "проблемний" офер не блокує решту фіду).
    """
    offers_seen = 0
    injected_ua = 0
    injected_ru = 0

    def _on_offer(m: re.Match) -> str:
        nonlocal offers_seen, injected_ua, injected_ru
        offers_seen += 1
        offer_id, tail_attrs, body = m.group(1), m.group(2), m.group(3)

        prom_params = extract_prom_params(body)
        if not prom_params:
            return m.group(0)

        body, ua_done = inject_params_into_tag(body, "description_ua", prom_params, _BLOCK_HEADER_UA)
        injected_ua += ua_done

        body, ru_done = inject_params_into_tag(body, "description", prom_params, _BLOCK_HEADER_RU)
        injected_ru += ru_done

        if not (ua_done or ru_done):
            return m.group(0)

        return f'<offer id="{offer_id}"{tail_attrs}>{body}</offer>'

    result = _OFFER_RE.sub(_on_offer, xml)

    print(
        f"🛍️  Rozetka «Особливості»/«Особенности»: {offers_seen} офферів перевірено "
        f"| UA вставлено: {injected_ua} | RU вставлено: {injected_ru}"
    )
    return result
