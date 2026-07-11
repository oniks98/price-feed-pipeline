"""
services/kasta_params_to_description_service.py
-------------------------------------------------
Будує HTML-блок «Особливості» / «Особенности» з PROM-параметрів (<param>)
і вставляє його в кінець CDATA-вмісту тегів <description_ua> та <description>
кожного офера Kasta-фіду.

Призначення: покупець на Kasta бачить характеристики товару прямо в тексті
опису — обома мовами, — навіть якщо їх немає у структурованому вигляді
на сторінці маркетплейсу.

Специфіка генератора Kasta (generate_kasta_feed.py) враховано так:

  1. Дві мовні версії опису.
     Kasta-фід (FEED_URL_PROM, languages=uk,ru) вже містить ОБИДВІ версії
     напряму з джерела: <name>/<description> (RU) і <name_ua>/<description_ua>
     (UA). Блок особливостей тому будується і вставляється в ОБИДВІ версії,
     з відповідним заголовком («Особливості» / «Особенности»).

  2. CDATA з джерела.
     На відміну від Epicenter (де CDATA з'являється пізніше, окремим кроком
     normalize_name_description_tags), у Kasta-фіді description-теги вже
     обгорнуті в CDATA одразу з Prom. Вставка блоку відбувається ВСЕРЕДИНІ
     CDATA-секції — перед закриваючим "]]>", а не перед "</description>".

  3. Без пропуску габаритів/ваги.
     Epicenter-версія цього сервісу пропускає системні параметри (вага,
     габарити, кратність) — бо вони дублюються окремими Epicenter-атрибутами
     <param paramcode="weight">, що генеруються пізніше в пайплайні.
     У Kasta-генераторі такого шару немає: сирі Prom <param> теги
     залишаються у фіді без перемаппінгу. Тому дублювання не виникає, і
     ця версія сервісу НЕ пропускає габарити/вагу — пропускаються лише
     параметри виробника/країни, які й так дублюються окремими тегами
     офера (<vendor>, <country_of_origin>).

  4. Рівень інтеграції — повний XML, а не тіло одного офера.
     generate_kasta_feed.py складається з послідовності простих
     `xml = func(xml)` викликів (add_name_ua, fill_missing_vendor, ...)
     без окремого per-offer диспетчера на кшталт inject_epicenter_attrs
     у Epicenter-генераторі. Тому головна публічна функція тут працює
     одразу на рівні повного XML і підключається одним рядком:

         from services.kasta_params_to_description_service import (
             inject_params_into_descriptions,
         )
         ...
         updated_xml = add_name_ua(updated_xml)
         updated_xml = inject_params_into_descriptions(updated_xml)
"""

from __future__ import annotations

import logging
import re
from typing import Final

_logger = logging.getLogger(__name__)

# Параметри, які вже присутні в офері як окремі теги (<vendor>, <country_of_origin>) —
# в блоці особливостей дублювати не треба.
# UA + RU форми (Prom-фід може віддавати назву параметра будь-якою мовою).
_SKIP_PARAMS: Final[frozenset[str]] = frozenset({
    "Компанія-виробник", "Країна-виробник",
    "Компания-производитель", "Страна-производитель",
})

# Заголовки блоку особливостей — окремо для кожної мовної версії опису.
_BLOCK_HEADER_UA: Final[str] = "Особливості"
_BLOCK_HEADER_RU: Final[str] = "Особенности"

# Kasta не документує ліміт символів на <description>/<description_ua>.
# Використовуємо той самий консервативний ліміт, що і Epicenter (12 100),
# як безпечний дефолт — за потреби уточни фактичний ліміт Kasta і зміни тут.
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
    дублікати об'єднуються через ", " (як у epicenter/rozetka сервісах).
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

    (?:\\s[^>]*)? матчить теги як з атрибутами (lang="ua"), так і без.
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
        _logger.debug("kasta params_to_description | <%s> не знайдено — пропуск", tag)
        return body, False

    is_cdata = match.group(1) is not None
    existing_content = match.group(1) if is_cdata else (match.group(2) or "")

    block = _trim_block_to_limit(existing_content, block, _DESCRIPTION_MAX_CHARS, header)
    if not block:
        return body, False

    insertion_point = match.end(1) if is_cdata else match.end(2)
    return body[:insertion_point] + block + body[insertion_point:], True


# ---------------------------------------------------------------------------
# Public API — рівень повного XML (відповідає конвенції generate_kasta_feed.py)
# ---------------------------------------------------------------------------

def inject_params_into_descriptions(xml: str) -> str:
    """
    Для кожного офера Kasta-фіду вставляє блок «Особливості» в
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
        f"📋 Kasta «Особливості»/«Особенности»: {offers_seen} офферів перевірено "
        f"| UA вставлено: {injected_ua} | RU вставлено: {injected_ru}"
    )
    return result
