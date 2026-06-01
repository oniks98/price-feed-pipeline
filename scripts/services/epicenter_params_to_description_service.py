"""
services/epicenter_params_to_description_service.py
----------------------------------------------------
Будує HTML-блок «Особливості» з PROM-параметрів і вставляє його
в кінець тегу <description_ua> офера.

Призначення: покупець на Епіцентрі бачить реальні характеристики
з PROM-фіду, навіть якщо вони не вкладаються в епіцентрівські фільтри.

Формат вставки (органічно продовжує наявний HTML):
    <div>
      <p><strong>Особливості:</strong></p>
      <ul>
        <li>Дальність читання: 10 см</li>
        <li>Підтримка стандартів: EM Marine (EM4100)</li>
        ...
      </ul>
    </div>

Цільовий тег (Prom-формат, до rename в normalize_name_description_tags):
    <description_ua> → майбутній <description lang="ua"> → заголовок "Особливості"

Системні параметри (габарити, вага, кратність) пропускаються —
вони вже передаються як атрибути Епіцентра.
"""

from __future__ import annotations

import logging
import re
from typing import Final

_logger = logging.getLogger(__name__)

# Параметри, які вже йдуть як атрибути Epicenter — в описі дублювати не треба.
# Розширюй список якщо додаєш нові системні атрибути.
# Параметри з _ATTRS (generate_epicenter_feed.py): UA + RU назви.
# Якщо додаєш новий системний атрибут — дублюй обидві мови тут.
_SKIP_PARAMS: Final[frozenset[str]] = frozenset({
    # UA
    "Ширина", "Висота", "Довжина", "Глибина", "Вага", "Кратність",
    "Компанія-виробник", "Країна-виробник",
    # RU (Prom-фід може віддавати будь-якою мовою)
    "Высота", "Длина", "Глубина", "Вес", "Кратность",
    "Компания-производитель", "Страна-производитель",
})

# Заголовок блоку особливостей (тільки UA)
_BLOCK_HEADER: Final[str] = "Особливості"

# Максимальна довжина опису в Epicenter (символів).
# Блок «Особливості» урізається якщо його додавання перевищує ліміт.
_DESCRIPTION_MAX_CHARS: Final[int] = 12_100

# Regex для закриваючого тегу <description> (Prom UK-only фід, до rename).
_DESC_CLOSE_RE: Final[re.Pattern[str]] = re.compile(
    r'(</description>)', re.IGNORECASE
)


def build_params_block(prom_params: dict[str, str]) -> str:
    """
    Будує HTML-блок «Особливості» з PROM-параметрів.

    Args:
        prom_params: {param_name: param_value} — вже розпарсені з офера.

    Returns:
        HTML-рядок для вставки в description_ua.
        Порожній рядок якщо немає параметрів для відображення.
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
        f"\n  <p><strong>{_BLOCK_HEADER}:</strong></p>"
        "\n  <ul>"
        f"\n        {rows}"
        "\n  </ul>"
        "\n</div>"
    )


def _trim_block_to_limit(existing: str, block: str, limit: int) -> str:
    """
    Повертає block, урізаний так щоб len(existing) + len(block) <= limit.

    Якщо навіть заголовок не вміщується — повертає порожній рядок.
    Урізає по цілих рядках (<li>…</li>) щоб не ламати HTML.
    """
    budget = limit - len(existing)
    if budget <= 0:
        return ""

    if len(block) <= budget:
        return block

    # Шукаємо рядки <li> та беремо стільки, скільки влазить
    header = (
        "\n<div>"
        f"\n  <p><strong>{_BLOCK_HEADER}:</strong></p>"
        "\n  <ul>\n        "
    )
    footer = "\n  </ul>\n</div>"
    overhead = len(header) + len(footer)

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
    return header + rows + footer


def _inject_before_close(body: str, pattern: re.Pattern[str], block: str) -> str:
    """Вставляє block перед першим входженням pattern. Якщо тег відсутній — body без змін."""
    if not pattern.search(body):
        return body
    return pattern.sub(lambda m: block + m.group(1), body, count=1)


def inject_params_into_description(
    body: str,
    prom_params: dict[str, str],
) -> str:
    """
    Вставляє блок «Особливості» перед закриваючим тегом </description>.

    Блок урізається по цілих <li>-рядках якщо додавання перевищує
    _DESCRIPTION_MAX_CHARS (12 100 символів — ліміт Epicenter).

    Якщо тег відсутній або параметрів немає — повертає body без змін.

    Args:
        body:        XML-тіло офера.
        prom_params: {param_name: param_value} з PROM-фіду.

    Returns:
        Оновлений body з вставленим блоком.
    """
    block = build_params_block(prom_params)
    if not block:
        return body

    # Визначаємо поточну довжину вмісту <description>
    desc_match = re.search(r'<description(?:\s[^>]*)?>(.*?)</description>', body, flags=re.DOTALL)
    existing_content = desc_match.group(1) if desc_match else ""

    block = _trim_block_to_limit(existing_content, block, _DESCRIPTION_MAX_CHARS)
    if not block:
        return body

    return _inject_before_close(body, _DESC_CLOSE_RE, block)