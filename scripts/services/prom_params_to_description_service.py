"""
services/prom_params_to_description_service.py
------------------------------------------------
Будує HTML-блок «Параметри» з PROM-параметрів і вставляє його
в кінець тегу <description_ua> офера.

Призначення: покупець на Епіцентрі бачить реальні характеристики
з PROM-фіду, навіть якщо вони не вкладаються в епіцентрівські фільтри.

Формат вставки (органічно продовжує наявний HTML в description_ua):
    <div>
      <p><strong>Параметри:</strong></p>
      <ul>
        <li>Дальність читання: 10 см</li>
        <li>Підтримка стандартів: EM Marine (EM4100)</li>
        ...
      </ul>
    </div>

Системні параметри (габарити, вага, кратність) пропускаються —
вони вже передаються як атрибути Епіцентра.
"""

from __future__ import annotations

import re
from typing import Final

# Параметри, які вже йдуть як атрибути Epicenter — в описі дублювати не треба.
# Розширюй список якщо додаєш нові системні атрибути.
_SKIP_PARAMS: Final[frozenset[str]] = frozenset({
    "Ширина", "Висота", "Довжина", "Глибина", "Вага", "Кратність",
    "Компанія-виробник", "Країна-виробник",
})

# Regex для знаходження закриваючого тегу </description_ua>
_DESC_UA_CLOSE_RE: Final[re.Pattern[str]] = re.compile(
    r'(</description_ua>)', re.IGNORECASE
)


def build_params_block(prom_params: dict[str, str]) -> str:
    """
    Будує HTML-блок з PROM-параметрів.

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
        "\n  <p><strong>Параметри:</strong></p>"
        "\n  <ul>"
        f"\n        {rows}"
        "\n  </ul>"
        "\n</div>"
    )


def inject_params_into_description(
    body: str,
    prom_params: dict[str, str],
) -> str:
    """
    Вставляє блок «Параметри» перед закриваючим </description_ua>.

    Якщо тег <description_ua> відсутній або параметрів немає — повертає body без змін.

    Args:
        body:        XML-тіло офера (рядок між <offer> ... </offer>).
        prom_params: {param_name: param_value} з PROM-фіду.

    Returns:
        Оновлений body з вставленим блоком.
    """
    block = build_params_block(prom_params)
    if not block:
        return body

    if not _DESC_UA_CLOSE_RE.search(body):
        return body  # тег відсутній — не чіпаємо

    return _DESC_UA_CLOSE_RE.sub(
        rf"{block}\1",
        body,
        count=1,
    )
