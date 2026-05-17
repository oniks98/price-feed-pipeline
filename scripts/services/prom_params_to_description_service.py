"""
services/prom_params_to_description_service.py
------------------------------------------------
Будує HTML-блок «Параметри / Параметры» з PROM-параметрів і вставляє його
в кінець тегів <description> (RU) та <description_ua> (UA) офера.

Призначення: покупець на Епіцентрі бачить реальні характеристики
з PROM-фіду, навіть якщо вони не вкладаються в епіцентрівські фільтри.

Формат вставки (органічно продовжує наявний HTML):
    <div>
      <p><strong>Параметри:</strong></p>
      <ul>
        <li>Дальність читання: 10 см</li>
        <li>Підтримка стандартів: EM Marine (EM4100)</li>
        ...
      </ul>
    </div>

Цільові теги (Prom-формат, до rename в normalize_name_description_tags):
    <description>    → майбутній <description lang="ru">  → заголовок "Параметры"
    <description_ua> → майбутній <description lang="ua">  → заголовок "Параметри"

Системні параметри (габарити, вага, кратність) пропускаються —
вони вже передаються як атрибути Епіцентра.
"""

from __future__ import annotations

import re
from typing import Final, Literal

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

# Локалізовані заголовки блоку параметрів
_BLOCK_HEADER: Final[dict[str, str]] = {
    "ua": "Параметри",
    "ru": "Параметры",
}

# Regex для знаходження закриваючих тегів опису (Prom-формат, до rename).
# <description>    → майбутній <description lang="ru">
# <description_ua> → майбутній <description lang="ua">
_DESC_RU_CLOSE_RE: Final[re.Pattern[str]] = re.compile(
    r'(</description>)', re.IGNORECASE
)
_DESC_UA_CLOSE_RE: Final[re.Pattern[str]] = re.compile(
    r'(</description_ua>)', re.IGNORECASE
)

Lang = Literal["ua", "ru"]


def build_params_block(prom_params: dict[str, str], lang: Lang = "ua") -> str:
    """
    Будує HTML-блок з PROM-параметрів.

    Args:
        prom_params: {param_name: param_value} — вже розпарсені з офера.
        lang:        мова заголовка блоку ("ua" | "ru").

    Returns:
        HTML-рядок для вставки в description.
        Порожній рядок якщо немає параметрів для відображення.
    """
    items = [
        f"<li>{name}: {value}</li>"
        for name, value in prom_params.items()
        if name not in _SKIP_PARAMS and value.strip()
    ]
    if not items:
        return ""

    header = _BLOCK_HEADER.get(lang, _BLOCK_HEADER["ua"])
    rows = "\n        ".join(items)
    return (
        "\n<div>"
        f"\n  <p><strong>{header}:</strong></p>"
        "\n  <ul>"
        f"\n        {rows}"
        "\n  </ul>"
        "\n</div>"
    )


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
    Вставляє блок «Параметри / Параметры» перед закриваючими тегами опису.

    Цільові теги (Prom-формат, до rename в normalize_name_description_tags):
        </description>    → майбутній <description lang="ru">  → блок з "Параметры"
        </description_ua> → майбутній <description lang="ua">  → блок з "Параметри"

    Якщо обидва теги відсутні або параметрів немає — повертає body без змін.
    Кожен тег оброблюється незалежно: відсутність одного не блокує вставку в інший.

    Args:
        body:        XML-тіло офера (рядок між <offer> ... </offer>).
        prom_params: {param_name: param_value} з PROM-фіду.

    Returns:
        Оновлений body з вставленими блоками.
    """
    block_ua = build_params_block(prom_params, lang="ua")
    if not block_ua:
        return body  # немає параметрів — обидва блоки будуть порожні

    block_ru = build_params_block(prom_params, lang="ru")

    body = _inject_before_close(body, _DESC_RU_CLOSE_RE, block_ru)
    body = _inject_before_close(body, _DESC_UA_CLOSE_RE, block_ua)
    return body