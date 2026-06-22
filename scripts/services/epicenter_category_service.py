"""
services/epicenter_category_service.py
---------------------------------------
Єдина точка читання листа «Маппінг» з epicenter_mappings.xlsx.

Аркуш читається один раз (lru_cache) і роздається всім споживачам.

Публічний інтерфейс:
    resolve_category(prom_category_id, prom_params)  -> CategoryEntry | None
    get_category(prom_category_id)                   -> CategoryEntry | None  (compat)
    get_category_map()                               -> dict[int, CategoryEntry]
    build_categories_xml(entries)                    -> str

CategoryEntry      = {"code": str, "name": str}
CategoryMappingRule — внутрішня модель одного рядка «Маппінгу».

Логіка resolve_category (в порядку пріоритету):
  1. Правила з param_names: шукаємо в prom_params за іменем аліасу;
     кожне sub-value (розбиття по ",") порівнюємо з option_names.
     Перше збіжне правило виграє.
  2. Plain-правила (param_names порожні): перше таке правило — fallback.
  3. Якщо всі правила мають param-фільтр, але жодне не збіглося →
     fallback на rules[0] + warning у лог.

Файл даних: data/markets/epicenter_mappings.xlsx
Аркуш:      Маппінг
Колонки:    prom_category_id (col 0) | epicenter_category_id (col 2)
            Назва (col 3) | prom_param_name (col 5) | prom_option_name (col 6)
"""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Final, TypedDict

import openpyxl

logger = logging.getLogger(__name__)

# services/ → scripts/ → project root → data/markets/
_XLSX_PATH: Final[Path] = (
    Path(__file__).parents[2] / "data" / "markets" / "epicenter_mappings.xlsx"
)
_SHEET_NAME: Final[str] = "Маппінг"

# Column indices (0-based) in «Маппінг»
_COL_PROM_CAT_ID:  Final[int] = 0   # prom_category_id
_COL_EPI_CAT_ID:   Final[int] = 2   # epicenter_category_id  (= set_code)
_COL_EPI_NAME:     Final[int] = 3   # Назва категорії Епіцентру
_COL_PARAM_NAME:   Final[int] = 5   # prom_param_name  (аліаси, ";" separated)
_COL_OPTION_NAME:  Final[int] = 6   # prom_option_name (значення, ";" separated)

_ALIAS_SEP: Final[str] = ";"        # роздільник всередині xlsx-рядків

# Лічильник fallback-промахів: (prom_cat_id, epi_code) → кількість товарів
_fallback_miss_counts: Counter[tuple[int, str]] = Counter()

# Перші N offer_id для кожного fallback-ключа (щоб показати в лозі конкретні товари).
# Більше _MAX_OFFER_IDS_LOGGED не зберігаємо — уникаємо memory leak на великих фідах.
_MAX_OFFER_IDS_LOGGED: Final[int] = 10
_fallback_miss_offer_ids: dict[tuple[int, str], list[str]] = {}


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

class CategoryEntry(TypedDict):
    code: str   # epicenter_category_id / set_code
    name: str   # Назва категорії Епіцентру


@dataclass(frozen=True)
class CategoryMappingRule:
    """Один рядок аркуша «Маппінг» з розпарсеними param-фільтрами."""
    code: str                     # epicenter_category_id
    name: str                     # Назва категорії Епіцентру
    param_names: frozenset[str]   # prom_param_name аліаси (порожньо → no filter)
    option_names: frozenset[str]  # prom_option_name значення (порожньо → no filter)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_aliases(raw: str | None) -> frozenset[str]:
    """Розбиває "A; B; C" → frozenset{"A", "B", "C"}. None → порожня множина."""
    if not raw:
        return frozenset()
    return frozenset(s.strip() for s in raw.split(_ALIAS_SEP) if s.strip())


# ---------------------------------------------------------------------------
# Cached loader — xlsx читається рівно один раз на процес
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _load_mapping_rules() -> dict[int, list[CategoryMappingRule]]:
    """
    Повертає повний індекс {prom_category_id: [CategoryMappingRule, ...]}.

    Один prom_category_id може мати кілька правил (param-based routing):
    порядок у списку відповідає порядку рядків у xlsx → визначає пріоритет.
    Рядки без prom_category_id або epicenter_category_id пропускаються.
    """
    if not _XLSX_PATH.exists():
        raise FileNotFoundError(
            f"epicenter_mappings.xlsx не знайдено: {_XLSX_PATH}"
        )

    wb = openpyxl.load_workbook(_XLSX_PATH, read_only=True, data_only=True)
    try:
        ws = wb[_SHEET_NAME]
    except KeyError:
        raise KeyError(f"Аркуш «{_SHEET_NAME}» не знайдено у {_XLSX_PATH}")

    result: dict[int, list[CategoryMappingRule]] = {}
    skipped = 0

    for row_idx, row in enumerate(ws.iter_rows(values_only=True), start=1):
        if row_idx == 1:
            continue  # заголовок

        prom_id_raw  = row[_COL_PROM_CAT_ID]
        epi_id_raw   = row[_COL_EPI_CAT_ID]
        epi_name_raw = row[_COL_EPI_NAME]

        if prom_id_raw is None or epi_id_raw is None:
            skipped += 1
            continue

        try:
            prom_id  = int(prom_id_raw)
            epi_code = str(int(epi_id_raw))  # нормалізуємо до рядка без .0
        except (ValueError, TypeError):
            logger.warning(
                "Рядок %d: неможливо конвертувати prom_id=%r / epi_id=%r — пропущено",
                row_idx, prom_id_raw, epi_id_raw,
            )
            skipped += 1
            continue

        epi_name = str(epi_name_raw).strip() if epi_name_raw else ""

        # Cols 5/6 можуть бути відсутні у старих xlsx (захист від IndexError)
        raw_param  = row[_COL_PARAM_NAME]  if len(row) > _COL_PARAM_NAME  else None
        raw_option = row[_COL_OPTION_NAME] if len(row) > _COL_OPTION_NAME else None

        rule = CategoryMappingRule(
            code=epi_code,
            name=epi_name,
            param_names=_parse_aliases(raw_param),
            option_names=_parse_aliases(raw_option),
        )
        result.setdefault(prom_id, []).append(rule)

    wb.close()

    param_rows = sum(1 for rules in result.values() for r in rules if r.param_names)
    plain_rows = sum(1 for rules in result.values() for r in rules if not r.param_names)
    logger.info(
        "Завантажено %d категорій Маппінгу (%d plain, %d param-based, пропущено: %d)",
        len(result), plain_rows, param_rows, skipped,
    )
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def resolve_category(
    prom_cat_id: int,
    prom_params: dict[str, str],
    offer_id: str | None = None,
) -> CategoryEntry | None:
    """
    Визначає категорію Epicenter для офера з урахуванням його атрибутів.

    Пріоритет (see module docstring для деталей):
    1. param-based правила → перше збіжне
    2. plain-правила (param_names порожні) → перше
    3. conservative fallback на rules[0] + warning

    offer_id — опціональний ідентифікатор офера для діагностики fallback-промахів.
    Передається у flush_fallback_warnings() для відображення у лозі.

    Не кидає виключень — безпечно для поштучної обробки товарів.
    """
    rules = _load_mapping_rules().get(prom_cat_id)
    if not rules:
        return None

    plain_fallback: CategoryMappingRule | None = None

    for rule in rules:
        if not rule.param_names:
            # Plain-правило (no param filter) → запам'ятовуємо як fallback
            if plain_fallback is None:
                plain_fallback = rule
            continue

        # Param-based: перевіряємо кожен аліас prom_param_name
        for param_name in rule.param_names:
            raw_value = prom_params.get(param_name)
            if raw_value is None:
                continue
            # Prom може об'єднувати кілька значень через ", " (multiselect)
            for sub_val in (v.strip() for v in raw_value.split(",")):
                if sub_val in rule.option_names:
                    return CategoryEntry(code=rule.code, name=rule.name)

    # Param-правила не спрацювали → plain fallback
    if plain_fallback is not None:
        return CategoryEntry(code=plain_fallback.code, name=plain_fallback.name)

    # Всі правила param-based, але жодне не збіглось → conservative fallback.
    # Не спамимо лог на кожен товар — накопичуємо лічильник, flush_fallback_warnings() в кінці ран.
    _key = (prom_cat_id, rules[0].code)
    _fallback_miss_counts[_key] += 1
    if offer_id is not None:
        _ids = _fallback_miss_offer_ids.setdefault(_key, [])
        if len(_ids) < _MAX_OFFER_IDS_LOGGED:
            _ids.append(offer_id)
    return CategoryEntry(code=rules[0].code, name=rules[0].name)


def flush_fallback_warnings() -> None:
    """
    Виводить зведений WARNING по всіх fallback-промахах і скидає лічильники.
    Викликати один раз після завершення обробки всіх товарів фіду.

    Замість N однакових рядків у лозі — один рядок на (prom_cat_id, code):
        prom_cat_id=53004 (×847): param-фільтр не збігся → fallback rules[0] (code=3528).
          offer ids: 111111, 222222, 333333 ...
    """
    if not _fallback_miss_counts:
        return
    for (prom_cat_id, code), count in sorted(_fallback_miss_counts.items()):
        offer_ids = _fallback_miss_offer_ids.get((prom_cat_id, code), [])
        ids_str = ", ".join(offer_ids)
        if count > len(offer_ids):
            ids_str += ", ..."
        logger.warning(
            "prom_cat_id=%d (×%d): param-фільтр не збігся ні з одним правилом "
            "→ fallback rules[0] (code=%s). Оновіть Маппінг або додайте plain-рядок.\n"
            "  offer ids: %s",
            prom_cat_id, count, code, ids_str,
        )
    _fallback_miss_counts.clear()
    _fallback_miss_offer_ids.clear()


def get_category(prom_category_id: int) -> CategoryEntry | None:
    """
    Backward-compat: повертає CategoryEntry без param-фільтрації.
    Еквівалентно resolve_category(id, {}).
    Для офер-рівневого роутингу використовуй resolve_category().
    """
    return resolve_category(prom_category_id, {})


def get_category_map() -> dict[int, CategoryEntry]:
    """Повний індекс {prom_category_id: CategoryEntry} — перше правило кожної категорії."""
    return {
        prom_id: CategoryEntry(code=rules[0].code, name=rules[0].name)
        for prom_id, rules in _load_mapping_rules().items()
    }


def build_categories_xml(entries: Iterable[CategoryEntry]) -> str:
    """
    Будує XML-блок <categories> з переданих CategoryEntry.

    Вхід — лише реально використані у фіді категорії (зібрані під час
    inject_epicenter_attrs). Сортуємо за code для детермінованого виводу.

    Epicenter використовує рядковий code (= set_code) як ідентифікатор категорії,
    тому атрибут id="..." містить саме цей code.

    Приклад виводу:
        <categories>
            <category id="12345">Кабелі та перехідники</category>
            <category id="67890">Мережеві фільтри</category>
        </categories>
    """
    sorted_entries = sorted(
        entries,
        key=lambda e: int(e["code"]) if e["code"].isdigit() else e["code"],
    )

    if not sorted_entries:
        logger.warning("build_categories_xml: передано порожній список категорій")
        return "<categories/>"

    lines = ["<categories>"]
    for entry in sorted_entries:
        lines.append(f'    <category id="{entry["code"]}">{entry["name"]}</category>')
    lines.append("</categories>")
    return "\n".join(lines)
